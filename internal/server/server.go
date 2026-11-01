// Package server ties everything together: Jellyfin reverse proxy, PlaybackInfo
// rewrite, cache and HLS sessions per MediaSource, plus the dashboard API.
package server

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/lucsoft/jellyfin-proxy/internal/events"
	"github.com/lucsoft/jellyfin-proxy/internal/hls"
	"github.com/lucsoft/jellyfin-proxy/internal/mkv"
	"github.com/lucsoft/jellyfin-proxy/internal/rangecache"
)

type Options struct {
	Upstream       *url.URL
	PublicURL      string
	ServerName     string
	WorkDir        string
	Cache          rangecache.Config
	HLS            hls.Config
	AudioCodec     string
	AudioBitrate   string
	AudioChannels  int
	CopyAudio      []string // codecs the client handles natively (copied as-is)
	DirectPlay     bool     // play MKV with client-compatible audio straight from the cache instead of HLS
	SegmentSeconds float64
	SessionIdle    time.Duration // close the session entirely
	CacheIdle      time.Duration // drop the cache once no session reads it anymore
	APIClient      *http.Client
	Events         *events.Log
}

type Server struct {
	opt   Options
	ev    *events.Log
	rp    *httputil.ReverseProxy
	local string // address of the loopback listener for ffmpeg

	locks  sync.Map // sourceID -> *sync.Mutex
	tokens tokenCache

	cacheLocks sync.Map // sourceID -> *sync.Mutex, see cacheFor

	mu        sync.Mutex
	sources   map[string]*Source
	caches    map[string]*rangecache.Cache
	sessions  map[string]*hls.Session
	timelines map[string]*mkv.Timeline
	stopTimer map[string]*time.Timer
}

// Source is a MediaSource as the proxy knows it from PlaybackInfo.
type Source struct {
	ID           string   `json:"id"`
	ItemID       string   `json:"item_id"`
	Name         string   `json:"name"`
	Container    string   `json:"container"`
	Size         int64    `json:"size"`
	Bitrate      int64    `json:"bitrate"`
	Streams      []Stream `json:"streams"`
	DefaultAudio int      `json:"default_audio"`
	Audio        int      `json:"audio"` // audio track chosen by the client
	Mode         string   `json:"mode"`  // "direct" or "hls"
	token        string
	Client       ClientState `json:"client"`
}

type Stream struct {
	Index    int    `json:"index"`
	Type     string `json:"type"`
	Codec    string `json:"codec"`
	Title    string `json:"title"`
	Channels int    `json:"channels"`
}

// ClientState is what the client itself reports about its playback.
type ClientState struct {
	Position float64   `json:"position"`
	Paused   bool      `json:"paused"`
	At       time.Time `json:"at"`
	Event    string    `json:"event"`
}

func New(opt Options) *Server {
	s := &Server{
		opt: opt, ev: opt.Events,
		sources: map[string]*Source{}, caches: map[string]*rangecache.Cache{},
		sessions: map[string]*hls.Session{}, timelines: map[string]*mkv.Timeline{},
		stopTimer: map[string]*time.Timer{},
	}
	s.rp = s.newReverseProxy()
	s.cleanWorkDir()
	go s.maintain()
	return s
}

// SetLocalAddr sets the loopback address ffmpeg uses to read the cache.
func (s *Server) SetLocalAddr(addr string) { s.local = addr }

// savedMaxAge: saved caches nobody has fetched for this long are deleted
// at startup.
const savedMaxAge = 24 * time.Hour

func (s *Server) cleanWorkDir() {
	entries, _ := os.ReadDir(s.opt.WorkDir)
	n, kept := 0, 0
	for _, e := range entries {
		path := filepath.Join(s.opt.WorkDir, e.Name())
		switch {
		case !e.IsDir():
		case strings.HasPrefix(e.Name(), "cache-"), strings.HasPrefix(e.Name(), "hls-"):
			os.RemoveAll(path) // leftovers from a crash: state unknown
			n++
		case strings.HasPrefix(e.Name(), "saved-"):
			st, err := os.Stat(filepath.Join(path, "state.json"))
			if err != nil || time.Since(st.ModTime()) > savedMaxAge {
				os.RemoveAll(path)
				n++
			} else {
				kept++
			}
		}
	}
	if n > 0 {
		s.ev.Emit("proxy", "startup_cleanup", events.Info,
			fmt.Sprintf("Deleted %d directories from a previous run", n))
	}
	if kept > 0 {
		s.ev.Emit("proxy", "startup_saved", events.Info,
			fmt.Sprintf("%d saved caches waiting for the next playback (deleted after %s without access)", kept, savedMaxAge))
	}
}

func (s *Server) lockFor(id string) *sync.Mutex {
	v, _ := s.locks.LoadOrStore(id, &sync.Mutex{})
	return v.(*sync.Mutex)
}

func short(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}

// ---------------------------------------------------------------- Cache and session

func (s *Server) source(id string) *Source {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sources[id]
}

// cacheFor returns the cache of a source, creating it if needed.
// A cache with a permanent error (e.g. an expired token) is replaced.
func (s *Server) cacheFor(ctx context.Context, src *Source) (*rangecache.Cache, error) {
	// One cache per source: the background preparation and the first media
	// request often arrive together. Without this lock both would open a
	// cache, and the second would push out the first (e.g. a restored one).
	// Separate from lockFor, which sessionFor already holds when it gets here.
	v, _ := s.cacheLocks.LoadOrStore(src.ID, &sync.Mutex{})
	l := v.(*sync.Mutex)
	l.Lock()
	defer l.Unlock()

	s.mu.Lock()
	c := s.caches[src.ID]
	s.mu.Unlock()
	if c != nil {
		err := c.Err()
		if err == nil {
			return c, nil
		}
		c.Close("permanent error, recreating: " + err.Error())
	}
	u := fmt.Sprintf("%s/Videos/%s/stream?static=true&mediaSourceId=%s",
		s.opt.Upstream, url.PathEscape(src.ItemID), url.QueryEscape(src.ID))
	if src.token != "" {
		u += "&api_key=" + url.QueryEscape(src.token)
	}
	c, err := rangecache.Open(ctx, s.opt.Cache, src.ID, short(src.ID), u)
	if err != nil {
		s.ev.Emit(short(src.ID), "cache_error", events.Error, "Could not create cache: "+err.Error())
		return nil, err
	}
	s.mu.Lock()
	s.caches[src.ID] = c
	s.mu.Unlock()
	return c, nil
}

// sessionFor returns the playback session. A running session with a
// different audio track is replaced. startSec > 0 starts ffmpeg right there.
func (s *Server) sessionFor(src *Source, audio int, startSec float64) (*hls.Session, error) {
	l := s.lockFor(src.ID)
	l.Lock()
	defer l.Unlock()

	s.mu.Lock()
	sess := s.sessions[src.ID]
	if t := s.stopTimer[src.ID]; t != nil {
		t.Stop()
		delete(s.stopTimer, src.ID)
	}
	s.mu.Unlock()
	if sess != nil {
		if sess.Audio().Index == audio {
			return sess, nil
		}
		old := sess.Audio().Label
		sess.Close(fmt.Sprintf("Audio track changed: %s -> %s", old, s.audioFor(src, audio).Label))
		s.mu.Lock()
		delete(s.sessions, src.ID)
		s.mu.Unlock()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	c, err := s.cacheFor(ctx, src)
	if err != nil {
		return nil, err
	}
	tl, err := s.timelineFor(ctx, src, c)
	if err != nil {
		return nil, err
	}

	input := fmt.Sprintf("http://%s/cache/%s", s.local, url.PathEscape(src.ID))
	sess, err = hls.New(s.opt.HLS, src.ID, short(src.ID), tl, input, s.audioFor(src, audio), src.videoCodec())
	if err != nil {
		return nil, err
	}
	if startSec > 0 {
		sess.Prepare(tl.SegmentAt(startSec), fmt.Sprintf("Client wants to resume at %s", clock(startSec)))
	} else {
		sess.Prepare(0, "Playback starts at the beginning")
	}
	s.mu.Lock()
	s.sessions[src.ID] = sess
	s.mu.Unlock()
	return sess, nil
}

// timelineFor reads the MKV index once per source (for HLS segments and
// for the dashboard, to convert bytes into movie time).
func (s *Server) timelineFor(ctx context.Context, src *Source, c *rangecache.Cache) (*mkv.Timeline, error) {
	s.mu.Lock()
	tl := s.timelines[src.ID]
	s.mu.Unlock()
	if tl != nil {
		return tl, nil
	}
	t0 := time.Now()
	tl, err := mkv.Read(c.ReaderAt(ctx), c.Size, s.opt.SegmentSeconds)
	if err != nil {
		s.ev.Emit(short(src.ID), "index_error", events.Error, "MKV index unreadable: "+err.Error())
		return nil, err
	}
	s.ev.Emit(short(src.ID), "index_read", events.Info,
		fmt.Sprintf("Read MKV index in %s: %d keyframes -> %d segments, duration %s",
			time.Since(t0).Round(time.Millisecond), tl.Keyframes, len(tl.Segments), clock(tl.Duration)))
	s.mu.Lock()
	s.timelines[src.ID] = tl
	s.mu.Unlock()
	return tl, nil
}

// directOK: if the client can handle this audio track itself, no ffmpeg is
// needed – it plays the MKV directly, with the bytes served from the cache.
func (s *Server) directOK(src *Source, audio int) bool {
	return s.opt.DirectPlay && s.audioFor(src, audio).Copy
}

// prepareDirect sets up cache and index for Direct Play and ends any HLS
// session of the same source that may still be running.
func (s *Server) prepareDirect(src *Source) {
	l := s.lockFor(src.ID)
	l.Lock()
	defer l.Unlock()
	s.mu.Lock()
	sess := s.sessions[src.ID]
	delete(s.sessions, src.ID)
	s.mu.Unlock()
	if sess != nil {
		sess.Close("Switching to Direct Play")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	c, err := s.cacheFor(ctx, src)
	if err == nil {
		_, err = s.timelineFor(ctx, src, c)
	}
	if err != nil {
		s.ev.Emit(short(src.ID), "prepare_error", events.Error, "Preparation failed: "+err.Error())
	}
}

// videoCodec returns the codec of the first video track according to Jellyfin ("hevc", "h264" …).
func (src *Source) videoCodec() string {
	for _, st := range src.Streams {
		if st.Type == "Video" {
			return strings.ToLower(st.Codec)
		}
	}
	return ""
}

// audioFor picks the audio track and target codec. client-compatible tracks are copied.
func (s *Server) audioFor(src *Source, index int) hls.Audio {
	a := hls.Audio{Index: index, Codec: s.opt.AudioCodec, Bitrate: s.opt.AudioBitrate, Channels: s.opt.AudioChannels}
	for _, st := range src.Streams {
		if st.Index != index || st.Type != "Audio" {
			continue
		}
		for _, ok := range s.opt.CopyAudio {
			if strings.EqualFold(st.Codec, ok) {
				a.Copy = true
			}
		}
		if a.Copy {
			a.Label = fmt.Sprintf("#%d %s (copied)", index, st.Title)
		} else {
			a.Label = fmt.Sprintf("#%d %s -> %s %dch", index, st.Title, a.Codec, a.Channels)
		}
		return a
	}
	a.Label = fmt.Sprintf("#%d -> %s %dch", index, a.Codec, a.Channels)
	return a
}

// stopSoon closes the session if the client doesn't resume within 20 s of
// reporting a stop (clients also report a stop when seeking).
func (s *Server) stopSoon(id, why string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sessions[id] == nil {
		return
	}
	if t := s.stopTimer[id]; t != nil {
		t.Stop()
	}
	s.stopTimer[id] = time.AfterFunc(20*time.Second, func() {
		s.mu.Lock()
		sess := s.sessions[id]
		delete(s.stopTimer, id)
		if sess == nil || sess.IdleFor() < 20*time.Second {
			s.mu.Unlock()
			return
		}
		delete(s.sessions, id)
		s.mu.Unlock()
		sess.Close(why)
	})
}

func (s *Server) maintain() {
	t := time.NewTicker(10 * time.Second)
	defer t.Stop()
	for range t.C {
		var closeS []*hls.Session
		var closeC []*rangecache.Cache
		s.mu.Lock()
		for id, sess := range s.sessions {
			if idle := sess.IdleFor(); idle > s.opt.SessionIdle {
				closeS = append(closeS, sess)
				delete(s.sessions, id)
			}
		}
		for id, c := range s.caches {
			if s.sessions[id] != nil {
				continue
			}
			if idle := c.Idle(); idle > s.opt.CacheIdle {
				closeC = append(closeC, c)
				delete(s.caches, id)
			}
		}
		s.mu.Unlock()
		for _, sess := range closeS {
			sess.Close(fmt.Sprintf("Client inactive for %s", s.opt.SessionIdle))
		}
		for _, c := range closeC {
			c.Close(fmt.Sprintf("no playback and not read for %s", s.opt.CacheIdle))
		}
	}
}

// Shutdown stops all ffmpeg processes, deletes the segments and saves the
// caches so the next start can pick them up.
func (s *Server) Shutdown() {
	s.mu.Lock()
	sessions, caches := s.sessions, s.caches
	s.sessions, s.caches = map[string]*hls.Session{}, map[string]*rangecache.Cache{}
	s.mu.Unlock()
	for _, sess := range sessions {
		sess.Close("Proxy shutting down")
	}
	for _, c := range caches {
		c.Save("Proxy shutting down")
	}
}

func clock(sec float64) string {
	t := int(sec)
	return fmt.Sprintf("%d:%02d:%02d", t/3600, t/60%60, t%60)
}
