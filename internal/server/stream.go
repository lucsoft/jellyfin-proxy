package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/lucsoft/jellyfin-proxy/internal/events"
	"github.com/lucsoft/jellyfin-proxy/internal/rangecache"
)

// handleHLS serves /proxy/hls/{id}/master.m3u8 and /proxy/hls/{id}/seg/{k}.ts
func (s *Server) handleHLS(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/proxy/hls/"), "/")
	if len(parts) < 2 {
		http.NotFound(w, r)
		return
	}
	if !s.requireAuth(w, r) {
		return
	}
	src := s.sourceFromURL(r, parts[0])
	if src == nil {
		http.Error(w, "unknown MediaSource – fetch PlaybackInfo first", http.StatusNotFound)
		return
	}
	s.adoptToken(src, r)
	audio := src.DefaultAudio
	if v, err := strconv.Atoi(queryCI(r, "audio")); err == nil {
		audio = v
	}
	switch {
	case len(parts) == 2 && strings.HasSuffix(parts[1], ".m3u8"):
		start, _ := strconv.ParseFloat(queryCI(r, "start"), 64)
		s.servePlaylist(w, r, src, audio, start)
	case len(parts) == 2 && parts[1] == "init.mp4":
		s.serveInit(w, r, src, audio)
	case len(parts) == 3 && parts[1] == "seg" && (strings.HasSuffix(parts[2], ".ts") || strings.HasSuffix(parts[2], ".m4s")):
		k, err := strconv.Atoi(strings.TrimSuffix(strings.TrimSuffix(parts[2], ".ts"), ".m4s"))
		if err != nil {
			http.NotFound(w, r)
			return
		}
		s.serveSegment(w, r, src, audio, k)
	default:
		http.NotFound(w, r)
	}
}

// sourceFromURL knows the source from PlaybackInfo – or rebuilds it from the
// URL parameters (e.g. after the proxy restarted mid-movie).
func (s *Server) sourceFromURL(r *http.Request, id string) *Source {
	if src := s.source(id); src != nil {
		return src
	}
	item := queryCI(r, "item")
	if item == "" {
		return nil
	}
	src := &Source{ID: id, ItemID: item, Container: "mkv", token: tokenFrom(r), DefaultAudio: -1, Name: id}
	s.mu.Lock()
	if old := s.sources[id]; old != nil {
		src = old
	} else {
		s.sources[id] = src
	}
	s.mu.Unlock()
	return src
}

func (s *Server) servePlaylist(w http.ResponseWriter, r *http.Request, src *Source, audio int, start float64) {
	sess, err := s.sessionFor(src, audio, start)
	if err != nil {
		http.Error(w, "Cannot prepare playback: "+err.Error(), http.StatusBadGateway)
		return
	}
	prefix := "/proxy/hls/" + url.PathEscape(src.ID) + "/seg/"
	q := url.Values{}
	q.Set("audio", strconv.Itoa(audio))
	q.Set("item", src.ItemID)
	if src.token != "" {
		q.Set("api_key", src.token)
	}
	ext := ".ts"
	if sess.FMP4() {
		ext = ".m4s"
	}
	suffix := ext + "?" + q.Encode()
	initURL := "/proxy/hls/" + url.PathEscape(src.ID) + "/init.mp4?" + q.Encode()
	w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
	w.Header().Set("Cache-Control", "no-cache")
	// t (start time) keeps the URL unique even if the segment length changes.
	w.Write(sess.Playlist(func(k int) string {
		return prefix + strconv.Itoa(k) + suffix + "&t=" + strconv.FormatFloat(sess.SegmentStart(k), 'f', 3, 64)
	}, initURL))
}

// serveInit serves the fMP4 init segment (ftyp+moov) for EXT-X-MAP.
func (s *Server) serveInit(w http.ResponseWriter, r *http.Request, src *Source, audio int) {
	sess, err := s.sessionFor(src, audio, 0)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	init, err := sess.Init(r.Context())
	if err != nil {
		if r.Context().Err() == nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
		}
		return
	}
	w.Header().Set("Content-Type", "video/mp4")
	http.ServeContent(w, r, "", time.Time{}, bytes.NewReader(init))
}

func (s *Server) serveSegment(w http.ResponseWriter, r *http.Request, src *Source, audio, k int) {
	sess, err := s.sessionFor(src, audio, 0)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	t := -1.0
	if v, err := strconv.ParseFloat(queryCI(r, "t"), 64); err == nil {
		t = v
	}
	k, _ = sess.Resolve(k, t)
	seg, err := sess.Segment(r.Context(), k)
	if err != nil {
		if r.Context().Err() == nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
		}
		return
	}
	f, err := os.Open(seg.Path)
	if err != nil {
		// deleted between "done" and delivery: should not happen
		http.Error(w, "segment vanished", http.StatusServiceUnavailable)
		return
	}
	defer f.Close()
	ct := "video/mp2t"
	if sess.FMP4() {
		ct = "video/mp4" // only moof+mdat; the header lives in the init segment
	}
	w.Header().Set("Content-Type", ct)
	w.Header().Set("Cache-Control", "no-cache")
	t0 := time.Now()
	http.ServeContent(w, r, "", time.Time{}, io.NewSectionReader(f, seg.Off, seg.Size))
	if r.Header.Get("Range") == "" && r.Context().Err() == nil {
		sess.Delivered(k, seg.Size, time.Since(t0))
	}
}

// ---------------------------------------------------------------- Loopback for ffmpeg

// LocalHandler is only reachable on 127.0.0.1: ffmpeg reads from the cache there.
func (s *Server) LocalHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /cache/{id}", s.handleCacheRead)
	mux.HandleFunc("HEAD /cache/{id}", s.handleCacheRead)
	return mux
}

func (s *Server) handleCacheRead(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	c := s.caches[r.PathValue("id")]
	s.mu.Unlock()
	if c == nil {
		http.Error(w, "no cache", http.StatusNotFound)
		return
	}
	s.serveCache(w, r, c, "ffmpeg")
}

// serveDirect serves the MKV to the client (Direct Play) from the cache.
func (s *Server) serveDirect(w http.ResponseWriter, r *http.Request, src *Source) {
	ctx, cancel := context.WithTimeout(r.Context(), time.Minute)
	c, err := s.cacheFor(ctx, src)
	cancel()
	if err != nil {
		http.Error(w, "Cache: "+err.Error(), http.StatusBadGateway)
		return
	}
	s.serveCache(w, r, c, "Client")
}

// serveCache answers a (range) request from the cache. The reader's
// position determines what gets prefetched.
func (s *Server) serveCache(w http.ResponseWriter, r *http.Request, c *rangecache.Cache, who string) {
	start, end, ok := parseRange(r.Header.Get("Range"), c.Size)
	if !ok {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", c.Size))
		w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
		return
	}
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Content-Type", "video/x-matroska")
	w.Header().Set("Content-Length", strconv.FormatInt(end-start+1, 10))
	status := http.StatusOK
	if r.Header.Get("Range") != "" {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, c.Size))
		status = http.StatusPartialContent
	}
	w.WriteHeader(status)
	if r.Method == http.MethodHead {
		return
	}

	rd := c.NewReader(fmt.Sprintf("%s from %.0f MB", who, float64(start)/1e6), start)
	defer rd.Close()
	buf := make([]byte, 256<<10)
	for pos := start; pos <= end; {
		n, err := rd.Read(r.Context(), buf[:min(int64(len(buf)), end-pos+1)])
		if n > 0 {
			if _, werr := w.Write(buf[:n]); werr != nil {
				return
			}
			pos += int64(n)
		}
		if err != nil {
			if !errors.Is(err, rangecache.ErrClosed) && r.Context().Err() == nil {
				s.ev.Emit(c.Label, "cache_read_error", events.Warn, who+" read aborted: "+err.Error())
			}
			return
		}
	}
}

// parseRange understands "bytes=a-", "bytes=a-b" and "bytes=-n".
func parseRange(h string, size int64) (start, end int64, ok bool) {
	if h == "" {
		return 0, size - 1, true
	}
	spec, found := strings.CutPrefix(h, "bytes=")
	if !found || strings.Contains(spec, ",") {
		return 0, 0, false
	}
	a, b, _ := strings.Cut(spec, "-")
	switch {
	case a == "":
		n, err := strconv.ParseInt(b, 10, 64)
		if err != nil || n <= 0 {
			return 0, 0, false
		}
		return max(size-n, 0), size - 1, true
	default:
		s, err := strconv.ParseInt(a, 10, 64)
		if err != nil || s >= size {
			return 0, 0, false
		}
		e := size - 1
		if b != "" {
			if e, err = strconv.ParseInt(b, 10, 64); err != nil || e < s {
				return 0, 0, false
			}
			e = min(e, size-1)
		}
		return s, e, true
	}
}
