package server

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/lucsoft/jellyfin-proxy/internal/events"
	"github.com/lucsoft/jellyfin-proxy/internal/hls"
	"github.com/lucsoft/jellyfin-proxy/internal/mkv"
	"github.com/lucsoft/jellyfin-proxy/internal/rangecache"
)

//go:embed dashboard.html
var dashboardHTML []byte

func (s *Server) handleDashboard(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write(dashboardHTML)
}

type State struct {
	Now      time.Time      `json:"now"`
	Upstream string         `json:"upstream"`
	Public   string         `json:"public"`
	Config   map[string]any `json:"config"`
	Sources  []SourceView   `json:"sources"`
	Events   []events.Event `json:"events"`
}

type SourceView struct {
	Source
	Label    string            `json:"label"`
	Duration float64           `json:"duration"`
	Cache    *rangecache.Stats `json:"cache,omitempty"`
	Session  *hls.Stats        `json:"session,omitempty"`
	Buffer   *BufferView       `json:"buffer,omitempty"`
}

// BufferView answers the real question: how much movie is ready ahead of the
// Client, and is the supply keeping up?
type BufferView struct {
	SegmentsSec float64 `json:"segments_sec"` // finished TS segments
	RawSec      float64 `json:"raw_sec"`      // raw data in the cache ahead of ffmpeg
	TotalSec    float64 `json:"total_sec"`
	NeedMbps    float64 `json:"need_mbps"` // average bitrate of the movie
	DownMbps    float64 `json:"down_mbps"` // download over the last 30 s
	Status      string  `json:"status"`
	Level       string  `json:"level"` // ok | warn | error
}

func (s *Server) handleState(w http.ResponseWriter, r *http.Request) {
	since, _ := strconv.ParseUint(r.URL.Query().Get("since"), 10, 64)
	st := State{
		Now: time.Now(), Upstream: s.opt.Upstream.String(), Public: s.opt.PublicURL,
		Config: map[string]any{
			"block_mib": s.opt.Cache.BlockSize >> 20, "workers": s.opt.Cache.Workers,
			"readahead_mib": s.opt.Cache.ReadAhead >> 20, "segment_sec": s.opt.SegmentSeconds,
			"max_ahead": s.opt.HLS.MaxAhead, "keep_behind": s.opt.HLS.KeepBehind,
			"audio": fmt.Sprintf("%s %s %dch", s.opt.AudioCodec, s.opt.AudioBitrate, s.opt.AudioChannels),
		},
		Events: s.ev.Since(since),
	}
	s.mu.Lock()
	ids := map[string]bool{}
	for id := range s.sources {
		ids[id] = true
	}
	for id := range s.caches {
		ids[id] = true
	}
	type snap struct {
		src  Source
		c    *rangecache.Cache
		sess *hls.Session
		dur  float64
		tl   bool
	}
	var snaps []snap
	for id := range ids {
		sn := snap{c: s.caches[id], sess: s.sessions[id]}
		if src := s.sources[id]; src != nil {
			sn.src = *src
		} else {
			sn.src = Source{ID: id}
		}
		if tl := s.timelines[id]; tl != nil {
			sn.dur, sn.tl = tl.Duration, true
		}
		snaps = append(snaps, sn)
	}
	s.mu.Unlock()

	for _, sn := range snaps {
		v := SourceView{Source: sn.src, Label: short(sn.src.ID), Duration: sn.dur}
		if sn.c != nil {
			cs := sn.c.Stats()
			v.Cache = &cs
		}
		if sn.sess != nil {
			ss := sn.sess.Stats()
			v.Session = &ss
		}
		if v.Cache == nil && v.Session == nil && time.Since(sn.src.Client.At) > time.Hour {
			continue // stop showing old, long-finished sources
		}
		v.Buffer = s.buffer(v)
		st.Sources = append(st.Sources, v)
	}
	sort.Slice(st.Sources, func(i, j int) bool {
		a, b := st.Sources[i], st.Sources[j]
		if (a.Session != nil) != (b.Session != nil) {
			return a.Session != nil // active playback first
		}
		return a.ID < b.ID
	})
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	json.NewEncoder(w).Encode(st)
}

// buffer converts bytes into movie time and sums up the state in one sentence.
// directBuffer: with Direct Play the client reads from the cache itself. The buffer
// is whatever lies contiguously ahead of its read position.
func directBuffer(v SourceView, tl *mkv.Timeline) *BufferView {
	b := &BufferView{DownMbps: v.Cache.Mbps30}
	if tl.Duration > 0 {
		b.NeedMbps = float64(v.Cache.Size) * 8 / tl.Duration / 1e6
	}
	var main *rangecache.ReaderStats
	for i := range v.Cache.Readers {
		if rd := &v.Cache.Readers[i]; main == nil || rd.Read > main.Read {
			main = rd
		}
	}
	switch {
	case v.Cache.Fatal != "":
		b.Level, b.Status = "error", "Server is not delivering the file: "+v.Cache.Fatal
	case main == nil:
		b.Level, b.Status = "ok", "Direct Play: cache ready, the client is not reading right now"
		return b
	default:
		b.RawSec = tl.TimeAtOffset(main.Pos+main.Ahead) - tl.TimeAtOffset(main.Pos)
		b.TotalSec = b.RawSec
		switch {
		case len(v.Cache.Waiting) > 0 && b.RawSec < 5:
			b.Level, b.Status = "error", "Buffer empty: the client is waiting for data from the server"
		case b.DownMbps > 0 && b.DownMbps < b.NeedMbps && b.RawSec < 60:
			b.Level, b.Status = "warn", fmt.Sprintf("Download (%.0f Mbit/s) slower than the movie (%.0f Mbit/s) – buffer shrinking", b.DownMbps, b.NeedMbps)
		default:
			b.Level, b.Status = "ok", "Direct Play: the client reads the MKV straight from the cache"
		}
	}
	return b
}

func (s *Server) buffer(v SourceView) *BufferView {
	if v.Cache == nil {
		return nil
	}
	s.mu.Lock()
	tl := s.timelines[v.ID]
	s.mu.Unlock()
	if tl == nil {
		return nil
	}
	if v.Session == nil {
		if v.Mode != "direct" {
			return nil
		}
		return directBuffer(v, tl)
	}
	b := &BufferView{SegmentsSec: v.Session.AheadSec, DownMbps: v.Cache.Mbps30}
	if tl.Duration > 0 {
		b.NeedMbps = float64(v.Cache.Size) * 8 / tl.Duration / 1e6
	}
	// Raw data: from the frontmost ffmpeg read position to the end of the contiguous range.
	if n := len(v.Cache.Readers); n > 0 {
		rd := v.Cache.Readers[n-1]
		b.RawSec = tl.TimeAtOffset(rd.Pos+rd.Ahead) - tl.TimeAtOffset(rd.Pos)
	}
	b.TotalSec = b.SegmentsSec + b.RawSec
	run := v.Session.Run
	switch {
	case v.Session.Failed != "":
		b.Level, b.Status = "error", "Playback failed: "+v.Session.Failed
	case v.Cache.Fatal != "":
		b.Level, b.Status = "error", "Server is not delivering the file: "+v.Cache.Fatal
	case v.Cache.CooldownMs > 0:
		b.Level, b.Status = "warn", "Server requests failing repeatedly, short pause"
	case run != nil && run.State == "paused":
		b.Level, b.Status = "ok", fmt.Sprintf("Enough buffered: ffmpeg paused, %s ready ahead of the client", clock(b.SegmentsSec))
	case len(v.Cache.Waiting) > 0 && b.SegmentsSec < 8:
		b.Level, b.Status = "error", "Buffer empty: ffmpeg is waiting for data from the server"
	case b.DownMbps > 0 && b.DownMbps < b.NeedMbps && b.TotalSec < 60:
		b.Level, b.Status = "warn", fmt.Sprintf("Download (%.0f Mbit/s) slower than the movie (%.0f Mbit/s) – buffer shrinking", b.DownMbps, b.NeedMbps)
	case run == nil:
		b.Level, b.Status = "ok", "ffmpeg idle – starts as soon as the client wants segments again"
	default:
		b.Level, b.Status = "ok", "Running"
	}
	return b
}
