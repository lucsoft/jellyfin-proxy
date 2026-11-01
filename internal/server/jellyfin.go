package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/lucsoft/jellyfin-proxy/internal/events"
)

type ctxKey int

const playbackInfoKey ctxKey = 1

// playbackRequest holds what the client asked for in its PlaybackInfo call.
type playbackRequest struct {
	ItemID   string
	SourceID string
	Audio    *int
	StartSec float64
	Token    string
	Subtitle *int     // SubtitleStreamIndex the client asked for (-1 = off)
	SubProfs []string // how the client can show subtitles, e.g. "pgssub:Encode"
}

// Handler returns the public handler (Client and dashboard).
func (s *Server) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := r.URL.Path
		lp := strings.ToLower(p)
		switch {
		case p == "/":
			s.handleDashboard(w, r)
		case p == "/proxy/api/state":
			s.handleState(w, r)
		case strings.HasPrefix(p, "/proxy/hls/"):
			s.handleHLS(w, r)
		case strings.HasPrefix(lp, "/items/") && strings.HasSuffix(lp, "/playbackinfo"):
			s.handlePlaybackInfo(w, r, strings.Split(p, "/")[2])
		case strings.HasPrefix(lp, "/videos/") && s.handleVideoStream(w, r):
		case strings.HasPrefix(lp, "/sessions/playing"):
			s.observeReport(r)
			s.rp.ServeHTTP(w, r)
		default:
			s.rp.ServeHTTP(w, r)
		}
	})
}

func (s *Server) newReverseProxy() *httputil.ReverseProxy {
	up := s.opt.Upstream
	return &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(up)
			pr.Out.Host = up.Host
			if pr.In.Context().Value(playbackInfoKey) != nil || isSystemInfo(pr.In) {
				pr.Out.Header.Del("Accept-Encoding") // we need to read the response
			}
		},
		Transport:      s.opt.APIClient.Transport,
		FlushInterval:  -1,
		ModifyResponse: s.modifyResponse,
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			s.ev.Emit("proxy", "upstream_error", events.Warn, fmt.Sprintf("%s %s: %v", r.Method, r.URL.Path, err))
			http.Error(w, "upstream: "+err.Error(), http.StatusBadGateway)
		},
	}
}

func isSystemInfo(r *http.Request) bool {
	return strings.HasPrefix(strings.ToLower(r.URL.Path), "/system/info")
}

func (s *Server) modifyResponse(resp *http.Response) error {
	pr, _ := resp.Request.Context().Value(playbackInfoKey).(*playbackRequest)
	if pr != nil && resp.StatusCode == http.StatusOK {
		s.rememberToken(pr.Token) // Jellyfin just accepted this token
	}
	info := isSystemInfo(resp.Request) && s.opt.ServerName != ""
	if (pr == nil && !info) || resp.StatusCode != http.StatusOK ||
		!strings.Contains(resp.Header.Get("Content-Type"), "json") {
		return nil
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return err
	}
	var root map[string]any
	if json.Unmarshal(body, &root) == nil {
		if pr != nil {
			s.rewritePlaybackInfo(root, pr)
		} else if _, ok := root["ServerName"]; ok {
			root["ServerName"] = s.opt.ServerName
		}
		if b, err := json.Marshal(root); err == nil {
			body = b
		}
	}
	resp.Body = io.NopCloser(bytes.NewReader(body))
	resp.ContentLength = int64(len(body))
	resp.Header.Set("Content-Length", strconv.Itoa(len(body)))
	resp.Header.Del("Content-Encoding")
	return nil
}

// ---------------------------------------------------------------- PlaybackInfo

func (s *Server) handlePlaybackInfo(w http.ResponseWriter, r *http.Request, itemID string) {
	pr := &playbackRequest{ItemID: itemID, Token: tokenFrom(r)}
	if r.Body != nil {
		body, _ := io.ReadAll(r.Body)
		r.Body = io.NopCloser(bytes.NewReader(body))
		var b struct {
			AudioStreamIndex    *int
			SubtitleStreamIndex *int
			StartTimeTicks      *int64
			MediaSourceId       string
			DeviceProfile       struct {
				SubtitleProfiles []struct{ Format, Method string }
			}
		}
		if json.Unmarshal(body, &b) == nil {
			pr.Audio, pr.SourceID, pr.Subtitle = b.AudioStreamIndex, b.MediaSourceId, b.SubtitleStreamIndex
			for _, sp := range b.DeviceProfile.SubtitleProfiles {
				pr.SubProfs = append(pr.SubProfs, sp.Format+":"+sp.Method)
			}
			if b.StartTimeTicks != nil {
				pr.StartSec = float64(*b.StartTimeTicks) / 1e7
			}
		}
		// Jellyfin does not know the virtual tracks; ask it for the real one.
		body = realAudioIndex(body)
		r.Body = io.NopCloser(bytes.NewReader(body))
		r.ContentLength = int64(len(body))
	}
	if v := queryCI(r, "SubtitleStreamIndex"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			pr.Subtitle = &n
		}
	}
	if v := queryCI(r, "AudioStreamIndex"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			pr.Audio = &n
			if real, rdr := splitAudio(n); rdr {
				q := r.URL.Query()
				for k := range q {
					if strings.EqualFold(k, "AudioStreamIndex") {
						q.Set(k, strconv.Itoa(real))
					}
				}
				r.URL.RawQuery = q.Encode()
			}
		}
	}
	if v := queryCI(r, "StartTimeTicks"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			pr.StartSec = float64(n) / 1e7
		}
	}
	if v := queryCI(r, "MediaSourceId"); v != "" {
		pr.SourceID = v
	}
	s.rp.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), playbackInfoKey, pr)))
}

// rewritePlaybackInfo redirects MKV sources to our HLS and remembers their
// metadata. Other containers stay untouched (the server handles them itself).
func (s *Server) rewritePlaybackInfo(root map[string]any, pr *playbackRequest) {
	list, _ := root["MediaSources"].([]any)
	for _, x := range list {
		ms, ok := x.(map[string]any)
		if !ok {
			continue
		}
		src := sourceFrom(ms, pr)
		if src == nil {
			continue
		}
		if !isMatroska(src.Container) {
			s.ev.Emit(short(src.ID), "passthrough", events.Info,
				fmt.Sprintf("%s is %s, not MKV: server decides on its own", src.Name, src.Container))
			continue
		}
		audio := src.DefaultAudio
		if pr.Audio != nil && (pr.SourceID == "" || pr.SourceID == src.ID) {
			audio = *pr.Audio
		}
		if s.opt.RDR {
			s.addRDRTracks(ms)
		}
		ms["DefaultAudioStreamIndex"] = audio // the client's choice, virtual tracks included

		src.Audio = audio
		src.Mode = "hls"
		if s.directOK(src, audio) {
			src.Mode = "direct"
		}
		s.mu.Lock()
		if old := s.sources[src.ID]; old != nil {
			src.Client = old.Client
			if src.token == "" {
				src.token = old.token
			}
		}
		s.sources[src.ID] = src
		s.mu.Unlock()

		a := s.audioFor(src, audio)
		s.logSubtitles(src, ms, pr)
		if src.Mode == "direct" {
			// Direct Play stays allowed; the client fetches /Videos/{id}/stream, and
			// the proxy serves that from the cache. Report the requested audio
			// track as the default so the player selects it.
			ms["SupportsDirectPlay"] = true
			ms["DefaultAudioStreamIndex"] = audio
			delete(ms, "TranscodingUrl")
			s.ev.Emit(short(src.ID), "playbackinfo", events.Info,
				fmt.Sprintf("Client requests %q (audio %s, start %s) -> Direct Play from the cache", src.Name, a.Label, clock(pr.StartSec)))
			if pr.SourceID == "" || pr.SourceID == src.ID {
				go s.prepareDirect(src)
			}
			continue
		}

		q := url.Values{}
		q.Set("audio", strconv.Itoa(audio))
		q.Set("item", src.ItemID)
		if pr.StartSec > 0 {
			q.Set("start", strconv.FormatFloat(pr.StartSec, 'f', 3, 64))
		}
		if src.token != "" {
			q.Set("api_key", src.token)
		}
		ms["SupportsDirectPlay"] = false
		ms["SupportsDirectStream"] = false
		ms["SupportsTranscoding"] = true
		ms["TranscodingUrl"] = "/proxy/hls/" + url.PathEscape(src.ID) + "/master.m3u8?" + q.Encode()
		ms["TranscodingSubProtocol"] = "hls"
		ms["TranscodingContainer"] = "ts"
		if s.opt.HLS.Format != "ts" {
			ms["TranscodingContainer"] = "mp4"
		}
		delete(ms, "DirectStreamUrl")

		s.ev.Emit(short(src.ID), "playbackinfo", events.Info,
			fmt.Sprintf("Client requests %q (audio %s, start %s) -> HLS via the proxy", src.Name, a.Label, clock(pr.StartSec)))
		// Prepare cache and index right away; the client is about to request the playlist.
		if pr.SourceID == "" || pr.SourceID == src.ID {
			go func(src *Source, audio int, start float64) {
				if _, err := s.sessionFor(src, audio, start); err != nil {
					s.ev.Emit(short(src.ID), "prepare_error", events.Error, "Preparation failed: "+err.Error())
				}
			}(src, audio, pr.StartSec)
		}
	}
}

func isMatroska(container string) bool {
	c := strings.ToLower(container)
	return strings.Contains(c, "mkv") || strings.Contains(c, "matroska") || strings.Contains(c, "webm")
}

func sourceFrom(ms map[string]any, pr *playbackRequest) *Source {
	id, _ := ms["Id"].(string)
	if id == "" {
		return nil
	}
	src := &Source{ID: id, ItemID: pr.ItemID, token: pr.Token, DefaultAudio: -1}
	src.Name, _ = ms["Name"].(string)
	src.Container, _ = ms["Container"].(string)
	if v, ok := ms["Size"].(float64); ok {
		src.Size = int64(v)
	}
	if v, ok := ms["Bitrate"].(float64); ok {
		src.Bitrate = int64(v)
	}
	if v, ok := ms["DefaultAudioStreamIndex"].(float64); ok {
		src.DefaultAudio = int(v)
	}
	streams, _ := ms["MediaStreams"].([]any)
	for _, x := range streams {
		m, ok := x.(map[string]any)
		if !ok {
			continue
		}
		st := Stream{}
		if v, ok := m["Index"].(float64); ok {
			st.Index = int(v)
		}
		st.Type, _ = m["Type"].(string)
		st.Codec, _ = m["Codec"].(string)
		st.Title, _ = m["DisplayTitle"].(string)
		if v, ok := m["Channels"].(float64); ok {
			st.Channels = int(v)
		}
		src.Streams = append(src.Streams, st)
		if st.Type == "Audio" && src.DefaultAudio < 0 {
			src.DefaultAudio = st.Index
		}
	}
	return src
}

// ---------------------------------------------------------------- Stream fallback

var videoStreamPath = regexp.MustCompile(`(?i)^/videos/([^/]+)/stream(\.[a-z0-9]+)?$`)

// handleVideoStream intercepts /Videos/{id}/stream in case the client wants to
// play directly despite the rewrite, and serves the HLS playlist instead.
func (s *Server) handleVideoStream(w http.ResponseWriter, r *http.Request) bool {
	m := videoStreamPath.FindStringSubmatch(r.URL.Path)
	if m == nil {
		return false
	}
	id := queryCI(r, "MediaSourceId")
	if id == "" {
		id = m[1]
	}
	src := s.source(id)
	if src == nil || !isMatroska(src.Container) {
		return false
	}
	audio := src.Audio
	if v, err := strconv.Atoi(queryCI(r, "AudioStreamIndex")); err == nil {
		audio = v
	}
	if !s.requireAuth(w, r) {
		return true
	}
	if s.directOK(src, audio) {
		s.serveDirect(w, r, src)
		return true
	}
	s.ev.Emit(short(id), "direct_intercept", events.Info,
		"Client wants to play directly, but the audio track needs transcoding – serving the HLS playlist instead")
	s.servePlaylist(w, r, src, audio, 0)
	return true
}

// ---------------------------------------------------------------- Playback reports

// observeReport listens in on what the client reports about its playback.
func (s *Server) observeReport(r *http.Request) {
	if r.Method != http.MethodPost || r.Body == nil {
		return
	}
	body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	body = realAudioIndex(body)
	r.Body = io.NopCloser(bytes.NewReader(body))
	r.ContentLength = int64(len(body))
	var rep struct {
		MediaSourceId string
		ItemId        string
		PositionTicks *int64
		IsPaused      bool
	}
	if json.Unmarshal(body, &rep) != nil {
		return
	}
	id := rep.MediaSourceId
	if id == "" {
		id = rep.ItemId
	}
	kind := "progress"
	switch strings.ToLower(strings.TrimPrefix(strings.ToLower(r.URL.Path), "/sessions/playing")) {
	case "":
		kind = "start"
	case "/stopped":
		kind = "stopped"
	}
	s.mu.Lock()
	src := s.sources[id]
	if src != nil {
		src.Client.At, src.Client.Paused, src.Client.Event = time.Now(), rep.IsPaused, kind
		if rep.PositionTicks != nil {
			src.Client.Position = float64(*rep.PositionTicks) / 1e7
		}
	}
	s.mu.Unlock()
	if src == nil {
		return
	}
	switch kind {
	case "start":
		s.ev.Emit(short(id), "client_start", events.Info, "Client reports: playback started")
	case "stopped":
		why := fmt.Sprintf("Client reports stop at %s", clock(src.Client.Position))
		s.ev.Emit(short(id), "client_stopped", events.Info, why+" – session ends if nothing arrives within 20 s")
		s.stopSoon(id, why)
	}
}

// ---------------------------------------------------------------- Helpers

var tokenRe = regexp.MustCompile(`(?i)\btoken="([^"]+)"`)

// tokenFrom finds the Jellyfin token in the query or headers.
func tokenFrom(r *http.Request) string {
	for _, k := range []string{"api_key", "ApiKey"} {
		if v := queryCI(r, k); v != "" {
			return v
		}
	}
	for _, h := range []string{"X-Emby-Token", "X-MediaBrowser-Token"} {
		if v := r.Header.Get(h); v != "" {
			return v
		}
	}
	for _, h := range []string{"Authorization", "X-Emby-Authorization"} {
		if m := tokenRe.FindStringSubmatch(r.Header.Get(h)); m != nil {
			return m[1]
		}
	}
	return ""
}

// queryCI reads a query parameter case-insensitively
// (Jellyfin itself is lenient here, and clients spell it differently).
func queryCI(r *http.Request, key string) string {
	for k, v := range r.URL.Query() {
		if strings.EqualFold(k, key) && len(v) > 0 {
			return v[0]
		}
	}
	return ""
}

// logSubtitles records what the client asked for and how Jellyfin wants to
// deliver the chosen subtitle track. Image subtitles (PGS) usually come
// back as "Encode" (burn into the video), which Direct Play and the
// copying HLS path cannot do – this makes that visible instead of silent.
func (s *Server) logSubtitles(src *Source, ms map[string]any, pr *playbackRequest) {
	if pr.Subtitle == nil || *pr.Subtitle < 0 {
		return
	}
	streams, _ := ms["MediaStreams"].([]any)
	for _, x := range streams {
		m, _ := x.(map[string]any)
		if m == nil || m["Type"] != "Subtitle" {
			continue
		}
		if idx, _ := m["Index"].(float64); int(idx) != *pr.Subtitle {
			continue
		}
		codec, _ := m["Codec"].(string)
		method, _ := m["DeliveryMethod"].(string)
		title, _ := m["DisplayTitle"].(string)
		level := events.Info
		msg := fmt.Sprintf("Subtitle #%d %s (%s): Jellyfin delivers it as %q", *pr.Subtitle, title, codec, method)
		if method == "Encode" {
			level = events.Warn
			msg += " – it would have to be burned into the video, which this proxy does not do, so the client shows nothing"
		}
		s.ev.Emit(short(src.ID), "subtitle", level, msg, "client_profiles", strings.Join(pr.SubProfs, ", "))
	}
}

// realAudioIndex replaces a virtual AudioStreamIndex (see rdrBase) in a JSON
// body with the real one before the request goes to Jellyfin.
func realAudioIndex(body []byte) []byte {
	var m map[string]any
	if json.Unmarshal(body, &m) != nil {
		return body
	}
	v, ok := m["AudioStreamIndex"].(float64)
	if !ok {
		return body
	}
	real, rdr := splitAudio(int(v))
	if !rdr {
		return body
	}
	m["AudioStreamIndex"] = real
	if b, err := json.Marshal(m); err == nil {
		return b
	}
	return body
}

// addRDRTracks lists a "reduced dynamic range" variant right after every
// audio track, so clients offer it in their normal audio menu.
func (s *Server) addRDRTracks(ms map[string]any) {
	streams, _ := ms["MediaStreams"].([]any)
	var out []any
	for _, x := range streams {
		out = append(out, x)
		m, _ := x.(map[string]any)
		if m == nil || m["Type"] != "Audio" {
			continue
		}
		idx, ok := m["Index"].(float64)
		if !ok || int(idx) >= rdrBase {
			continue
		}
		v := make(map[string]any, len(m))
		for k, val := range m {
			v[k] = val
		}
		title, _ := m["DisplayTitle"].(string)
		v["Index"] = rdrBase + int(idx)
		v["DisplayTitle"] = title + " · Reduced dynamic range"
		v["Title"] = v["DisplayTitle"]
		v["Codec"] = s.opt.AudioCodec
		v["IsDefault"] = false
		if ch, ok := m["Channels"].(float64); !ok || int(ch) > s.opt.AudioChannels {
			v["Channels"] = s.opt.AudioChannels
		}
		delete(v, "Profile")
		delete(v, "BitRate")
		out = append(out, v)
	}
	ms["MediaStreams"] = out
}
