package server

import (
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/lucsoft/jellyfin-proxy/internal/events"
	"github.com/lucsoft/jellyfin-proxy/internal/hls"
	"github.com/lucsoft/jellyfin-proxy/internal/rangecache"
	"github.com/lucsoft/jellyfin-proxy/internal/testmedia"
)

// fakeJellyfin plays the server: PlaybackInfo and a file that supports ranges.
func fakeJellyfin(t *testing.T, file string) *httptest.Server {
	data, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /Items/{item}/PlaybackInfo", func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.Header.Get("Authorization"), `Token="secret"`) {
			http.Error(w, "auth", 401)
			return
		}
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		io.WriteString(w, `{"MediaSources":[{"Id":"src1","Name":"Test movie","Container":"mkv","Size":`+
			itoa(len(data))+`,"SupportsDirectPlay":true,"DefaultAudioStreamIndex":1,"MediaStreams":[
			{"Index":0,"Type":"Video","Codec":"h264"},
			{"Index":1,"Type":"Audio","Codec":"aac","DisplayTitle":"AAC Stereo","Channels":2},
			{"Index":2,"Type":"Audio","Codec":"ac3","DisplayTitle":"AC3 Stereo","Channels":2}]}],"PlaySessionId":"x"}`)
	})
	mux.HandleFunc("GET /Videos/{item}/stream", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("api_key") != "secret" {
			http.Error(w, "auth", 401)
			return
		}
		http.ServeContent(w, r, "f.mkv", time.Time{}, strings.NewReader(string(data)))
	})
	mux.HandleFunc("GET /Users/Me", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Emby-Token") != "secret" {
			http.Error(w, "auth", 401)
			return
		}
		w.Write([]byte(`{"Name":"test"}`))
	})
	mux.HandleFunc("POST /Sessions/Playing/Progress", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) })
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	return ts
}

func itoa(n int) string { return strconv.Itoa(n) }

func newTestServer(t *testing.T, file string, directPlay bool) (*Server, *httptest.Server, *events.Log) {
	t.Helper()
	jf := fakeJellyfin(t, file)
	up, _ := url.Parse(jf.URL)
	ev := events.New(500, nil)
	dir := t.TempDir()
	srv := New(Options{
		DirectPlay: directPlay,
		Upstream:   up, PublicURL: "http://tv", WorkDir: dir,
		Cache: rangecache.Config{Dir: dir, BlockSize: 256 << 10, Workers: 4, Reserve: 1,
			ReadAhead: 2 << 20, KeepBehind: 1 << 20, Retries: 3, Stall: 5 * time.Second,
			Client: http.DefaultClient, Events: ev},
		HLS: hls.Config{FFmpeg: "ffmpeg", Dir: dir, MaxAhead: 50, KeepBehind: 50, SeekGap: 2,
			IdleStop: time.Minute, Events: ev},
		AudioCodec: "ac3", AudioBitrate: "192k", AudioChannels: 2, CopyAudio: []string{"ac3"},
		SegmentSeconds: 4, SessionIdle: time.Hour, CacheIdle: time.Hour,
		APIClient: http.DefaultClient, Events: ev,
	})
	t.Cleanup(srv.Shutdown)
	loop, _ := net.Listen("tcp", "127.0.0.1:0")
	srv.SetLocalAddr(loop.Addr().String())
	go http.Serve(loop, srv.LocalHandler())
	tv := httptest.NewServer(srv.Handler())
	t.Cleanup(tv.Close)
	return srv, tv, ev
}

func TestEndToEnd(t *testing.T) {
	file := testmedia.MKV(t)
	_, tv, _ := newTestServer(t, file, false)

	// 1. PlaybackInfo with audio track 2 (AC3 -> copied)
	req, _ := http.NewRequest("POST", tv.URL+"/Items/item1/PlaybackInfo",
		strings.NewReader(`{"AudioStreamIndex":2,"MediaSourceId":"src1"}`))
	req.Header.Set("Authorization", `MediaBrowser Client="x", Token="secret"`)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	var pi struct{ MediaSources []map[string]any }
	json.NewDecoder(resp.Body).Decode(&pi)
	resp.Body.Close()
	ms := pi.MediaSources[0]
	if ms["SupportsDirectPlay"] != false {
		t.Fatalf("not rewritten: %v", ms)
	}
	tu, _ := ms["TranscodingUrl"].(string)
	if !strings.Contains(tu, "/proxy/hls/src1/master.m3u8") || !strings.Contains(tu, "audio=2") {
		t.Fatalf("TranscodingUrl %q", tu)
	}

	// 2. Playlist
	body := getBody(t, tv.URL+tu)
	if !strings.Contains(body, "#EXT-X-PLAYLIST-TYPE:VOD") {
		t.Fatal(body)
	}
	var segs []string
	init := ""
	for _, l := range strings.Split(body, "\n") {
		if strings.HasPrefix(l, "/proxy/hls/") {
			segs = append(segs, l)
		}
		if u, ok := strings.CutPrefix(l, `#EXT-X-MAP:URI="`); ok {
			init = getBody(t, tv.URL+strings.TrimSuffix(u, `"`))
		}
	}
	if init == "" {
		t.Fatal("fMP4 playlist without EXT-X-MAP")
	}
	// 3. Segments: start, then jump to the end (each prefixed with the init segment)
	for _, k := range []int{0, 1, len(segs) - 1} {
		path := t.TempDir() + "/s.mp4"
		os.WriteFile(path, []byte(init+getBody(t, tv.URL+segs[k])), 0o644)
		out, err := exec.Command("ffprobe", "-v", "error", "-show_entries", "stream=codec_name", "-of", "csv=p=0", path).CombinedOutput()
		if err != nil || !strings.Contains(string(out), "h264") || !strings.Contains(string(out), "ac3") {
			t.Fatalf("segment %d broken: %v %s", k, err, out)
		}
	}

	// 4. Dashboard API
	var st State
	json.Unmarshal([]byte(getBody(t, tv.URL+"/proxy/api/state")), &st)
	if len(st.Sources) != 1 || st.Sources[0].Session == nil || st.Sources[0].Cache == nil {
		t.Fatalf("state: %+v", st)
	}
	if st.Sources[0].Buffer == nil {
		t.Error("no buffer evaluation")
	}
	for _, e := range st.Events {
		t.Logf("%-16s %-5s %s", e.Kind, e.Level, e.Msg)
	}
}

func getBody(t *testing.T, u string) string {
	t.Helper()
	resp, err := http.Get(u)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Fatalf("%s: %d %s", u, resp.StatusCode, b)
	}
	return string(b)
}

// With a client-compatible audio track Direct Play stays allowed, and /Videos/{id}/stream
// serves the original MKV byte-for-byte from the cache – ranges included.
func TestDirectPlayFromCache(t *testing.T) {
	file := testmedia.MKV(t)
	data, _ := os.ReadFile(file)
	_, tv, _ := newTestServer(t, file, true)

	req, _ := http.NewRequest("POST", tv.URL+"/Items/item1/PlaybackInfo",
		strings.NewReader(`{"AudioStreamIndex":2,"MediaSourceId":"src1"}`))
	req.Header.Set("Authorization", `MediaBrowser Token="secret"`)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	var pi struct{ MediaSources []map[string]any }
	json.NewDecoder(resp.Body).Decode(&pi)
	resp.Body.Close()
	ms := pi.MediaSources[0]
	if ms["SupportsDirectPlay"] != true || ms["TranscodingUrl"] != nil || ms["DefaultAudioStreamIndex"] != float64(2) {
		t.Fatalf("Direct Play not preserved: %v", ms)
	}

	u := tv.URL + "/Videos/item1/stream?static=true&MediaSourceId=src1&api_key=secret"
	if got := getBody(t, u); got != string(data) {
		t.Fatalf("whole file: %d bytes, expected %d", len(got), len(data))
	}
	rq, _ := http.NewRequest("GET", u, nil)
	rq.Header.Set("Range", "bytes=1000-1999")
	rr, err := http.DefaultClient.Do(rq)
	if err != nil {
		t.Fatal(err)
	}
	part, _ := io.ReadAll(rr.Body)
	rr.Body.Close()
	if rr.StatusCode != 206 || string(part) != string(data[1000:2000]) {
		t.Fatalf("Range: HTTP %d, %d bytes", rr.StatusCode, len(part))
	}
}

// Media from the cache is only served with a token Jellyfin accepts – also
// for sources that are already cached, and also for tokens the proxy has
// never seen in a PlaybackInfo request.
func TestStreamsRequireValidToken(t *testing.T) {
	file := testmedia.MKV(t)
	srv, tv, _ := newTestServer(t, file, true)

	req, _ := http.NewRequest("POST", tv.URL+"/Items/item1/PlaybackInfo",
		strings.NewReader(`{"AudioStreamIndex":2,"MediaSourceId":"src1"}`))
	req.Header.Set("Authorization", `MediaBrowser Token="secret"`)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	status := func(u string, hdr map[string]string) int {
		rq, _ := http.NewRequest("GET", tv.URL+u, nil)
		for k, v := range hdr {
			rq.Header.Set(k, v)
		}
		rs, err := http.DefaultClient.Do(rq)
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, rs.Body)
		rs.Body.Close()
		return rs.StatusCode
	}
	direct := "/Videos/item1/stream?static=true&MediaSourceId=src1"
	hls := "/proxy/hls/src1/master.m3u8?audio=1&item=item1"
	for _, c := range []struct {
		name string
		url  string
		hdr  map[string]string
		want int
	}{
		{"direct without token", direct, nil, 401},
		{"direct with wrong token", direct + "&api_key=nope", nil, 401},
		{"hls without token", hls, nil, 401},
		{"hls with wrong header token", hls, map[string]string{"X-Emby-Token": "nope"}, 401},
		{"direct with token", direct + "&api_key=secret", nil, 200},
		{"hls with header token", hls, map[string]string{"Authorization": `MediaBrowser Token="secret"`}, 200},
	} {
		if got := status(c.url, c.hdr); got != c.want {
			t.Errorf("%s: HTTP %d, want %d", c.name, got, c.want)
		}
	}

	// A token never used for PlaybackInfo is checked against Jellyfin once.
	srv.tokens.mu.Lock()
	delete(srv.tokens.entries, "secret")
	srv.tokens.mu.Unlock()
	if got := status(direct+"&api_key=secret", nil); got != 200 {
		t.Errorf("token checked against Jellyfin: HTTP %d, want 200", got)
	}
}

// The background preparation and the first media request ask for the cache
// at the same time; they must end up with one and the same cache.
func TestConcurrentCacheForOpensOneCache(t *testing.T) {
	file := testmedia.MKV(t)
	srv, _, _ := newTestServer(t, file, true)
	jf := fakeJellyfin(t, file)
	srv.opt.Upstream, _ = url.Parse(jf.URL)
	src := &Source{ID: "src1", ItemID: "item1", token: "secret"}
	got := make(chan *rangecache.Cache, 8)
	for i := 0; i < cap(got); i++ {
		go func() {
			c, err := srv.cacheFor(t.Context(), src)
			if err != nil {
				t.Error(err)
			}
			got <- c
		}()
	}
	first := <-got
	for i := 1; i < cap(got); i++ {
		if c := <-got; c != first {
			t.Fatal("two different caches for the same source")
		}
	}
}
