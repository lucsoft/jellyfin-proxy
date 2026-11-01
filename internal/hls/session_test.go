package hls

import (
	"context"
	"math"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/lucsoft/jellyfin-proxy/internal/events"
	"github.com/lucsoft/jellyfin-proxy/internal/mkv"
	"github.com/lucsoft/jellyfin-proxy/internal/testmedia"
)

func newTestSession(t *testing.T, cfg Config) (*Session, *mkv.Timeline, *events.Log) {
	t.Helper()
	file := testmedia.MKV(t)
	f, err := os.Open(file)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	st, _ := f.Stat()
	tl, err := mkv.Read(f, st.Size(), 4)
	if err != nil {
		t.Fatal(err)
	}
	ev := events.New(200, nil)
	cfg.FFmpeg, cfg.Dir, cfg.Events = "ffmpeg", t.TempDir(), ev
	cfg.InputArgs = []string{"-readrate", "8"} // 40 s of movie in 5 s
	if cfg.IdleStop == 0 {
		cfg.IdleStop = time.Minute
	}
	s, err := New(cfg, "id", "id", tl, file, Audio{Index: 2, Codec: "ac3", Bitrate: "192k", Channels: 2, Label: "AC3"}, "h264")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close("test") })
	return s, tl, ev
}

// startPTS returns the first video timestamp of a segment.
func startPTS(t *testing.T, path string) float64 {
	t.Helper()
	out, err := exec.Command("ffprobe", "-v", "error", "-select_streams", "v:0",
		"-show_entries", "packet=pts_time", "-of", "csv=p=0", "-read_intervals", "%+#1", path).Output()
	if err != nil {
		t.Fatal(err)
	}
	v, _ := strconv.ParseFloat(strings.Trim(strings.TrimSpace(string(out)), ","), 64)
	return v
}

func get(t *testing.T, s *Session, k int) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	f, err := s.Segment(ctx, k)
	if err != nil {
		t.Fatalf("segment %d: %v", k, err)
	}
	return f.Path
}

func TestSegmentsHaveAbsoluteTimestampsAcrossRestarts(t *testing.T) {
	s, tl, ev := newTestSession(t, Config{MaxAhead: 50, KeepBehind: 50, SeekGap: 1, Format: "ts"})
	if n := len(tl.Segments); n < 6 {
		t.Fatalf("too few segments: %d", n)
	}
	// mpegts shifts all timestamps by a constant 1.4 s.
	check := func(k int, path string) {
		got := startPTS(t, path) - 1.4
		if math.Abs(got-tl.Segments[k].Start) > 0.02 {
			t.Errorf("segment %d starts at %.3f, playlist says %.3f", k, got, tl.Segments[k].Start)
		}
	}
	check(0, get(t, s, 0))
	check(1, get(t, s, 1))
	// jump forward -> restart at segment 5
	check(5, get(t, s, 5))
	// jump back to an existing segment: no restart needed
	check(1, get(t, s, 1))
	st := s.Stats()
	if st.Restarts < 1 {
		t.Errorf("expected at least one restart, stats: %+v", st.Run)
	}
	found := false
	for _, e := range ev.Since(0) {
		if e.Kind == "ffmpeg_start" && strings.Contains(e.Msg, "Jump forward") {
			found = true
		}
	}
	if !found {
		t.Error("restart without a reason in the event log")
	}
}

func TestPausesWhenFarAheadOfTV(t *testing.T) {
	s, _, ev := newTestSession(t, Config{MaxAhead: 2, KeepBehind: 1, SeekGap: 3})
	get(t, s, 0)
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if r := s.Stats().Run; r != nil && r.State == "paused" {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if r := s.Stats().Run; r == nil || r.State != "paused" {
		t.Fatalf("ffmpeg not paused: %+v", r)
	}
	// client keeps fetching -> ffmpeg is woken up and delivers to the end
	for k := 1; k < len(s.tl.Segments); k++ {
		get(t, s, k)
	}
	if st := s.Stats(); st.Deleted == 0 {
		t.Error("old segments were not deleted")
	}
	// Pausing is routine: exactly one event per run, no matter how often it paused.
	pauses := 0
	for _, e := range ev.Since(0) {
		if e.Kind == "ffmpeg_pause" {
			pauses++
		}
	}
	if pauses != 1 {
		t.Errorf("%d pause events, expected 1", pauses)
	}
}

func TestPlaylistIsCompleteVOD(t *testing.T) {
	s, tl, _ := newTestSession(t, Config{MaxAhead: 5, KeepBehind: 5, SeekGap: 3})
	pl := string(s.Playlist(func(k int) string { return "seg/" + strconv.Itoa(k) + ".m4s" }, "init.mp4"))
	if !strings.Contains(pl, `#EXT-X-MAP:URI="init.mp4"`) || !strings.Contains(pl, "#EXT-X-VERSION:7") {
		t.Fatal(pl)
	}
	if !strings.Contains(pl, "#EXT-X-PLAYLIST-TYPE:VOD") || !strings.HasSuffix(pl, "#EXT-X-ENDLIST\n") {
		t.Fatal(pl)
	}
	if n := strings.Count(pl, "#EXTINF"); n != len(tl.Segments) {
		t.Fatalf("%d EXTINF, %d segments", n, len(tl.Segments))
	}
}

// A segment number from a playlist with a different segment length is mapped
// to the correct movie position via the start time sent along with it.
func TestResolveStalePlaylistByTime(t *testing.T) {
	s, tl, ev := newTestSession(t, Config{MaxAhead: 5, KeepBehind: 5, SeekGap: 3})
	k := len(tl.Segments) - 2
	if got, stale := s.Resolve(k, tl.Segments[k].Start); got != k || stale {
		t.Fatalf("matching time: %d %v", got, stale)
	}
	if got, stale := s.Resolve(1, tl.Segments[k].Start+0.5); got != k || !stale {
		t.Fatalf("stale number: got %d, expected %d", got, k)
	}
	if got, _ := s.Resolve(3, -1); got != 3 {
		t.Fatalf("without a time the number must apply: %d", got)
	}
	n := 0
	for _, e := range ev.Since(0) {
		if e.Kind == "stale_playlist" {
			n++
		}
	}
	s.Resolve(2, tl.Segments[k].Start)
	if n != 1 {
		t.Fatalf("%d warnings, expected exactly one", n)
	}
}

// When resuming, AVPlay first fetches segments 0–2 and then jumps to the
// resume position. The requests at the start must not kill the prepared
// ffmpeg run.
func TestResumeSurvivesHeadRequests(t *testing.T) {
	s, tl, ev := newTestSession(t, Config{MaxAhead: 50, KeepBehind: 50, SeekGap: 1})
	resume := len(tl.Segments) - 3
	s.Prepare(resume, "resume")
	for k := 0; k < headSegs; k++ {
		get(t, s, k)
	}
	get(t, s, resume)
	if st := s.Stats(); st.Restarts != 0 || st.Run.Start != resume {
		t.Fatalf("run was replaced: %d restarts, start %d", st.Restarts, st.Run.Start)
	}
	for _, e := range ev.Since(0) {
		if e.Kind == "ffmpeg_start" && strings.Contains(e.Msg, "Jump") {
			t.Errorf("unexpected restart: %s", e.Msg)
		}
	}
}

// fMP4: the init section from the first run plus media data from different
// runs form a gap-free decodable stream with correct timestamps.
func TestFMP4InitAndFragmentsAcrossRuns(t *testing.T) {
	s, tl, _ := newTestSession(t, Config{MaxAhead: 50, KeepBehind: 50, SeekGap: 1})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	last := len(tl.Segments) - 1
	var media []byte
	for _, k := range []int{0, 1, last - 1, last} { // with a restart in between
		f, err := s.Segment(ctx, k)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := os.ReadFile(f.Path)
		if string(b[f.Off+4:f.Off+8]) != "moof" {
			t.Fatalf("segment %d does not start with moof: %q", k, b[f.Off+4:f.Off+8])
		}
		if k == last-1 {
			media = nil // new run: only check its own fragments as contiguous
		}
		media = append(media, b[f.Off:]...)
	}
	init, err := s.Init(ctx)
	if err != nil {
		t.Fatal(err)
	}
	joined := t.TempDir() + "/joined.mp4"
	os.WriteFile(joined, append(append([]byte(nil), init...), media...), 0o644)
	if out, err := exec.Command("ffmpeg", "-v", "error", "-i", joined, "-f", "null", "-").CombinedOutput(); err != nil || len(out) > 0 {
		t.Fatalf("does not decode cleanly: %v %s", err, out)
	}
	// First timestamp = segment start + fixed 2 s offset.
	if got := startPTS(t, joined) - 2; math.Abs(got-tl.Segments[last-1].Start) > 0.02 {
		t.Fatalf("starts at %.3f, expected %.3f", got, tl.Segments[last-1].Start)
	}
	if s.Stats().Restarts < 1 {
		t.Error("no restart – the test then checks nothing across runs")
	}
}
