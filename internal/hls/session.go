// Package hls uses ffmpeg to turn an MKV into HLS segments with fixed
// boundaries known in advance (see package mkv).
//
// The proxy – not ffmpeg – decides which segments exist:
//   - the playlist is VOD and contains the whole movie from the start,
//   - ffmpeg is paused (SIGSTOP) when it gets too far ahead of the client,
//   - ffmpeg is restarted at segment k when the client jumps there,
//   - the proxy deletes old segments based on the client position.
//
// Each of these decisions ends up in the event log with its reason.
package hls

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"jellyfin-proxy/internal/events"
	"jellyfin-proxy/internal/mkv"
)

type Config struct {
	FFmpeg     string
	Dir        string
	MaxAhead   int           // how many segments ffmpeg may be ahead of the client
	KeepBehind int           // how many segments are kept behind the client
	SeekGap    int           // if the client jumps further ahead than this: restart instead of waiting
	IdleStop   time.Duration // stop ffmpeg when the client fetches nothing for this long
	InputArgs  []string      // extra input options (tests use them to throttle)
	Format     string        // "fmp4" (default) or "ts"
	Events     *events.Log
}

// Audio describes the audio track. Index is the global stream index in the
// source (MediaStream.Index in Jellyfin), -1 = first audio track.
type Audio struct {
	Index    int
	Copy     bool // track is already client-compatible and is only copied
	Codec    string
	Bitrate  string
	Channels int
	Label    string
}

var ErrClosed = errors.New("session closed")

type Session struct {
	cfg        Config
	ID         string
	Label      string
	tl         *mkv.Timeline
	input      string
	audio      Audio
	videoCodec string
	dir        string
	created    time.Time
	stopRuns   sync.WaitGroup

	mu          sync.Mutex
	cond        *sync.Cond
	ready       map[int]int64 // segment -> size of the media data
	mediaOff    map[int]int64 // fMP4: start of the media data in the file
	init        []byte        // fMP4: shared init section
	run         *run
	head        *run // short extra run for the movie start when resuming
	runSeq      int
	lastReq     int
	lastReqAt   time.Time
	closed      bool
	failed      error
	failures    int // consecutive runs without a single segment
	restarts    int
	deleted     int
	served      int
	servedB     int64
	slowSends   int
	staleWarned bool
	recent      []Request
}

type run struct {
	id      int
	start   int
	next    int
	cmd     *exec.Cmd
	reason  string
	began   time.Time
	paused  bool
	pauses  int
	exited  bool
	err     error
	stderr  []string
	listOff int64
	list    string
	dir     string // own directory: finished segments are renamed out of here
}

type Request struct {
	At         time.Time `json:"at"`
	Segment    int       `json:"segment"`
	WaitMs     int64     `json:"wait_ms"`
	Bytes      int64     `json:"bytes"`
	Outcome    string    `json:"outcome"`
	TransferMs int64     `json:"transfer_ms"` // transfer proxy -> client
	Mbps       float64   `json:"mbps"`        // achieved rate to the client
	NeedMbps   float64   `json:"need_mbps"`   // bitrate of this segment
}

// Delivered records how long transferring segment k to the client took.
// If it takes longer than the segment itself, the client cannot keep up: the
// bottleneck is then between proxy and client (e.g. Wi-Fi), not the server.
func (s *Session) Delivered(k int, bytes int64, d time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	seg := s.tl.Segments[k]
	for i := len(s.recent) - 1; i >= 0; i-- {
		r := &s.recent[i]
		if r.Segment != k || r.TransferMs != 0 {
			continue
		}
		r.TransferMs = max(d.Milliseconds(), 1)
		r.Mbps = float64(bytes) * 8 / max(d.Seconds(), 0.001) / 1e6
		r.NeedMbps = float64(bytes) * 8 / seg.Duration / 1e6
		break
	}
	if d.Seconds() > seg.Duration*0.8 {
		s.slowSends++
		s.cfg.Events.Emit(s.Label, "client_slow", events.Warn,
			fmt.Sprintf("Segment %d (%.0f MB, %.1f s of movie) took %.1f s to reach the client = %.0f Mbit/s, needed %.0f Mbit/s – bottleneck between proxy and client (Wi-Fi?)",
				k, float64(bytes)/1e6, seg.Duration, d.Seconds(),
				float64(bytes)*8/d.Seconds()/1e6, float64(bytes)*8/seg.Duration/1e6))
	}
}

func New(cfg Config, id, label string, tl *mkv.Timeline, input string, audio Audio, videoCodec string) (*Session, error) {
	dir, err := os.MkdirTemp(cfg.Dir, "hls-"+label+"-")
	if err != nil {
		return nil, err
	}
	s := &Session{
		cfg: cfg, ID: id, Label: label, tl: tl, input: input, audio: audio, dir: dir, videoCodec: videoCodec,
		created: time.Now(), ready: map[int]int64{}, mediaOff: map[int]int64{}, lastReq: -1, lastReqAt: time.Now(),
	}
	s.cond = sync.NewCond(&s.mu)
	go s.maintain()
	return s, nil
}

func (s *Session) Audio() Audio { return s.audio }

// Prepare starts ffmpeg ahead of time at the position where the client will begin.
func (s *Session) Prepare(seg int, reason string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.run == nil && !s.closed {
		s.startLocked(seg, reason)
		if seg >= headSegs {
			s.startHeadLocked()
		}
	}
}

// Playlist returns the complete VOD playlist. segURL builds the URL per segment.
// initURL is only needed for fMP4 (EXT-X-MAP).
func (s *Session) Playlist(segURL func(int) string, initURL string) []byte {
	target := 0.0
	for _, seg := range s.tl.Segments {
		target = max(target, seg.Duration)
	}
	version := 3
	if s.FMP4() {
		version = 7 // EXT-X-MAP for fMP4
	}
	var b strings.Builder
	fmt.Fprintf(&b, "#EXTM3U\n#EXT-X-VERSION:%d\n#EXT-X-PLAYLIST-TYPE:VOD\n", version)
	fmt.Fprintf(&b, "#EXT-X-TARGETDURATION:%d\n#EXT-X-MEDIA-SEQUENCE:0\n#EXT-X-INDEPENDENT-SEGMENTS\n", int(target+0.999))
	if s.FMP4() {
		fmt.Fprintf(&b, "#EXT-X-MAP:URI=\"%s\"\n", initURL)
	}
	for _, seg := range s.tl.Segments {
		fmt.Fprintf(&b, "#EXTINF:%.6f,\n%s\n", seg.Duration, segURL(seg.Index))
	}
	b.WriteString("#EXT-X-ENDLIST\n")
	return []byte(b.String())
}

// FMP4 reports whether the session produces fMP4 segments (otherwise MPEG-TS).
func (s *Session) FMP4() bool { return s.cfg.Format != "ts" }

// File describes a finished segment on disk. With fMP4 the media data is
// preceded by the header (ftyp+moov), which only the init section needs.
type File struct {
	Path string
	Off  int64
	Size int64
}

// Init returns the shared fMP4 init section (ftyp+moov). It comes from the
// first finished segment and is valid for all runs: apart from the total
// duration (mvhd), ffmpeg writes it identically for every segment.
func (s *Session) Init(ctx context.Context) ([]byte, error) {
	stop := context.AfterFunc(ctx, func() {
		s.mu.Lock()
		s.cond.Broadcast()
		s.mu.Unlock()
	})
	defer stop()
	s.mu.Lock()
	defer s.mu.Unlock()
	for s.init == nil {
		switch {
		case s.closed:
			return nil, ErrClosed
		case s.failed != nil:
			return nil, s.failed
		case ctx.Err() != nil:
			return nil, ctx.Err()
		}
		if s.run == nil || s.run.exited {
			s.startLocked(max(s.lastReq, 0), "init section needed")
		}
		s.cond.Wait()
	}
	return s.init, nil
}

// SegmentStart returns the movie time at which segment k begins.
func (s *Session) SegmentStart(k int) float64 { return s.tl.Segments[k].Start }

// Resolve checks whether segment k really begins at movie time t. If the client
// still has a playlist with a different segment length (e.g. after the proxy
// was restarted with other flags), the number points to the wrong position;
// then the time wins, and the segment containing t is served.
func (s *Session) Resolve(k int, t float64) (int, bool) {
	segs := s.tl.Segments
	if t < 0 || (k >= 0 && k < len(segs) && math.Abs(segs[k].Start-t) < 0.01) {
		return k, false
	}
	n := s.tl.SegmentAt(t)
	s.mu.Lock()
	first := !s.staleWarned
	s.staleWarned = true
	s.mu.Unlock()
	if first {
		s.cfg.Events.Emit(s.Label, "stale_playlist", events.Warn,
			fmt.Sprintf("Client is using a stale playlist (segment %d at %s) – serving segment %d at the correct movie position. Restart playback for a clean timeline.",
				k, clock(t), n))
	}
	return n, true
}

// Segment returns the path of segment k as soon as it is finished.
func (s *Session) Segment(ctx context.Context, k int) (File, error) {
	if k < 0 || k >= len(s.tl.Segments) {
		return File{}, fmt.Errorf("segment %d does not exist", k)
	}
	stop := context.AfterFunc(ctx, func() {
		s.mu.Lock()
		s.cond.Broadcast()
		s.mu.Unlock()
	})
	defer stop()

	t0 := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastReq, s.lastReqAt = k, time.Now()
	why := "" // why the client has to wait, for the event log
	headBusy := k < headSegs && s.head != nil && !s.head.exited
	if _, ok := s.ready[k]; !ok && !s.closed && s.failed == nil && !headBusy {
		if reason := s.needRestartLocked(k); reason != "" {
			s.startLocked(k, reason)
			why = "ffmpeg had to restart (" + reason + ")"
		}
	}
	s.housekeepLocked()
	for {
		switch {
		case s.closed:
			s.noteLocked(k, t0, 0, "session closed")
			return File{}, ErrClosed
		case s.failed != nil:
			s.noteLocked(k, t0, 0, "error")
			return File{}, s.failed
		case ctx.Err() != nil:
			s.noteLocked(k, t0, 0, "Client gave up")
			return File{}, ctx.Err()
		}
		if size, ok := s.ready[k]; ok {
			s.served++
			s.servedB += size
			s.noteLocked(k, t0, size, "ok")
			if w := time.Since(t0); w > time.Second {
				s.cfg.Events.Emit(s.Label, "client_wait", events.Warn,
					fmt.Sprintf("Client had to wait %.1f s for segment %d (%s): %s", w.Seconds(), k, clock(s.tl.Segments[k].Start), why))
			}
			return File{Path: s.segPath(k), Off: s.mediaOff[k], Size: size}, nil
		}
		if k < headSegs && s.head != nil && !s.head.exited {
			why = "movie start is being produced in parallel"
		} else if reason := s.needRestartLocked(k); reason != "" {
			s.startLocked(k, reason)
			why = "ffmpeg had to restart (" + reason + ")"
		} else if why == "" && s.run != nil {
			why = fmt.Sprintf("ffmpeg was only at segment %d – probably waiting for data from the server", s.run.next)
		}
		s.cond.Wait()
	}
}

// needRestartLocked decides whether we can wait for the running ffmpeg or
// whether it must be restarted at k – and why.
func (s *Session) needRestartLocked(k int) string {
	r := s.run
	switch {
	case r == nil:
		return fmt.Sprintf("Client requests segment %d, ffmpeg is not running", k)
	case r.exited && k >= r.next:
		if r.err != nil {
			return fmt.Sprintf("ffmpeg crashed (%v), client is waiting for segment %d", r.err, k)
		}
		return fmt.Sprintf("ffmpeg had finished, segment %d is needed again", k)
	case k < r.start:
		return fmt.Sprintf("Jump back to %s (segment %d), ffmpeg only started at segment %d",
			clock(s.tl.Segments[k].Start), k, r.start)
	case k < r.next:
		return fmt.Sprintf("Jump back to segment %d: it was already produced but has been deleted", k)
	case k > r.next+s.cfg.SeekGap:
		return fmt.Sprintf("Jump forward to %s: ffmpeg is only at segment %d, client wants %d",
			clock(s.tl.Segments[k].Start), r.next, k)
	}
	return ""
}

func (s *Session) noteLocked(k int, t0 time.Time, size int64, outcome string) {
	s.recent = append(s.recent, Request{At: t0, Segment: k, WaitMs: time.Since(t0).Milliseconds(), Bytes: size, Outcome: outcome})
	if len(s.recent) > 40 {
		s.recent = s.recent[len(s.recent)-40:]
	}
}

func (s *Session) ext() string {
	if s.FMP4() {
		return "mp4"
	}
	return "ts"
}

func (s *Session) segPath(k int) string {
	return filepath.Join(s.dir, fmt.Sprintf("seg_%05d.%s", k, s.ext()))
}

// ---------------------------------------------------------------- ffmpeg

func (s *Session) startLocked(k int, reason string) {
	if s.run != nil {
		s.killLocked(s.run)
		s.restarts++
	}
	r := s.launchLocked(k, len(s.tl.Segments), reason)
	s.run = r
	if r.exited {
		s.failures++
		s.checkFailuresLocked(r.err)
		return
	}
	s.cfg.Events.Emit(s.Label, "ffmpeg_start", events.Info,
		fmt.Sprintf("ffmpeg starting at segment %d (%s): %s", k, clock(s.tl.Segments[k].Start), reason),
		"run", r.id, "segment", k)
}

// headSegs: how many segments from the movie start AVPlay fetches before it
// jumps to the resume position.
const headSegs = 3

// startHeadLocked produces the first segments in a second, short ffmpeg run
// when resuming. Without it, the client's request for segment 0 would kill the
// prepared run at the resume position – and a second later the client would need
// it after all (measured: 9 s of waiting).
func (s *Session) startHeadLocked() {
	if s.head != nil || len(s.tl.Segments) <= headSegs {
		return
	}
	s.head = s.launchLocked(0, headSegs, "movie start for the client's entry")
}

// launchLocked starts ffmpeg for segments [k, end) and watches it.
func (s *Session) launchLocked(k, end int, reason string) *run {
	s.runSeq++
	r := &run{id: s.runSeq, start: k, next: k, reason: reason, began: time.Now(),
		dir: filepath.Join(s.dir, fmt.Sprintf("run%d", s.runSeq))}
	r.list = filepath.Join(r.dir, "list.csv")
	os.MkdirAll(r.dir, 0o755)

	segs := s.tl.Segments
	var cuts []string
	for _, seg := range segs[k+1 : end] {
		cuts = append(cuts, strconv.FormatFloat(seg.Start-segs[k].Start, 'f', 3, 64))
	}
	args := []string{"-hide_banner", "-nostdin", "-loglevel", "warning", "-y"}
	if k > 0 {
		args = append(args, "-ss", strconv.FormatFloat(segs[k].Start, 'f', 3, 64))
	}
	args = append(args, s.cfg.InputArgs...)
	args = append(args, "-i", s.input, "-map", "0:v:0")
	if s.audio.Index >= 0 {
		args = append(args, "-map", fmt.Sprintf("0:%d", s.audio.Index))
	} else {
		args = append(args, "-map", "0:a:0?")
	}
	args = append(args, "-c:v", "copy")
	if s.audio.Copy {
		args = append(args, "-c:a", "copy")
	} else {
		args = append(args, "-c:a", s.audio.Codec, "-b:a", s.audio.Bitrate, "-ac", strconv.Itoa(s.audio.Channels))
	}
	// -copyts: timestamps stay absolute so that segments from different runs
	// fit together seamlessly. segment_times are relative to the start.
	if end < len(segs) {
		args = append(args, "-t", strconv.FormatFloat(segs[end].Start-segs[k].Start, 'f', 3, 64))
	}
	args = append(args, "-copyts")
	if s.FMP4() {
		// One fragment per keyframe, no edit list: this way every segment's
		// header is identical and works as the shared init section; tfdt stays
		// absolute. The fixed offset keeps the AC3 pre-roll above zero.
		args = append(args, "-avoid_negative_ts", "disabled", "-output_ts_offset", "2",
			"-f", "segment", "-segment_format", "mp4", "-segment_format_options",
			"movflags=+frag_keyframe+empty_moov+default_base_moof+frag_discont+delay_moov+negative_cts_offsets:use_editlist=0")
		if s.videoCodec == "hevc" {
			args = append(args, "-tag:v", "hvc1") // HLS players expect this
		}
	} else {
		args = append(args, "-f", "segment", "-segment_format", "mpegts")
	}
	args = append(args, "-segment_time_delta", "0.05", "-segment_start_number", strconv.Itoa(k),
		"-segment_list", r.list, "-segment_list_type", "csv")
	if len(cuts) > 0 {
		args = append(args, "-segment_times", strings.Join(cuts, ","))
	}
	// Each run writes into its own directory. If it wrote seg_N.ts directly,
	// after a backward jump it would overwrite segments the client may be
	// downloading right now.
	args = append(args, filepath.Join(r.dir, "seg_%05d."+s.ext()))

	r.cmd = exec.Command(s.cfg.FFmpeg, args...)
	stderr, _ := r.cmd.StderrPipe()
	if err := r.cmd.Start(); err != nil {
		r.exited, r.err = true, err
		return r
	}
	s.stopRuns.Add(2)
	go s.readStderr(r, stderr)
	go s.watch(r)
	return r
}

func (s *Session) killLocked(r *run) {
	if r.exited || r.cmd == nil || r.cmd.Process == nil {
		return
	}
	if r.paused {
		r.cmd.Process.Signal(syscall.SIGCONT)
	}
	r.cmd.Process.Kill()
}

func (s *Session) readStderr(r *run, pipe interface{ Read([]byte) (int, error) }) {
	defer s.stopRuns.Done()
	sc := bufio.NewScanner(pipe)
	for sc.Scan() {
		s.mu.Lock()
		r.stderr = append(r.stderr, sc.Text())
		if len(r.stderr) > 30 {
			r.stderr = r.stderr[len(r.stderr)-30:]
		}
		s.mu.Unlock()
	}
}

// watch follows the segment list and detects when the process exits.
func (s *Session) watch(r *run) {
	defer s.stopRuns.Done()
	done := make(chan error, 1)
	go func() { done <- r.cmd.Wait() }()
	t := time.NewTicker(150 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case err := <-done:
			s.mu.Lock()
			s.pollListLocked(r)
			r.exited, r.err = true, err
			s.onExitLocked(r)
			s.cond.Broadcast()
			s.mu.Unlock()
			os.RemoveAll(r.dir)
			return
		case <-t.C:
			s.mu.Lock()
			if s.pollListLocked(r) {
				s.housekeepLocked()
				s.cond.Broadcast()
			}
			s.mu.Unlock()
		}
	}
}

// pollListLocked reads new lines of the csv segment list. A line only
// appears once the segment has been completely written.
func (s *Session) pollListLocked(r *run) bool {
	f, err := os.Open(r.list)
	if err != nil {
		return false
	}
	defer f.Close()
	f.Seek(r.listOff, 0)
	rd := bufio.NewReader(f)
	got := false
	for {
		line, err := rd.ReadString('\n')
		if err != nil {
			break // incomplete line: next time
		}
		r.listOff += int64(len(line))
		name := strings.SplitN(line, ",", 2)[0]
		var k int
		if _, err := fmt.Sscanf(filepath.Base(name), "seg_%05d.", &k); err != nil {
			continue
		}
		src := filepath.Join(r.dir, filepath.Base(name))
		if _, ok := s.ready[k]; ok {
			os.Remove(src) // already present and cut identically
		} else if st, err := os.Stat(src); err == nil {
			var off int64
			if s.FMP4() {
				init, err := readInit(src)
				if err != nil {
					s.cfg.Events.Emit(s.Label, "fmp4_error", events.Error,
						fmt.Sprintf("Segment %d has no readable fMP4 header: %v", k, err))
					continue
				}
				off = int64(len(init))
				if s.init == nil {
					s.init = init
				}
			}
			if os.Rename(src, s.segPath(k)) == nil {
				s.ready[k] = st.Size() - off
				s.mediaOff[k] = off
			}
		}
		r.next = k + 1
		got = true
		s.failures = 0
	}
	return got
}

func (s *Session) onExitLocked(r *run) {
	if s.run != r {
		return // replaced on purpose
	}
	switch {
	case s.closed:
	case r.err == nil:
		s.cfg.Events.Emit(s.Label, "ffmpeg_done", events.Info,
			fmt.Sprintf("ffmpeg reached the end of the movie (segments %d–%d)", r.start, r.next-1))
	default:
		if r.next == r.start {
			s.failures++
		}
		tail := strings.Join(lastN(r.stderr, 4), " | ")
		s.cfg.Events.Emit(s.Label, "ffmpeg_crash", events.Error,
			fmt.Sprintf("ffmpeg (run %d) exited with %v after segment %d: %s", r.id, r.err, r.next-1, tail))
		s.checkFailuresLocked(r.err)
	}
}

func (s *Session) checkFailuresLocked(err error) {
	if s.failures >= 3 {
		s.failed = fmt.Errorf("ffmpeg failed %d times in a row without producing a segment: %v", s.failures, err)
		s.cfg.Events.Emit(s.Label, "session_failed", events.Error, s.failed.Error())
	}
}

// ---------------------------------------------------------------- control

// housekeepLocked pauses/resumes ffmpeg based on its distance to the client and
// deletes segments the client no longer needs.
func (s *Session) housekeepLocked() {
	r := s.run
	tv := s.lastReq
	if r != nil && !r.exited && r.cmd.Process != nil {
		ahead := r.next - max(tv, r.start)
		switch {
		case !r.paused && ahead > s.cfg.MaxAhead:
			r.cmd.Process.Signal(syscall.SIGSTOP)
			r.paused = true
			r.pauses++
			// In normal operation pausing/resuming just follows the client's pace.
			// Only the first time per run is worth an event.
			if r.pauses == 1 {
				s.cfg.Events.Emit(s.Label, "ffmpeg_pause", events.Info,
					fmt.Sprintf("Buffer full (%d segments, %s ahead of the client): ffmpeg now pauses in step with the client",
						ahead, clock(s.span(tv+1, r.next))))
			}
		case r.paused && ahead <= s.cfg.MaxAhead/2:
			r.cmd.Process.Signal(syscall.SIGCONT)
			r.paused = false
		}
	}
	if tv < 0 {
		return
	}
	far := 0
	for k := range s.ready {
		behind := k < tv-s.cfg.KeepBehind
		stale := k > tv+s.cfg.MaxAhead+s.cfg.SeekGap && (r == nil || k < r.start || k >= r.next)
		if !behind && !stale {
			continue
		}
		os.Remove(s.segPath(k))
		delete(s.ready, k)
		if behind {
			s.deleted++
		} else {
			far++
		}
	}
	if far > 0 {
		s.cfg.Events.Emit(s.Label, "segments_drop", events.Info,
			fmt.Sprintf("Deleted %d segments: after a backward jump they were far ahead of the client", far))
	}
}

func (s *Session) span(from, to int) float64 {
	var d float64
	for k := max(from, 0); k < to && k < len(s.tl.Segments); k++ {
		d += s.tl.Segments[k].Duration
	}
	return d
}

func (s *Session) maintain() {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	for range t.C {
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			return
		}
		if r := s.run; r != nil && !r.exited && time.Since(s.lastReqAt) > s.cfg.IdleStop {
			s.cfg.Events.Emit(s.Label, "ffmpeg_idle", events.Info,
				fmt.Sprintf("Client has not fetched any segments for %s: ffmpeg stopped (segments and cache are kept)",
					time.Since(s.lastReqAt).Round(time.Second)))
			s.killLocked(r)
			s.run = nil
		}
		s.mu.Unlock()
	}
}

// IdleFor returns how long the client has not fetched anything.
func (s *Session) IdleFor() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	return time.Since(s.lastReqAt)
}

// Close stops ffmpeg and deletes all segments.
func (s *Session) Close(reason string) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	if s.run != nil {
		s.killLocked(s.run)
	}
	if s.head != nil {
		s.killLocked(s.head)
	}
	n := len(s.ready)
	s.cond.Broadcast()
	s.mu.Unlock()
	s.stopRuns.Wait()
	os.RemoveAll(s.dir)
	s.cfg.Events.Emit(s.Label, "session_close", events.Info,
		fmt.Sprintf("Playback session closed (%d segments deleted): %s", n, reason), "reason", reason)
}

func clock(sec float64) string {
	t := int(sec)
	return fmt.Sprintf("%d:%02d:%02d", t/3600, t/60%60, t%60)
}

func lastN(xs []string, n int) []string {
	if len(xs) > n {
		return xs[len(xs)-n:]
	}
	return xs
}

// readInit reads ftyp+moov from the start of an fMP4 file.
func readInit(path string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var off int64
	hdr := make([]byte, 8)
	for i := 0; i < 16; i++ {
		if _, err := f.ReadAt(hdr, off); err != nil {
			return nil, err
		}
		size := int64(binary.BigEndian.Uint32(hdr[:4]))
		if size < 8 {
			return nil, fmt.Errorf("invalid box at %d", off)
		}
		off += size
		if string(hdr[4:8]) == "moov" {
			init := make([]byte, off)
			_, err := f.ReadAt(init, 0)
			return init, err
		}
	}
	return nil, errors.New("no moov at start of file")
}
