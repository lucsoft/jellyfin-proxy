// jellyfin-proxy — a local transcoding proxy for Jellyfin.
//
// Idea:
//
//	TV (Moonfin/Jellyfin-Client)
//	     │  all API calls
//	     ▼
//	this proxy ────────────────► Jellyfin server (e.g. at a friend's)
//	     ▲                              │
//	     │  /Items/{id}/PlaybackInfo    │  passed through 1:1,
//	     │  gets REWRITTEN              │  but MediaSources modified
//	     │                              ▼
//	     │  /proxy/stream/{id}
//	     │  └─ ffmpeg (VideoToolbox) reads the direct-play stream
//	     │     from the real server and delivers HLS to the TV
//	     ▼
//	TV plays
//
// So the proxy does video+audio encoding locally on the Mac.
// As a result the real server never transcodes.
package main

import (
	"bufio"
	"compress/gzip"
	"context"
	"crypto/tls"
	_ "embed"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// ---------------------------------------------------------------- Configuration

type Config struct {
	Upstream       *url.URL // real Jellyfin server
	Listen         string   // e.g. :8099
	PublicURL      string   // how the TV reaches the proxy, e.g. http://192.168.1.20:8099
	FFmpeg         string
	VideoCodec     string
	VideoBitrate   string
	AudioCodec     string
	AudioBitrate   string
	AudioChannels  int
	HWAccel        bool
	MaxHeight      int
	TranscodeDir   string
	Verbose        bool
	Insecure       bool
	ServerName     string // overrides ServerName in /System/Info*
	Realtime       bool   // optimize VideoToolbox for speed
	ToneMap        bool   // HDR -> SDR tone mapping
	CopyVideo      bool   // pass video through unchanged (transcode audio only)
	UsePlacebo     bool   // libplacebo (GPU) instead of CPU tone mapping
	TraceFile      string // JSONL trace for timing
	PreloadMB      int    // head preload in MB: this much is loaded BEFORE ffmpeg starts
	WindowMB       int    // read-ahead window: how far the preload may run ahead of the reader
	PreloadWorkers int    // parallel range downloads
}

var cfg Config

// ---------------------------------------------------------------- Proxy setup

// ---------------------------------------------------------------- Preload

// preloader downloads the source into a local file using N parallel range
// requests. A single HTTP connection does not get past TCP slow start at
// ~18 Mbit/s; 16 parallel connections reach ~83 Mbit/s.
type preloader struct {
	sourceID string
	baseURL  string
	path     string
	total    int64
	file     *os.File

	mu        sync.Mutex
	cond      *sync.Cond
	loaded    map[int64]bool // finished chunks (offset)
	busy      map[int64]bool // in progress
	failed    error
	closed    bool
	urgent    chan int64
	nextSeq   int64
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	lastTouch time.Time
	timerOnce sync.Once

	// window: only load up to readPos + window (prevents loading everything)
	readPos int64
	window  int64
	ctx     context.Context

	// worker tracking: one span per connection (start, end, bytes)
	workers []workerSpan
	// ring buffer of recent requests
	events []preloadEvent
}

type workerSpan struct {
	ID     int
	Offset int64 // MB
	Start  time.Time
	End    time.Time
	Bytes  int64
	Mbps   float64
	Active bool
	Err    string
}

type preloadEvent struct {
	At     time.Time
	Kind   string // chunk_ok | chunk_err | seek | head | close
	Detail string
	Ms     int64
}

// advance sets the read position (ffmpeg has read up to here).
func (p *preloader) advance(pos int64) {
	p.mu.Lock()
	// K1: on a seek (MKV cues are often at the end of the file!) readPos must
	// not jump to the maximum, otherwise the loader downloads the whole movie.
	if pos < p.readPos-8*preloadChunk || pos > p.readPos+8*preloadChunk {
		// seek detected: reset the window
		p.readPos = pos
		p.nextSeq = (pos / preloadChunk) * preloadChunk
	} else if pos > p.readPos {
		p.readPos = pos
	}
	p.lastTouch = time.Now()
	p.mu.Unlock()
}

// blocked reader: returns bytes at offset, waits until the chunk is loaded.
// This replaces "ffmpeg reads a file with zero-filled gaps".
func (p *preloader) readAt(pb []byte, off int64) (int, error) {
	if off >= p.total {
		return 0, io.EOF
	}
	want := int64(len(pb))
	if off+want > p.total {
		want = p.total - off
	}
	read := int64(0)
	waitID := newSpan(0, "preload.wait", map[string]string{"off_mb": fmt.Sprintf("%.0f", float64(off)/1e6)})
	var waited time.Duration
	for read < want {
		cur := off + read
		chunkStart := (cur / preloadChunk) * preloadChunk
		// wait for this chunk
		var ferr error
		for {
			p.mu.Lock()
			ready := p.loaded[chunkStart]
			ferr = p.failed
			postMortem := p.closed
			p.mu.Unlock()
			if ready || ferr != nil || postMortem {
				break
			}
			// request the chunk and wait
			select {
			case p.urgent <- chunkStart:
			default:
			}
			time.Sleep(20 * time.Millisecond)
		}
		if ferr != nil {
			endSpan(waitID, "err", map[string]string{"err": ferr.Error()})
			return int(read), ferr
		}
		// read from the chunk
		n := chunkStart + preloadChunk - cur
		if n > want-read {
			n = want - read
		}
		m, err := p.file.ReadAt(pb[read:read+n], cur)
		read += int64(m)
		if err != nil && err != io.EOF {
			return int(read), err
		}
		if m == 0 {
			return int(read), io.ErrUnexpectedEOF
		}
		p.advance(off + read)
	}
	waited = time.Since(spans[0].Start) // placeholder, corrected below
	_ = waited
	endSpan(waitID, "ok", nil)
	if read >= want {
		return int(read), nil
	}
	return int(read), io.EOF
}

const maxEvents = 40

func (p *preloader) pushEventLocked(e preloadEvent) {
	p.events = append(p.events, e)
	if len(p.events) > maxEvents {
		p.events = p.events[len(p.events)-maxEvents:]
	}
}

// Stats returns a snapshot for the dashboard.
func (p *preloader) stats() map[string]interface{} {
	p.mu.Lock()
	defer p.mu.Unlock()
	loadedBytes := int64(0)
	for range p.loaded {
		loadedBytes += preloadChunk
	}
	ws := make([]map[string]interface{}, len(p.workers))
	for i, w := range p.workers {
		if w.Start.IsZero() {
			continue
		}
		dur := int64(0)
		if w.Active {
			dur = time.Since(w.Start).Milliseconds()
		} else {
			dur = w.End.Sub(w.Start).Milliseconds()
		}
		ws[i] = map[string]interface{}{
			"id": w.ID, "offset_mb": w.Offset, "ms": dur,
			"mbps": w.Mbps, "active": w.Active, "err": w.Err,
			"bytes_mb": float64(w.Bytes) / 1e6,
		}
	}
	evs := make([]map[string]interface{}, len(p.events))
	for i, e := range p.events {
		evs[i] = map[string]interface{}{
			"at": e.At.Format("15:04:05.000"), "kind": e.Kind,
			"detail": e.Detail, "ms": e.Ms,
		}
	}
	return map[string]interface{}{
		"workers":     ws,
		"events":      evs,
		"total_mb":    float64(p.total) / 1e6,
		"loaded_mb":   float64(loadedBytes) / 1e6,
		"read_pos_mb": float64(p.readPos) / 1e6,
		"ahead_mb":    float64(loadedBytes-p.readPos) / 1e6,
		"window_mb":   float64(p.window) / 1e6,
		"chunks":      len(p.loaded),
		"failed":      p.failed != nil,
	}
}

// touch updates the timestamp and starts the idle cleanup.
func (p *preloader) touch() {
	p.mu.Lock()
	p.lastTouch = time.Now()
	p.mu.Unlock()
	p.timerOnce.Do(func() { go p.idleWatch() })
}

// idleWatch cleans up the preload when nothing has been read for a while.
func (p *preloader) idleWatch() {
	const idle = 5 * time.Minute
	t := time.NewTicker(30 * time.Second)
	defer t.Stop()
	for range t.C {
		p.mu.Lock()
		closed := p.closed
		last := p.lastTouch
		p.mu.Unlock()
		if closed {
			return
		}
		if time.Since(last) > idle {
			log.Printf("[preload] %s: inactive for %.0f min - cleaning up", short(p.sourceID), idle.Minutes())
			p.close()
			jobsMu.Lock()
			if preloaders[p.sourceID] == p { // M1: do not remove the newer one
				delete(preloaders, p.sourceID)
			}
			jobsMu.Unlock()
			return
		}
	}
}

// HTTP/1.1 instead of HTTP/2: 16 separate TCP connections deliver
// ~104 Mbit/s, HTTP/2 with a single congestion window only ~32 Mbit/s.
var preloadClient *http.Client

// buildPreloadClient must run AFTER flag.Parse() (H5: cfg.Insecure).
func buildPreloadClient() {
	preloadClient = &http.Client{
		Timeout: 10 * time.Minute,
		Transport: &http.Transport{
			ForceAttemptHTTP2:   false, // HTTP/1.1: 3x faster than HTTP/2
			MaxIdleConnsPerHost: 32,
			TLSClientConfig:     &tls.Config{InsecureSkipVerify: cfg.Insecure},
			IdleConnTimeout:     90 * time.Second,
		},
	}
}

const (
	preloadChunk    = 32 << 20 // 32 MB per range
	preloadParallel = 16       // 16 connections -> ~83 Mbit/s instead of 18
)

// newPreloader starts the background download. headMB is preloaded
// blocking so that ffmpeg starts with enough read-ahead.
func newPreloader(sourceID, baseURL string, headMB int) (*preloader, error) {
	req, _ := http.NewRequest("GET", baseURL, nil)
	req.Header.Set("Range", "bytes=0-0")
	resp, err := preloadClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("head: %w", err)
	}
	total := resp.ContentLength
	if cr := resp.Header.Get("Content-Range"); cr != "" {
		if i := strings.LastIndex(cr, "/"); i >= 0 {
			if n, e := strconv.ParseInt(cr[i+1:], 10, 64); e == nil {
				total = n
			}
		}
	}
	resp.Body.Close()
	if total <= 0 {
		return nil, fmt.Errorf("unknown size")
	}

	dir, err := os.MkdirTemp(cfg.TranscodeDir, "jfpreload-"+sanitize(sourceID)+"-")
	if err != nil {
		return nil, err
	}
	path := filepath.Join(dir, "preload.bin")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	// IMPORTANT: only allocate as much as needed. A truncate to the full
	// movie size (59 GB here) has eaten all free disk space before.
	// We grow dynamically via WriteAt.
	reserve := int64(headMB)<<20 + int64(preloadChunk)*4
	if reserve > total {
		reserve = total
	}
	if err := f.Truncate(reserve); err != nil {
		f.Close()
		return nil, err
	}

	ctx0, cancel := context.WithCancel(context.Background())
	p := &preloader{
		sourceID: sourceID,
		baseURL:  baseURL,
		path:     path,
		total:    total,
		file:     f,
		loaded:   map[int64]bool{},
		busy:     map[int64]bool{},
		workers:  make([]workerSpan, preloadParallel),
		urgent:   make(chan int64, 64),
		nextSeq:  0,
		cancel:   cancel,
		ctx:      ctx0,
	}
	p.cond = sync.NewCond(&p.mu)
	win := int64(cfg.WindowMB) << 20
	if win < int64(headMB)<<20 {
		win = int64(headMB) << 20
	}
	p.window = win

	// IMPORTANT (K3): workers start only AFTER the head preload further down.

	// load the head, blocking
	headBytes := int64(headMB) << 20
	if headBytes > total {
		headBytes = total
	}
	p.touch()
	t0 := time.Now()
	// load the head directly: all chunks in [0,headBytes) in parallel, without
	// going through the sequential loader (it would displace them).
	var hw sync.WaitGroup
	var herr error
	var hmu sync.Mutex
	headWorkers := preloadParallel
	sem := make(chan struct{}, headWorkers)
	for off := int64(0); off < headBytes; off += preloadChunk {
		o := off
		hw.Add(1)
		sem <- struct{}{}
		go func() {
			defer hw.Done()
			defer func() { <-sem }()
			p.mu.Lock()
			if p.busy[o] || p.loaded[o] {
				p.mu.Unlock()
				return
			}
			p.busy[o] = true
			p.mu.Unlock()
			end := o + preloadChunk - 1
			if end >= p.total {
				end = p.total - 1
			}
			tc := time.Now()
			req, _ := http.NewRequest("GET", p.baseURL, nil)
			req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", o, end))
			resp, err := preloadClient.Do(req)
			if err != nil {
				hmu.Lock()
				if herr == nil {
					herr = err
				}
				hmu.Unlock()
				return
			}
			defer resp.Body.Close()
			if resp.StatusCode != 206 {
				hmu.Lock()
				if herr == nil {
					herr = fmt.Errorf("http %d (expected 206)", resp.StatusCode)
				}
				hmu.Unlock()
				return
			}
			expect := end - o + 1
			n, err := io.Copy(io.NewOffsetWriter(p.file, o), io.LimitReader(resp.Body, expect))
			if err != nil {
				hmu.Lock()
				if herr == nil {
					herr = err
				}
				hmu.Unlock()
				return
			}
			if n != expect {
				hmu.Lock()
				if herr == nil {
					herr = fmt.Errorf("short body: %d of %d", n, expect)
				}
				hmu.Unlock()
				return
			}
			p.mu.Lock()
			p.loaded[o] = true
			delete(p.busy, o)
			p.mu.Unlock()
			trace(traceEvent{Event: "preload_chunk", ID: short8(p.sourceID), Bytes: n,
				Ms: time.Since(tc).Seconds() * 1000,
				Detail: fmt.Sprintf("head off=%.0f MB %.0f MB @ %.0f Mbit/s",
					float64(o)/1e6, float64(n)/1e6, float64(n)*8/time.Since(tc).Seconds()/1e6)})
		}()
	}
	hw.Wait()
	if herr != nil {
		p.close()
		return nil, herr
	}
	p.nextSeq = headBytes

	// only now start the sequential background loader.
	for i := 0; i < preloadParallel; i++ {
		p.wg.Add(1)
		go p.worker(i)
	}
	el := time.Since(t0)
	mbps := float64(headBytes) * 8 / el.Seconds() / 1e6
	trace(traceEvent{Event: "preload_head", ID: short8(sourceID), Bytes: headBytes,
		Ms: el.Seconds() * 1000,
		Detail: fmt.Sprintf("%.0f Mbit/s, %.0f MB before ffmpeg start (%d parallel)",
			mbps, float64(headBytes)/1e6, preloadParallel)})
	log.Printf("[preload] %s: %.0f MB in %.1fs (%.0f Mbit/s) - ffmpeg can start",
		short(sourceID), float64(headBytes)/1e6, el.Seconds(), mbps)
	return p, nil
}

// requestRange schedules all chunks in [start,end) (high priority).
func (p *preloader) requestRange(start, end int64) {
	off := (start / preloadChunk) * preloadChunk
	for off < end {
		select {
		case p.urgent <- off:
		default:
			// channel full: the sequential loader fetches it later
		}
		off += preloadChunk
	}
}

func (p *preloader) waitForRegion(start, end int64, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		p.mu.Lock()
		ok := true
		for off := (start / preloadChunk) * preloadChunk; off < end; off += preloadChunk {
			if !p.loaded[off] {
				ok = false
				break
			}
		}
		ferr := p.failed
		p.mu.Unlock()
		if ok {
			return nil
		}
		if ferr != nil {
			return ferr
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("preload timeout")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (p *preloader) worker(id int) {
	defer p.wg.Done()
	ctx := p.ctx
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		// priority: urgent requests first
		var off int64 = -1
		select {
		case o := <-p.urgent:
			off = o
		default:
		}

		if off < 0 {
			p.mu.Lock()
			if p.closed {
				p.mu.Unlock()
				return
			}
			// point 3: window. Never load more than readPos+window ahead,
			// otherwise the whole movie ends up on disk.
			limit := p.readPos + p.window
			if p.closed || p.failed != nil || p.nextSeq >= p.total || p.nextSeq > limit {
				p.mu.Unlock()
				time.Sleep(200 * time.Millisecond)
				continue
			}
			off = p.nextSeq
			p.nextSeq += preloadChunk
			if p.busy[off] || p.loaded[off] {
				p.mu.Unlock()
				continue
			}
			p.busy[off] = true
			p.mu.Unlock()
		} else {
			p.mu.Lock()
			if p.loaded[off] || p.busy[off] {
				p.mu.Unlock()
				continue
			}
			p.busy[off] = true
			p.mu.Unlock()
		}

		p.fetchChunk(ctx, off, id)
	}
}

func (p *preloader) fetchChunk(ctx context.Context, off int64, wid int) {
	end := off + preloadChunk - 1
	if end >= p.total {
		end = p.total - 1
	}
	expect := end - off + 1

	// point 8: retry with backoff instead of a global fail
	p.mu.Lock()
	if wid < len(p.workers) {
		p.workers[wid] = workerSpan{ID: wid, Offset: off / 1e6, Start: time.Now(), Active: true}
	}
	p.mu.Unlock()

	var lastErr error
	var n int64
	var t0 time.Time
	succeeded := false
	for attempt := 0; attempt < 3 && !succeeded; attempt++ {
		if attempt > 0 {
			time.Sleep(time.Duration(attempt) * 500 * time.Millisecond)
		}
		t0 = time.Now()
		req, _ := http.NewRequestWithContext(ctx, "GET", p.baseURL, nil)
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", off, end))
		resp, err := preloadClient.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		// point 7: accept ONLY 206. A 200 would write the start of the file
		// at offset off -> corrupt video.
		if resp.StatusCode != 206 {
			resp.Body.Close()
			lastErr = fmt.Errorf("http %d (expected 206)", resp.StatusCode)
			continue
		}
		n, err = io.Copy(io.NewOffsetWriter(p.file, off), io.LimitReader(resp.Body, expect))
		resp.Body.Close()
		if err != nil {
			lastErr = err
			continue
		}
		if n != expect {
			lastErr = fmt.Errorf("short body: %d of %d", n, expect)
			continue
		}
		succeeded = true
	}
	if !succeeded {
		p.mu.Lock()
		delete(p.busy, off) // point 8: reset busy
		if wid < len(p.workers) {
			p.workers[wid] = workerSpan{ID: wid, Offset: off / 1e6, Start: t0,
				End: time.Now(), Active: false, Err: fmt.Sprint(lastErr)}
		}
		p.pushEventLocked(preloadEvent{At: time.Now(), Kind: "chunk_err",
			Detail: fmt.Sprintf("w%d off=%.0f MB: %v", wid, float64(off)/1e6, lastErr)})
		p.mu.Unlock()
		p.fail(lastErr)
		return
	}
	p.mu.Lock()
	p.loaded[off] = true
	delete(p.busy, off)
	p.lastTouch = time.Now()
	if wid < len(p.workers) {
		el := time.Since(t0).Seconds()
		mbps := 0.0
		if el > 0 {
			mbps = float64(n) * 8 / el / 1e6
		}
		p.workers[wid] = workerSpan{ID: wid, Offset: off / 1e6, Start: t0,
			End: time.Now(), Bytes: n, Mbps: mbps, Active: false}
	}
	p.pushEventLocked(preloadEvent{At: time.Now(), Kind: "chunk_ok",
		Detail: fmt.Sprintf("w%d off=%.0f MB", wid, float64(off)/1e6),
		Ms:     int64(time.Since(t0).Milliseconds())})
	p.mu.Unlock()
	p.cond.Broadcast()
	trace(traceEvent{Event: "preload_chunk", ID: short8(p.sourceID), Bytes: n,
		Ms: time.Since(t0).Seconds() * 1000,
		Detail: fmt.Sprintf("off=%.0f MB %.0f MB @ %.0f Mbit/s",
			float64(off)/1e6, float64(n)/1e6, float64(n)*8/time.Since(t0).Seconds()/1e6)})
}

func (p *preloader) fail(err error) {
	p.mu.Lock()
	if p.failed == nil {
		p.failed = err
	}
	p.mu.Unlock()
	p.cond.Broadcast()
	trace(traceEvent{Event: "preload_err", ID: short8(p.sourceID), Detail: err.Error()})
}

func (p *preloader) close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	p.mu.Unlock()
	p.cancel()
	p.file.Close()
	p.wg.Wait() // K2: let the workers finish, then clean up
	// free disk space
	os.Remove(p.path)
	os.RemoveAll(filepath.Dir(p.path))
	trace(traceEvent{Event: "preload_closed", ID: short8(p.sourceID)})
}

// probeUpstream measures TTFB and throughput of the source stream.// probeUpstream measures TTFB and throughput of the source stream.// probeUpstream measures TTFB and throughput of the source stream.
func probeUpstream(sourceID, token string) {
	u := fmt.Sprintf("%s/Videos/%s/stream?static=true", cfg.Upstream.String(), url.PathEscape(sourceID))
	if token != "" {
		u += "&api_key=" + url.QueryEscape(token)
	}
	start := time.Now()
	req, err := http.NewRequest("GET", u, nil)
	if err != nil {
		return
	}
	req.Header.Set("Range", "bytes=0-8388607") // first 8 MB
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		trace(traceEvent{Event: "upstream_error", ID: short8(sourceID), Detail: err.Error()})
		return
	}
	defer resp.Body.Close()
	ttfb := time.Since(start)
	n, _ := io.Copy(io.Discard, resp.Body)
	total := time.Since(start)
	mbps := float64(n) * 8 / total.Seconds() / 1e6
	trace(traceEvent{
		Event:  "upstream_probe",
		ID:     short8(sourceID),
		Bytes:  n,
		Ms:     total.Seconds() * 1000,
		Detail: fmt.Sprintf("ttfb=%.0fms rate=%.1fMbit/s http=%d", ttfb.Seconds()*1000, mbps, resp.StatusCode),
	})
}

// ---------------------------------------------------------------- Trace

var (
	traceMu sync.Mutex
	traceFh *os.File
)

type traceEvent struct {
	TS     string  `json:"ts"`
	Event  string  `json:"event"`
	ID     string  `json:"id,omitempty"`
	Seg    string  `json:"seg,omitempty"`
	Bytes  int64   `json:"bytes,omitempty"`
	Ms     float64 `json:"ms,omitempty"`
	Detail string  `json:"detail,omitempty"`
}

func trace(ev traceEvent) {
	if traceFh == nil {
		return
	}
	ev.TS = time.Now().Format("15:04:05.000")
	traceMu.Lock()
	defer traceMu.Unlock()
	b, _ := json.Marshal(ev)
	traceFh.Write(append(b, '\n'))
	traceFh.Sync()
}

func short8(s string) string {
	if len(s) > 8 {
		return s[:8]
	}
	return s
}

// ---------------------------------------------------------------- Proxy setup

func newProxy() *httputil.ReverseProxy {
	rp := httputil.NewSingleHostReverseProxy(cfg.Upstream)

	origDirector := rp.Director
	rp.Director = func(r *http.Request) {
		origDirector(r)
		// set the Host header to upstream (otherwise problems with vhosts)
		r.Host = cfg.Upstream.Host
		// request PlaybackInfo and other JSON APIs uncompressed
		// so we can rewrite the response. Images/videos may
		// still be compressed.
		if strings.Contains(r.URL.Path, "PlaybackInfo") {
			r.Header.Del("Accept-Encoding")
		}
		// leave range requests and auth untouched
		if cfg.Verbose {
			log.Printf("[api] %s %s", r.Method, r.URL.Path)
		}
	}

	rp.Transport = &http.Transport{
		TLSClientConfig:     &tls.Config{InsecureSkipVerify: cfg.Insecure},
		MaxIdleConns:        100,
		IdleConnTimeout:     90 * time.Second,
		DisableCompression:  true,  // do not compress video
		ForceAttemptHTTP2:   false, // HTTP/1.1: better parallelism
		MaxIdleConnsPerHost: 32,
	}

	rp.ModifyResponse = func(resp *http.Response) error {
		ct := resp.Header.Get("Content-Type")
		if !strings.Contains(ct, "application/json") {
			return nil
		}
		pathIsPlayback := strings.Contains(resp.Request.URL.Path, "PlaybackInfo")
		pathIsInfo := strings.HasPrefix(resp.Request.URL.Path, "/System/Info")
		if !pathIsPlayback && !(pathIsInfo && cfg.ServerName != "") {
			return nil
		}
		var src io.Reader = resp.Body
		// if the server sends gzip anyway, decompress it.
		if strings.Contains(resp.Header.Get("Content-Encoding"), "gzip") {
			gz, gerr := gzip.NewReader(resp.Body)
			if gerr == nil {
				src = gz
				resp.Header.Del("Content-Encoding")
			}
		}
		body, err := io.ReadAll(src)
		resp.Body.Close()
		if err != nil {
			return err
		}
		if pathIsInfo {
			var m map[string]interface{}
			if json.Unmarshal(body, &m) == nil {
				if _, ok := m["ServerName"]; ok {
					m["ServerName"] = cfg.ServerName
					if nb, e := json.Marshal(m); e == nil {
						body = nb
					}
				}
			}
			resp.Body = io.NopCloser(strings.NewReader(string(body)))
			resp.ContentLength = int64(len(body))
			resp.Header.Set("Content-Length", fmt.Sprint(len(body)))
			return nil
		}
		rewritten, err := rewritePlaybackInfo(body)
		if err != nil {
			if cfg.Verbose {
				log.Printf("[warn] PlaybackInfo not rewritten: %v", err)
			}
			resp.Body = io.NopCloser(strings.NewReader(string(body)))
			return nil
		}
		resp.Body = io.NopCloser(strings.NewReader(string(rewritten)))
		resp.ContentLength = int64(len(rewritten))
		resp.Header.Set("Content-Length", fmt.Sprint(len(rewritten)))
		return nil
	}
	return rp
}

// rewritePlaybackInfo redirects every MediaSource to our own stream.
func rewritePlaybackInfo(body []byte) ([]byte, error) {
	var root map[string]interface{}
	if err := json.Unmarshal(body, &root); err != nil {
		return nil, err
	}
	sources, ok := root["MediaSources"].([]interface{})
	if !ok {
		return body, nil
	}
	for _, s := range sources {
		src, ok := s.(map[string]interface{})
		if !ok {
			continue
		}
		id, _ := src["Id"].(string)
		if id == "" {
			continue
		}
		// set everything to "we transcode" — the TV then fetches the stream
		// via our URL instead of directly from the server.
		src["SupportsDirectPlay"] = false
		src["SupportsDirectStream"] = false
		src["SupportsTranscoding"] = true
		src["DirectStreamUrl"] = nil
		streamURL := fmt.Sprintf("%s/proxy/stream/%s/master.m3u8", cfg.PublicURL, url.PathEscape(id))
		src["TranscodingUrl"] = "/proxy/stream/" + url.PathEscape(id) + "/master.m3u8"
		src["TranscodingSubProtocol"] = "hls"
		src["TranscodingContainer"] = "ts"

		// The player reads the HDR status from the MediaStreams.
		// We may only report SDR if we actually tone map to SDR.
		// With -copyvideo HDR/HDR10+ stays unchanged -> keep the original metadata.
		lieAboutHDR := cfg.ToneMap && !cfg.CopyVideo
		if streams, ok := src["MediaStreams"].([]interface{}); ok && lieAboutHDR {
			for _, st := range streams {
				if m, ok := st.(map[string]interface{}); ok {
					if t, _ := m["Type"].(string); t == "Video" {
						m["VideoRange"] = "SDR"
						m["VideoRangeType"] = "SDR"
						m["ColorSpace"] = "bt709"
						m["ColorTransfer"] = "bt709"
						m["ColorPrimaries"] = "bt709"
						m["BitDepth"] = 8.0
						delete(m, "VideoDoViTitle")
					}
				}
			}
		}
		if cfg.Verbose {
			log.Printf("[rewrite] source %s -> %s", id, streamURL)
		}
	}
	return json.Marshal(root)
}

// ---------------------------------------------------------------- Stream handling

var (
	placeboOnce   sync.Once
	placeboResult bool
)

// placeboWorks checks once whether libplacebo starts on this system.
// Without Vulkan/MoltenVK it fails with VK_ERROR_INCOMPATIBLE_DRIVER.
func placeboWorks() bool {
	placeboOnce.Do(func() {
		cmd := exec.Command(cfg.FFmpeg, "-hide_banner", "-loglevel", "error",
			"-f", "lavfi", "-i", "testsrc2=size=320x240:rate=1:duration=1",
			"-vf", "libplacebo=tonemapping=st2094-40:format=yuv420p",
			"-f", "null", "-")
		placeboResult = cmd.Run() == nil
		if !placeboResult {
			log.Printf("[warn] libplacebo not available (Vulkan missing) - using CPU tone mapping")
		}
	})
	return placeboResult
}

// buildFFmpegArgs builds the command line for a given MediaSource.
func buildFFmpegArgs(inputURL, playlistPath, segPath string, copyVideoArgsLocal bool) []string {
	args := []string{
		"-hide_banner", "-loglevel", "warning",
		"-nostdin",
	}
	// hardware decode: takes a huge load off the CPU for 4K 10-bit HDR.
	if cfg.HWAccel && !cfg.CopyVideo {
		args = append(args, "-hwaccel", "videotoolbox")
	}
	args = append(args, "-analyzeduration", "20M", "-probesize", "20M")
	if !copyVideoArgsLocal {
		// only for the direct upstream stream: time out if the server does not deliver.
		// With the local preload endpoint the reader intentionally blocks until the
		// chunk is there - an rw_timeout would wrongly abort here.
		args = append(args, "-rw_timeout", "30000000")
	}
	// These options only apply to the HTTP protocol reader on upstream.
	// With the local preload endpoint ffmpeg would otherwise abort with
	// "Option request_size not found".
	if !copyVideoArgsLocal {
		args = append(args,
			"-request_size", "33554432",
			"-initial_request_size", "33554432",
			"-multiple_requests", "1",
		)
	}
	args = append(args, "-i", inputURL)

	if cfg.CopyVideo {
		// pass video through unchanged: no decode, no encode, no tone map.
		args = append(args, "-c:v", "copy")
	} else {
		// build the tone mapping filter chain
		var vf []string
		if cfg.ToneMap {
			if cfg.UsePlacebo && placeboWorks() {
				vf = append(vf, "libplacebo=tonemapping=st2094-40:colorspace=bt709:color_primaries=bt709:color_trc=bt709:range=tv:format=yuv420p")
			} else {
				vf = append(vf, "format=yuv420p10le,tonemap=mobius:desat=0:peak=100,format=yuv420p")
			}
		}
		if cfg.MaxHeight > 0 {
			vf = append(vf, fmt.Sprintf("scale=-2:'min(%d,ih)'", cfg.MaxHeight))
		}
		if cfg.ToneMap && !cfg.UsePlacebo {
			vf = append(vf, "setparams=range=tv:color_primaries=bt709:color_trc=bt709:colorspace=bt709")
		}
		if len(vf) > 0 {
			args = append(args, "-vf", strings.Join(vf, ","))
		}

		if cfg.HWAccel {
			args = append(args, "-c:v", "hevc_videotoolbox", "-allow_sw", "1", "-tag:v", "hvc1")
			if cfg.Realtime {
				args = append(args, "-realtime", "1", "-prio_speed", "1")
			}
		} else {
			args = append(args, "-c:v", cfg.VideoCodec, "-preset", "veryfast")
		}
		if cfg.VideoBitrate != "" {
			args = append(args, "-b:v", cfg.VideoBitrate)
		}
		args = append(args, "-pix_fmt", "yuv420p")
	}

	// audio: always to AC3 (your TV cannot do DTS/TrueHD)
	args = append(args,
		"-c:a", cfg.AudioCodec,
		"-b:a", cfg.AudioBitrate,
		"-ac", fmt.Sprint(cfg.AudioChannels),
		"-map", "0:v:0", "-map", "0:a:0",
	)

	// HLS output
	args = append(args,
		"-f", "hls",
		"-hls_time", "4",
		"-hls_list_size", "6",
		"-hls_segment_type", "mpegts",
		"-hls_flags", "independent_segments+temp_file+delete_segments",
		"-hls_segment_filename", segPath,
		playlistPath,
	)
	return args
}

// TranscodeJob manages a running ffmpeg process.
type TranscodeJob struct {
	sourceID string
	lastUse  time.Time
	mu       sync.Mutex
	cmd      *exec.Cmd
	dir      string
	playlist string
	done     chan struct{}
	started  time.Time
	err      error
}

// span follows the OpenTelemetry data model: TraceID, SpanID, ParentSpanID,
// start/end in nanoseconds, typed attributes and status.
type span struct {
	TraceID   uint64
	SpanID    uint64
	ParentID  uint64
	Name      string
	Kind      string // server | client | internal
	StartNs   int64
	EndNs     int64
	Status    string // unset | ok | error
	StatusMsg string
	Attrs     map[string]interface{}
}

var (
	spanMu  sync.Mutex
	spans   []*span
	spanSeq uint64
)

const maxSpans = 400

// rootTrace starts a new trace (an incoming request).
func rootTrace(name string, attrs map[string]interface{}) (uint64, uint64) {
	spanMu.Lock()
	defer spanMu.Unlock()
	spanSeq++
	traceID := spanSeq
	spanSeq++
	spanID := spanSeq
	sp := &span{
		TraceID: traceID, SpanID: spanID, ParentID: 0,
		Name: name, Kind: "server", StartNs: time.Now().UnixNano(),
		Status: "unset", Attrs: attrs,
	}
	if sp.Attrs == nil {
		sp.Attrs = map[string]interface{}{}
	}
	spans = append(spans, sp)
	if len(spans) > maxSpans {
		spans = spans[len(spans)-maxSpans:]
	}
	return traceID, spanID
}

// childSpan attaches a child span to the parent.
func childSpan(traceID, parentID uint64, name, kind string, attrs map[string]interface{}) uint64 {
	if parentID == 0 {
		return 0
	}
	spanMu.Lock()
	defer spanMu.Unlock()
	spanSeq++
	id := spanSeq
	sp := &span{
		TraceID: traceID, SpanID: id, ParentID: parentID,
		Name: name, Kind: kind, StartNs: time.Now().UnixNano(),
		Status: "unset", Attrs: attrs,
	}
	if sp.Attrs == nil {
		sp.Attrs = map[string]interface{}{}
	}
	spans = append(spans, sp)
	if len(spans) > maxSpans {
		spans = spans[len(spans)-maxSpans:]
	}
	return id
}

func setAttr(spanID uint64, k string, v interface{}) {
	if spanID == 0 {
		return
	}
	spanMu.Lock()
	defer spanMu.Unlock()
	for i := len(spans) - 1; i >= 0; i-- {
		if spans[i].SpanID == spanID {
			spans[i].Attrs[k] = v
			return
		}
	}
}

func closeSpan(spanID uint64, status string, msg string) {
	if spanID == 0 {
		return
	}
	spanMu.Lock()
	defer spanMu.Unlock()
	for i := len(spans) - 1; i >= 0; i-- {
		if spans[i].SpanID == spanID {
			if spans[i].EndNs == 0 {
				spans[i].EndNs = time.Now().UnixNano()
			}
			if spans[i].Status == "unset" || status == "error" {
				spans[i].Status = status
				spans[i].StatusMsg = msg
			}
			return
		}
	}
}

var (
	jobsMu     sync.Mutex
	jobs       = map[string]*TranscodeJob{}
	preloaders = map[string]*preloader{}
	// jobLocks prevents several parallel playlist requests from
	// starting a transcode at the same time (race -> 4x ffmpeg + 4x preload).
	jobLocks sync.Map // sourceID -> *sync.Mutex
)

func lockFor(sourceID string) *sync.Mutex {
	v, _ := jobLocks.LoadOrStore(sourceID, &sync.Mutex{})
	return v.(*sync.Mutex)
}

func getJob(sourceID string) *TranscodeJob {
	jobsMu.Lock()
	job := jobs[sourceID]
	jobsMu.Unlock()
	if job == nil {
		return nil
	}
	// 9: stop serving a dead job -> allow a restart
	select {
	case <-job.done:
		jobsMu.Lock()
		if jobs[sourceID] == job {
			delete(jobs, sourceID)
		}
		jobsMu.Unlock()
		return nil
	default:
		return job
	}
}

// startJob starts ffmpeg for a MediaSource and returns the job object.
func startJob(r *http.Request, sourceID string) (*TranscodeJob, error) {
	dir, err := os.MkdirTemp(cfg.TranscodeDir, "jfproxy-"+sanitize(sourceID)+"-")
	if err != nil {
		return nil, err
	}
	playlist := filepath.Join(dir, "master.m3u8")
	segPattern := filepath.Join(dir, "seg%05d.ts")
	isLocalPreload := false

	// take auth from the incoming request.
	token := r.URL.Query().Get("api_key")
	if token == "" {
		token = r.URL.Query().Get("ApiKey")
	}
	if token == "" {
		token = r.Header.Get("X-Emby-Token")
	}
	if token == "" {
		// MediaBrowser token (older clients)
		if v := r.Header.Get("X-MediaBrowser-Token"); v != "" {
			token = v
		}
	}

	// direct-play input from the real server.
	inputURL := fmt.Sprintf("%s/Videos/%s/stream?static=true", cfg.Upstream.String(), url.PathEscape(sourceID))
	if token != "" {
		inputURL += "&api_key=" + url.QueryEscape(token)
	}

	// optional: preload into a local file — ffmpeg then reads from disk
	// instead of waiting on a high-latency HTTP stream.
	if cfg.PreloadMB > 0 {
		pl, perr := newPreloader(sourceID, inputURL, cfg.PreloadMB)
		if perr != nil {
			log.Printf("[preload] failed (%v) - using direct stream", perr)
			trace(traceEvent{Event: "preload_error", ID: short8(sourceID), Detail: perr.Error()})
		} else {
			jobsMu.Lock()
			if old := preloaders[sourceID]; old != nil {
				old.close() // do not leak the old preloader
			}
			preloaders[sourceID] = pl
			jobsMu.Unlock()
			// ffmpeg gets a blocking HTTP endpoint, NOT a file:
			// that way there are no zero-filled gaps and no premature EOF.
			inputURL = fmt.Sprintf("http://127.0.0.1%s/preload/%s/stream",
				cfg.Listen, url.PathEscape(sourceID))
			isLocalPreload = true
			if cfg.Verbose {
				log.Printf("[preload] ffmpeg reads via %s", inputURL)
			}
		}
	}

	args := buildFFmpegArgs(inputURL, playlist, segPattern, isLocalPreload)
	cmd := exec.Command(cfg.FFmpeg, args...)
	cmd.Env = os.Environ()

	if cfg.Verbose {
		log.Printf("[ffmpeg] start %s (token=%v) -> %s", short(sourceID), token != "", dir)
	}
	trace(traceEvent{Event: "ffmpeg_start", ID: short8(sourceID), Detail: "copy=" + fmt.Sprint(cfg.CopyVideo)})
	// upstream probe: TTFB + throughput of the first 8 MB
	go probeUpstream(sourceID, token)

	stderr, err := cmd.StderrPipe()
	if err != nil {
		os.RemoveAll(dir)
		return nil, err
	}

	job := &TranscodeJob{
		sourceID: sourceID,
		lastUse:  time.Now(),
		cmd:      cmd,
		dir:      dir,
		playlist: playlist,
		done:     make(chan struct{}),
		started:  time.Now(),
	}

	if err := cmd.Start(); err != nil {
		os.RemoveAll(dir)
		return nil, err
	}

	// watcher: trace when each segment was created
	go func() {
		seen := map[string]bool{}
		t := time.NewTicker(200 * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-job.done:
				return
			case <-t.C:
				matches, _ := filepath.Glob(filepath.Join(dir, "seg*.ts"))
				for _, m := range matches {
					base := filepath.Base(m)
					if seen[base] {
						continue
					}
					seen[base] = true
					if st, err := os.Stat(m); err == nil {
						trace(traceEvent{
							Event:  "seg_created",
							ID:     short8(sourceID),
							Seg:    base,
							Bytes:  st.Size(),
							Ms:     time.Since(job.started).Seconds() * 1000,
							Detail: "age_since_start",
						})
					}
				}
			}
		}
	}()

	go func() {
		sc := bufio.NewScanner(stderr)
		for sc.Scan() {
			line := sc.Text()
			if cfg.Verbose || strings.Contains(strings.ToLower(line), "error") {
				log.Printf("[ffmpeg %s] %s", short(sourceID), line)
			}
		}
	}()

	// H4: stop ffmpeg when the TV has not fetched anything for a while.
	go func() {
		t := time.NewTicker(30 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-job.done:
				return
			case <-t.C:
				job.mu.Lock()
				last := job.lastUse
				job.mu.Unlock()
				if time.Since(last) > 3*time.Minute {
					log.Printf("[job] %s: 3 min without requests - stopping ffmpeg", short(sourceID))
					if job.cmd != nil && job.cmd.Process != nil {
						job.cmd.Process.Kill()
					}
					if pl := func() *preloader { jobsMu.Lock(); defer jobsMu.Unlock(); return preloaders[sourceID] }(); pl != nil {
						pl.close()
					}
					return
				}
			}
		}
	}()

	go func() {
		err := cmd.Wait()
		job.mu.Lock()
		job.err = err
		job.mu.Unlock()
		close(job.done)
		if cfg.Verbose {
			log.Printf("[ffmpeg] end %s (%v, %v)", short(sourceID), err, time.Since(job.started).Round(time.Second))
		}
		trace(traceEvent{Event: "ffmpeg_end", ID: short8(sourceID), Ms: time.Since(job.started).Seconds() * 1000, Detail: fmt.Sprint(err)})
		go func() {
			time.Sleep(2 * time.Minute)
			jobsMu.Lock()
			if jobs[sourceID] == job {
				delete(jobs, sourceID)
			}
			jobsMu.Unlock()
			os.RemoveAll(dir)
		}()
	}()

	jobsMu.Lock()
	jobs[sourceID] = job
	jobsMu.Unlock()

	return job, nil
}

// handleStream serves /proxy/stream/{id}/master.m3u8 and /proxy/stream/{id}/segXXXXX.ts
func handleStream(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, "/proxy/stream/")
	parts := strings.SplitN(rest, "/", 2)
	if len(parts) != 2 {
		http.Error(w, "bad path", http.StatusBadRequest)
		return
	}
	sourceID := parts[0]
	file := parts[1]

	job := getJob(sourceID)
	if job == nil {
		mu := lockFor(sourceID)
		mu.Lock()
		job = getJob(sourceID)
		if job == nil {
			var err error
			job, err = startJob(r, sourceID)
			if err != nil {
				mu.Unlock()
				log.Printf("[error] start job: %v", err)
				http.Error(w, "transcode start failed: "+err.Error(), http.StatusInternalServerError)
				return
			}
		}
		mu.Unlock()
	}

	// wait for the playlist
	waitFor(time.Now().Add(60*time.Second), func() bool {
		if _, err := os.Stat(job.playlist); err == nil {
			return true
		}
		select {
		case <-job.done:
			return true
		default:
		}
		time.Sleep(100 * time.Millisecond)
		return false
	})

	if file == "master.m3u8" {
		servePlaylistFor(w, r, job, sourceID)
		return
	}
	if strings.HasSuffix(file, ".ts") {
		serveSegment(w, r, job, file)
		return
	}
	http.NotFound(w, r)
}

func waitFor(deadline time.Time, cond func() bool) {
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
	}
}

func servePlaylistFor(w http.ResponseWriter, r *http.Request, job *TranscodeJob, sourceID string) {
	job.mu.Lock()
	job.lastUse = time.Now()
	job.mu.Unlock()
	// wait until the playlist exists or the job ends
	waitFor(time.Now().Add(60*time.Second), func() bool {
		if _, err := os.Stat(job.playlist); err == nil {
			return true
		}
		select {
		case <-job.done:
			return true
		default:
		}
		time.Sleep(100 * time.Millisecond)
		return false
	})
	data, err := os.ReadFile(job.playlist)
	if err != nil {
		job.mu.Lock()
		ferr := job.err
		job.mu.Unlock()
		http.Error(w, fmt.Sprintf("playlist not ready: %v (%v)", err, ferr), http.StatusInternalServerError)
		return
	}
	// rewrite segment names to our own path
	// so the client fetches them via /proxy/stream/{id}/....
	base := "/proxy/stream/" + url.PathEscape(sourceID) + "/"
	var out []string
	for _, line := range strings.Split(string(data), "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed != "" && !strings.HasPrefix(trimmed, "#") && strings.HasSuffix(trimmed, ".ts") {
			out = append(out, base+trimmed)
		} else {
			out = append(out, line)
		}
	}
	w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
	w.Header().Set("Cache-Control", "no-cache")
	w.Write([]byte(strings.Join(out, "\n")))
}

func serveSegment(w http.ResponseWriter, r *http.Request, job *TranscodeJob, name string) {
	job.mu.Lock()
	job.lastUse = time.Now()
	job.mu.Unlock()
	path := filepath.Join(job.dir, filepath.Base(name))
	reqStart := time.Now()
	waitFor(time.Now().Add(90*time.Second), func() bool {
		if _, err := os.Stat(path); err == nil {
			return true
		}
		select {
		case <-job.done:
			return true
		default:
		}
		time.Sleep(100 * time.Millisecond)
		return false
	})
	waitMs := time.Since(reqStart).Seconds() * 1000
	f, err := os.Open(path)
	if err != nil {
		trace(traceEvent{Event: "seg_404", ID: short8(job.sourceID), Seg: filepath.Base(name), Ms: waitMs})
		http.NotFound(w, r)
		return
	}
	defer f.Close()
	st, _ := f.Stat()
	var n int64
	w.Header().Set("Content-Type", "video/mp2t")
	http.ServeContent(w, r, name, time.Now(), f)
	if st != nil {
		n = st.Size()
	}
	trace(traceEvent{
		Event:  "seg_served",
		ID:     short8(job.sourceID),
		Seg:    filepath.Base(name),
		Bytes:  n,
		Ms:     time.Since(reqStart).Seconds() * 1000,
		Detail: fmt.Sprintf("wait=%.0fms", waitMs),
	})
}

// handleVideoIntercept intercepts /Videos/{id}/stream.*.
// Everything else under /Videos/ (e.g. /Images) passes through normally.
func handleVideoIntercept(rp http.Handler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// only intercept exactly /Videos/{id}/stream....
		// /System/... and everything else never gets here (different path).
		rest := strings.TrimPrefix(r.URL.Path, "/Videos/")
		parts := strings.SplitN(rest, "/", 2)
		if len(parts) == 2 && parts[0] != "" && strings.HasPrefix(parts[1], "stream") {
			sourceID := parts[0]
			if cfg.Verbose {
				log.Printf("[intercept] %s -> HLS transcode", r.URL.Path)
			}
			job := getJob(sourceID)
			if job == nil {
				mu := lockFor(sourceID)
				mu.Lock()
				job = getJob(sourceID) // double-check under lock
				if job == nil {
					var err error
					job, err = startJob(r, sourceID)
					if err != nil {
						mu.Unlock()
						log.Printf("[error] start job %s: %v", short(sourceID), err)
						http.Error(w, "transcode start failed: "+err.Error(), http.StatusInternalServerError)
						return
					}
				}
				mu.Unlock()
			}
			servePlaylistFor(w, r, job, sourceID)
			return
		}
		// pass everything else (e.g. /Videos/{id}/Images) through normally.
		rp.ServeHTTP(w, r)
	}
}

// handlePreloadServe serves the stream from the preload buffer.
// Every read blocks until the chunk is loaded -> no blank frames, no
// premature EOF. ffmpeg speaks HTTP/1.1 with this endpoint.
func handlePreloadServe(w http.ResponseWriter, r *http.Request) {
	// M4: this endpoint serves video data without Jellyfin auth.
	// Only localhost may use it (ffmpeg runs on the same host).
	host, _, _ := net.SplitHostPort(r.RemoteAddr)
	if host != "127.0.0.1" && host != "::1" {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	// /preload/{id}/stream
	rest := strings.TrimPrefix(r.URL.Path, "/preload/")
	parts := strings.SplitN(rest, "/", 2)
	if len(parts) != 2 {
		http.NotFound(w, r)
		return
	}
	sourceID := parts[0]
	jobsMu.Lock()
	pl := preloaders[sourceID]
	jobsMu.Unlock()
	if pl == nil {
		http.Error(w, "no preload", http.StatusNotFound)
		return
	}
	pl.touch()
	w.Header().Set("Content-Type", "video/x-matroska")
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Content-Length", fmt.Sprint(pl.total))

	// H2: answer Range correctly (206 + exact end, 416 on overflow)
	start, end := int64(0), pl.total-1
	status := http.StatusOK
	if rh := r.Header.Get("Range"); strings.HasPrefix(rh, "bytes=") {
		var s1 int64
		spec := strings.TrimPrefix(rh, "bytes=")
		if i := strings.Index(spec, "-"); i >= 0 {
			var it int64
			if k, err := fmt.Sscanf(spec[:i], "%d", &s1); k == 1 && err == nil {
				if s1 >= pl.total {
					w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", pl.total))
					w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
					return
				}
				start = s1
				if rest := spec[i+1:]; rest != "" {
					if k2, err2 := fmt.Sscanf(rest, "%d", &it); k2 == 1 && err2 == nil {
						if it < start {
							w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", pl.total))
							w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
							return
						}
						end = it
						if end >= pl.total {
							end = pl.total - 1
						}
					}
				}
				status = http.StatusPartialContent
			}
		}
	}
	w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, pl.total))
	w.Header().Set("Content-Length", fmt.Sprint(end-start+1))
	w.WriteHeader(status)

	buf := make([]byte, 256<<10)
	pos := start
	for pos <= end {
		select {
		case <-r.Context().Done():
			return // client gone
		default:
		}
		if int64(len(buf)) > end-pos+1 {
			buf = buf[:end-pos+1]
		}
		n, err := pl.readAt(buf, pos)
		if n > 0 {
			if _, werr := w.Write(buf[:n]); werr != nil {
				return
			}
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			pos += int64(n)
		}
		if err != nil {
			return
		}
	}
}

// ---------------------------------------------------------------- Health

//go:embed dashboard.html
var dashboardHTML string

func handleDashboard(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	io.WriteString(w, dashboardHTML)
}

func handleRequests(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	spanMu.Lock()
	out := make([]map[string]interface{}, 0, len(spans))
	for _, sp := range spans {
		dur := int64(0)
		if !sp.End.IsZero() {
			dur = sp.End.Sub(sp.Start).Milliseconds()
		} else {
			dur = time.Since(sp.Start).Milliseconds()
		}
		att := map[string]string{}
		for k, v := range sp.Attrs {
			att[k] = v
		}
		out = append(out, map[string]interface{}{
			"id":     sp.ID,
			"parent": sp.ParentID,
			"name":   sp.Name,
			"start":  sp.Start.Format("15:04:05.000"),
			"ms":     dur,
			"status": sp.Status,
			"attrs":  att,
			"open":   sp.End.IsZero(),
		})
	}
	spanMu.Unlock()
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	enc.Encode(out)
}

func handleStats(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	jobsMu.Lock()
	stats := map[string]interface{}{}
	for id, pl := range preloaders {
		stats[id[:8]] = pl.stats()
	}
	jobsMu.Unlock()
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	enc.Encode(stats)
}

func handleHealth(w http.ResponseWriter, _ *http.Request) {
	jobsMu.Lock()
	n := len(jobs)
	jobsMu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"upstream":   cfg.Upstream.String(),
		"public":     cfg.PublicURL,
		"active":     n,
		"hwaccel":    cfg.HWAccel,
		"videocodec": cfg.VideoCodec,
		"audiocodec": cfg.AudioCodec,
	})
}

// ---------------------------------------------------------------- Helpers

func sanitize(s string) string {
	var b strings.Builder
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' {
			b.WriteRune(r)
		}
	}
	if b.Len() > 32 {
		return b.String()[:32]
	}
	return b.String()
}

func short(s string) string {
	if len(s) > 8 {
		return s[:8]
	}
	return s
}

// ---------------------------------------------------------------- main

func main() {
	var upstream string
	flag.StringVar(&upstream, "upstream", "", "URL of the real Jellyfin server, e.g. http://192.168.1.50:8096")
	flag.StringVar(&cfg.Listen, "listen", ":8099", "listen address")
	flag.StringVar(&cfg.PublicURL, "public", "", "URL at which the TV reaches the proxy (default: http://<local-ip><listen>)")
	flag.StringVar(&cfg.FFmpeg, "ffmpeg", "ffmpeg", "path to ffmpeg")
	flag.BoolVar(&cfg.HWAccel, "hwaccel", true, "use VideoToolbox hardware encoding")
	flag.StringVar(&cfg.VideoCodec, "vcodec", "libx264", "software video codec (when hwaccel=0)")
	flag.StringVar(&cfg.VideoBitrate, "vbitrate", "12M", "video bitrate")
	flag.IntVar(&cfg.MaxHeight, "maxheight", 0, "maximum height (0 = no scaling)")
	flag.StringVar(&cfg.AudioCodec, "acodec", "ac3", "target audio codec (ac3 = Dolby Digital)")
	flag.StringVar(&cfg.AudioBitrate, "abitrate", "640k", "audio bitrate")
	flag.IntVar(&cfg.AudioChannels, "achannels", 6, "audio channels (6 = 5.1)")
	flag.StringVar(&cfg.TranscodeDir, "tmp", "", "directory for HLS segments (default: os temp)")
	flag.BoolVar(&cfg.Verbose, "v", false, "verbose logging")
	flag.BoolVar(&cfg.Insecure, "insecure", false, "ignore upstream TLS certificates")
	flag.StringVar(&cfg.ServerName, "servername", "", "override the server display name (e.g. 'Mac-Proxy')")
	flag.BoolVar(&cfg.Realtime, "realtime", true, "optimize VideoToolbox for speed (realtime + prio_speed)")
	flag.BoolVar(&cfg.ToneMap, "tonemap", true, "tone map HDR to SDR (required for 8-bit output)")
	flag.BoolVar(&cfg.CopyVideo, "copyvideo", false, "pass video through unchanged, audio only -> AC3")
	flag.BoolVar(&cfg.UsePlacebo, "placebo", false, "use libplacebo (GPU) for tone mapping")
	flag.StringVar(&cfg.TraceFile, "trace", "", "JSONL trace file for timing analysis")
	flag.IntVar(&cfg.PreloadMB, "preload", 64, "head preload in MB (blocking before ffmpeg start, 0 = off)")
	flag.IntVar(&cfg.WindowMB, "window", 576, "read-ahead window in MB (background preload)")
	flag.IntVar(&cfg.PreloadWorkers, "preload-workers", 4, "parallel range downloads for preload")
	flag.Parse()

	if upstream == "" {
		fmt.Fprintln(os.Stderr, "ERROR: -upstream is required")
		flag.Usage()
		os.Exit(2)
	}
	u, err := url.Parse(strings.TrimRight(upstream, "/"))
	if err != nil {
		log.Fatalf("invalid upstream URL: %v", err)
	}
	cfg.Upstream = u
	buildPreloadClient()

	if cfg.TraceFile != "" {
		f, err := os.OpenFile(cfg.TraceFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			log.Printf("WARNING: trace file not writable: %v", err)
		} else {
			traceFh = f
			log.Printf("  trace    : %s", cfg.TraceFile)
		}
	}

	if cfg.TranscodeDir == "" {
		cfg.TranscodeDir = filepath.Join(os.TempDir(), "jellyfin-proxy")
	}
	os.MkdirAll(cfg.TranscodeDir, 0o755)

	// clean up orphaned directories from earlier runs.
	if entries, err := os.ReadDir(cfg.TranscodeDir); err == nil {
		removed := 0
		for _, e := range entries {
			if e.IsDir() && (strings.HasPrefix(e.Name(), "jfpreload-") || strings.HasPrefix(e.Name(), "jfproxy-")) {
				os.RemoveAll(filepath.Join(cfg.TranscodeDir, e.Name()))
				removed++
			}
		}
		if removed > 0 {
			log.Printf("  cleanup  : removed %d old transcode directories", removed)
		}
	}

	if cfg.PublicURL == "" {
		host := localIP()
		cfg.PublicURL = fmt.Sprintf("http://%s%s", host, cfg.Listen)
	}
	cfg.PublicURL = strings.TrimRight(cfg.PublicURL, "/")

	log.Printf("Jellyfin proxy starting")
	log.Printf("  upstream : %s", cfg.Upstream)
	log.Printf("  listen   : %s", cfg.Listen)
	log.Printf("  public   : %s", cfg.PublicURL)
	log.Printf("  hwaccel  : %v  vcodec=%s vbitrate=%s", cfg.HWAccel, cfg.VideoCodec, cfg.VideoBitrate)
	log.Printf("  audio    : %s %s %dch", cfg.AudioCodec, cfg.AudioBitrate, cfg.AudioChannels)

	rp := newProxy()

	mux := http.NewServeMux()
	mux.HandleFunc("/proxy/health", handleHealth)
	mux.HandleFunc("/proxy/stats", handleStats)
	mux.HandleFunc("/proxy/requests", handleRequests)

	mux.HandleFunc("/preload/", handlePreloadServe)
	mux.HandleFunc("/proxy/stream/", handleStream)
	mux.HandleFunc("/Videos/", handleVideoIntercept(rp))
	// catch-all: dashboard on "/", everything else to the real Jellyfin.
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			handleDashboard(w, r)
			return
		}
		rp.ServeHTTP(w, r)
	})

	srv := &http.Server{
		Addr:              cfg.Listen,
		Handler:           logMiddleware(mux),
		ReadHeaderTimeout: 20 * time.Second,
		// no WriteTimeout — streams are long
	}

	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
		<-sig
		log.Printf("shutting down…")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(ctx)
	}()

	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}

type statusRecorder struct {
	http.ResponseWriter
	status int
	bytes  int64
}

func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

func (s *statusRecorder) Write(b []byte) (int, error) {
	if s.status == 0 {
		s.status = 200
	}
	n, err := s.ResponseWriter.Write(b)
	s.bytes += int64(n)
	return n, err
}

func (s *statusRecorder) Flush() {
	if f, ok := s.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func logMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w}
		next.ServeHTTP(rec, r)
		id := newSpan(0, r.Method+" "+r.URL.Path, map[string]string{
			"status": fmt.Sprint(rec.status),
			"kb":     fmt.Sprint(rec.bytes / 1024),
		})
		endSpan(id, "ok", nil)
		_ = start
		if cfg.Verbose {
			log.Printf("%s %s (%v)", r.Method, r.URL.Path, time.Since(start).Round(time.Millisecond))
		}
	})
}

// localIP determines the Mac's primary LAN IP.
func localIP() string {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		return "127.0.0.1"
	}
	defer conn.Close()
	addr, ok := conn.LocalAddr().(*net.UDPAddr)
	if !ok {
		return "127.0.0.1"
	}
	return addr.IP.String()
}
