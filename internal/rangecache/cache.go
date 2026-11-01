// Package rangecache downloads a remote file into a local cache using many
// parallel, LARGE range requests and serves it locally, byte-exact.
//
// Core idea: the server sees a few large sequential reads (block size,
// default 32 MiB), which is easy on its disk. Readers (ffmpeg) read in small
// chunks and only wait until exactly the bytes they need are present, not
// until a whole block is finished.
//
// Each block is stored as its own (sparse) file so old ranges can be freed
// by simply deleting them.
package rangecache

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"jellyfin-proxy/internal/events"
)

type Config struct {
	Dir        string        // parent directory for cache data
	BlockSize  int64         // size of one server request and one cache file
	Workers    int           // parallel connections to the server
	Reserve    int           // of those, kept free for waiting readers
	ReadAhead  int64         // how much is preloaded ahead of each reader
	KeepBehind int64         // how much is kept behind the rearmost reader
	Retries    int           // attempts per request before a short pause
	Stall      time.Duration // abort a request if no bytes arrive for this long
	Client     *http.Client
	Events     *events.Log
}

// urgentSlack: if a running request is at most this far before the wanted
// offset, a reader waits for it instead of starting a new one.
const urgentSlack = 4 << 20

// sequentialAfter: a reader only counts as sequential, and gets read-ahead,
// after reading this many bytes.
const sequentialAfter = 2 << 20

// minSplit: smaller remainders are no longer split across two connections.
const minSplit = 2 << 20

var ErrClosed = errors.New("cache closed")

type Cache struct {
	cfg   Config
	ID    string
	Label string
	url   string
	Size  int64
	dir   string

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu       sync.Mutex
	cond     *sync.Cond
	have     intervals
	fetches  map[*fetch]struct{}
	readers  map[*Reader]struct{}
	waits    map[int64]int
	files    map[int64]*blockFile
	fetchSeq int
	closed   bool
	fatal    error
	cooldown time.Time
	rate     rateMeter
	fetched  int64
	evicted  int64
	splits   int
	created  time.Time
	lastUse  time.Time
}

type blockFile struct {
	f    *os.File
	pins int
}

type fetch struct {
	id       int
	start    int64
	cur      int64
	end      int64 // may be shortened from outside
	urgent   bool
	split    time.Time // last split
	began    time.Time
	attempts int
}

// Open determines the file size via a range request and creates the cache.
func Open(ctx context.Context, cfg Config, id, label, url string) (*Cache, error) {
	size, err := probeSize(ctx, cfg.Client, url)
	if err != nil {
		return nil, err
	}
	dir, have, restored := restore(cfg, id, size)
	if !restored {
		if dir, err = os.MkdirTemp(cfg.Dir, "cache-"+label+"-"); err != nil {
			return nil, err
		}
	}
	cctx, cancel := context.WithCancel(context.Background())
	c := &Cache{
		cfg: cfg, ID: id, Label: label, url: url, Size: size, dir: dir,
		ctx: cctx, cancel: cancel,
		fetches: map[*fetch]struct{}{},
		readers: map[*Reader]struct{}{},
		waits:   map[int64]int{},
		files:   map[int64]*blockFile{},
		created: time.Now(), lastUse: time.Now(),
	}
	c.have = have
	c.cond = sync.NewCond(&c.mu)
	c.wg.Add(1)
	go c.maintain()
	if restored {
		cfg.Events.Emit(label, "cache_restored", events.Info,
			fmt.Sprintf("Restored saved cache from the last run: %.0f MB already local", float64(have.total())/1e6))
		return c, nil
	}
	cfg.Events.Emit(label, "cache_open", events.Info,
		fmt.Sprintf("Cache created: %.2f GB, server requests of %.0f MB, %d connections",
			float64(size)/1e9, float64(cfg.BlockSize)/1e6, cfg.Workers))
	return c, nil
}

func probeSize(ctx context.Context, client *http.Client, url string) (int64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Range", "bytes=0-0")
	resp, err := client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("determine size: %w", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusPartialContent {
		return 0, fmt.Errorf("determine size: HTTP %d (range not supported or token invalid)", resp.StatusCode)
	}
	cr := resp.Header.Get("Content-Range")
	i := strings.LastIndex(cr, "/")
	if i < 0 {
		return 0, fmt.Errorf("determine size: Content-Range %q", cr)
	}
	size, err := strconv.ParseInt(cr[i+1:], 10, 64)
	if err != nil || size <= 0 {
		return 0, fmt.Errorf("determine size: Content-Range %q", cr)
	}
	return size, nil
}

// Close stops all downloads and deletes the data. reason ends up in the event log.
func (c *Cache) Close(reason string) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	have := c.have.total()
	c.cond.Broadcast()
	c.mu.Unlock()

	c.cancel()
	c.wg.Wait()
	c.mu.Lock()
	for _, bf := range c.files {
		bf.f.Close()
	}
	c.mu.Unlock()
	os.RemoveAll(c.dir)
	c.cfg.Events.Emit(c.Label, "cache_close", events.Warn,
		fmt.Sprintf("Cache discarded (%.0f MB were local): %s", float64(have)/1e6, reason),
		"reason", reason)
}

func (c *Cache) blockOf(off int64) int64  { return off / c.cfg.BlockSize }
func (c *Cache) blockEnd(off int64) int64 { return min((c.blockOf(off)+1)*c.cfg.BlockSize, c.Size) }
func (c *Cache) lastBlock() int64         { return (c.Size - 1) / c.cfg.BlockSize }
func (c *Cache) blockStart(b int64) int64 { return b * c.cfg.BlockSize }

// fileLocked opens (or creates) a block's file and pins it.
func (c *Cache) fileLocked(b int64) (*blockFile, error) {
	bf := c.files[b]
	if bf == nil {
		f, err := os.OpenFile(filepath.Join(c.dir, fmt.Sprintf("b%06d", b)), os.O_CREATE|os.O_RDWR, 0o644)
		if err != nil {
			return nil, err
		}
		bf = &blockFile{f: f}
		c.files[b] = bf
	}
	bf.pins++
	return bf, nil
}

// ---------------------------------------------------------------- Reading

// ReadAt reads from off. It waits until at least one byte is present and
// then returns as many contiguous bytes as possible (at most up to the end
// of the block). Waiting registers the offset as urgent.
func (c *Cache) ReadAt(ctx context.Context, p []byte, off int64) (int, error) {
	if off >= c.Size {
		return 0, io.EOF
	}
	stop := context.AfterFunc(ctx, func() {
		c.mu.Lock()
		c.cond.Broadcast()
		c.mu.Unlock()
	})
	defer stop()

	c.mu.Lock()
	waiting := false
	var avail int64
	for {
		if c.closed {
			c.unwaitLocked(off, waiting)
			c.mu.Unlock()
			return 0, ErrClosed
		}
		if c.fatal != nil {
			c.unwaitLocked(off, waiting)
			c.mu.Unlock()
			return 0, c.fatal
		}
		if err := ctx.Err(); err != nil {
			c.unwaitLocked(off, waiting)
			c.mu.Unlock()
			return 0, err
		}
		if avail = c.have.contiguousFrom(off); avail > 0 {
			break
		}
		if !waiting {
			waiting = true
			c.waits[off]++
			c.scheduleLocked()
		}
		c.cond.Wait()
	}
	c.unwaitLocked(off, waiting)
	c.lastUse = time.Now()
	n := min(int64(len(p)), avail, c.blockEnd(off)-off)
	b := c.blockOf(off)
	bf, err := c.fileLocked(b)
	c.mu.Unlock()
	if err != nil {
		return 0, err
	}
	m, err := bf.f.ReadAt(p[:n], off-c.blockStart(b))
	c.mu.Lock()
	bf.pins--
	c.mu.Unlock()
	if err == io.EOF && int64(m) == n {
		err = nil
	}
	return m, err
}

func (c *Cache) unwaitLocked(off int64, waiting bool) {
	if !waiting {
		return
	}
	if c.waits[off]--; c.waits[off] <= 0 {
		delete(c.waits, off)
	}
}

// ReaderAt is an io.ReaderAt that reads fully (for parsers).
func (c *Cache) ReaderAt(ctx context.Context) io.ReaderAt { return fullReaderAt{c, ctx} }

type fullReaderAt struct {
	c   *Cache
	ctx context.Context
}

func (r fullReaderAt) ReadAt(p []byte, off int64) (int, error) {
	n := 0
	for n < len(p) {
		m, err := r.c.ReadAt(r.ctx, p[n:], off+int64(n))
		n += m
		if err != nil {
			return n, err
		}
	}
	return n, nil
}

// Reader is a sequential reader with a read head. Its position controls
// what gets preloaded. One Reader corresponds to one ffmpeg connection.
type Reader struct {
	c      *Cache
	Name   string
	pos    int64
	opened time.Time
	read   int64
}

func (c *Cache) NewReader(name string, pos int64) *Reader {
	r := &Reader{c: c, Name: name, pos: pos, opened: time.Now()}
	c.mu.Lock()
	c.readers[r] = struct{}{}
	c.lastUse = time.Now()
	c.scheduleLocked()
	c.mu.Unlock()
	return r
}

func (r *Reader) Read(ctx context.Context, p []byte) (int, error) {
	r.c.mu.Lock()
	pos := r.pos
	r.c.mu.Unlock()
	n, err := r.c.ReadAt(ctx, p, pos)
	if n > 0 {
		r.c.mu.Lock()
		r.pos += int64(n)
		r.read += int64(n)
		r.c.scheduleLocked()
		r.c.mu.Unlock()
	}
	return n, err
}

func (r *Reader) Close() {
	r.c.mu.Lock()
	delete(r.c.readers, r)
	r.c.lastUse = time.Now()
	r.c.mu.Unlock()
}

// ---------------------------------------------------------------- Scheduling

// scheduleLocked starts new requests: first for waiting readers, then
// read-ahead in front of each read head, nearest gap first.
func (c *Cache) scheduleLocked() {
	if c.closed || c.fatal != nil || time.Now().Before(c.cooldown) {
		return
	}

	// 1. Urgent: a reader is waiting at exactly this offset.
	offs := make([]int64, 0, len(c.waits))
	for off := range c.waits {
		offs = append(offs, off)
	}
	sort.Slice(offs, func(i, j int) bool { return offs[i] < offs[j] })
	for _, off := range offs {
		if len(c.fetches) >= c.cfg.Workers {
			return
		}
		if c.have.contiguousFrom(off) > 0 {
			continue
		}
		end := min(c.blockEnd(off), c.have.nextStart(off, c.Size))
		var covering *fetch
		for f := range c.fetches {
			switch {
			case f.cur <= off && off < f.end && off-f.cur <= urgentSlack:
				covering = f // will pass by shortly
			case f.cur <= off && off < f.end:
				// too far away: the old request ends here, we take over
				end = min(end, f.end)
				f.end = off
			case f.cur > off:
				end = min(end, f.cur)
			}
		}
		if covering == nil {
			c.startLocked(off, min(end, off+c.piece()), true)
		} else {
			c.splitLocked(covering)
		}
	}

	// 2. Read-ahead only for readers that really read sequentially. On startup
	// ffmpeg probes header and index at several places – read-ahead isn't
	// worth it there and would tie up connections at the file start after every seek.
	cursors := make([]int64, 0, len(c.readers))
	for r := range c.readers {
		if r.read >= sequentialAfter {
			cursors = append(cursors, r.pos)
		}
	}
	sort.Slice(cursors, func(i, j int) bool { return cursors[i] < cursors[j] })

	// Read-ahead requests no reader needs anymore (e.g. after a seek) end at
	// the next chunk instead of loading the rest of the block.
	for f := range c.fetches {
		if f.urgent {
			continue
		}
		needed := false
		for _, pos := range cursors {
			if f.cur+c.cfg.BlockSize >= pos && f.cur < pos+c.cfg.ReadAhead {
				needed = true
			}
		}
		for off := range c.waits {
			if off >= f.cur && off < f.end {
				needed = true
			}
		}
		for r := range c.readers { // fresh readers also need the block ahead of them
			if f.end > r.pos && f.cur < r.pos+c.cfg.BlockSize {
				needed = true
			}
		}
		if !needed {
			f.end = f.cur
		}
	}

	// Some connections stay free for urgent requests.
	budget := c.cfg.Workers - c.cfg.Reserve
	for _, pos := range cursors {
		limit := min(pos+c.cfg.ReadAhead, c.Size)
		x := pos
		for x < limit && len(c.fetches) < budget {
			x = c.firstUnclaimedLocked(x)
			if x >= limit {
				break
			}
			end := min(c.blockEnd(x), c.nextClaimedLocked(x), c.Size)
			// Catching up (start, seek): with less than two blocks ahead of the
			// read head, quarter blocks are loaded. A whole block would only
			// arrive after block size / speed of ONE connection (32 MiB at
			// 6 Mbit/s = 45 s); quarters arrive four times as fast. Once there
			// is a buffer, whole blocks run again (easier on the server disk).
			if c.have.contiguousFrom(pos) < 2*c.cfg.BlockSize {
				end = min(end, x+c.piece())
			}
			c.startLocked(x, end, false)
			x = end
		}
	}
}

// splitLocked splits the request a reader is currently waiting on: a free
// connection takes over the back half of the remainder. Without this, ffmpeg
// crawls through the block at the speed of ONE connection (~5 Mbit/s) while
// the blocks behind it are long finished – visible as a sawtooth in the dashboard.
func (c *Cache) splitLocked(f *fetch) {
	rest := f.end - f.cur
	if rest < 2*minSplit || time.Since(f.split) < time.Second || len(c.fetches) >= c.cfg.Workers {
		return
	}
	mid := f.cur + rest/2
	end := f.end
	f.end = mid
	f.split = time.Now()
	c.startLocked(mid, end, true)
	c.splits++
}

// piece is the request size while catching up.
func (c *Cache) piece() int64 { return max(c.cfg.BlockSize/4, 1<<20) }

// firstUnclaimedLocked: first byte from x that is neither present nor being loaded.
func (c *Cache) firstUnclaimedLocked(x int64) int64 {
	for x < c.Size {
		if n := c.have.contiguousFrom(x); n > 0 {
			x += n
			continue
		}
		moved := false
		for f := range c.fetches {
			if f.cur <= x && x < f.end {
				x = f.end
				moved = true
			}
		}
		if !moved {
			return x
		}
	}
	return x
}

// nextClaimedLocked: next byte after x that is already present or being loaded.
func (c *Cache) nextClaimedLocked(x int64) int64 {
	n := c.have.nextStart(x, c.Size)
	for f := range c.fetches {
		if f.cur > x && f.cur < n {
			n = f.cur
		}
	}
	return n
}

func (c *Cache) startLocked(start, end int64, urgent bool) {
	if start >= end {
		return
	}
	c.fetchSeq++
	f := &fetch{id: c.fetchSeq, start: start, cur: start, end: end, urgent: urgent, began: time.Now()}
	c.fetches[f] = struct{}{}
	c.wg.Add(1)
	go c.runFetch(f)
}

// ---------------------------------------------------------------- Fetching

type permanentError struct{ error }

func (c *Cache) runFetch(f *fetch) {
	defer c.wg.Done()
	defer func() {
		c.mu.Lock()
		delete(c.fetches, f)
		c.scheduleLocked()
		c.cond.Broadcast()
		c.mu.Unlock()
	}()

	for {
		err := c.fetchOnce(f)
		if err == nil || c.ctx.Err() != nil {
			return
		}
		c.mu.Lock()
		f.attempts++
		done := f.cur >= f.end
		var perm permanentError
		if errors.As(err, &perm) {
			c.fatal = perm.error
			c.cond.Broadcast()
			c.mu.Unlock()
			c.cfg.Events.Emit(c.Label, "fetch_fatal", events.Error,
				"Server refuses the file, cache gives up: "+perm.Error())
			return
		}
		if done {
			c.mu.Unlock()
			return
		}
		if f.attempts >= c.cfg.Retries {
			c.cooldown = time.Now().Add(5 * time.Second)
			c.mu.Unlock()
			c.cfg.Events.Emit(c.Label, "fetch_giveup", events.Error,
				fmt.Sprintf("Request at %.0f MB given up after %d attempts, pausing 5 s: %v",
					float64(f.cur)/1e6, f.attempts, err))
			return
		}
		c.mu.Unlock()
		c.cfg.Events.Emit(c.Label, "fetch_retry", events.Warn,
			fmt.Sprintf("Request at %.0f MB aborted (attempt %d), resuming: %v",
				float64(f.cur)/1e6, f.attempts, err))
		select {
		case <-time.After(time.Duration(f.attempts) * 500 * time.Millisecond):
		case <-c.ctx.Done():
			return
		}
	}
}

// fetchOnce loads [f.cur, f.end) in one request. It ends early if f.end
// was shortened or another fetch has already loaded the next bytes.
func (c *Cache) fetchOnce(f *fetch) error {
	c.mu.Lock()
	pos, end := f.cur, f.end
	b := c.blockOf(pos)
	bf, err := c.fileLocked(b)
	c.mu.Unlock()
	if err != nil {
		return err
	}
	defer func() {
		c.mu.Lock()
		bf.pins--
		c.mu.Unlock()
	}()
	if pos >= end {
		return nil
	}

	ctx, cancel := context.WithCancel(c.ctx)
	defer cancel()
	stall := time.AfterFunc(c.cfg.Stall, cancel)
	defer stall.Stop()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.url, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", pos, end-1))
	resp, err := c.cfg.Client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	switch {
	case resp.StatusCode == http.StatusUnauthorized, resp.StatusCode == http.StatusForbidden,
		resp.StatusCode == http.StatusNotFound:
		return permanentError{fmt.Errorf("HTTP %d", resp.StatusCode)}
	case resp.StatusCode != http.StatusPartialContent:
		// Only accept 206: a 200 would deliver the file start at the wrong offset.
		return fmt.Errorf("HTTP %d instead of 206", resp.StatusCode)
	}
	if cr := resp.Header.Get("Content-Range"); !strings.HasPrefix(cr, fmt.Sprintf("bytes %d-", pos)) {
		return fmt.Errorf("wrong Content-Range %q", cr)
	}

	base := c.blockStart(b)
	buf := make([]byte, 256<<10)
	for {
		want := min(int64(len(buf)), end-pos)
		n, rerr := io.ReadFull(resp.Body, buf[:want])
		if n > 0 {
			stall.Reset(c.cfg.Stall)
			c.mu.Lock()
			limit := f.end
			c.mu.Unlock()
			m := min(int64(n), limit-pos)
			if m > 0 {
				if _, err := bf.f.WriteAt(buf[:m], pos-base); err != nil {
					return err
				}
				now := time.Now()
				c.mu.Lock()
				c.have.add(pos, pos+m)
				pos += m
				f.cur = pos
				c.fetched += m
				c.rate.add(m, now)
				c.cond.Broadcast()
				stop := f.cur >= f.end || c.have.contiguousFrom(f.cur) > 0
				c.mu.Unlock()
				if stop {
					return nil
				}
			} else {
				return nil // shortened: an urgent request has taken over
			}
		}
		if rerr != nil {
			if pos >= end {
				return nil
			}
			if ctx.Err() != nil && c.ctx.Err() == nil {
				return fmt.Errorf("%s without data", c.cfg.Stall)
			}
			return rerr
		}
	}
}

// ---------------------------------------------------------------- Cleanup

func (c *Cache) maintain() {
	defer c.wg.Done()
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-t.C:
		}
		c.mu.Lock()
		c.evictLocked()
		c.scheduleLocked() // e.g. after the pause has expired
		c.mu.Unlock()
	}
}

// evictLocked deletes whole block files no reader needs anymore: far behind
// the rearmost read head or far ahead of the frontmost one.
// First and last block stay (ffmpeg reads header and index on every start).
func (c *Cache) evictLocked() {
	if len(c.readers) == 0 {
		return
	}
	lo, hi := int64(-1), int64(0)
	for r := range c.readers {
		if lo < 0 || r.pos < lo {
			lo = r.pos
		}
		hi = max(hi, r.pos)
	}
	var behind, ahead int64
	for b, bf := range c.files {
		if b == 0 || b == c.lastBlock() || bf.pins > 0 {
			continue
		}
		s, e := c.blockStart(b), c.blockEnd(c.blockStart(b))
		busy := false
		for f := range c.fetches {
			if f.cur < e && f.end > s {
				busy = true
			}
		}
		if busy {
			continue
		}
		switch {
		case e <= lo-c.cfg.KeepBehind:
			behind += c.have.covered(s, e)
		case s >= hi+c.cfg.ReadAhead+c.cfg.BlockSize:
			ahead += c.have.covered(s, e)
		default:
			continue
		}
		bf.f.Close()
		os.Remove(bf.f.Name())
		delete(c.files, b)
		c.have.remove(s, e)
	}
	c.evicted += behind + ahead
	if ahead > 0 {
		c.cfg.Events.Emit(c.Label, "cache_evict", events.Info,
			fmt.Sprintf("%.0f MB discarded: far ahead of the read head after a backward seek", float64(ahead)/1e6))
	}
	// Blocks behind the read head are routine and only show up in the stats.
}

// Err returns the permanent error (e.g. invalid token), otherwise nil.
func (c *Cache) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return ErrClosed
	}
	return c.fatal
}

// Idle reports how long nobody has read (0 if someone is reading right now).
func (c *Cache) Idle() time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.readers) > 0 || len(c.waits) > 0 {
		return 0
	}
	return time.Since(c.lastUse)
}

// ContiguousEnd returns the end of the contiguous present range from off.
func (c *Cache) ContiguousEnd(off int64) int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return off + c.have.contiguousFrom(off)
}
