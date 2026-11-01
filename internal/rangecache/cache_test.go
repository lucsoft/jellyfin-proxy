package rangecache

import (
	"bytes"
	"context"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"jellyfin-proxy/internal/events"
)

// rangeServer serves data with range support, throttled per connection,
// and breaks off every failEvery-th request midway.
type rangeServer struct {
	data      []byte
	bps       int64 // bytes/s per connection, 0 = unthrottled
	failEvery int64
	requests  atomic.Int64
	mu        sync.Mutex
	ranges    []string
}

func (s *rangeServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	n := s.requests.Add(1)
	rh := strings.TrimPrefix(r.Header.Get("Range"), "bytes=")
	parts := strings.SplitN(rh, "-", 2)
	start, _ := strconv.ParseInt(parts[0], 10, 64)
	end := int64(len(s.data)) - 1
	if parts[1] != "" {
		end, _ = strconv.ParseInt(parts[1], 10, 64)
	}
	s.mu.Lock()
	s.ranges = append(s.ranges, rh)
	s.mu.Unlock()
	w.Header().Set("Content-Range", "bytes "+parts[0]+"-"+strconv.FormatInt(end, 10)+"/"+strconv.Itoa(len(s.data)))
	w.Header().Set("Content-Length", strconv.FormatInt(end-start+1, 10))
	w.WriteHeader(http.StatusPartialContent)
	body := s.data[start : end+1]
	if s.failEvery > 0 && n%s.failEvery == 0 && len(body) > 1000 {
		w.Write(body[:len(body)/2])
		panic(http.ErrAbortHandler) // connection breaks off
	}
	const piece = 16 << 10
	for len(body) > 0 {
		k := min(piece, len(body))
		if _, err := w.Write(body[:k]); err != nil {
			return
		}
		body = body[k:]
		if s.bps > 0 {
			time.Sleep(time.Duration(int64(k) * int64(time.Second) / s.bps))
		}
	}
}

func testCache(t *testing.T, srv *rangeServer, mod func(*Config)) *Cache {
	t.Helper()
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	cfg := Config{
		Dir: t.TempDir(), BlockSize: 1 << 20, Workers: 6, Reserve: 2,
		ReadAhead: 4 << 20, KeepBehind: 1 << 20, Retries: 3, Stall: 5 * time.Second,
		Client: ts.Client(), Events: events.New(100, nil),
	}
	if mod != nil {
		mod(&cfg)
	}
	c, err := Open(context.Background(), cfg, "abc", "abc", ts.URL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close("test") })
	return c
}

func randomData(n int) []byte {
	b := make([]byte, n)
	rand.New(rand.NewSource(1)).Read(b)
	return b
}

func readAll(t *testing.T, r *Reader) []byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var out bytes.Buffer
	buf := make([]byte, 100_000)
	for {
		n, err := r.Read(ctx, buf)
		out.Write(buf[:n])
		if err == io.EOF {
			return out.Bytes()
		}
		if err != nil {
			t.Fatalf("read at %d: %v", out.Len(), err)
		}
	}
}

func TestSequentialReadIsExact(t *testing.T) {
	srv := &rangeServer{data: randomData(10<<20 + 12345)}
	c := testCache(t, srv, nil)
	r := c.NewReader("t", 0)
	defer r.Close()
	if got := readAll(t, r); !bytes.Equal(got, srv.data) {
		t.Fatal("data differs")
	}
}

func TestRetriesResumeAfterBrokenConnection(t *testing.T) {
	srv := &rangeServer{data: randomData(6 << 20), failEvery: 3}
	c := testCache(t, srv, nil)
	r := c.NewReader("t", 0)
	defer r.Close()
	if got := readAll(t, r); !bytes.Equal(got, srv.data) {
		t.Fatal("data differs")
	}
}

// A reader must not wait for the whole block: at 1 MiB/s per connection
// the first byte of an 8 MiB block has to arrive right away.
func TestReaderDoesNotWaitForWholeBlock(t *testing.T) {
	srv := &rangeServer{data: randomData(32 << 20), bps: 1 << 20}
	c := testCache(t, srv, func(cfg *Config) { cfg.BlockSize = 8 << 20 })
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	buf := make([]byte, 1000)
	t0 := time.Now()
	off := int64(20<<20 + 777) // in the middle of the third block
	n, err := c.ReadAt(ctx, buf, off)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf[:n], srv.data[off:off+int64(n)]) {
		t.Fatal("data differs")
	}
	if el := time.Since(t0); el > time.Second {
		t.Fatalf("first byte after %v – apparently waiting for the whole block", el)
	}
}

func TestEvictsBehindReader(t *testing.T) {
	srv := &rangeServer{data: randomData(12 << 20)}
	c := testCache(t, srv, nil)
	r := c.NewReader("t", 0)
	defer r.Close()
	readAll(t, r)
	c.mu.Lock()
	c.evictLocked()
	have := c.have.total()
	c.mu.Unlock()
	// Kept: first block, last block, KeepBehind (1 MiB) before the end.
	if have > 3<<20 {
		t.Fatalf("%d MB still present after reading", have>>20)
	}
}

func TestIntervals(t *testing.T) {
	var iv intervals
	iv.add(10, 20)
	iv.add(30, 40)
	iv.add(20, 30)
	if len(iv) != 1 || iv[0] != (span{10, 40}) {
		t.Fatalf("merge: %v", iv)
	}
	iv.remove(15, 25)
	if len(iv) != 2 || iv.total() != 20 {
		t.Fatalf("remove: %v", iv)
	}
	if iv.contiguousFrom(12) != 3 || iv.contiguousFrom(15) != 0 || iv.nextStart(15, 100) != 25 {
		t.Fatalf("query: %v", iv)
	}
}

// A reader waiting at the end of a running block must not crawl through the
// block at the speed of one connection: free connections take over the back
// half of the remainder.
func TestWaitingReaderSplitsSlowFetch(t *testing.T) {
	srv := &rangeServer{data: randomData(16 << 20), bps: 1 << 20}
	c := testCache(t, srv, func(cfg *Config) {
		cfg.BlockSize, cfg.Workers, cfg.Reserve, cfg.ReadAhead = 16<<20, 6, 2, 16<<20
	})
	// One large request without catch-up mode, as in normal operation.
	c.mu.Lock()
	c.startLocked(0, 16<<20, false)
	c.mu.Unlock()
	r := c.NewReader("t", 0)
	defer r.Close()
	t0 := time.Now()
	if got := readAll(t, r); !bytes.Equal(got, srv.data) {
		t.Fatal("data differs")
	}
	// One connection would need 16 s.
	if el := time.Since(t0); el > 9*time.Second {
		t.Fatalf("16 MiB in %v – request was not split", el)
	}
	if c.Stats().Splits == 0 {
		t.Fatal("no split counted")
	}
}

// A saved cache is restored on the next open and serves the same data
// without loading it again.
func TestSaveAndRestore(t *testing.T) {
	srv := &rangeServer{data: randomData(6 << 20)}
	ts := httptest.NewServer(srv)
	defer ts.Close()
	cfg := Config{Dir: t.TempDir(), BlockSize: 1 << 20, Workers: 4, Reserve: 1,
		ReadAhead: 8 << 20, KeepBehind: 8 << 20, Retries: 3, Stall: 5 * time.Second,
		Client: ts.Client(), Events: events.New(100, nil)}
	c, err := Open(context.Background(), cfg, "film", "film", ts.URL)
	if err != nil {
		t.Fatal(err)
	}
	r := c.NewReader("t", 0)
	readAll(t, r)
	r.Close()
	c.Save("test")

	before := srv.requests.Load()
	c2, err := Open(context.Background(), cfg, "film", "film", ts.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close("test")
	if c2.Stats().Have != int64(len(srv.data)) {
		t.Fatalf("restored: %d of %d bytes", c2.Stats().Have, len(srv.data))
	}
	r2 := c2.NewReader("t", 0)
	defer r2.Close()
	if got := readAll(t, r2); !bytes.Equal(got, srv.data) {
		t.Fatal("data differs after restore")
	}
	// only the size check, no data requests
	if n := srv.requests.Load() - before; n > 1 {
		t.Fatalf("%d requests after restore, expected 1", n)
	}
}
