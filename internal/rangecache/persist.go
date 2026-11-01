package rangecache

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/lucsoft/jellyfin-proxy/internal/events"
)

// A cache saved on shutdown lives in <Dir>/saved-<ID> and, besides the
// block files, contains a state.json with the present ranges. The token or
// URL is deliberately not stored; it comes fresh from PlaybackInfo on the
// next request.

type savedState struct {
	ID        string     `json:"id"`
	Size      int64      `json:"size"`
	BlockSize int64      `json:"block_size"`
	Have      [][2]int64 `json:"have"`
	Saved     time.Time  `json:"saved"`
}

// SavedDir is the directory holding a source's saved cache.
func SavedDir(dir, id string) string { return filepath.Join(dir, "saved-"+sanitize(id)) }

// Save stops all downloads like Close but keeps the data for the next
// start of the proxy.
func (c *Cache) Save(reason string) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	c.cond.Broadcast()
	c.mu.Unlock()

	c.cancel()
	c.wg.Wait() // nobody writes from here on, have is complete
	c.mu.Lock()
	for _, bf := range c.files {
		bf.f.Sync()
		bf.f.Close()
	}
	st := savedState{ID: c.ID, Size: c.Size, BlockSize: c.cfg.BlockSize, Saved: time.Now()}
	for _, x := range c.have {
		st.Have = append(st.Have, [2]int64{x.s, x.e})
	}
	have := c.have.total()
	c.mu.Unlock()

	err := func() error {
		b, err := json.Marshal(st)
		if err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(c.dir, "state.json"), b, 0o600); err != nil {
			return err
		}
		dst := SavedDir(c.cfg.Dir, c.ID)
		os.RemoveAll(dst)
		return os.Rename(c.dir, dst)
	}()
	if err != nil {
		os.RemoveAll(c.dir)
		c.cfg.Events.Emit(c.Label, "cache_close", events.Warn,
			fmt.Sprintf("Cache could not be saved and was discarded (%v): %s", err, reason))
		return
	}
	c.cfg.Events.Emit(c.Label, "cache_saved", events.Info,
		fmt.Sprintf("Cache saved (%.0f MB) for the next start: %s", float64(have)/1e6, reason))
}

// restore takes over a saved cache if it matches the file.
// Afterwards the directory is a normal working directory again.
func restore(cfg Config, id string, size int64) (dir string, have intervals, ok bool) {
	src := SavedDir(cfg.Dir, id)
	b, err := os.ReadFile(filepath.Join(src, "state.json"))
	if err != nil {
		return "", nil, false
	}
	var st savedState
	if json.Unmarshal(b, &st) != nil || st.ID != id || st.Size != size || st.BlockSize != cfg.BlockSize {
		os.RemoveAll(src) // doesn't match (different file or block size)
		return "", nil, false
	}
	dir, err = os.MkdirTemp(cfg.Dir, "cache-"+sanitize(id)[:min(8, len(sanitize(id)))]+"-")
	if err != nil {
		return "", nil, false
	}
	os.Remove(dir)
	if os.Rename(src, dir) != nil {
		return "", nil, false
	}
	// Remove state.json right away: if the proxy crashes later, the ranges
	// in it would no longer be reliable.
	os.Remove(filepath.Join(dir, "state.json"))
	for _, x := range st.Have {
		have.add(x[0], x[1])
	}
	return dir, have, true
}

func sanitize(s string) string {
	b := []byte{}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_' {
			b = append(b, c)
		}
	}
	return string(b)
}
