package rangecache

import (
	"sort"
	"time"
)

type Stats struct {
	ID         string        `json:"id"`
	Label      string        `json:"label"`
	Size       int64         `json:"size"`
	BlockSize  int64         `json:"block_size"`
	Have       int64         `json:"have"`
	Fetched    int64         `json:"fetched"`
	Evicted    int64         `json:"evicted"`
	Splits     int           `json:"splits"`
	Mbps5      float64       `json:"mbps_5s"`
	Mbps30     float64       `json:"mbps_30s"`
	Readers    []ReaderStats `json:"readers"`
	Fetches    []FetchStats  `json:"fetches"`
	Waiting    []int64       `json:"waiting"`
	Map        []float64     `json:"map"` // fill level per section 0..1
	Fatal      string        `json:"fatal,omitempty"`
	CooldownMs int64         `json:"cooldown_ms,omitempty"`
	IdleMs     int64         `json:"idle_ms"`
	AgeMs      int64         `json:"age_ms"`
}

type ReaderStats struct {
	Name  string `json:"name"`
	Pos   int64  `json:"pos"`
	Ahead int64  `json:"ahead"` // contiguously present from the read head
	Read  int64  `json:"read"`
	AgeMs int64  `json:"age_ms"`
}

type FetchStats struct {
	ID       int     `json:"id"`
	Start    int64   `json:"start"`
	Cur      int64   `json:"cur"`
	End      int64   `json:"end"`
	Urgent   bool    `json:"urgent"`
	Mbps     float64 `json:"mbps"`
	AgeMs    int64   `json:"age_ms"`
	Attempts int     `json:"attempts"`
}

const mapBuckets = 240

func (c *Cache) Stats() Stats {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	s := Stats{
		ID: c.ID, Label: c.Label, Size: c.Size, BlockSize: c.cfg.BlockSize,
		Have: c.have.total(), Fetched: c.fetched, Evicted: c.evicted, Splits: c.splits,
		Mbps5: c.rate.mbps(5, now), Mbps30: c.rate.mbps(30, now),
		AgeMs: now.Sub(c.created).Milliseconds(),
	}
	if c.fatal != nil {
		s.Fatal = c.fatal.Error()
	}
	if d := c.cooldown.Sub(now); d > 0 {
		s.CooldownMs = d.Milliseconds()
	}
	if len(c.readers) == 0 && len(c.waits) == 0 {
		s.IdleMs = now.Sub(c.lastUse).Milliseconds()
	}
	for r := range c.readers {
		s.Readers = append(s.Readers, ReaderStats{
			Name: r.Name, Pos: r.pos, Ahead: c.have.contiguousFrom(r.pos),
			Read: r.read, AgeMs: now.Sub(r.opened).Milliseconds(),
		})
	}
	sort.Slice(s.Readers, func(i, j int) bool { return s.Readers[i].Pos < s.Readers[j].Pos })
	for f := range c.fetches {
		el := now.Sub(f.began).Seconds()
		fs := FetchStats{ID: f.id, Start: f.start, Cur: f.cur, End: f.end, Urgent: f.urgent,
			AgeMs: now.Sub(f.began).Milliseconds(), Attempts: f.attempts}
		if el > 0 {
			fs.Mbps = float64(f.cur-f.start) * 8 / el / 1e6
		}
		s.Fetches = append(s.Fetches, fs)
	}
	sort.Slice(s.Fetches, func(i, j int) bool { return s.Fetches[i].Start < s.Fetches[j].Start })
	for off := range c.waits {
		s.Waiting = append(s.Waiting, off)
	}
	s.Map = make([]float64, mapBuckets)
	step := (c.Size + mapBuckets - 1) / mapBuckets
	for i := range s.Map {
		lo := int64(i) * step
		hi := min(lo+step, c.Size)
		if hi > lo {
			s.Map[i] = float64(c.have.covered(lo, hi)) / float64(hi-lo)
		}
	}
	return s
}
