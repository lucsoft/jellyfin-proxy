package hls

import (
	"sort"
	"time"
)

type Stats struct {
	ID            string       `json:"id"`
	Label         string       `json:"label"`
	Audio         string       `json:"audio"`
	Segments      int          `json:"segments"`
	Duration      float64      `json:"duration"`
	Ready         [][2]int     `json:"ready"`      // contiguous ranges of finished segments
	ReadyTime     [][2]float64 `json:"ready_time"` // the same ranges in movie time
	ClientSegment int          `json:"client_segment"`
	ClientTime    float64      `json:"client_time"`
	ClientIdleMs  int64        `json:"client_idle_ms"`
	AheadSec      float64      `json:"ahead_sec"` // finished segments without gaps after the client position
	Run           *RunStats    `json:"run,omitempty"`
	Restarts      int          `json:"restarts"`
	Deleted       int          `json:"deleted"`
	Served        int          `json:"served"`
	ServedBytes   int64        `json:"served_bytes"`
	SlowSends     int          `json:"slow_sends"`
	Recent        []Request    `json:"recent"`
	Failed        string       `json:"failed,omitempty"`
	AgeMs         int64        `json:"age_ms"`
}

type RunStats struct {
	ID        int      `json:"id"`
	Start     int      `json:"start"`
	StartTime float64  `json:"start_time"`
	Next      int      `json:"next"`
	NextTime  float64  `json:"next_time"`
	State     string   `json:"state"`
	Pauses    int      `json:"pauses"`
	Reason    string   `json:"reason"`
	AgeMs     int64    `json:"age_ms"`
	Pid       int      `json:"pid"`
	Err       string   `json:"err,omitempty"`
	Stderr    []string `json:"stderr,omitempty"`
}

func (s *Session) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := Stats{
		ID: s.ID, Label: s.Label, Audio: s.audio.Label, Segments: len(s.tl.Segments),
		Duration: s.tl.Duration, ClientSegment: s.lastReq, ClientIdleMs: time.Since(s.lastReqAt).Milliseconds(),
		Restarts: s.restarts, Deleted: s.deleted, Served: s.served, ServedBytes: s.servedB, SlowSends: s.slowSends,
		Recent: append([]Request(nil), s.recent...), AgeMs: time.Since(s.created).Milliseconds(),
	}
	if s.lastReq >= 0 {
		st.ClientTime = s.tl.Segments[s.lastReq].Start
		for k := s.lastReq + 1; ; k++ {
			if _, ok := s.ready[k]; !ok {
				break
			}
			st.AheadSec += s.tl.Segments[k].Duration
		}
	}
	if s.failed != nil {
		st.Failed = s.failed.Error()
	}
	ks := make([]int, 0, len(s.ready))
	for k := range s.ready {
		ks = append(ks, k)
	}
	sort.Ints(ks)
	for _, k := range ks {
		if n := len(st.Ready); n > 0 && st.Ready[n-1][1] == k-1 {
			st.Ready[n-1][1] = k
		} else {
			st.Ready = append(st.Ready, [2]int{k, k})
		}
	}
	for _, rg := range st.Ready {
		a, b := s.tl.Segments[rg[0]], s.tl.Segments[rg[1]]
		st.ReadyTime = append(st.ReadyTime, [2]float64{a.Start, b.Start + b.Duration})
	}
	if r := s.run; r != nil {
		rs := &RunStats{ID: r.id, Pauses: r.pauses, Start: r.start, StartTime: s.tl.Segments[r.start].Start, Next: r.next, Reason: r.reason,
			AgeMs: time.Since(r.began).Milliseconds(), Stderr: lastN(r.stderr, 8)}
		if r.next < len(s.tl.Segments) {
			rs.NextTime = s.tl.Segments[r.next].Start
		} else {
			rs.NextTime = s.tl.Duration
		}
		if r.cmd != nil && r.cmd.Process != nil {
			rs.Pid = r.cmd.Process.Pid
		}
		switch {
		case r.exited && r.err != nil:
			rs.State, rs.Err = "crashed", r.err.Error()
		case r.exited:
			rs.State = "done"
		case r.paused:
			rs.State = "paused"
		default:
			rs.State = "running"
		}
		st.Run = rs
	}
	return st
}
