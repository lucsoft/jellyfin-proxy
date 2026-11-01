// Package events collects events with their reason so the dashboard can
// explain WHY something happened (restart, pause, cache discarded …).
package events

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"
)

type Level string

const (
	Info  Level = "info"
	Warn  Level = "warn"
	Error Level = "error"
)

type Event struct {
	Seq    uint64         `json:"seq"`
	Time   time.Time      `json:"time"`
	Source string         `json:"source"` // short source ID or "proxy"
	Kind   string         `json:"kind"`   // machine-readable, e.g. "ffmpeg_restart"
	Level  Level          `json:"level"`
	Msg    string         `json:"msg"` // for humans: what and why
	Attrs  map[string]any `json:"attrs,omitempty"`
}

// Log is a ring buffer of the latest events, optionally backed by a JSONL file.
type Log struct {
	mu     sync.Mutex
	seq    uint64
	ring   []Event
	max    int
	file   *os.File
	logger *slog.Logger
}

func New(max int, logger *slog.Logger) *Log {
	return &Log{max: max, logger: logger}
}

// SetFile additionally appends every event as a JSON line to path.
func (l *Log) SetFile(path string) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	l.mu.Lock()
	l.file = f
	l.mu.Unlock()
	return nil
}

// Emit records an event. kv alternates keys and values.
func (l *Log) Emit(source, kind string, level Level, msg string, kv ...any) {
	ev := Event{Time: time.Now(), Source: source, Kind: kind, Level: level, Msg: msg}
	if len(kv) > 0 {
		ev.Attrs = make(map[string]any, len(kv)/2)
		for i := 0; i+1 < len(kv); i += 2 {
			ev.Attrs[fmt.Sprint(kv[i])] = kv[i+1]
		}
	}

	l.mu.Lock()
	l.seq++
	ev.Seq = l.seq
	l.ring = append(l.ring, ev)
	if len(l.ring) > l.max {
		l.ring = l.ring[len(l.ring)-l.max:]
	}
	if l.file != nil {
		if b, err := json.Marshal(ev); err == nil {
			l.file.Write(append(b, '\n'))
		}
	}
	l.mu.Unlock()

	if l.logger != nil {
		attrs := append([]any{"src", source, "kind", kind}, kv...)
		switch level {
		case Error:
			l.logger.Error(msg, attrs...)
		case Warn:
			l.logger.Warn(msg, attrs...)
		default:
			l.logger.Info(msg, attrs...)
		}
	}
}

// Since returns all events with Seq > seq (oldest first).
func (l *Log) Since(seq uint64) []Event {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]Event, 0, len(l.ring))
	for _, ev := range l.ring {
		if ev.Seq > seq {
			out = append(out, ev)
		}
	}
	return out
}
