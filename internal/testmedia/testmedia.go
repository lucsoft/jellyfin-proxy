// Package testmedia generates a small MKV for tests with irregular
// keyframes and two audio tracks (AAC + AC3), similar to a real movie.
package testmedia

import (
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
)

var (
	once sync.Once
	path string
	err  error
)

// MKV returns the path to the test file (generated once per test run).
// Without ffmpeg the test is skipped.
func MKV(t *testing.T) string {
	t.Helper()
	if _, lerr := exec.LookPath("ffmpeg"); lerr != nil {
		t.Skip("ffmpeg missing")
	}
	once.Do(func() {
		dir, _ := os.MkdirTemp("", "jfproxy-testmedia-")
		path = filepath.Join(dir, "test.mkv")
		// Keyframes at irregular intervals (1.3–3.7 s) like real footage.
		cmd := exec.Command("ffmpeg", "-hide_banner", "-loglevel", "error", "-y",
			"-f", "lavfi", "-i", "testsrc2=size=320x180:rate=24:duration=40",
			"-f", "lavfi", "-i", "sine=frequency=440:duration=40",
			"-f", "lavfi", "-i", "sine=frequency=880:duration=40",
			"-map", "0:v", "-map", "1:a", "-map", "2:a",
			"-c:v", "libx264", "-preset", "ultrafast", "-g", "1000", "-sc_threshold", "0",
			"-force_key_frames", "0,1.5,4,5.3,9,11.2,13,16.7,18,21.5,24,25.3,29,31.2,33,36.7,38",
			"-c:a:0", "aac", "-c:a:1", "ac3", "-ac", "2",
			path)
		var out []byte
		out, err = cmd.CombinedOutput()
		if err != nil {
			err = &ffErr{err, string(out)}
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	return path
}

type ffErr struct {
	err error
	out string
}

func (e *ffErr) Error() string { return e.err.Error() + ": " + e.out }

// Keyframes returns the keyframe times according to ffprobe (reference for tests).
func Keyframes(t *testing.T, file string) []float64 {
	t.Helper()
	out, err := exec.Command("ffprobe", "-v", "error", "-select_streams", "v:0",
		"-skip_frame", "nokey", "-show_entries", "frame=pts_time", "-of", "csv=p=0", file).Output()
	if err != nil {
		t.Fatal(err)
	}
	var ts []float64
	for _, l := range strings.Fields(string(out)) {
		v, _ := strconv.ParseFloat(strings.TrimSuffix(l, ","), 64)
		ts = append(ts, v)
	}
	return ts
}
