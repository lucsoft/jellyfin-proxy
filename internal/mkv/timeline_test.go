package mkv

import (
	"math"
	"os"
	"testing"

	"jellyfin-proxy/internal/testmedia"
)

func TestTimelineMatchesFFprobeKeyframes(t *testing.T) {
	file := testmedia.MKV(t)
	f, err := os.Open(file)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	st, _ := f.Stat()
	tl, err := Read(f, st.Size(), 4)
	if err != nil {
		t.Fatal(err)
	}
	want := testmedia.Keyframes(t, file)
	if tl.Keyframes != len(want) {
		t.Fatalf("keyframes: %d in index, ffprobe sees %d", tl.Keyframes, len(want))
	}
	if math.Abs(tl.Duration-40) > 0.2 {
		t.Fatalf("duration %.3f", tl.Duration)
	}
	sum := 0.0
	for i, s := range tl.Segments {
		if s.Duration < 3.99 && i < len(tl.Segments)-1 {
			t.Errorf("segment %d only %.3f s", i, s.Duration)
		}
		if math.Abs(s.Start-sum) > 1e-6 {
			t.Errorf("segment %d starts at %.3f, expected %.3f", i, s.Start, sum)
		}
		sum += s.Duration
		if i > 0 && !contains(want, s.Start) {
			t.Errorf("segment %d starts at %.3f, but there is no keyframe there", i, s.Start)
		}
	}
	t.Logf("%d keyframes -> %d segments", tl.Keyframes, len(tl.Segments))
}

func contains(ts []float64, v float64) bool {
	for _, x := range ts {
		if math.Abs(x-v) < 0.002 {
			return true
		}
	}
	return false
}
