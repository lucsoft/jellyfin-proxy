package rangecache

import "time"

// rateMeter counts bytes in per-second buckets over the last 60 s.
type rateMeter struct {
	buckets [60]int64
	stamp   [60]int64 // Unix second the bucket belongs to
}

func (r *rateMeter) add(n int64, now time.Time) {
	sec := now.Unix()
	i := sec % 60
	if r.stamp[i] != sec {
		r.stamp[i] = sec
		r.buckets[i] = 0
	}
	r.buckets[i] += n
}

// mbps returns the average over the last `window` full seconds.
func (r *rateMeter) mbps(window int, now time.Time) float64 {
	sec := now.Unix()
	var sum int64
	for k := 1; k <= window; k++ {
		s := sec - int64(k)
		i := s % 60
		if r.stamp[i] == s {
			sum += r.buckets[i]
		}
	}
	return float64(sum) * 8 / float64(window) / 1e6
}
