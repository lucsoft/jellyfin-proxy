package rangecache

import "sort"

// span is a half-open byte interval [s, e).
type span struct{ s, e int64 }

// intervals is a sorted, non-overlapping set of byte ranges.
type intervals []span

// find returns the index of the first interval with e > off.
func (iv intervals) find(off int64) int {
	return sort.Search(len(iv), func(i int) bool { return iv[i].e > off })
}

func (iv *intervals) add(s, e int64) {
	if s >= e {
		return
	}
	a := *iv
	i := a.find(s - 1) // first interval that reaches s
	j := i
	for j < len(a) && a[j].s <= e {
		if a[j].s < s {
			s = a[j].s
		}
		if a[j].e > e {
			e = a[j].e
		}
		j++
	}
	out := append(a[:i:i], span{s, e})
	*iv = append(out, a[j:]...)
}

func (iv *intervals) remove(s, e int64) {
	if s >= e {
		return
	}
	var out intervals
	for _, x := range *iv {
		if x.e <= s || x.s >= e {
			out = append(out, x)
			continue
		}
		if x.s < s {
			out = append(out, span{x.s, s})
		}
		if x.e > e {
			out = append(out, span{e, x.e})
		}
	}
	*iv = out
}

// contiguousFrom returns how many contiguous bytes are present from off.
func (iv intervals) contiguousFrom(off int64) int64 {
	i := iv.find(off)
	if i < len(iv) && iv[i].s <= off {
		return iv[i].e - off
	}
	return 0
}

// nextStart returns the start of the next present range after off
// (or limit if none comes before it). off itself must be missing.
func (iv intervals) nextStart(off, limit int64) int64 {
	i := iv.find(off)
	if i < len(iv) && iv[i].s < limit {
		return iv[i].s
	}
	return limit
}

// covered sums the present bytes in [s, e).
func (iv intervals) covered(s, e int64) int64 {
	var n int64
	for _, x := range iv {
		lo, hi := max(x.s, s), min(x.e, e)
		if hi > lo {
			n += hi - lo
		}
	}
	return n
}

func (iv intervals) total() int64 {
	var n int64
	for _, x := range iv {
		n += x.e - x.s
	}
	return n
}
