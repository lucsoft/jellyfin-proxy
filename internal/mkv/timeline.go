// Package mkv reads the keyframes from the Matroska index (Cues) and builds
// fixed HLS segment boundaries from them. Because the boundaries are known
// up front, the proxy can serve a complete VOD playlist (correct timeline,
// seek anywhere) and restart ffmpeg at any segment – segments from different
// ffmpeg runs are cut identically and interchangeable.
package mkv

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
)

const (
	idEBML        = 0x1A45DFA3
	idSegment     = 0x18538067
	idSeekHead    = 0x114D9B74
	idSeek        = 0x4DBB
	idSeekID      = 0x53AB
	idSeekPos     = 0x53AC
	idInfo        = 0x1549A966
	idTimescale   = 0x2AD7B1
	idDuration    = 0x4489
	idTracks      = 0x1654AE6B
	idTrackEntry  = 0xAE
	idTrackNumber = 0xD7
	idTrackType   = 0x83
	idCues        = 0x1C53BB6B
	idCuePoint    = 0xBB
	idCueTime     = 0xB3
	idCueTrackPos = 0xB7
	idCueTrack    = 0xF7
	idCueCluster  = 0xF1
	idCluster     = 0x1F43B675
)

type Keyframe struct {
	Time   float64 // seconds
	Offset int64   // file offset of the cluster
}

type Segment struct {
	Index    int
	Start    float64
	Duration float64
	Offset   int64 // file offset of the start cluster
}

type Timeline struct {
	Duration  float64
	Keyframes int
	Segments  []Segment
}

// SegmentAt returns the index of the segment containing time t.
func (tl *Timeline) SegmentAt(t float64) int {
	i := sort.Search(len(tl.Segments), func(i int) bool { return tl.Segments[i].Start > t })
	return max(i-1, 0)
}

// TimeAtOffset estimates the playback time for a file offset.
func (tl *Timeline) TimeAtOffset(off int64) float64 {
	segs := tl.Segments
	i := sort.Search(len(segs), func(i int) bool { return segs[i].Offset > off })
	if i == 0 {
		return 0
	}
	s := segs[i-1]
	if i == len(segs) {
		return s.Start
	}
	next := segs[i]
	if next.Offset == s.Offset {
		return s.Start
	}
	return s.Start + s.Duration*float64(off-s.Offset)/float64(next.Offset-s.Offset)
}

// Read builds the timeline. target is the desired segment length in seconds.
func Read(r io.ReaderAt, size int64, target float64) (*Timeline, error) {
	p := parser{r: r, size: size}

	id, ebmlSize, dataOff, err := p.header(0)
	if err != nil || id != idEBML {
		return nil, fmt.Errorf("not a Matroska file")
	}
	off := dataOff + ebmlSize
	id, segSize, segData, err := p.header(off)
	if err != nil || id != idSegment {
		return nil, fmt.Errorf("no Segment element")
	}
	segEnd := size
	if segSize >= 0 {
		segEnd = min(segData+segSize, size)
	}

	var cuesPos, infoPos, tracksPos int64 = -1, -1, -1
	seekHeads := []int64{}
	// Walk the header area up to the first cluster.
	for off = segData; off < segEnd; {
		id, sz, data, err := p.header(off)
		if err != nil {
			return nil, err
		}
		if id == idCluster || sz < 0 {
			break
		}
		switch id {
		case idSeekHead:
			seekHeads = append(seekHeads, off)
		case idInfo:
			infoPos = off
		case idTracks:
			tracksPos = off
		case idCues:
			cuesPos = off
		}
		off = data + sz
	}
	// SeekHeads point to Cues at the end of the file (and possibly more SeekHeads).
	for i := 0; i < len(seekHeads) && i < 4; i++ {
		entries, err := p.seekHead(seekHeads[i])
		if err != nil {
			return nil, err
		}
		for _, e := range entries {
			pos := segData + e.pos
			switch e.id {
			case idCues:
				if cuesPos < 0 {
					cuesPos = pos
				}
			case idInfo:
				if infoPos < 0 {
					infoPos = pos
				}
			case idTracks:
				if tracksPos < 0 {
					tracksPos = pos
				}
			case idSeekHead:
				if pos != seekHeads[i] {
					seekHeads = append(seekHeads, pos)
				}
			}
		}
	}
	if infoPos < 0 || tracksPos < 0 {
		return nil, fmt.Errorf("Info or Tracks missing")
	}
	if cuesPos < 0 {
		return nil, fmt.Errorf("file has no index (Cues) – no fixed timeline without a keyframe list")
	}

	scale, duration, err := p.info(infoPos)
	if err != nil {
		return nil, err
	}
	video, err := p.videoTrack(tracksPos)
	if err != nil {
		return nil, err
	}
	kfs, err := p.cues(cuesPos, video, scale, segData)
	if err != nil {
		return nil, err
	}
	if len(kfs) == 0 {
		return nil, fmt.Errorf("index contains no keyframes of the video track")
	}
	if duration <= 0 {
		duration = kfs[len(kfs)-1].Time + target
	}
	return &Timeline{Duration: duration, Keyframes: len(kfs), Segments: buildSegments(kfs, duration, target)}, nil
}

// buildSegments groups keyframes into segments of at least target seconds.
// A very short final segment is merged into the second to last one.
func buildSegments(kfs []Keyframe, duration, target float64) []Segment {
	var segs []Segment
	for i := 0; i < len(kfs); {
		j := i + 1
		for j < len(kfs) && kfs[j].Time-kfs[i].Time < target-0.001 {
			j++
		}
		start := kfs[i].Time
		if i == 0 {
			start = 0
		}
		end := duration
		if j < len(kfs) {
			end = kfs[j].Time
		}
		segs = append(segs, Segment{Index: len(segs), Start: start, Duration: end - start, Offset: kfs[i].Offset})
		i = j
	}
	if n := len(segs); n >= 2 && segs[n-1].Duration < target/2 {
		segs[n-2].Duration += segs[n-1].Duration
		segs = segs[:n-1]
	}
	return segs
}

// ---------------------------------------------------------------- EBML

type parser struct {
	r    io.ReaderAt
	size int64
}

func (p *parser) read(off int64, n int) ([]byte, error) {
	if off < 0 || off+int64(n) > p.size {
		return nil, io.ErrUnexpectedEOF
	}
	b := make([]byte, n)
	_, err := p.r.ReadAt(b, off)
	return b, err
}

// header reads an element's ID and size; size -1 = unknown.
func (p *parser) header(off int64) (id uint64, size int64, data int64, err error) {
	b, err := p.read(off, min(12, int(p.size-off)))
	if err != nil {
		return 0, 0, 0, err
	}
	id, n, err := vint(b, true)
	if err != nil {
		return 0, 0, 0, err
	}
	sz, m, err := vint(b[n:], false)
	if err != nil {
		return 0, 0, 0, err
	}
	size = int64(sz)
	if sz == math.MaxUint64 {
		size = -1
	}
	return id, size, off + int64(n+m), nil
}

// vint decodes a variable-length EBML number. IDs keep the marker bit.
func vint(b []byte, keepMarker bool) (uint64, int, error) {
	if len(b) == 0 || b[0] == 0 {
		return 0, 0, errors.New("invalid EBML number")
	}
	n := 1
	for mask := byte(0x80); b[0]&mask == 0; mask >>= 1 {
		n++
	}
	if n > 8 || len(b) < n {
		return 0, 0, errors.New("invalid EBML number")
	}
	v := uint64(b[0])
	allOnes := true
	if !keepMarker {
		v &= uint64(0xFF >> n)
		allOnes = v == uint64(0xFF>>n)
	}
	for i := 1; i < n; i++ {
		v = v<<8 | uint64(b[i])
		if b[i] != 0xFF {
			allOnes = false
		}
	}
	if !keepMarker && allOnes {
		return math.MaxUint64, n, nil
	}
	return v, n, nil
}

type child struct {
	id   uint64
	data []byte
}

// children reads an element completely and splits its content.
func (p *parser) children(off int64, want uint64) ([]child, error) {
	id, size, data, err := p.header(off)
	if err != nil {
		return nil, err
	}
	if id != want {
		return nil, fmt.Errorf("expected element %X at %d, found %X", want, off, id)
	}
	if size < 0 || size > 64<<20 {
		return nil, fmt.Errorf("element %X has implausible size %d", id, size)
	}
	b, err := p.read(data, int(size))
	if err != nil {
		return nil, err
	}
	return split(b)
}

func split(b []byte) ([]child, error) {
	var out []child
	for len(b) > 0 {
		id, n, err := vint(b, true)
		if err != nil {
			return nil, err
		}
		sz, m, err := vint(b[n:], false)
		if err != nil {
			return nil, err
		}
		if sz > uint64(len(b)-n-m) {
			return nil, errors.New("element extends beyond its parent")
		}
		out = append(out, child{id, b[n+m : n+m+int(sz)]})
		b = b[n+m+int(sz):]
	}
	return out, nil
}

func uintVal(b []byte) uint64 {
	var v uint64
	for _, x := range b {
		v = v<<8 | uint64(x)
	}
	return v
}

func floatVal(b []byte) float64 {
	switch len(b) {
	case 4:
		return float64(math.Float32frombits(binary.BigEndian.Uint32(b)))
	case 8:
		return math.Float64frombits(binary.BigEndian.Uint64(b))
	}
	return 0
}

type seekEntry struct {
	id  uint64
	pos int64
}

func (p *parser) seekHead(off int64) ([]seekEntry, error) {
	cs, err := p.children(off, idSeekHead)
	if err != nil {
		return nil, err
	}
	var out []seekEntry
	for _, c := range cs {
		if c.id != idSeek {
			continue
		}
		sub, err := split(c.data)
		if err != nil {
			return nil, err
		}
		var e seekEntry
		for _, s := range sub {
			switch s.id {
			case idSeekID:
				e.id = uintVal(s.data)
			case idSeekPos:
				e.pos = int64(uintVal(s.data))
			}
		}
		out = append(out, e)
	}
	return out, nil
}

func (p *parser) info(off int64) (scale uint64, duration float64, err error) {
	cs, err := p.children(off, idInfo)
	if err != nil {
		return 0, 0, err
	}
	scale = 1_000_000
	var dur float64
	for _, c := range cs {
		switch c.id {
		case idTimescale:
			scale = uintVal(c.data)
		case idDuration:
			dur = floatVal(c.data)
		}
	}
	return scale, dur * float64(scale) / 1e9, nil
}

func (p *parser) videoTrack(off int64) (uint64, error) {
	cs, err := p.children(off, idTracks)
	if err != nil {
		return 0, err
	}
	for _, c := range cs {
		if c.id != idTrackEntry {
			continue
		}
		sub, err := split(c.data)
		if err != nil {
			return 0, err
		}
		var num, typ uint64
		for _, s := range sub {
			switch s.id {
			case idTrackNumber:
				num = uintVal(s.data)
			case idTrackType:
				typ = uintVal(s.data)
			}
		}
		if typ == 1 {
			return num, nil
		}
	}
	return 0, errors.New("no video track")
}

func (p *parser) cues(off int64, track, scale uint64, segData int64) ([]Keyframe, error) {
	cs, err := p.children(off, idCues)
	if err != nil {
		return nil, err
	}
	var kfs []Keyframe
	for _, c := range cs {
		if c.id != idCuePoint {
			continue
		}
		sub, err := split(c.data)
		if err != nil {
			return nil, err
		}
		var t uint64
		for _, s := range sub {
			if s.id == idCueTime {
				t = uintVal(s.data)
			}
		}
		for _, s := range sub {
			if s.id != idCueTrackPos {
				continue
			}
			pos, err := split(s.data)
			if err != nil {
				return nil, err
			}
			var trk, cl uint64
			for _, x := range pos {
				switch x.id {
				case idCueTrack:
					trk = uintVal(x.data)
				case idCueCluster:
					cl = uintVal(x.data)
				}
			}
			if trk == track {
				kfs = append(kfs, Keyframe{Time: float64(t) * float64(scale) / 1e9, Offset: segData + int64(cl)})
			}
		}
	}
	sort.Slice(kfs, func(i, j int) bool { return kfs[i].Time < kfs[j].Time })
	// remove duplicate timestamps
	out := kfs[:0]
	for _, k := range kfs {
		if len(out) == 0 || k.Time > out[len(out)-1].Time {
			out = append(out, k)
		}
	}
	return out, nil
}
