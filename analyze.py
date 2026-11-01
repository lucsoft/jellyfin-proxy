#!/usr/bin/env python3
"""jellyfin-proxy trace analysis.

Reads the JSONL trace file and shows per segment:
  - size and implied bitrate
  - time since the previous segment
  - realtime factor (< 1.0 = buffer shrinking)

Also: upstream probe, client wait times, overall statistics.

Usage:  python3 analyze.py /tmp/jf-trace.jsonl
"""
import json
import sys
from collections import defaultdict


def load(path):
    events = []
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                events.append(json.loads(line))
            except json.JSONDecodeError:
                pass
    return events


def hms(ms):
    sec = ms / 1000.0
    return f"{int(sec // 60)}:{sec % 60:05.2f}"


def main():
    if len(sys.argv) < 2:
        print("Usage: python3 analyze.py <trace.jsonl>")
        return 1
    events = load(sys.argv[1])
    if not events:
        print("no events found")
        return 1

    by_job = defaultdict(list)
    for ev in events:
        by_job[ev.get("id", "-")].append(ev)

    print(f"Trace: {sys.argv[1]}  ({len(events)} events, {len(by_job)} sessions)\n")

    for job_id, evs in by_job.items():
        segs = [e for e in evs if e["event"] == "seg_created"]
        probes = [e for e in evs if e["event"] == "upstream_probe"]
        served = [e for e in evs if e["event"] == "seg_served"]
        if not segs:
            continue

        print(f"═══ Session {job_id} ═══")

        for p in probes:
            print(f"  Upstream: {p.get('detail','')}  ({p.get('bytes',0)/1e6:.1f} MB in {p['ms']/1000:.1f}s)")

        start = segs[0]["ms"]
        prev = start
        total_bytes = 0
        total_video = 0.0
        slow = 0

        print(f"\n  {'Segment':<14}{'MB':>7}{'Δ time':>9}{'Mbit/s':>9}{'Realtime':>10}")
        print("  " + "─" * 48)
        for s in segs:
            mb = s.get("bytes", 0) / 1e6
            t = s["ms"]
            dt = (t - prev) / 1000.0
            # 4 s of video per segment (hls_time)
            rt = dt / 4.0 if dt > 0 else 0
            mbit = (mb * 8 / dt) if dt > 0 else 0
            total_bytes += s.get("bytes", 0)
            total_video += 4.0
            if rt > 1.0 or rt < 0.9:
                slow += 1
            marker = " ⚠" if rt > 1.0 else ""
            print(f"  {s.get('seg',''):<14}{mb:>7.1f}{dt:>8.1f}s{mbit:>9.1f}{rt:>9.2f}x{marker}")
            prev = t

        span = (segs[-1]["ms"] - start) / 1000.0
        if span > 0:
            overall = total_video / span
            rate = (total_bytes * 8 / span) / 1e6
            print("  " + "─" * 48)
            print(f"  Total:  {total_bytes/1e6:.0f} MB, {total_video:.0f}s video in {span:.1f}s")
            print(f"          {rate:.1f} Mbit/s, realtime factor {overall:.2f}x")
            if overall < 1.0:
                print(f"          ❌ Below realtime — buffer shrinking ({overall*100:.0f}% needed)")
            else:
                print(f"          ✅ Above realtime — buffer growing")

        if served:
            waits = []
            for s in served:
                d = s.get("detail", "")
                if d.startswith("wait="):
                    try:
                        waits.append(float(d[5:].rstrip("ms")))
                    except ValueError:
                        pass
            if waits:
                print(f"\n  Client wait time: Ø {sum(waits)/len(waits):.0f} ms, "
                      f"max {max(waits):.0f} ms ({len(waits)} segments)")
                stalled = sum(1 for w in waits if w > 500)
                if stalled:
                    print(f"          ⚠ {stalled} segments had to wait >500ms")
        print()

    return 0


if __name__ == "__main__":
    sys.exit(main())
