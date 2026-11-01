# jellyfin-proxy

Local transcoding proxy for Jellyfin. The TV connects to this proxy, the proxy
talks to the real Jellyfin server (e.g. at a friend's place) and
**transcodes locally on the Mac** with Apple VideoToolbox.

```
TV (Moonfin)  ──►  Proxy (Mac, :8099)  ──►  Jellyfin (friend)
                        │
                        └─ ffmpeg + VideoToolbox → HLS (HEVC + AC3)
                              ▲
                              └────── TV receives the finished stream
```

## Why

Samsung TVs (2021) support **neither DTS nor TrueHD**. Jellyfin then transcodes
live on the friend's server — that is slow and costs their CPU.
With this proxy your Mac does the work locally with hardware encoding.

## Installation

Only Go and ffmpeg required — no external Go modules.

```bash
go build -o jfproxy .
```

## Usage

```bash
./jfproxy -upstream http://192.168.1.50:8096 -listen :8099
```

Then in the TV client enter the **Mac's IP with port 8099** instead of the server IP:

```
http://192.168.1.20:8099
```

## Options

| Flag | Default | Meaning |
|---|---|---|
| `-upstream` | (required) | URL of the real Jellyfin server |
| `-listen` | `:8099` | Listen address |
| `-public` | auto | URL at which the TV reaches the proxy |
| `-hwaccel` | `true` | VideoToolbox hardware encoding |
| `-vcodec` | `libx264` | Software codec when `-hwaccel=0` |
| `-vbitrate` | `12M` | Video bitrate |
| `-maxheight` | `0` | Scaling (e.g. `1080`) |
| `-acodec` | `ac3` | Target audio codec — **ac3 = Dolby Digital** |
| `-abitrate` | `640k` | Audio bitrate |
| `-achannels` | `6` | 6 = 5.1 |
| `-tmp` | OS temp | Directory for HLS segments |
| `-v` | `false` | Verbose logging |
| `-insecure` | `false` | Ignore TLS certificates |

## Example: reduce 4K material to 1080p

```bash
./jfproxy \
  -upstream http://192.168.1.50:8096 \
  -listen :8099 \
  -vbitrate 12M \
  -maxheight 1080 \
  -acodec ac3 \
  -achannels 6 \
  -v
```

## What the proxy does

1. **All API calls** are passed through 1:1 to the real server
   (auth, sessions, progress, search — everything)
2. **Only `POST /Items/{id}/PlaybackInfo`** is intercepted and rewritten:
   - `SupportsDirectPlay` → `false`
   - `TranscodingUrl` → `/proxy/stream/{id}/master.m3u8`
3. **The TV fetches the stream via the proxy**, the proxy starts ffmpeg,
   reads the **direct-play stream** from the real server and delivers **HLS**

## Limitations

- **Seeking** only works within the already buffered segments
  (HLS with `hls_list_size 0` produces linearly)
- **One transcode per MediaSource** — a restart re-encodes from scratch
- The Mac must **be running during playback**
- Double bandwidth: server → Mac → TV

## Status / debug

```bash
curl http://127.0.0.1:8099/proxy/health
```

```json
{"upstream":"...","public":"...","active":0,"hwaccel":true,"audiocodec":"ac3"}
```

## Tested

With ffmpeg 9.0.2 on macOS (Apple Silicon):

```
Input:  HEVC 1080p
Output: HEVC (hevc_videotoolbox) + AC3 5.1
→ HLS segment with correct codecs ✅
```
