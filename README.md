# jellyfin-proxy

A local proxy between a Jellyfin client and a remote Jellyfin server. It solves two problems:

- The client cannot play DTS/TrueHD → audio is converted to AC3 when needed, video is only copied (4K HDR stays intact).
- The server delivers only ~18 Mbit/s per connection → the proxy downloads with 16 parallel, **large** range requests.

```
Client ──Direct Play or HLS (LAN)──► Proxy ──few large range requests (HTTP/1.1)──► Jellyfin
                                       │
                                       ├─ rangecache: block files, readable byte by byte
                                       ├─ mkv: keyframes from the MKV index → fixed segment boundaries
                                       └─ hls: ffmpeg (-c:v copy, audio → AC3) per session
```

## How it decides

- **Audio track the client can play (AC3, EAC3, AAC)** – Direct Play: the client reads the original MKV, the bytes come from the local cache.
- **Audio track the client cannot play (TrueHD, DTS)** – HLS: ffmpeg copies the video, converts the audio to AC3 and writes fMP4 segments.
- **A "Reduced dynamic range" track** – HLS as well: the proxy adds such a variant for every audio track and runs it through a compressor, for late-night watching.
- **Video other than H.264/HEVC (e.g. VC-1)** – the proxy's HLS can only copy video, and HLS players do not take VC-1. Such files get no "Reduced dynamic range" tracks; Direct Play still comes from the cache, everything else (an audio track the client cannot play, a retry after a playback error) is left to Jellyfin, which transcodes.
- **Not an MKV** – the request is passed through unchanged and the server decides.

## Packages

- **`internal/rangecache`** – Downloads the file in blocks (default 32 MiB = one server request) over N connections. Readers wait only for the bytes they need, not for whole blocks. Reads ahead of each reader and releases data behind it. While catching up (start, seek) it uses quarter blocks and splits the request a reader is waiting on. Saves itself on shutdown and is picked up again on the next start.
- **`internal/mkv`** – Reads the cues from the MKV and turns the keyframes into segment boundaries.
- **`internal/hls`** – A VOD playlist covering the whole movie. ffmpeg is paused (SIGSTOP) when it gets too far ahead of the client and restarted at segment k on a seek. Segments from different ffmpeg runs are cut identically and share one fMP4 init section.
- **`internal/server`** – Reverse proxy to Jellyfin, PlaybackInfo rewrite (MKV only), Direct Play from the cache, playback report tracking and the dashboard API.
- **`internal/events`** – Event log with the reason behind every decision, shown on the dashboard.

ffmpeg reads the cache through its own listener on `127.0.0.1:<random>`, which is never reachable from the LAN.

## Requirements

- Go (version in `go.mod`) to build
- ffmpeg on the `PATH` (only needed when audio has to be converted, and for the tests)
- A Jellyfin server that answers HTTP range requests for `/Videos/{id}/stream?static=true`

## Install

```bash
go install github.com/lucsoft/jellyfin-proxy/cmd/jfproxy@latest
```

or from a checkout:

```bash
go build -o bin/jfproxy ./cmd/jfproxy
```

## Run

```bash
./bin/jfproxy -upstream https://jellyfin.example.com -listen :4070 -public http://192.168.1.20:4070 -servername Jellyfin-Proxy -events /tmp/jf-events.jsonl
```

Point the Jellyfin client at the proxy (`http://<proxy-host>:4070`) instead of the server. The dashboard is at `http://<proxy-host>:4070/` and shows per movie:

- **Buffer ahead of the client** in movie time (finished segments + raw data in the cache), plus download rate vs. the movie's bitrate, as charts over the last 10 minutes
- **Timelines** of finished segments with client and ffmpeg positions, and of what is cached across the file
- **Playback state** with the reason ffmpeg last started, running server requests and every client request with wait and transfer time
- **Events**: restarts, seeks, released data and errors, each with its reason

## Upgrading without dropping streams

Rebuild the binary in place and ask the running proxy to hand over to it:

```bash
go build -o bin/jfproxy ./cmd/jfproxy
```

```bash
./bin/jfproxy -upgrade -dir <same -dir as the running proxy>
```

`-upgrade` sends `SIGHUP` to the running proxy (its pid is in `<dir>/jfproxy.pid`). It starts the new binary with the same flags, passes on the listening socket, the work dir lock and the list of known sources, and stops accepting connections. The new process serves every new request right away; the old one finishes the streams that are still open – a Direct Play stream is one long response the client would not reopen – and then exits. If the new binary fails to start, the old process keeps serving.

A plain stop (`SIGTERM`/`SIGINT`) still ends open streams after a few seconds.

## Security

- **Media access needs a Jellyfin token.** Stream, segment and init requests are only served with a token (`api_key` or the `Authorization` header) that Jellyfin accepts. The proxy checks unknown tokens against `/Users/Me` and caches the result for a few minutes.
- **The dashboard has no login.** `/` and `/proxy/api/state` show titles, positions and the event log to anyone who can reach the proxy. Run it only on a network you trust, or bind it to a specific interface with `-listen`.
- **Tokens appear in URLs.** Like Jellyfin's own transcoding URLs, the playlists the proxy generates carry `api_key` in segment URLs. Use HTTPS in front of the proxy if the network between client and proxy is not trusted.
- The cache (`-dir`, default in the system temp folder) holds parts of the media files. It is dropped after 15 minutes without playback; on shutdown it is saved, and saved caches older than 24 hours are deleted on the next start.

## Flags

- **`-block`** (32) – MiB per server request.
- **`-workers` / `-reserve`** (16 / 2) – Connections in total / kept free for urgent reads.
- **`-readahead`** (1536) – MiB of raw data loaded ahead of the reader.
- **`-segment`** (4) – Target segment length in seconds.
- **`-maxahead`** (60) – Segments ffmpeg may be ahead of the client before it is paused.
- **`-segbehind`** (15) – Segments kept behind the client for short rewinds.
- **`-seekgap`** (6) – A forward jump larger than this restarts ffmpeg instead of waiting.
- **`-acodec` / `-abitrate` / `-achannels`** (ac3 / 640k / 6) – Target audio format.
- **`-copyaudio`** (ac3,eac3,aac) – Audio codecs the client can play; these are copied, never converted.
- **`-rdr`** (true) – Offer a "Reduced dynamic range" variant of every audio track. Picking it plays via HLS with the audio run through a compressor (quiet passages louder, loud ones softer).
- **`-rdrfilter`** – The ffmpeg filter for those tracks; the default is an `acompressor` + `alimiter` chain.
- **`-directplay`** (true) – Serve MKVs with a client-compatible audio track directly from the cache instead of HLS.
- **`-hlsformat`** (fmp4) – HLS segment format: `fmp4` (init section + `.m4s`, usual for HEVC/HDR) or `ts`.

## Testing without a client

```bash
go test ./...
```

The tests create a small MKV with ffmpeg and check the cache (throttling, dropped connections), the keyframe index, segment timestamps across ffmpeg restarts, fMP4 init sections across runs, and the whole flow against a fake Jellyfin, including Direct Play.

For a live demo there is `cmd/fakejellyfin`, which serves any MKV throttled per connection:

```bash
go build -o bin/fakejellyfin ./cmd/fakejellyfin
```

```bash
./bin/fakejellyfin -file movie.mkv -mbit 18
```

## Limits

- MKV with an index (cues) only; other containers are passed through to the server unchanged.
- Video is always copied, there is no video transcoding or tone mapping. The first version with VideoToolbox transcoding is in the first commit.
- Switching to an incompatible audio track during Direct Play only works if the client requests new PlaybackInfo.

## License

[MIT](LICENSE)
