// jfproxy sits between the client and the Jellyfin server. It loads MKV movies into
// a local cache using many large range requests, ffmpeg turns them into HLS
// (video copied, audio to AC3), and the client fetches small segments locally.
package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/lucsoft/jellyfin-proxy/internal/events"
	"github.com/lucsoft/jellyfin-proxy/internal/hls"
	"github.com/lucsoft/jellyfin-proxy/internal/rangecache"
	"github.com/lucsoft/jellyfin-proxy/internal/server"
)

func main() {
	var (
		upstream   = flag.String("upstream", "", "URL of the Jellyfin server (required)")
		listen     = flag.String("listen", ":4070", "listen address for client and dashboard")
		public     = flag.String("public", "", "URL the client uses to reach the proxy (default: http://<LAN-IP><listen>)")
		serverName = flag.String("servername", "", "override the server name in /System/Info")
		insecure   = flag.Bool("insecure", false, "skip verification of the server's TLS certificate")
		workDir    = flag.String("dir", filepath.Join(os.TempDir(), "jellyfin-proxy"), "working directory for cache and segments")
		ffmpeg     = flag.String("ffmpeg", "ffmpeg", "path to ffmpeg")

		blockMiB  = flag.Int("block", 32, "MiB per server request (large = few sequential reads on the server disk)")
		workers   = flag.Int("workers", 16, "parallel connections to the server")
		reserve   = flag.Int("reserve", 2, "of those, kept free for urgent reads (seeks)")
		aheadMiB  = flag.Int("readahead", 1536, "MiB of raw data prefetched ahead of ffmpeg")
		behindMiB = flag.Int("keepbehind", 64, "MiB of raw data kept behind ffmpeg")
		stall     = flag.Duration("stall", 30*time.Second, "abort a request if no data arrives for this long")

		segSec     = flag.Float64("segment", 4, "target length of HLS segments in seconds")
		maxAhead   = flag.Int("maxahead", 60, "segments ffmpeg may run ahead of the client (then it pauses)")
		keepBehind = flag.Int("segbehind", 15, "segments kept behind the client (for short rewinds)")
		seekGap    = flag.Int("seekgap", 6, "if the client jumps further ahead than this, ffmpeg is restarted there")

		acodec    = flag.String("acodec", "ac3", "target audio codec")
		abitrate  = flag.String("abitrate", "640k", "audio bitrate")
		achannels = flag.Int("achannels", 6, "audio channels")
		hlsFormat = flag.String("hlsformat", "fmp4", "HLS segment format: fmp4 or ts")
		rdr       = flag.Bool("rdr", true, "offer a reduced-dynamic-range variant of every audio track (played via HLS)")
		rdrFilter = flag.String("rdrfilter", server.DefaultRDRFilter,
			"ffmpeg audio filter for the reduced-dynamic-range tracks")
		directPlay = flag.Bool("directplay", true, "play MKV with a client-compatible audio track straight from the cache instead of HLS")
		copyAudio  = flag.String("copyaudio", "ac3,eac3,aac", "audio codecs the client supports (copied instead of encoded)")

		eventsFile = flag.String("events", "", "also write events as JSONL to this file")
		verbose    = flag.Bool("v", false, "log every proxy request")
		upgrade    = flag.Bool("upgrade", false, "ask the running proxy (same -dir) to hand over to this binary without dropping streams, then exit")
	)
	flag.Parse()
	if *upgrade {
		if err := requestUpgrade(*workDir); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	// A successor may outlive the pipe its predecessor's stdout went to;
	// without this, the first log line after that would kill it.
	signal.Ignore(syscall.SIGPIPE)

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	if *upstream == "" {
		fmt.Fprintln(os.Stderr, "-upstream is missing")
		flag.Usage()
		os.Exit(2)
	}
	up, err := url.Parse(strings.TrimRight(*upstream, "/"))
	if err != nil || up.Scheme == "" || up.Host == "" {
		fmt.Fprintf(os.Stderr, "invalid -upstream URL %q\n", *upstream)
		os.Exit(2)
	}
	if *public == "" {
		_, port, _ := net.SplitHostPort(*listen)
		*public = fmt.Sprintf("http://%s:%s", lanIP(), port)
	}
	if err := os.MkdirAll(*workDir, 0o755); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	// Only one proxy per working directory. A new start waits until the old
	// one is done – otherwise it would clean up the old one's cache while that
	// one is still saving it. A graceful successor inherits the lock instead.
	lock, err := takeLock(*workDir)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	ev := events.New(500, logger)
	if *eventsFile != "" {
		if err := ev.SetFile(*eventsFile); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}

	tlsCfg := &tls.Config{InsecureSkipVerify: *insecure}
	// Downloads: force HTTP/1.1. HTTP/2 multiplexes all requests over one
	// TCP connection with a single congestion window – measured 3x slower.
	dl := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		TLSClientConfig:       tlsCfg,
		MaxIdleConnsPerHost:   *workers + 4,
		IdleConnTimeout:       90 * time.Second,
		ResponseHeaderTimeout: 30 * time.Second,
		DisableCompression:    true,
		Protocols:             new(http.Protocols),
	}
	dl.Protocols.SetHTTP1(true)
	api := &http.Transport{
		Proxy:               http.ProxyFromEnvironment,
		TLSClientConfig:     tlsCfg,
		MaxIdleConnsPerHost: 16,
		IdleConnTimeout:     90 * time.Second,
		DisableCompression:  true,
	}

	srv := server.New(server.Options{
		Upstream: up, PublicURL: strings.TrimRight(*public, "/"), ServerName: *serverName, WorkDir: *workDir,
		// The predecessor is still finishing streams from its directories.
		SkipCleanup: isSuccessor(),
		Cache: rangecache.Config{
			Dir: *workDir, BlockSize: int64(*blockMiB) << 20, Workers: *workers, Reserve: *reserve,
			ReadAhead: int64(*aheadMiB) << 20, KeepBehind: int64(*behindMiB) << 20,
			Retries: 4, Stall: *stall, Client: &http.Client{Transport: dl}, Events: ev,
		},
		HLS: hls.Config{
			FFmpeg: *ffmpeg, Dir: *workDir, MaxAhead: *maxAhead, KeepBehind: *keepBehind,
			SeekGap: *seekGap, Format: *hlsFormat, IdleStop: 3 * time.Minute, Events: ev,
		},
		AudioCodec: *acodec, AudioBitrate: *abitrate, AudioChannels: *achannels,
		CopyAudio:      strings.Split(*copyAudio, ","),
		DirectPlay:     *directPlay,
		RDR:            *rdr,
		RDRFilter:      *rdrFilter,
		SegmentSeconds: *segSec,
		SessionIdle:    30 * time.Minute,
		CacheIdle:      15 * time.Minute,
		APIClient:      &http.Client{Transport: api},
		Events:         ev,
	})

	// ffmpeg reads the cache through a dedicated listener bound only to
	// loopback – so the unauthenticated endpoint is never exposed on the LAN.
	loop, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	srv.SetLocalAddr(loop.Addr().String())
	go http.Serve(loop, srv.LocalHandler())
	if isSuccessor() {
		srv.ImportSources(sourcesFile(*workDir))
	}

	var h http.Handler = srv.Handler()
	if *verbose {
		h = logRequests(logger, h)
	}
	httpSrv := &http.Server{Addr: *listen, Handler: h, ReadHeaderTimeout: 20 * time.Second}
	ln, err := openListener(*listen)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	// Serve returns as soon as Shutdown starts; stopped is closed only once
	// Shutdown has finished the open connections.
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
		for s := range sig {
			if s == syscall.SIGHUP {
				// Hand over to the (possibly rebuilt) binary; on failure keep serving.
				if err := handOver(ln, lock, srv, *workDir, logger); err != nil {
					ev.Emit("proxy", "upgrade_failed", events.Error, "Graceful upgrade failed, keeping this process: "+err.Error())
					continue
				}
				ev.Emit("proxy", "upgrade", events.Info, "Handed over to a new process; this one finishes its open streams and exits")
				drain(httpSrv, srv)
				return
			}
			logger.Info("shutting down …")
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			httpSrv.Shutdown(ctx)
			cancel()
			return
		}
	}()

	writePID(*workDir)
	signalReady()
	logger.Info("jellyfin-proxy running", "upstream", up.String(), "listen", *listen, "public", *public,
		"dashboard", *public+"/", "workdir", *workDir, "pid", os.Getpid(), "successor", isSuccessor())
	if err := httpSrv.Serve(ln); err != nil && err != http.ErrServerClosed {
		logger.Error(err.Error())
		os.Exit(1)
	}
	<-stopped
	srv.Shutdown()
}

func logRequests(l *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t0 := time.Now()
		sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(sw, r)
		l.Info(r.Method+" "+r.URL.Path, "status", sw.status, "ms", time.Since(t0).Milliseconds(),
			"range", r.Header.Get("Range"), "from", r.RemoteAddr, "ua", r.UserAgent())
	})
}

// statusWriter remembers the response status for the request log. Unwrap
// keeps http.ResponseController (flushing, deadlines) working through it.
type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// lanIP determines the address this machine uses to reach the network.
func lanIP() string {
	c, err := net.Dial("udp", "192.0.2.1:9")
	if err != nil {
		return "127.0.0.1"
	}
	defer c.Close()
	return c.LocalAddr().(*net.UDPAddr).IP.String()
}

// lockWorkDir holds an exclusive lock on dir/.lock until the process exits.
func lockWorkDir(dir string, wait time.Duration) (*os.File, error) {
	f, err := os.OpenFile(filepath.Join(dir, ".lock"), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	deadline := time.Now().Add(wait)
	for {
		err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			return f, nil // f stays open, the lock holds until exit
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("working directory %s is still held by another jfproxy after %s", dir, wait)
		}
		time.Sleep(200 * time.Millisecond)
	}
}
