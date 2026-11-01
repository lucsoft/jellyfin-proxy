package main

// Graceful upgrade: SIGHUP starts the binary again (usually freshly built)
// and hands it the listening socket and the work dir lock. The new process
// takes all new connections at once; the old one stops accepting, finishes
// the streams that are still open – a Direct Play stream is one long HTTP
// response the client would not reopen – and then exits.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/lucsoft/jellyfin-proxy/internal/server"
)

const (
	envUpgrade = "JFPROXY_UPGRADE" // set in the successor's environment
	fdListener = 3                 // ExtraFiles[0]
	fdLock     = 4                 // ExtraFiles[1]
	fdReady    = 5                 // ExtraFiles[2]: successor writes one byte when it serves
	drainLimit = 12 * time.Hour    // longest a predecessor keeps finishing streams
)

func isSuccessor() bool { return os.Getenv(envUpgrade) == "1" }

func sourcesFile(workDir string) string { return filepath.Join(workDir, "handover-sources.json") }
func pidFile(workDir string) string     { return filepath.Join(workDir, "jfproxy.pid") }

// listen returns the inherited socket for a successor, a fresh one otherwise.
func openListener(addr string) (net.Listener, error) {
	if !isSuccessor() {
		return net.Listen("tcp", addr)
	}
	ln, err := net.FileListener(os.NewFile(fdListener, "listener"))
	if err != nil {
		return nil, fmt.Errorf("inherited listener: %w", err)
	}
	return ln, nil
}

// takeLock holds the work dir lock: inherited from the predecessor, or taken.
func takeLock(dir string) (*os.File, error) {
	if isSuccessor() {
		return os.NewFile(fdLock, "lock"), nil
	}
	return lockWorkDir(dir, 30*time.Second)
}

// signalReady tells the predecessor that this process accepts connections.
func signalReady() {
	if !isSuccessor() {
		return
	}
	f := os.NewFile(fdReady, "ready")
	f.Write([]byte{1})
	f.Close()
}

// handOver starts the successor and waits until it serves. It returns an
// error (and the current process simply keeps going) if that fails.
func handOver(ln net.Listener, lock *os.File, srv *server.Server, workDir string, logger *slog.Logger) error {
	tcp, ok := ln.(*net.TCPListener)
	if !ok {
		return errors.New("listener is not TCP")
	}
	lnFile, err := tcp.File() // a duplicate; closing ours later keeps the socket alive
	if err != nil {
		return err
	}
	defer lnFile.Close()
	if err := srv.ExportSources(sourcesFile(workDir)); err != nil {
		logger.Warn("could not hand over the source list", "err", err)
	}
	readyR, readyW, err := os.Pipe()
	if err != nil {
		return err
	}
	defer readyR.Close()

	exe, err := os.Executable()
	if err != nil {
		return err
	}
	cmd := exec.Command(exe, os.Args[1:]...)
	cmd.Env = append(os.Environ(), envUpgrade+"=1")
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	cmd.ExtraFiles = []*os.File{lnFile, lock, readyW}
	// Own process group: stopping the old process (or its supervisor's
	// group) must not take the successor down with it.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		readyW.Close()
		return err
	}
	readyW.Close()

	ready := make(chan error, 1)
	go func() {
		b := make([]byte, 1)
		_, err := readyR.Read(b)
		ready <- err
	}()
	select {
	case err := <-ready:
		if err != nil {
			cmd.Process.Kill()
			return fmt.Errorf("successor exited before it was ready: %w", err)
		}
	case <-time.After(30 * time.Second):
		cmd.Process.Kill()
		return errors.New("successor not ready within 30 s")
	}
	go cmd.Wait() // reap it if it ever exits while we are still around
	logger.Info("handed over to the new process", "pid", cmd.Process.Pid)
	return nil
}

// drain stops accepting, finishes open responses and returns when they are
// done (or after drainLimit).
func drain(httpSrv *http.Server, srv *server.Server) {
	srv.PrepareDrain()
	ctx, cancel := context.WithTimeout(context.Background(), drainLimit)
	defer cancel()
	httpSrv.Shutdown(ctx)
}

// writePID records this process for `jfproxy -upgrade`.
func writePID(workDir string) {
	os.WriteFile(pidFile(workDir), []byte(strconv.Itoa(os.Getpid())), 0o644)
}

// requestUpgrade sends SIGHUP to the running proxy of this work dir.
func requestUpgrade(workDir string) error {
	b, err := os.ReadFile(pidFile(workDir))
	if err != nil {
		return fmt.Errorf("no running proxy found (%w)", err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(b)))
	if err != nil {
		return fmt.Errorf("invalid pid file: %w", err)
	}
	if err := syscall.Kill(pid, syscall.SIGHUP); err != nil {
		return fmt.Errorf("signal pid %d: %w", pid, err)
	}
	fmt.Printf("asked jfproxy (pid %d) to hand over to the current binary\n", pid)
	return nil
}
