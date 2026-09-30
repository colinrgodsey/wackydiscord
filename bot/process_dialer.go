package bot

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"os/exec"
	"sync"
	"syscall"

	"github.com/colinrgodsey/wackypub/pkg/stdio"
)

// ProcessDialer is the gRPC dialer that makes the stdio child reconnectable: every
// dial attempt (initial connect, transport drop, backoff cycle, idle re-entry)
// spawns a fresh wackypub stdio-serve child, so a child death degrades to a
// transport disconnect that gRPC heals itself, instead of a dead conn the bot
// is stuck on.
//
// The child is tied to the bot's lifecycle context, NOT to the dial context:
// grpc-go's createTransport cancels the connect context the moment the dial
// succeeds, so a child spawned under the dial context would die immediately
// after every successful dial. If the dial context expires mid-spawn, the
// child just started is reaped before the error is returned, so failed dials
// leave no orphans.
type ProcessDialer struct {
	mu         sync.Mutex
	bin        string
	wsDir      string
	lifecycle  context.Context
	activeCmd  *exec.Cmd
	activeConn *stdio.Conn
	lastCmd    *exec.Cmd // most recent spawn attempt (live or reaped); diagnostics and tests
	closed     bool
	lastErr    error
}

// NewProcessDialer returns a dialer that spawns bin stdio-serve with CWD wsDir.
// A live child is owned until Close or replaced by a later dial; the lifecycle
// context only refuses NEW spawns. What ends a running child is the pipe EOF,
// not context cancellation, by design.
func NewProcessDialer(lifecycle context.Context, bin, wsDir string) *ProcessDialer {
	if bin == "" {
		bin = DefaultWackypubBin
	}
	return &ProcessDialer{bin: bin, wsDir: wsDir, lifecycle: lifecycle}
}

// Dial implements the grpc.WithContextDialer contract. It is serialized:
// pick_first dials one subchannel at a time, but the resolver-reset path can
// re-enter, and two concurrent children would leave one holding a live stdin
// pipe forever.
func (d *ProcessDialer) Dial(ctx context.Context, target string) (net.Conn, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return nil, errors.New("process dialer is closed")
	}
	if d.lifecycle != nil && d.lifecycle.Err() != nil {
		return nil, d.lifecycle.Err()
	}

	// Tear down the previous child if its transport is still open. gRPC normally
	// closes its transport before re-dialing, but the reset path can dial again
	// while the old conn is live; that edge must not leak a child.
	if d.activeConn != nil {
		_ = d.activeConn.Close()
		d.activeConn = nil
		d.activeCmd = nil
	}

	cmd := exec.Command(d.bin, "stdio-serve")
	cmd.Dir = d.wsDir
	conn, err := stdio.DialCommand(ctx, cmd, reapGrace)
	if err != nil {
		d.lastCmd = cmd
		d.lastErr = err
		return nil, fmt.Errorf("spawning %s stdio-serve in %s: %w", d.bin, d.wsDir, err)
	}
	if err := ctx.Err(); err != nil {
		// The dial deadline expired mid-spawn. The child is deliberately NOT
		// tied to ctx (see type doc), so reap it explicitly: no orphan on a
		// failed dial.
		if closeErr := conn.Close(); closeErr != nil {
			err = errors.Join(err, closeErr)
		}
		d.lastCmd = cmd
		d.lastErr = err
		return nil, err
	}
	d.activeCmd = cmd
	d.activeConn = conn
	d.lastCmd = cmd
	d.lastErr = nil
	return conn, nil
}

// lastDialError reports the last spawn failure, or nil if the last spawn succeeded.
func (d *ProcessDialer) lastDialError() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.lastErr
}

// childAlive reports whether the active child still exists in the process table, via
// kill(pid, 0): ESRCH means the kernel has reaped it. A missing active cmd (no
// completed dial) counts as alive: the watchdog only acts on a confirmed dead child.
// cmd.ProcessState is deliberately NOT read here: it is written by cmd.Wait in the
// reap goroutine, and reading it from this one would race.
func (d *ProcessDialer) childAlive() bool {
	d.mu.Lock()
	cmd := d.activeCmd
	d.mu.Unlock()
	if cmd == nil || cmd.Process == nil {
		return true
	}
	return syscall.Kill(cmd.Process.Pid, 0) != syscall.ESRCH
}

// activePID returns the pid of the live child, or 0 when none is running.
func (d *ProcessDialer) activePID() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.activeCmd != nil && d.activeCmd.Process != nil {
		return d.activeCmd.Process.Pid
	}
	return 0
}

// Close refuses new spawns and reaps the live child. Idempotent.
func (d *ProcessDialer) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return nil
	}
	d.closed = true
	var err error
	if d.activeConn != nil {
		err = d.activeConn.Close()
	}
	d.activeConn = nil
	d.activeCmd = nil
	d.lastCmd = nil
	return err
}

// isPermanentSpawnError reports whether a spawn error is a property of the binary
// or the environment (missing binary, not executable) rather than a transient
// failure: those cannot be fixed by re-dialing, so the watchdog can trip
// immediately instead of waiting out the threshold. Go has used different error
// types for fork/exec failures across versions - *exec.Error on PATH lookup
// failures, and *fs.PathError (Go 1.26) when the fork itself fails - so both
// classes count; anything else falls through to the threshold path.
func isPermanentSpawnError(err error) bool {
	var execErr *exec.Error
	if errors.As(err, &execErr) {
		return true
	}
	var pathErr *fs.PathError
	if !errors.As(err, &pathErr) {
		return false
	}
	errno, ok := pathErr.Err.(syscall.Errno)
	return ok && (errno == syscall.ENOENT || errno == syscall.ENOEXEC)
}
