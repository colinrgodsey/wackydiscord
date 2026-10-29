package bot

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

// deadPID returns the PID of a process that has definitely exited, which is what the
// stale-lock tests need: the exact failure mode that made the liveness check
// non-negotiable is a killed holder leaving a 7-byte lock file with a dead PID in it.
func deadPID(t *testing.T) int {
	t.Helper()
	cmd := exec.Command("true")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start dead-pid probe: %v", err)
	}
	pid := cmd.Process.Pid
	if err := cmd.Wait(); err != nil {
		t.Fatalf("wait dead-pid probe: %v", err)
	}
	return pid
}

func TestSessionLockTurnActive(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, sessionLockName)
	if sessionLockTurnActive(path) {
		t.Fatal("a missing lock file must be idle (agent has never had a turn)")
	}
	cases := []struct {
		label   string
		content string
	}{
		{"an empty file (cleanly released lock)", ""},
		{"an unparseable hint", "not-a-pid\n"},
	}
	for _, tc := range cases {
		if err := os.WriteFile(path, []byte(tc.content), 0o644); err != nil {
			t.Fatal(err)
		}
		if sessionLockTurnActive(path) {
			t.Fatalf("%s must be idle", tc.label)
		}
	}
	if err := os.WriteFile(path, []byte(strconv.Itoa(deadPID(t))+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if sessionLockTurnActive(path) {
		t.Fatal("a stale PID from a killed holder must be idle")
	}
	if err := os.WriteFile(path, []byte(strconv.Itoa(os.Getpid())+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if !sessionLockTurnActive(path) {
		t.Fatal("a live holder's PID must be turning")
	}
}

func TestTypingDueRateLimit(t *testing.T) {
	b := &Bot{}
	base := time.Now()
	if !b.typingDue("chan_a", base) {
		t.Fatal("the first refresh for a channel must be due")
	}
	if b.typingDue("chan_a", base.Add(time.Second)) {
		t.Fatal("a second refresh within the interval must be suppressed")
	}
	if !b.typingDue("chan_a", base.Add(typingRefreshInterval)) {
		t.Fatal("the refresh must be due again after the interval")
	}
	if !b.typingDue("chan_b", base.Add(time.Second)) {
		t.Fatal("other channels must not share the rate limit")
	}
}

func TestLockTypingWatcherTracksTurn(t *testing.T) {
	wsDir := t.TempDir()
	agentDir := filepath.Join(wsDir, "tester")
	if err := os.MkdirAll(agentDir, 0o755); err != nil {
		t.Fatal(err)
	}
	st, err := NewState(filepath.Join(wsDir, "state.json"))
	if err != nil {
		t.Fatalf("NewState: %v", err)
	}
	b := &Bot{WsDir: wsDir, State: st}
	b.lockTyping = newLockTyping(b)
	b.lockTyping.pollEvery = 50 * time.Millisecond
	b.lockTyping.start(context.Background())
	b.lockTyping.watch("tester")
	w := b.lockTyping.watcherFor("tester")
	if w == nil {
		t.Fatal("the watcher must be live after watch")
	}
	defer b.lockTyping.unwatch("tester")
	lockPath := w.lockPath

	waitFor := func(label string, cond func() bool) {
		t.Helper()
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			if cond() {
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
		t.Fatalf("timed out waiting for %s", label)
	}

	// A live PID: turning. The first write creates the file; the watcher picks it up
	// by arming on the next tick (or immediately once armed).
	if err := os.WriteFile(lockPath, []byte(strconv.Itoa(os.Getpid())+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	waitFor("turning on a live PID", func() bool { return w.turning.Load() })

	// A stale PID from a killed holder: idle. This is the case a bare non-empty check
	// gets wrong - the file is non-empty and the kernel lock is free.
	if err := os.WriteFile(lockPath, []byte(strconv.Itoa(deadPID(t))+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	waitFor("idle on a dead PID", func() bool { return !w.turning.Load() })

	// Truncated to 0 (clean release): idle.
	if err := os.WriteFile(lockPath, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	waitFor("idle on an empty file", func() bool { return !w.turning.Load() })
}
