package bot

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"syscall"
	"testing"
	"time"

	agentv1 "github.com/colinrgodsey/wackypub/pkg/agent/v1"
)

// testChildBin is the minimal stdio child built once for the dialer tests.
var testChildBin string

func TestMain(m *testing.M) {
	bin, err := buildTestChild()
	if err != nil {
		fmt.Fprintln(os.Stderr, "building stdio test child:", err)
		os.Exit(1)
	}
	testChildBin = bin
	os.Exit(m.Run())
}

// buildTestChild compiles internal/stdiochild, the minimal AgentService-over-stdio
// process the dialer tests spawn in place of wackypub.
func buildTestChild() (string, error) {
	dir, err := os.MkdirTemp("", "stdiochild-*")
	if err != nil {
		return "", err
	}
	bin := filepath.Join(dir, "stdiochild")
	cmd := exec.Command("go", "build", "-o", bin, "github.com/colinrgodsey/wackydiscord/internal/stdiochild")
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("go build ./internal/stdiochild: %w: %s", err, out)
	}
	return bin, nil
}

func newTestWs(t *testing.T) string {
	t.Helper()
	ws := t.TempDir()
	if err := os.WriteFile(filepath.Join(ws, RootMarkerFile), nil, 0o644); err != nil {
		t.Fatalf("writing workspace marker: %v", err)
	}
	return ws
}

func spawnTestServer(t *testing.T, bin string) *SpawnedServer {
	t.Helper()
	srv, err := SpawnServer(context.Background(), bin, newTestWs(t))
	if err != nil {
		t.Fatalf("SpawnServer: %v", err)
	}
	t.Cleanup(func() { srv.Close() })
	return srv
}

var childID = regexp.MustCompile(`^child(\d+)$`)

// listChildPID runs ListAgents and returns the pid the response carries.
func listChildPID(t *testing.T, c AgentClient) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := c.ListAgents(ctx, &agentv1.ListAgentsRequest{})
	if err != nil {
		t.Fatalf("ListAgents: %v", err)
	}
	for _, id := range resp.AgentIds {
		if m := childID.FindStringSubmatch(id); m != nil {
			pid, _ := strconv.Atoi(m[1])
			return pid
		}
	}
	t.Fatalf("ListAgents returned no child id: %v", resp.AgentIds)
	return 0
}

// Acceptance: idle window. Production disables the gRPC idle reaper (idle timeout 0),
// so an idle gap leaves the child untouched and the first RPC after it works. The
// original 30-minute window cannot be waited out in a test;
// TestIdleTimeoutReplacesChild proves the mechanism on a short clock.
func TestChildSurvivesIdleWindow(t *testing.T) {
	srv := spawnTestServer(t, testChildBin)
	pid1 := listChildPID(t, srv)

	time.Sleep(3 * time.Second)

	pid2 := listChildPID(t, srv)
	if pid1 == 0 {
		t.Fatal("ListAgents did not reach a child")
	}
	if pid1 != pid2 {
		t.Fatalf("child was replaced during idle (pid %d -> %d): idle reaper still active", pid1, pid2)
	}
}

// Control for the mechanism above: the same client with a 2s idle reaper replaces the
// child after the gap - this is the pre-fix death spiral, now just a transport-level
// replacement instead of a bot restart.
func TestIdleTimeoutReplacesChild(t *testing.T) {
	srv, err := spawnServerWithIdleTimeout(context.Background(), testChildBin, newTestWs(t), 2*time.Second)
	if err != nil {
		t.Fatalf("spawnServerWithIdleTimeout: %v", err)
	}
	t.Cleanup(func() { srv.Close() })

	pid1 := listChildPID(t, srv)

	time.Sleep(5 * time.Second)

	pid2 := listChildPID(t, srv)
	if pid1 == 0 {
		t.Fatal("ListAgents did not reach a child")
	}
	if pid1 == pid2 {
		t.Fatalf("2s idle reaper did not replace the child (pid %d unchanged)", pid1)
	}
}

// Acceptance: transport kill. SIGKILL the child mid-uptime; the next RPC triggers a
// re-dial, the dialer spawns a NEW child, and the RPC succeeds. No bot exit: a
// 5s-threshold watchdog watching the whole episode stays quiet.
func TestReconnectAfterChildDeath(t *testing.T) {
	srv := spawnTestServer(t, testChildBin)
	pid1 := listChildPID(t, srv)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fatal := srv.watchConnectivityFor(ctx, 5*time.Second)

	if err := syscall.Kill(pid1, syscall.SIGKILL); err != nil {
		t.Fatalf("SIGKILL child %d: %v", pid1, err)
	}

	// The RPC racing the kill can hit the dying transport or fail-fast while the channel
	// is not Ready, so the post-kill RPC is retried until the re-dial lands: the next
	// message that gets through must reach a NEW child. Bounded, like a user retrying.
	deadline := time.Now().Add(10 * time.Second)
	pid2 := 0
	var lastErr error
	for time.Now().Before(deadline) {
		rctx, rcancel := context.WithTimeout(context.Background(), 2*time.Second)
		resp, err := srv.ListAgents(rctx, &agentv1.ListAgentsRequest{})
		rcancel()
		if err == nil {
			for _, id := range resp.AgentIds {
				if m := childID.FindStringSubmatch(id); m != nil {
					pid2, _ = strconv.Atoi(m[1])
				}
			}
			break
		}
		lastErr = err
		time.Sleep(200 * time.Millisecond)
	}
	if pid2 == 0 {
		t.Fatalf("no RPC succeeded after SIGKILL (last error: %v)", lastErr)
	}
	if pid2 == pid1 {
		t.Fatalf("RPC after SIGKILL reached the dead child (pid %d)", pid1)
	}

	select {
	case err := <-fatal:
		t.Fatalf("watchdog fired after a recovered child death: %v", err)
	case <-time.After(2 * time.Second):
	}
}

// Acceptance: watchdog, unrecoverable spawn failure. The child is killed and the
// binary is removed, so every re-spawn fails; the watchdog trips on the permanent
// spawn error - well inside the 1-minute threshold, so the trip is provably the
// permanence path, not the clock.
func TestWatchdogUnrecoverableSpawnFailure(t *testing.T) {
	srv, err := SpawnServer(context.Background(), testChildBin, newTestWs(t))
	if err != nil {
		t.Fatalf("SpawnServer: %v", err)
	}
	t.Cleanup(func() { srv.Close() })
	pid1 := listChildPID(t, srv)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fatal := srv.watchConnectivityFor(ctx, time.Minute)

	if err := os.Rename(testChildBin, testChildBin+".away"); err != nil {
		t.Fatalf("removing child binary: %v", err)
	}
	t.Cleanup(func() { _ = os.Rename(testChildBin+".away", testChildBin) })

	if err := syscall.Kill(pid1, syscall.SIGKILL); err != nil {
		t.Fatalf("SIGKILL child %d: %v", pid1, err)
	}

	// Force dials; the re-spawns now fail with the binary missing.
	rctx, rcancel := context.WithTimeout(context.Background(), 3*time.Second)
	_, _ = srv.ListAgents(rctx, &agentv1.ListAgentsRequest{})
	rcancel()

	select {
	case err := <-fatal:
		if err == nil {
			t.Fatal("watchdog fired with a nil error")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("watchdog did not fire on an unrecoverable spawn failure")
	}
}

// Acceptance: watchdog, stuck transport. A child that exits at spawn makes every dial
// fail at the handshake, so the channel stays broken and the threshold trips. The
// short threshold stands in for the production 15s.
func TestWatchdogStuckTransport(t *testing.T) {
	dying := filepath.Join(t.TempDir(), "dying-child")
	if err := os.WriteFile(dying, []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
		t.Fatalf("writing dying child: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	srv, err := SpawnServer(ctx, dying, newTestWs(t))
	if err != nil {
		t.Fatalf("SpawnServer: %v", err)
	}
	t.Cleanup(func() { srv.Close() })

	fatal := srv.watchConnectivityFor(ctx, 500*time.Millisecond)

	rctx, rcancel := context.WithTimeout(context.Background(), 2*time.Second)
	_, _ = srv.ListAgents(rctx, &agentv1.ListAgentsRequest{})
	rcancel()

	select {
	case err := <-fatal:
		if err == nil {
			t.Fatal("watchdog fired with a nil error")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("watchdog did not fire on a stuck transport")
	}
}

// Acceptance: no orphan children after a failed dial. A dial whose context has already
// expired must reap the child it just spawned.
func TestNoOrphanChildOnExpiredDialContext(t *testing.T) {
	d := NewProcessDialer(context.Background(), testChildBin, newTestWs(t))
	defer d.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := d.Dial(ctx, "passthrough:///stdio"); err == nil {
		t.Fatal("expected the dial to fail on an already-canceled context")
	}

	cmd := d.lastCmd
	if cmd == nil {
		t.Fatal("no spawn attempt recorded")
	}
	if cmd.ProcessState == nil {
		t.Fatal("child not reaped after failed dial (ProcessState nil)")
	}
	if err := syscall.Kill(cmd.Process.Pid, 0); err == nil {
		t.Fatalf("orphan child %d still alive after failed dial", cmd.Process.Pid)
	}
}
