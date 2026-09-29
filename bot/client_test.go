package bot

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestSpawnedServer_DiedOnChildSigkillMidUptime(t *testing.T) {
	if _, err := os.Stat("/home/moltbot/.local/bin/wackypub"); err != nil {
		t.Skip("wackypub binary not found, skipping live stdio-serve acceptance test")
	}

	wsDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(wsDir, RootMarkerFile), []byte("test workspace"), 0644); err != nil {
		t.Fatalf("writing root marker: %v", err)
	}

	server, err := SpawnServer(context.Background(), "wackypub", wsDir)
	if err != nil {
		t.Fatalf("SpawnServer failed: %v", err)
	}
	defer func() {
		_ = server.Close()
	}()

	pid := server.Pid()
	if pid <= 0 {
		t.Fatalf("expected valid child PID, got %d", pid)
	}

	// Kill child mid-uptime with SIGKILL
	if err := syscall.Kill(pid, syscall.SIGKILL); err != nil {
		t.Fatalf("failed to SIGKILL child %d: %v", pid, err)
	}

	select {
	case err := <-server.Died():
		if err == nil {
			t.Fatal("expected non-nil error on server.Died() after SIGKILL")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for server.Died() after SIGKILL")
	}
}

func TestSpawnedServer_CloseDoesNotTriggerDied(t *testing.T) {
	if _, err := os.Stat("/home/moltbot/.local/bin/wackypub"); err != nil {
		t.Skip("wackypub binary not found, skipping live stdio-serve acceptance test")
	}

	wsDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(wsDir, RootMarkerFile), []byte("test workspace"), 0644); err != nil {
		t.Fatalf("writing root marker: %v", err)
	}

	server, err := SpawnServer(context.Background(), "wackypub", wsDir)
	if err != nil {
		t.Fatalf("SpawnServer failed: %v", err)
	}

	if err := server.Close(); err != nil {
		t.Fatalf("server.Close() failed: %v", err)
	}

	select {
	case err := <-server.Died():
		t.Fatalf("unexpected error received on server.Died() after intentional Close: %v", err)
	case <-time.After(200 * time.Millisecond):
		// Success: no died error sent after intentional Close
	}
}

func TestBot_StartExitsWhenChildDies(t *testing.T) {
	if _, err := os.Stat("/home/moltbot/.local/bin/wackypub"); err != nil {
		t.Skip("wackypub binary not found, skipping live stdio-serve acceptance test")
	}

	wsDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(wsDir, RootMarkerFile), []byte("test workspace"), 0644); err != nil {
		t.Fatalf("writing root marker: %v", err)
	}

	server, err := SpawnServer(context.Background(), "wackypub", wsDir)
	if err != nil {
		t.Fatalf("SpawnServer failed: %v", err)
	}
	defer func() {
		_ = server.Close()
	}()

	b := &Bot{
		WsDir:  wsDir,
		Client: server,
		server: server,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- b.Start(ctx)
	}()

	pid := server.Pid()
	if pid <= 0 {
		t.Fatalf("expected valid child PID, got %d", pid)
	}

	// Kill child mid-uptime with SIGKILL
	if err := syscall.Kill(pid, syscall.SIGKILL); err != nil {
		t.Fatalf("failed to SIGKILL child %d: %v", pid, err)
	}

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("expected non-nil error when downstream server died, got nil")
		}
		if !strings.Contains(err.Error(), "downstream wackypub protocol server died") {
			t.Fatalf("expected downstream died error message, got: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for b.Start to exit after downstream server died")
	}
}
