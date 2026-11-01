package bot

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestProcessDialer_ChildEnvExcludesBotToken pins the security fix on the
// stdio-serve spawn site: the child's environment must not carry the bot's
// DISCORD_BOT_TOKEN. The fake child is not a real stdio-serve, so the dial
// fails; the point is that the child starts with the stripped env and
// records it.
func TestProcessDialer_ChildEnvExcludesBotToken(t *testing.T) {
	ws := t.TempDir()
	dumpPath := filepath.Join(ws, "child-env-dump")
	bin := filepath.Join(ws, "fake-stdio-serve")
	script := `#!/bin/sh
env > ` + dumpPath + `
sleep 3
`
	if err := os.WriteFile(bin, []byte(script), 0755); err != nil {
		t.Fatalf("writing fake stdio child: %v", err)
	}
	t.Setenv(EnvDiscordBotToken, "supersecret")

	d := NewProcessDialer(context.Background(), bin, ws)
	defer d.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, _ = d.Dial(ctx, "envdump")

	// The child writes the dump at start and the dial deadline (2s) is before
	// its sleep (3s), so the file exists by the time Dial returns; poll only
	// to be safe against a slow scheduler.
	deadline := time.Now().Add(5 * time.Second)
	var raw []byte
	var err error
	for time.Now().Before(deadline) {
		raw, err = os.ReadFile(dumpPath)
		if err == nil && len(raw) > 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("reading child env dump: %v", err)
	}
	if strings.Contains(string(raw), "DISCORD_BOT_TOKEN=") {
		t.Errorf("stdio-serve child env carried DISCORD_BOT_TOKEN")
	}
	if !strings.Contains(string(raw), "PATH=") {
		t.Errorf("stdio-serve child lost the rest of the environment: %q", string(raw))
	}
}
