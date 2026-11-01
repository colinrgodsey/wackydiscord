package bot

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// writeEnvDumpModelCLI drops a fake model CLI that first records what it sees
// for DISCORD_BOT_TOKEN, then answers "model get" like the real one.
func writeEnvDumpModelCLI(t *testing.T, dir, dumpPath string) string {
	t.Helper()
	bin := filepath.Join(dir, "fake-wackyacp-envdump")
	script := `#!/bin/sh
if [ "${1:-}" = "model" ] && [ "${2:-}" = "get" ]; then
  printenv DISCORD_BOT_TOKEN > ` + dumpPath + `
  echo '{"model":"fake-model","configOptions":[]}'
  exit 0
fi
exit 1
`
	if err := os.WriteFile(bin, []byte(script), 0755); err != nil {
		t.Fatalf("writing fake model CLI: %v", err)
	}
	return bin
}

// TestBridgedModelGet_ChildEnvExcludesBotToken pins the security fix on the
// model-CLI spawn site: the child (and anything it spawns) must not inherit
// the bot's DISCORD_BOT_TOKEN, while the rest of the environment is intact.
func TestBridgedModelGet_ChildEnvExcludesBotToken(t *testing.T) {
	var dumpPath string
	b, _, _, _ := setupBridgedBotManifest(t, func(ws string) string {
		agentFolder := filepath.Join(ws, "mtest")
		if err := os.MkdirAll(agentFolder, 0755); err != nil {
			t.Fatalf("mkdir agent folder: %v", err)
		}
		dumpPath = filepath.Join(ws, "child-env-dump")
		bin := writeEnvDumpModelCLI(t, ws, dumpPath)
		return "mtest: " + bin + "\n"
	})
	t.Setenv(EnvDiscordBotToken, "supersecret")

	model, err := b.bridgedModelGet("mtest")
	if err != nil {
		t.Fatalf("bridgedModelGet: %v", err)
	}
	if model != "fake-model" {
		t.Fatalf("model = %q, want fake-model", model)
	}
	raw, err := os.ReadFile(dumpPath)
	if err != nil {
		t.Fatalf("reading env dump: %v", err)
	}
	if strings.TrimSpace(string(raw)) != "" {
		t.Errorf("model-CLI child env carried DISCORD_BOT_TOKEN: %q", string(raw))
	}
}
