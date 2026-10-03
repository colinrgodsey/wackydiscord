package bot

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bwmarrin/discordgo"
)

// writeFakeModelCLI drops an executable stand-in for the wackyacp binary that
// answers "model get" with the same JSON shape the model CLI prints.
func writeFakeModelCLI(t *testing.T, dir, model string) string {
	t.Helper()
	bin := filepath.Join(dir, "fake-wackyacp")
	script := "#!/bin/sh\n" +
		"if [ \"${1:-}\" = \"model\" ] && [ \"${2:-}\" = \"get\" ]; then\n" +
		"  echo '{\"model\":\"" + model + "\",\"configOptions\":[]}'\n" +
		"  exit 0\n" +
		"fi\n" +
		"exit 1\n"
	if err := os.WriteFile(bin, []byte(script), 0755); err != nil {
		t.Fatalf("writing fake model CLI: %v", err)
	}
	return bin
}

// writeFailingModelCLI drops a bridge binary that exists (so the bind-time
// InspectAgent bridge check passes) but fails as a model CLI.
func writeFailingModelCLI(t *testing.T, dir string) string {
	t.Helper()
	bin := filepath.Join(dir, "failing-wackyacp")
	if err := os.WriteFile(bin, []byte("#!/bin/sh\nexit 3\n"), 0755); err != nil {
		t.Fatalf("writing failing model CLI: %v", err)
	}
	return bin
}

// TestBindBridged_ModelCLIConfirmed verifies the model-cli card's consumer
// path: /bind execs the route's bridge binary in model-CLI form with CWD =
// the agent folder, parses the JSON, and the confirmed model is carried on
// the binding and surfaced by /status.
func TestBindBridged_ModelCLIConfirmed(t *testing.T) {
	b, _, spy, _ := setupBridgedBotManifest(t, func(ws string) string {
		agentFolder := filepath.Join(ws, "mtest")
		if err := os.MkdirAll(agentFolder, 0755); err != nil {
			t.Fatalf("mkdir agent folder: %v", err)
		}
		bin := writeFakeModelCLI(t, ws, "fake-model-7")
		return "mtest: " + bin + " --harness-cmd=agy --agent-folder=" + agentFolder + "\n"
	})

	i := makeInteraction("bind", []*discordgo.ApplicationCommandInteractionDataOption{
		{Name: "agent", Type: discordgo.ApplicationCommandOptionString, Value: any("mtest")},
	})
	b.handleBindCommand(b.Session, i)

	out := spy.allOutput()
	if !strings.Contains(out, "Channel bound to agent **mtest**") {
		t.Fatalf("expected successful bind, got: %q", out)
	}
	if !strings.Contains(out, "model: `bridged`") {
		t.Fatalf("expected the ack to carry the placeholder, got: %q", out)
	}
	// ...and is edited in place with the CLI-confirmed model (post-ack).
	if !strings.Contains(out, "model: `fake-model-7`") {
		t.Fatalf("expected the CLI-confirmed model in the bind reply, got: %q", out)
	}
	bnd := b.State.GetBinding("chan_bridged")
	if bnd == nil || bnd.Model != "fake-model-7" {
		t.Fatalf("binding.Model not carried: %+v", bnd)
	}

	i = makeInteraction("status", nil)
	b.handleStatusCommand(b.Session, i)
	out = spy.allOutput()
	if !strings.Contains(out, "**Model:** `fake-model-7`") {
		t.Fatalf("expected /status to surface the confirmed model, got: %q", out)
	}
}

// TestBindBridged_ModelCLIFallback verifies that a bridge that passes the
// bind-time InspectAgent check but fails as a model CLI does not block the
// bind: the historical "bridged" placeholder is used instead.
func TestBindBridged_ModelCLIFallback(t *testing.T) {
	b, _, spy, _ := setupBridgedBotManifest(t, func(ws string) string {
		agentFolder := filepath.Join(ws, "flaky")
		if err := os.MkdirAll(agentFolder, 0755); err != nil {
			t.Fatalf("mkdir agent folder: %v", err)
		}
		bin := writeFailingModelCLI(t, ws)
		return "flaky: " + bin + " --harness-cmd=agy --agent-folder=" + agentFolder + "\n"
	})

	i := makeInteraction("bind", []*discordgo.ApplicationCommandInteractionDataOption{
		{Name: "agent", Type: discordgo.ApplicationCommandOptionString, Value: any("flaky")},
	})
	b.handleBindCommand(b.Session, i)

	out := spy.allOutput()
	if !strings.Contains(out, "Channel bound to agent **flaky**") {
		t.Fatalf("expected the bind to succeed despite the model CLI failure, got: %q", out)
	}
	if !strings.Contains(out, "model: `bridged`") {
		t.Fatalf("expected the bridged placeholder on CLI failure, got: %q", out)
	}
	// CLI failure leaves the ack untouched: no second (edited) copy exists.
	if n := strings.Count(out, "Channel bound to agent **flaky**"); n != 1 {
		t.Fatalf("expected exactly the ack and no edit, got %d copies: %q", n, out)
	}
	bnd := b.State.GetBinding("chan_bridged")
	if bnd == nil || bnd.Model != "" {
		t.Fatalf("binding.Model should stay empty on CLI failure: %+v", bnd)
	}
}
