package bot

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
	agentv1 "github.com/colinrgodsey/wackypub/pkg/agent/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type bridgedSpy struct {
	mu           sync.Mutex
	messages     []string
	interactions []string
	webhooks     []string
}

func (s *bridgedSpy) addMessage(content string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.messages = append(s.messages, content)
}

func (s *bridgedSpy) addInteraction(content string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.interactions = append(s.interactions, content)
}

func (s *bridgedSpy) allOutput() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var all []string
	all = append(all, s.interactions...)
	all = append(all, s.messages...)
	all = append(all, s.webhooks...)
	return strings.Join(all, "\n")
}

func setupBridgedBot(t *testing.T) (*Bot, *fakeAgent, *bridgedSpy, string) {
	return setupBridgedBotManifest(t, func(ws string) string {
		return "agy: /home/moltbot/workspace/projects/wackyacp/bin/wackyacp --agent-folder=" + filepath.Join(ws, "agy") + "\n" +
			"broken: /nonexistent/bridge-binary --agent-folder=" + filepath.Join(ws, "broken") + "\n" +
			"custom: /usr/local/bin/wackycustom --flag\n"
	})
}

// setupBridgedBotManifest is setupBridgedBot with the REMOTE_MANIFEST content
// delegated to manifestFor(ws).
func setupBridgedBotManifest(t *testing.T, manifestFor func(ws string) string) (*Bot, *fakeAgent, *bridgedSpy, string) {
	t.Helper()
	ws := t.TempDir()

	_ = os.WriteFile(filepath.Join(ws, "WACKYPUB_ROOT"), []byte(""), 0644)
	_ = os.WriteFile(filepath.Join(ws, RemoteManifestFile), []byte(manifestFor(ws)), 0644)

	st, err := NewState(filepath.Join(ws, ".wackydiscord.json"))
	if err != nil {
		t.Fatalf("NewState: %v", err)
	}
	_, _, _ = st.TryClaim("owner_1")

	sdk := newFakeAgent(ws)
	spy := &bridgedSpy{}

	s, err := discordgo.New("Bot dummy_token")
	if err != nil {
		t.Fatalf("discordgo.New: %v", err)
	}

	s.Client.Transport = fakeRoundTripper(func(req *http.Request) (*http.Response, error) {
		body, _ := io.ReadAll(req.Body)
		var payload struct {
			Content string `json:"content"`
			Data    *struct {
				Content string `json:"content"`
			} `json:"data"`
		}
		_ = json.Unmarshal(body, &payload)

		text := payload.Content
		if text == "" && payload.Data != nil {
			text = payload.Data.Content
		}

		if strings.Contains(req.URL.Path, "/interactions/") {
			spy.addInteraction(text)
		} else {
			spy.addMessage(text)
		}

		return &http.Response{
			StatusCode: 200,
			Body:       io.NopCloser(strings.NewReader("{}")),
			Header:     make(http.Header),
		}, nil
	})

	b := &Bot{
		WsDir:   ws,
		State:   st,
		Client:  sdk,
		Session: s,
	}

	return b, sdk, spy, ws
}

func makeInteraction(name string, opts []*discordgo.ApplicationCommandInteractionDataOption) *discordgo.InteractionCreate {
	return &discordgo.InteractionCreate{
		Interaction: &discordgo.Interaction{
			ID:        "int_" + name,
			Token:     "tok_" + name,
			ChannelID: "chan_bridged",
			Type:      discordgo.InteractionApplicationCommand,
			Data: discordgo.ApplicationCommandInteractionData{
				Name:    name,
				Options: opts,
			},
			Member: &discordgo.Member{User: &discordgo.User{ID: "owner_1"}},
		},
	}
}

func makeMessage(content string) *discordgo.MessageCreate {
	return &discordgo.MessageCreate{
		Message: &discordgo.Message{
			ID:        "msg_1",
			ChannelID: "chan_bridged",
			Content:   content,
			Author:    &discordgo.User{ID: "owner_1"},
		},
	}
}

// TestBridgedBinding_Success verifies that a channel binds to a bridged agent cleanly without requiring session.jsonl.
func TestBridgedBinding_Success(t *testing.T) {
	b, _, spy, _ := setupBridgedBot(t)

	i := makeInteraction("bind", []*discordgo.ApplicationCommandInteractionDataOption{
		{Name: "agent", Type: discordgo.ApplicationCommandOptionString, Value: any("agy")},
	})
	b.handleBindCommand(b.Session, i)

	output := spy.allOutput()
	if !strings.Contains(output, "Channel bound to agent **agy**") {
		t.Fatalf("expected successful bind message, got: %q", output)
	}
	if !strings.Contains(output, "model: `bridged`") {
		t.Fatalf("expected model to be bridged, got: %q", output)
	}

	bnd := b.State.GetBinding("chan_bridged")
	if bnd == nil || bnd.AgentID != "agy" {
		t.Fatalf("binding not saved in state: %+v", bnd)
	}
	if !b.isBridged("agy") {
		t.Fatal("expected isBridged to be true for agy")
	}
}

// TestBridgedBinding_BridgeMissingBinary verifies that missing bridge executable refuses bind with operator diagnostic.
func TestBridgedBinding_BridgeMissingBinary(t *testing.T) {
	b, _, spy, _ := setupBridgedBot(t)

	i := makeInteraction("bind", []*discordgo.ApplicationCommandInteractionDataOption{
		{Name: "agent", Type: discordgo.ApplicationCommandOptionString, Value: any("broken")},
	})
	b.handleBindCommand(b.Session, i)

	output := spy.allOutput()
	if !strings.Contains(output, "Cannot bind to broken: bridge executable not found") {
		t.Fatalf("expected executable not found error, got: %q", output)
	}
}

// TestBridgedBinding_BridgeProcessStartFailure verifies that bridge startup crashes are reported cleanly.
func TestBridgedBinding_BridgeProcessStartFailure(t *testing.T) {
	b, sdk, spy, _ := setupBridgedBot(t)
	sdk.mu.Lock()
	sdk.bridgeInspectErr = status.Error(codes.Unavailable, "bridge process failed to start: exit status 1")
	sdk.mu.Unlock()

	i := makeInteraction("bind", []*discordgo.ApplicationCommandInteractionDataOption{
		{Name: "agent", Type: discordgo.ApplicationCommandOptionString, Value: any("agy")},
	})
	b.handleBindCommand(b.Session, i)

	output := spy.allOutput()
	if !strings.Contains(output, "bridge process failed to start") {
		t.Fatalf("expected bridge process start failure, got: %q", output)
	}
}

// TestBridgedBinding_StatusNotFound verifies that codes.NotFound classifies as bridge executable not found.
func TestBridgedBinding_StatusNotFound(t *testing.T) {
	b, sdk, spy, _ := setupBridgedBot(t)
	sdk.mu.Lock()
	sdk.bridgeInspectErr = status.Error(codes.NotFound, "bridge binary missing")
	sdk.mu.Unlock()

	i := makeInteraction("bind", []*discordgo.ApplicationCommandInteractionDataOption{
		{Name: "agent", Type: discordgo.ApplicationCommandOptionString, Value: any("agy")},
	})
	b.handleBindCommand(b.Session, i)

	output := spy.allOutput()
	if !strings.Contains(output, "Cannot bind to agy: bridge executable not found") {
		t.Fatalf("expected executable not found error, got: %q", output)
	}
}

// TestBridgedBinding_GenericErrorDegradation verifies unclassified upstream errors degrade to generic bridge error.
func TestBridgedBinding_GenericErrorDegradation(t *testing.T) {
	b, sdk, spy, _ := setupBridgedBot(t)
	sdk.mu.Lock()
	sdk.bridgeInspectErr = fmt.Errorf("unexpected upstream bridge socket failure")
	sdk.mu.Unlock()

	i := makeInteraction("bind", []*discordgo.ApplicationCommandInteractionDataOption{
		{Name: "agent", Type: discordgo.ApplicationCommandOptionString, Value: any("agy")},
	})
	b.handleBindCommand(b.Session, i)

	output := spy.allOutput()
	if !strings.Contains(output, "Cannot bind to agy: bridge error: unexpected upstream bridge socket failure") {
		t.Fatalf("expected generic bridge error degradation, got: %q", output)
	}
}

// TestBridgedBinding_HonestFalseAgentDirExists verifies that bridged agents bind and resolve successfully even when AgentDirExists is false.
func TestBridgedBinding_HonestFalseAgentDirExists(t *testing.T) {
	b, _, spy, _ := setupBridgedBot(t)

	// In setupBridgedBot, fakeAgent returns AgentDirExists: false for agy
	i := makeInteraction("bind", []*discordgo.ApplicationCommandInteractionDataOption{
		{Name: "agent", Type: discordgo.ApplicationCommandOptionString, Value: any("agy")},
	})
	b.handleBindCommand(b.Session, i)

	output := spy.allOutput()
	if !strings.Contains(output, "Channel bound to agent **agy**") {
		t.Fatalf("expected successful bind despite AgentDirExists: false, got: %q", output)
	}

	// Also verify resolveMessageContext does not fail with "does not exist"
	bnd, ok := b.resolveMessageContext(b.Session, "chan_bridged")
	if !ok || bnd == nil || bnd.AgentID != "agy" {
		t.Fatalf("resolveMessageContext failed: ok=%v, bnd=%+v", ok, bnd)
	}
}

// TestBridgedTurn_StreamedResponse verifies streaming chat execution for bridged agents via AddAndGenerateTurnStream.
func TestBridgedTurn_StreamedResponse(t *testing.T) {
	b, sdk, spy, ws := setupBridgedBot(t)
	_ = b.State.SetBinding(&ChannelBinding{ChannelID: "chan_bridged", AgentID: "agy"})

	sdk.mu.Lock()
	sdk.addAndGenStreams = []func(*agentv1.AddAndGenerateTurnStreamResponse){
		func(r *agentv1.AddAndGenerateTurnStreamResponse) { r.Text = "Hello " },
		func(r *agentv1.AddAndGenerateTurnStreamResponse) { r.Text = "world from agy!" },
	}
	sdk.mu.Unlock()

	b.HandleMessageCreate(b.Session, makeMessage("ping"))

	output := spy.allOutput()
	if !strings.Contains(output, "Hello world from agy!") {
		t.Fatalf("expected streamed response in output, got: %q", output)
	}

	bnd := b.State.GetBinding("chan_bridged")
	if bnd.IsGenerating {
		t.Fatal("expected IsGenerating to be reset to false after turn")
	}

	// Verify no session.jsonl was written to local agent folder
	agentSessionPath := filepath.Join(ws, "agy", sessionFileName)
	if _, err := os.Stat(agentSessionPath); !os.IsNotExist(err) {
		t.Fatal("bridged agent turn must not write local session.jsonl")
	}
}

// TestBridgedTurn_ToolActivityVerbose verifies that tool activity streams live in verbose mode.
func TestBridgedTurn_ToolActivityVerbose(t *testing.T) {
	b, sdk, spy, _ := setupBridgedBot(t)
	_ = b.State.SetBinding(&ChannelBinding{ChannelID: "chan_bridged", AgentID: "agy", Verbose: true})

	sdk.mu.Lock()
	sdk.addAndGenStreams = []func(*agentv1.AddAndGenerateTurnStreamResponse){
		func(r *agentv1.AddAndGenerateTurnStreamResponse) {
			r.ToolCall = &agentv1.ToolCall{
				CallId:      "call_1",
				ToolName:    "bash",
				ArgsSummary: "echo test",
			}
		},
		func(r *agentv1.AddAndGenerateTurnStreamResponse) {
			r.ToolCallUpdate = &agentv1.ToolCallUpdate{
				CallId:   "call_1",
				ToolName: "bash",
				Status:   "completed",
			}
		},
		func(r *agentv1.AddAndGenerateTurnStreamResponse) {
			r.Text = "Done executing."
		},
	}
	sdk.mu.Unlock()

	b.HandleMessageCreate(b.Session, makeMessage("run tool"))

	output := spy.allOutput()
	if !strings.Contains(output, "Done executing.") {
		t.Fatalf("expected assistant text in output, got: %q", output)
	}
}

// TestBridgedTurn_BusyRejection verifies that codes.ResourceExhausted produces the friendly busy message.
func TestBridgedTurn_BusyRejection(t *testing.T) {
	b, sdk, spy, _ := setupBridgedBot(t)
	_ = b.State.SetBinding(&ChannelBinding{ChannelID: "chan_bridged", AgentID: "agy"})

	sdk.mu.Lock()
	sdk.addAndGenErr = status.Error(codes.ResourceExhausted, "another prompt turn is already in flight for this agent")
	sdk.mu.Unlock()

	b.HandleMessageCreate(b.Session, makeMessage("hello"))

	output := spy.allOutput()
	if !strings.Contains(output, "agy is busy") || !strings.Contains(output, "Another turn is currently in flight") {
		t.Fatalf("expected busy rejection message, got: %q", output)
	}

	bnd := b.State.GetBinding("chan_bridged")
	if bnd.IsGenerating {
		t.Fatal("expected IsGenerating to be reset after busy rejection")
	}
}

// TestBridgedTurn_BridgeError verifies that unexpected bridge crashes surface clearly.
func TestBridgedTurn_BridgeError(t *testing.T) {
	b, sdk, spy, _ := setupBridgedBot(t)
	_ = b.State.SetBinding(&ChannelBinding{ChannelID: "chan_bridged", AgentID: "agy"})

	sdk.mu.Lock()
	sdk.addAndGenErr = fmt.Errorf("BridgeProcessDiedError: exit code 137")
	sdk.mu.Unlock()

	b.HandleMessageCreate(b.Session, makeMessage("crash"))

	output := spy.allOutput()
	if !strings.Contains(output, "Bridge error (agy)") || !strings.Contains(output, "137") {
		t.Fatalf("expected bridge error surface, got: %q", output)
	}

	bnd := b.State.GetBinding("chan_bridged")
	if bnd.IsGenerating {
		t.Fatal("expected IsGenerating to be reset after bridge error")
	}
}

// TestBridgedTurn_StopCommand verifies cancelling an in-flight bridged turn via /stop.
func TestBridgedTurn_StopCommand(t *testing.T) {
	b, sdk, spy, _ := setupBridgedBot(t)
	_ = b.State.SetBinding(&ChannelBinding{ChannelID: "chan_bridged", AgentID: "agy"})

	sdk.hold = make(chan struct{})
	sdk.started = make(chan struct{})

	turnDone := make(chan struct{})
	go func() {
		defer close(turnDone)
		b.HandleMessageCreate(b.Session, makeMessage("start long task"))
	}()

	select {
	case <-sdk.started:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for turn to start")
	}

	bnd := b.State.GetBinding("chan_bridged")
	if !bnd.IsGenerating {
		t.Fatal("expected channel to be generating while turn is held")
	}

	// Trigger /stop command
	iStop := makeInteraction("stop", nil)
	b.handleStopCommand(b.Session, iStop)

	select {
	case <-turnDone:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for turn to finish after stop")
	}

	output := spy.allOutput()
	if !strings.Contains(output, "Turn stopped") && !strings.Contains(output, "Turn cancelled") {
		t.Fatalf("expected stop confirmation, got: %q", output)
	}

	bndAfter := b.State.GetBinding("chan_bridged")
	if bndAfter.IsGenerating {
		t.Fatal("expected IsGenerating to be reset after stop")
	}
}

// TestBridged_OperationMatrixDegradation verifies graceful degradation on unsupported operations.
func TestBridged_OperationMatrixDegradation(t *testing.T) {
	b, _, spy, _ := setupBridgedBot(t)
	_ = b.State.SetBinding(&ChannelBinding{ChannelID: "chan_bridged", AgentID: "agy"})

	t.Run("aside degradation", func(t *testing.T) {
		i := makeInteraction("aside", []*discordgo.ApplicationCommandInteractionDataOption{
			{Name: "question", Type: discordgo.ApplicationCommandOptionString, Value: any("what is your plan?")},
		})
		b.handleAsideCommand(b.Session, i)

		output := spy.allOutput()
		if !strings.Contains(output, "Aside is not supported for bridged agent **agy**") {
			t.Errorf("expected aside unsupported message, got: %q", output)
		}
	})

	t.Run("add degradation", func(t *testing.T) {
		i := makeInteraction("add", []*discordgo.ApplicationCommandInteractionDataOption{
			{Name: "message", Type: discordgo.ApplicationCommandOptionString, Value: any("staged note")},
		})
		b.handleAddCommand(b.Session, i)

		output := spy.allOutput()
		if !strings.Contains(output, "Queuing input is not supported for bridged agent **agy**") {
			t.Errorf("expected add unsupported message, got: %q", output)
		}
	})

	t.Run("compact degradation", func(t *testing.T) {
		i := makeInteraction("compact", nil)
		b.handleCompactCommand(b.Session, i)

		output := spy.allOutput()
		if !strings.Contains(output, "Compaction is not supported for bridged agent **agy**") {
			t.Errorf("expected compact unsupported message, got: %q", output)
		}
	})

	t.Run("fill degradation", func(t *testing.T) {
		i := makeInteraction("fill", nil)
		b.handleFillCommand(b.Session, i)

		output := spy.allOutput()
		if !strings.Contains(output, "Backfill is not supported for bridged agent **agy**") {
			t.Errorf("expected fill unsupported message, got: %q", output)
		}
	})
}

// TestBridged_ImageAttachmentDelegated verifies that image attachments are passed through
// to the protocol client (downstream bridge binary decides acceptance/refusal, no proactive rejection).
func TestBridged_ImageAttachmentDelegated(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "image/png")
		_, _ = w.Write([]byte("\x89PNG\r\n\x1a\nfake-png-bytes"))
	}))
	defer srv.Close()

	b, _, _, _ := setupBridgedBot(t)

	// Send an attachment with image MIME
	atts := []*discordgo.MessageAttachment{
		{
			ID:          "att_1",
			Filename:    "screenshot.png",
			ContentType: "image/png",
			URL:         srv.URL + "/image.png",
		},
	}

	result, err := b.ProcessAttachments(context.Background(), "agy", atts)
	if err != nil {
		t.Fatalf("ProcessAttachments failed: %v", err)
	}

	// Verify no proactive bot rejection notice was generated
	for _, notice := range result.Notices {
		if strings.Contains(notice, "Image attachments are not supported for bridged agents") {
			t.Fatalf("unexpected proactive rejection notice: %s", notice)
		}
	}
	if result.ImagesDownloaded != 0 {
		t.Fatalf("expected 0 downloaded images when bridge refuses, got: %d", result.ImagesDownloaded)
	}
}

// TestBridged_NameCollision_RemoteManifestWins verifies that REMOTE_MANIFEST strictly wins over local directory presence.
func TestBridged_NameCollision_RemoteManifestWins(t *testing.T) {
	b, sdk, spy, ws := setupBridgedBot(t)

	// Create local folder for agy with AGENTS.md and runtime.json
	localAgyDir := filepath.Join(ws, "agy")
	_ = os.MkdirAll(localAgyDir, 0755)
	_ = os.WriteFile(filepath.Join(localAgyDir, "AGENTS.md"), []byte("Local AGENTS prompt"), 0644)
	_ = os.WriteFile(filepath.Join(localAgyDir, "runtime.json"), []byte(`{"model":"local-model"}`), 0644)

	// REMOTE_MANIFEST already has agy routed
	if !b.isBridged("agy") {
		t.Fatal("REMOTE_MANIFEST must take precedence over local folder presence")
	}

	_ = b.State.SetBinding(&ChannelBinding{ChannelID: "chan_bridged", AgentID: "agy"})
	sdk.mu.Lock()
	sdk.addAndGenStreams = []func(*agentv1.AddAndGenerateTurnStreamResponse){
		func(r *agentv1.AddAndGenerateTurnStreamResponse) { r.Text = "Bridged answer" },
	}
	sdk.mu.Unlock()

	b.HandleMessageCreate(b.Session, makeMessage("ping"))

	output := spy.allOutput()
	if !strings.Contains(output, "Bridged answer") {
		t.Fatalf("expected response from bridged stream, got: %q", output)
	}

	// Verify no session turns were written to local session.jsonl
	turns, _ := readSessionTurns(localAgyDir)
	if len(turns) != 0 {
		t.Fatalf("expected 0 turns in local folder session.jsonl, got %d", len(turns))
	}
}

// TestBridged_StatusAndAgents verifies /status and /agents output for bridged agents.
func TestBridged_StatusAndAgents(t *testing.T) {
	b, _, spy, _ := setupBridgedBot(t)
	_ = b.State.SetBinding(&ChannelBinding{ChannelID: "chan_bridged", AgentID: "agy"})

	t.Run("status command", func(t *testing.T) {
		i := makeInteraction("status", nil)
		b.handleStatusCommand(b.Session, i)

		output := spy.allOutput()
		if !strings.Contains(output, "Model:** `bridged` (wackyacp bridge)") {
			t.Errorf("expected bridged model in status output, got: %q", output)
		}
	})

	t.Run("status command with custom bridge binary", func(t *testing.T) {
		_ = b.State.SetBinding(&ChannelBinding{ChannelID: "chan_bridged", AgentID: "custom"})
		i := makeInteraction("status", nil)
		b.handleStatusCommand(b.Session, i)

		output := spy.allOutput()
		if !strings.Contains(output, "Model:** `bridged` (wackycustom bridge)") {
			t.Errorf("expected custom bridge binary in status output, got: %q", output)
		}
	})

	t.Run("agents command", func(t *testing.T) {
		i := makeInteraction("agents", nil)
		b.handleAgentsCommand(b.Session, i)

		output := spy.allOutput()
		if !strings.Contains(output, "**`agy`** — Model: `bridged`") {
			t.Errorf("expected agy with bridged model in agents output, got: %q", output)
		}
	})
}

// TestBridged_ValidateBindings verifies the startup validation path (bot.go ValidateBindings) for bridged agents:
// - A manifest-only agent bound to a channel emits NO missing-agent warning
// - When the probe errors, an accessible-bridged-agent warning is emitted instead
func TestBridged_ValidateBindings(t *testing.T) {
	b, sdk, _, _ := setupBridgedBot(t)

	// 1. Manifest-only agent (agy) bound to a channel:
	// Local folder for agy does NOT exist (AgentDirExists is false in fakeAgent).
	_ = b.State.SetBinding(&ChannelBinding{ChannelID: "chan_bridged", AgentID: "agy"})

	// Probe succeeds: must produce NO warnings (specifically, no missing-agent warning)
	warnings := b.ValidateBindings()
	if len(warnings) != 0 {
		t.Fatalf("expected 0 warnings for healthy manifest-only bridged agent, got: %v", warnings)
	}

	// 2. Probe errors: must produce accessible-bridged-agent warning, NOT missing-agent warning
	sdk.mu.Lock()
	sdk.bridgeInspectErr = status.Error(codes.Unavailable, "bridge offline")
	sdk.mu.Unlock()

	warningsErr := b.ValidateBindings()
	if len(warningsErr) != 1 {
		t.Fatalf("expected 1 warning when bridged probe errors, got: %v", warningsErr)
	}
	if !strings.Contains(warningsErr[0], "bound to inaccessible bridged agent \"agy\"") {
		t.Fatalf("expected inaccessible bridged agent warning, got: %q", warningsErr[0])
	}
	if strings.Contains(warningsErr[0], "missing agent") {
		t.Fatalf("must not report missing agent for bridged agent, got: %q", warningsErr[0])
	}
}
