package bot

import (
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

	"github.com/bwmarrin/discordgo"
	"github.com/colinrgodsey/wackypub/pkg/agent"
	"google.golang.org/genai"
)

// compactSpy records what wackydiscord sent back to Discord. A deferred command answers
// twice, so the first response type is kept apart from the content of the last one.
type compactSpy struct {
	mu        sync.Mutex
	requests  int
	firstType discordgo.InteractionResponseType
	content   string
	flags     discordgo.MessageFlags
}

func (s *compactSpy) snapshot() (int, discordgo.InteractionResponseType, string, discordgo.MessageFlags) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.requests, s.firstType, s.content, s.flags
}

// setupCompactBot builds a bot bound to agent "bob" in a throwaway workspace.
func setupCompactBot(t *testing.T, runtimeJSON string) (*Bot, *compactSpy, string) {
	t.Helper()

	cwd, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	ws := t.TempDir()
	_ = os.WriteFile(filepath.Join(ws, "WACKYPUB_ROOT"), []byte(""), 0644)
	_ = os.WriteFile(filepath.Join(ws, agent.AllowedAgentsFile), []byte("bob\n"), 0644)

	// The A2A authorization check walks up from the process working directory, so leaving it
	// wherever go test started would pass or fail on whichever WACKYPUB_ROOT or
	// WACKYPUB_ALLOWED_AGENTS happens to sit above the checkout. Pin it to this fixture.
	if err := os.Chdir(ws); err != nil {
		t.Fatalf("Chdir: %v", err)
	}
	t.Cleanup(func() { _ = os.Chdir(cwd) })
	agentDir := filepath.Join(ws, "bob")
	if err := os.MkdirAll(agentDir, 0755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	_ = os.WriteFile(filepath.Join(agentDir, "AGENTS.md"), []byte("Bob prompt"), 0644)
	if runtimeJSON != "" {
		_ = os.WriteFile(filepath.Join(agentDir, "runtime.json"), []byte(runtimeJSON), 0644)
	}

	st, err := NewState(filepath.Join(ws, ".wackydiscord.json"))
	if err != nil {
		t.Fatalf("NewState: %v", err)
	}
	s, err := discordgo.New("Bot dummy_token")
	if err != nil {
		t.Fatalf("discordgo.New: %v", err)
	}

	spy := &compactSpy{}
	s.Client.Transport = fakeRoundTripper(func(req *http.Request) (*http.Response, error) {
		body, _ := io.ReadAll(req.Body)
		var payload struct {
			Type    int    `json:"type"`
			Content string `json:"content"`
			Flags   int    `json:"flags"`
			Data    *struct {
				Content string `json:"content"`
				Flags   int    `json:"flags"`
			} `json:"data"`
		}
		_ = json.Unmarshal(body, &payload)

		spy.mu.Lock()
		spy.requests++
		if spy.requests == 1 {
			spy.firstType = discordgo.InteractionResponseType(payload.Type)
		}
		if payload.Content != "" {
			spy.content = payload.Content
			spy.flags = discordgo.MessageFlags(payload.Flags)
		} else if payload.Data != nil && payload.Data.Content != "" {
			spy.content = payload.Data.Content
			spy.flags = discordgo.MessageFlags(payload.Data.Flags)
		}
		spy.mu.Unlock()

		return &http.Response{
			StatusCode: 200,
			Body:       io.NopCloser(strings.NewReader("{}")),
			Header:     make(http.Header),
		}, nil
	})

	b := &Bot{WsDir: ws, State: st, SDK: agent.NewSDK(ws), Session: s}
	if err := b.State.SetBinding(&ChannelBinding{ChannelID: "chan_compact", AgentID: "bob"}); err != nil {
		t.Fatalf("SetBinding: %v", err)
	}
	return b, spy, agentDir
}

func compactInteraction(force bool) *discordgo.InteractionCreate {
	var opt *discordgo.ApplicationCommandInteractionDataOption
	if force {
		opt = &discordgo.ApplicationCommandInteractionDataOption{
			Name: "force", Type: discordgo.ApplicationCommandOptionBoolean, Value: any(true),
		}
	}
	return compactInteractionOption(opt)
}

// compactInteractionOption builds a /compact interaction carrying an arbitrary option, so a
// caller can also hand over one of the wrong type.
func compactInteractionOption(opt *discordgo.ApplicationCommandInteractionDataOption) *discordgo.InteractionCreate {
	var opts []*discordgo.ApplicationCommandInteractionDataOption
	if opt != nil {
		opts = append(opts, opt)
	}
	return &discordgo.InteractionCreate{
		Interaction: &discordgo.Interaction{
			ID:        "int_compact",
			Token:     "tok_compact",
			ChannelID: "chan_compact",
			Type:      discordgo.InteractionApplicationCommand,
			Data: discordgo.ApplicationCommandInteractionData{
				Name:    "compact",
				Options: opts,
			},
			Member: &discordgo.Member{User: &discordgo.User{ID: "owner_1"}},
		},
	}
}

// seedSession writes n user/model pairs so the agent has something to archive.
func seedSession(t *testing.T, agentDir string, n int) {
	t.Helper()
	var turns []*genai.Content
	for i := 1; i <= n; i++ {
		turns = append(turns,
			genai.NewContentFromText(fmt.Sprintf("user turn %d talking about something specific", i), "user"),
			genai.NewContentFromText(fmt.Sprintf("model turn %d answering about that same thing", i), "model"),
		)
	}
	if err := agent.WriteSessionTurns(agentDir, turns); err != nil {
		t.Fatalf("WriteSessionTurns: %v", err)
	}
}

func sessionTurnCount(t *testing.T, agentDir string) int {
	t.Helper()
	turns, err := agent.ReadSessionTurns(agentDir)
	if err != nil {
		t.Fatalf("ReadSessionTurns: %v", err)
	}
	return len(turns)
}

// TestSlashGate_CompactBlockedForNonOwner pins the auth gate: /compact mutates an agent's
// session, so a claimed bot must refuse it from anyone who is not the owner.
func TestSlashGate_CompactBlockedForNonOwner(t *testing.T) {
	b, capturedResp, capturedFlags := setupTestBotWithSpy(t)
	if ok, _, _ := b.State.TryClaim("owner_1"); !ok {
		t.Fatalf("TryClaim failed")
	}
	_ = b.State.SetBinding(&ChannelBinding{ChannelID: "chan_compact", AgentID: "bob"})

	i := &discordgo.InteractionCreate{
		Interaction: &discordgo.Interaction{
			ID:        "int_compact_gate",
			Token:     "tok_compact_gate",
			ChannelID: "chan_compact",
			Type:      discordgo.InteractionApplicationCommand,
			Data:      discordgo.ApplicationCommandInteractionData{Name: "compact"},
			Member:    &discordgo.Member{User: &discordgo.User{ID: "attacker"}},
		},
	}
	b.HandleInteraction(b.Session, i)

	if !strings.Contains(*capturedResp, "claimed by another user") {
		t.Errorf("expected ownership rejection, got %q", *capturedResp)
	}
	if *capturedFlags&discordgo.MessageFlagsEphemeral == 0 {
		t.Errorf("expected ephemeral flag, got %v", *capturedFlags)
	}
}

// TestCompactCommand_BusyRejection: compaction rewrites session.jsonl while the runner is
// appending to it, so it must refuse outright rather than queue behind a live turn.
func TestCompactCommand_BusyRejection(t *testing.T) {
	b, spy, agentDir := setupCompactBot(t, `{"model":"test-model","endpoint":"http://127.0.0.1:1/v1","contextWindow":1000000}`)
	seedSession(t, agentDir, 2)
	if err := b.State.SetBinding(&ChannelBinding{ChannelID: "chan_compact", AgentID: "bob", IsGenerating: true}); err != nil {
		t.Fatalf("SetBinding: %v", err)
	}

	b.handleCompactCommand(b.Session, compactInteraction(true))

	_, _, content, flags := spy.snapshot()
	if !strings.Contains(content, "currently generating") {
		t.Errorf("expected busy rejection, got %q", content)
	}
	if flags&discordgo.MessageFlagsEphemeral == 0 {
		t.Errorf("expected ephemeral flag, got %v", flags)
	}
	if n := sessionTurnCount(t, agentDir); n != 4 {
		t.Errorf("a rejected compaction must not touch the session, %d turns remain, want 4", n)
	}
}

// TestCompactCommand_RespectsGatesAndReportsReason: the default must leave an
// under-threshold session alone and say why with the numbers, not a bare no.
func TestCompactCommand_RespectsGatesAndReportsReason(t *testing.T) {
	b, spy, agentDir := setupCompactBot(t, `{"model":"test-model","endpoint":"http://127.0.0.1:1/v1","contextWindow":1000000}`)
	seedSession(t, agentDir, 3)

	b.handleCompactCommand(b.Session, compactInteraction(false))

	_, _, content, _ := spy.snapshot()
	if !strings.Contains(content, "Nothing to compact") {
		t.Errorf("expected the gate to skip, got %q", content)
	}
	if !strings.Contains(content, "compaction threshold") || !strings.Contains(content, "force:true") {
		t.Errorf("expected the threshold numbers and the force escape hatch, got %q", content)
	}
	if n := sessionTurnCount(t, agentDir); n != 6 {
		t.Errorf("a skipped compaction removed turns: %d remain, want 6", n)
	}
}

// TestCompactCommand_ForceArchivesAndReportsCounts: force overrides the gates the default
// respects, the reply carries turn counts, and the caller is deferred first because
// summarizing is a model call. The window is generous on purpose, so only force can archive.
func TestCompactCommand_ForceArchivesAndReportsCounts(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"choices":[{"message":{"role":"assistant","content":"* archived summary of earlier turns"},"finish_reason":"stop"}]}`)
	}))
	defer srv.Close()

	b, spy, agentDir := setupCompactBot(t, fmt.Sprintf(`{"model":"test-model","endpoint":%q,"contextWindow":1000000}`, srv.URL))
	seedSession(t, agentDir, 4)

	b.handleCompactCommand(b.Session, compactInteraction(true))

	requests, firstType, content, _ := spy.snapshot()
	if firstType != discordgo.InteractionResponseDeferredChannelMessageWithSource {
		t.Errorf("expected the deferred ack first, got response type %v", firstType)
	}
	if requests < 2 {
		t.Errorf("expected an ack and then an edited verdict, saw %d requests", requests)
	}
	if !strings.Contains(content, "Compacted bob") || !strings.Contains(content, "archived") {
		t.Errorf("expected a compaction verdict with counts, got %q", content)
	}
	if !strings.Contains(content, "compacted context") {
		t.Errorf("expected the fresh-context note, got %q", content)
	}
	n := sessionTurnCount(t, agentDir)
	if n >= 8 {
		t.Errorf("a forced compaction archived nothing: %d turns remain", n)
	}
	if counts := fmt.Sprintf("(8 to %d)", n); !strings.Contains(content, counts) {
		t.Errorf("expected the archived counts %s in the verdict, got %q", counts, content)
	}
	mem, err := os.ReadFile(filepath.Join(agentDir, "MEMORY.md"))
	if err != nil || !strings.Contains(string(mem), "archived summary") {
		t.Errorf("expected the summary in MEMORY.md, got %q (err %v)", string(mem), err)
	}
}

// TestCompactCommand_EmptySessionIsACleanNoOp covers the other D44 gate: forcing an agent with
// nothing to archive is a no-op, reported as one rather than as a failure.
func TestCompactCommand_EmptySessionIsACleanNoOp(t *testing.T) {
	b, spy, _ := setupCompactBot(t, `{"model":"test-model","endpoint":"http://127.0.0.1:1/v1","contextWindow":1000000}`)

	b.handleCompactCommand(b.Session, compactInteraction(true))

	_, _, content, _ := spy.snapshot()
	if !strings.Contains(content, "Nothing to compact") || !strings.Contains(content, "no session turns") {
		t.Errorf("expected a clean no-op report, got %q", content)
	}
	if strings.Contains(content, "Compaction failed") {
		t.Errorf("an empty session must not be reported as a failure, got %q", content)
	}
}

// TestCompactCommand_ForceOptionOfWrongTypeIsIgnored: BoolValue panics when Discord sends the
// option as another type, and a panic in an interaction handler loses the turn.
func TestCompactCommand_ForceOptionOfWrongTypeIsIgnored(t *testing.T) {
	b, spy, agentDir := setupCompactBot(t, `{"model":"test-model","endpoint":"http://127.0.0.1:1/v1","contextWindow":1000000}`)
	seedSession(t, agentDir, 2)

	i := compactInteractionOption(&discordgo.ApplicationCommandInteractionDataOption{
		Name: "force", Type: discordgo.ApplicationCommandOptionString, Value: any("yes"),
	})
	b.handleCompactCommand(b.Session, i)

	_, _, content, _ := spy.snapshot()
	if !strings.Contains(content, "Nothing to compact") {
		t.Errorf("a mistyped force option must fall back to the gated default, got %q", content)
	}
}

// TestCompactCommand_RoutedAgentAsksTheBridge: a channel bound to a REMOTE_MANIFEST agent has
// no local session, so the command must go out through dispatch instead of compacting nothing
// locally and reporting success.
func TestCompactCommand_RoutedAgentAsksTheBridge(t *testing.T) {
	b, spy, agentDir := setupCompactBot(t, `{"model":"test-model","endpoint":"http://127.0.0.1:1/v1","contextWindow":1000000}`)
	seedSession(t, agentDir, 2)
	_ = os.WriteFile(filepath.Join(b.WsDir, agent.RemoteManifestFile),
		[]byte("bob: /nonexistent/bridge-binary --agent-folder="+agentDir+"\n"), 0644)

	b.handleCompactCommand(b.Session, compactInteraction(false))

	_, _, content, _ := spy.snapshot()
	if !strings.Contains(content, "bob") {
		t.Errorf("expected the routed agent to be named, got %q", content)
	}
	if strings.Contains(content, "Compacted") || strings.Contains(content, "Nothing to compact") {
		t.Errorf("routed agent was handled locally instead of dispatched: %q", content)
	}
	if n := sessionTurnCount(t, agentDir); n != 4 {
		t.Errorf("the bridge path must not touch the local session, %d turns remain, want 4", n)
	}
}
