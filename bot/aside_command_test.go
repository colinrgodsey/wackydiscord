package bot

import (
	"encoding/json"
	"errors"
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
	"github.com/colinrgodsey/wackypub/pkg/agent"
	agentv1 "github.com/colinrgodsey/wackypub/pkg/agent/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// asideInteraction builds a /aside interaction carrying the question option, or no options at
// all when question is the empty-marker NO_OPTION.
func asideInteraction(question string) *discordgo.InteractionCreate {
	opts := []*discordgo.ApplicationCommandInteractionDataOption{
		{Name: "question", Type: discordgo.ApplicationCommandOptionString, Value: any(question)},
	}
	return asideInteractionOptions(opts)
}

func asideInteractionOptions(opts []*discordgo.ApplicationCommandInteractionDataOption) *discordgo.InteractionCreate {
	return &discordgo.InteractionCreate{
		Interaction: &discordgo.Interaction{
			ID:        "int_aside",
			Token:     "tok_aside",
			ChannelID: "chan_compact",
			Type:      discordgo.InteractionApplicationCommand,
			Data: discordgo.ApplicationCommandInteractionData{
				Name:    "aside",
				Options: opts,
			},
			Member: &discordgo.Member{User: &discordgo.User{ID: "owner_1"}},
		},
	}
}

// answeringRuntime is a fake model that returns reply, recording the bodies it was asked about.
func answeringRuntime(t *testing.T, reply string, bodies *[]string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		*bodies = append(*bodies, string(body))
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"choices": []map[string]any{
				{"index": 0, "message": map[string]any{"role": "assistant", "content": reply}, "finish_reason": "stop"},
			},
			"usage": map[string]any{"prompt_tokens": 120, "completion_tokens": 12, "total_tokens": 132},
		})
	}))
}

func runtimeJSON(url string) string {
	return fmt.Sprintf(`{"model":"test-model","endpoint":"%s/v1","contextWindow":1000000}`, url)
}

func TestAsideCommand_PostsAnswerAndPersistsNothing(t *testing.T) {
	var bodies []string
	srv := answeringRuntime(t, "The PR is blocked on the flaky watcher test.", &bodies)
	defer srv.Close()

	b, spy, agentDir := setupCompactBot(t, runtimeJSON(srv.URL))
	seedSession(t, agentDir, 2)

	b.handleAsideCommand(b.Session, asideInteraction("what is blocking the PR?"))

	_, _, content, _ := spy.snapshot()
	if !strings.Contains(content, "blocked on the flaky watcher test") {
		t.Fatalf("expected the answer in the reply, got %q", content)
	}
	if !strings.HasPrefix(content, "💬 ") {
		t.Errorf("expected the reply to read as an aside, got %q", content)
	}

	joined := strings.Join(bodies, "\n")
	if !strings.Contains(joined, "what is blocking the PR?") {
		t.Error("the question never reached the model")
	}
	if !strings.Contains(joined, "model turn 2 answering about that same thing") {
		t.Error("the aside did not carry the agent's accumulated session context")
	}

	// The whole point of an aside: nothing it touched survives it.
	if n := sessionTurnCount(t, agentDir); n != 4 {
		t.Errorf("aside persisted turns: session has %d, want the original 4", n)
	}
	if _, err := os.Stat(filepath.Join(agentDir, "MEMORY.md")); !os.IsNotExist(err) {
		t.Errorf("aside wrote MEMORY.md (err %v)", err)
	}
}

// The aside's distinguishing property is that it neither takes the session lock nor joins the
// busy-rejection list: asking while the agent generates is the feature.
func TestAsideCommand_QuestionsAgentMidGeneration(t *testing.T) {
	var bodies []string
	srv := answeringRuntime(t, "Still waiting on CI.", &bodies)
	defer srv.Close()

	b, spy, agentDir := setupCompactBot(t, runtimeJSON(srv.URL))
	seedSession(t, agentDir, 1)
	// GetBinding hands out a copy, so the flag has to be persisted for the handler to see it.
	binding := b.State.GetBinding("chan_compact")
	binding.IsGenerating = true
	if err := b.State.SetBinding(binding); err != nil {
		t.Fatalf("SetBinding: %v", err)
	}

	b.handleAsideCommand(b.Session, asideInteraction("status?"))

	_, _, content, _ := spy.snapshot()
	if strings.Contains(content, "currently generating") {
		t.Fatalf("aside busy-rejected a generating agent: %q", content)
	}
	if !strings.Contains(content, "Still waiting on CI") {
		t.Errorf("expected the answer despite a live generation, got %q", content)
	}
	if n := sessionTurnCount(t, agentDir); n != 2 {
		t.Errorf("session has %d turns, want the original 2", n)
	}
}

func TestAsideCommand_UnboundChannelIsRejected(t *testing.T) {
	b, spy, _ := setupCompactBot(t, "")
	if err := b.State.RemoveBinding("chan_compact"); err != nil {
		t.Fatalf("RemoveBinding: %v", err)
	}

	b.handleAsideCommand(b.Session, asideInteraction("anything"))

	_, _, content, flags := spy.snapshot()
	if !strings.Contains(content, "not bound") {
		t.Errorf("expected the unbound notice, got %q", content)
	}
	if flags != discordgo.MessageFlagsEphemeral {
		t.Errorf("expected an ephemeral refusal, flags %d", flags)
	}
}

func TestAsideCommand_EmptyOrWrongTypeQuestionIsRejected(t *testing.T) {
	cases := map[string][]*discordgo.ApplicationCommandInteractionDataOption{
		"missing":   nil,
		"blank":     {{Name: "question", Type: discordgo.ApplicationCommandOptionString, Value: any("   ")}},
		"wrongType": {{Name: "question", Type: discordgo.ApplicationCommandOptionInteger, Value: any(7)}},
	}
	for name, opts := range cases {
		t.Run(name, func(t *testing.T) {
			b, spy, _ := setupCompactBot(t, "")
			b.handleAsideCommand(b.Session, asideInteractionOptions(opts))

			_, _, content, flags := spy.snapshot()
			if !strings.Contains(content, "needs a question") {
				t.Errorf("expected the usage notice, got %q", content)
			}
			if flags != discordgo.MessageFlagsEphemeral {
				t.Errorf("expected an ephemeral refusal, flags %d", flags)
			}
		})
	}
}

func TestSlashGate_AsideBlockedForNonOwner(t *testing.T) {
	b, spy, _ := setupCompactBot(t, "")
	if _, _, err := b.State.TryClaim("owner_1"); err != nil {
		t.Fatalf("TryClaim: %v", err)
	}

	inter := asideInteraction("ask me anything")
	inter.Member = &discordgo.Member{User: &discordgo.User{ID: "random_lurker"}}
	b.HandleInteraction(b.Session, inter)

	_, reqType, content, _ := spy.snapshot()
	if reqType != discordgo.InteractionResponseChannelMessageWithSource {
		t.Errorf("expected a direct reply, got type %d", reqType)
	}
	if !strings.Contains(content, "claimed by another user") {
		t.Errorf("expected the claim refusal, got %q", content)
	}
}

// A bridged agent's refusal is a designed answer, so it has to arrive as itself rather than as
// a generic failure the operator then debugs for an hour.
func TestAsideCommand_BridgedUnimplementedSurfacesBridgeMessage(t *testing.T) {
	bridgeMsg := "aside is not supported over the ACP bridge: bridged harness sessions cannot fork the agent's accumulated context"
	unimpl := status.Errorf(codes.Unimplemented, "%s", bridgeMsg)

	cases := map[string]error{
		"status error":   unimpl,
		"wrapped status": fmt.Errorf("dispatch to bridge: %w", unimpl),
	}
	for name, err := range cases {
		t.Run(name, func(t *testing.T) {
			msg, ok := asideUnsupportedMessage("bob", err)
			if !ok {
				t.Fatalf("Unimplemented not recognised: %v", err)
			}
			if !strings.Contains(msg, bridgeMsg) || !strings.Contains(msg, "bob") {
				t.Errorf("bridge explanation not surfaced verbatim: %q", msg)
			}
		})
	}

	for name, err := range map[string]error{
		"other code":  status.Error(codes.InvalidArgument, "question cannot be empty"),
		"plain error": errors.New("connection reset by peer"),
		"nil":         nil,
	} {
		if msg, ok := asideUnsupportedMessage("bob", err); ok {
			t.Errorf("%s misread as an unsupported-bridge refusal: %q", name, msg)
		}
	}
}

func TestAsideCommand_RoutedAgentGoesToTheBridge(t *testing.T) {
	b, spy, agentDir := setupCompactBot(t, `{"model":"test-model","endpoint":"http://127.0.0.1:1/v1","contextWindow":1000000}`)
	seedSession(t, agentDir, 2)
	_ = os.WriteFile(filepath.Join(b.WsDir, agent.RemoteManifestFile),
		[]byte("bob: /nonexistent/bridge-binary --agent-folder="+agentDir+"\n"), 0644)

	b.handleAsideCommand(b.Session, asideInteraction("anything the bridge knows"))

	_, _, content, _ := spy.snapshot()
	if strings.Contains(content, "Aside ·") {
		t.Errorf("an answer was fabricated locally for a routed agent: %q", content)
	}
	if !strings.Contains(content, "bob") {
		t.Errorf("expected the unreachable agent to be named, got %q", content)
	}
	if n := sessionTurnCount(t, agentDir); n != 4 {
		t.Errorf("routed path touched the local session: %d turns, want 4", n)
	}
}

func TestAsideReply_FooterReportsDenialsWarningsAndUsage(t *testing.T) {
	reply := asideReply("bob", &agentv1.AsideQuestionResponse{
		Text:        "I would check the CI log.",
		ToolDenials: 2,
		Warnings:    []string{"runtime fell back to the default model"},
		Usage:       &agentv1.TurnUsage{TotalTokens: 9100},
	})
	for _, want := range []string{"Aside", "2 tool attempt(s) denied", "fell back to the default model", "9100 tokens"} {
		if !strings.Contains(reply, want) {
			t.Errorf("footer missing %q in %q", want, reply)
		}
	}

	if got := asideReply("bob", &agentv1.AsideQuestionResponse{Text: "   "}); !strings.Contains(got, "empty aside answer") {
		t.Errorf("expected an explicit empty-answer notice, got %q", got)
	}

	plain := asideReply("bob", &agentv1.AsideQuestionResponse{Text: "Clean answer."})
	if strings.Contains(plain, "tokens") || strings.Contains(plain, "denied") {
		t.Errorf("footer should stay off a metadata-free answer: %q", plain)
	}
}

// The per-agent mutex is the only thing keeping two people asking at once from each paying for
// a full context turn simultaneously, so it has to serialize the same agent and leave other
// agents alone.
func TestLockAgentAsideSerializesPerAgent(t *testing.T) {
	st, err := NewState(filepath.Join(t.TempDir(), ".wackydiscord.json"))
	if err != nil {
		t.Fatalf("NewState: %v", err)
	}

	var mu sync.Mutex
	var running, overlaps int
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			unlock := st.LockAgentAside("bob")
			mu.Lock()
			running++
			if running > 1 {
				overlaps++
			}
			mu.Unlock()
			time.Sleep(10 * time.Millisecond)
			mu.Lock()
			running--
			mu.Unlock()
			unlock()
		}()
	}
	wg.Wait()
	if overlaps != 0 {
		t.Fatalf("%d concurrent asides against one agent overlapped, want serialized", overlaps)
	}

	bobUnlock := st.LockAgentAside("bob")
	other := make(chan struct{})
	go func() {
		annaUnlock := st.LockAgentAside("anna")
		annaUnlock()
		close(other)
	}()
	select {
	case <-other:
	case <-time.After(2 * time.Second):
		t.Fatal("an aside for another agent was blocked behind bob's aside")
	}
	bobUnlock()
}
