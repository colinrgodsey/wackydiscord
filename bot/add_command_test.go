package bot

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/bwmarrin/discordgo"
	"github.com/colinrgodsey/wackypub/pkg/agent"
	agentv1 "github.com/colinrgodsey/wackypub/pkg/agent/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func addInteraction(message string) *discordgo.InteractionCreate {
	return addInteractionOptions([]*discordgo.ApplicationCommandInteractionDataOption{
		{Name: "message", Type: discordgo.ApplicationCommandOptionString, Value: any(message)},
	})
}

func addInteractionOptions(opts []*discordgo.ApplicationCommandInteractionDataOption) *discordgo.InteractionCreate {
	return &discordgo.InteractionCreate{
		Interaction: &discordgo.Interaction{
			ID:        "int_add",
			Token:     "tok_add",
			ChannelID: "chan_compact",
			Type:      discordgo.InteractionApplicationCommand,
			Data: discordgo.ApplicationCommandInteractionData{
				Name:    "add",
				Options: opts,
			},
			Member: &discordgo.Member{User: &discordgo.User{ID: "owner_1"}},
		},
	}
}

func sessionRoles(t *testing.T, agentDir string) []string {
	t.Helper()
	turns, err := agent.ReadSessionTurns(agentDir)
	if err != nil {
		t.Fatalf("ReadSessionTurns: %v", err)
	}
	roles := make([]string, 0, len(turns))
	for _, turn := range turns {
		roles = append(roles, turn.Role)
	}
	return roles
}

// unusedRuntime is a model endpoint that must never be called: /add queues input, it does not
// spend a generation.
func unusedRuntime(t *testing.T, hits *int32) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(hits, 1)
		w.Header().Set("Content-Type", "application/json")
		_ = w
	}))
}

func markGenerating(t *testing.T, b *Bot, channelID string) {
	t.Helper()
	// GetBinding hands out a copy, so the flag has to be persisted for the handler to see it.
	binding := b.State.GetBinding(channelID)
	binding.IsGenerating = true
	if err := b.State.SetBinding(binding); err != nil {
		t.Fatalf("SetBinding: %v", err)
	}
}

func TestAddCommand_QueuesTurnWithoutGenerating(t *testing.T) {
	var hits int32
	srv := unusedRuntime(t, &hits)
	defer srv.Close()

	b, spy, agentDir := setupCompactBot(t, runtimeJSON(srv.URL))
	seedSession(t, agentDir, 2)

	b.handleAddCommand(b.Session, addInteraction("the staging key rotated this morning"))

	_, _, content, flags := spy.snapshot()
	if !strings.Contains(content, "Queued for **bob**") {
		t.Fatalf("expected a queued confirmation, got %q", content)
	}
	if flags != 0 {
		t.Errorf("the confirmation should be visible to the channel, flags %d", flags)
	}

	roles := sessionRoles(t, agentDir)
	if len(roles) != 5 {
		t.Fatalf("session has %d turns, want the 4 seeded plus the queued one", len(roles))
	}
	if roles[4] != "user" {
		t.Errorf("queued turn has role %q, want user", roles[4])
	}
	for i, role := range roles {
		if role == "model" && i > 3 {
			t.Errorf("a model turn appeared at %d: /add generated", i)
		}
	}
	if n := atomic.LoadInt32(&hits); n != 0 {
		t.Errorf("the model was called %d times; /add must not generate", n)
	}

	// The text must arrive verbatim, or the operator is guessing at what was stored.
	turns, err := agent.ReadSessionTurns(agentDir)
	if err != nil {
		t.Fatalf("ReadSessionTurns: %v", err)
	}
	if got := agent.ContentText(turns[4]); !strings.Contains(got, "the staging key rotated this morning") {
		t.Errorf("queued turn text is %q", got)
	}
}

// An add during a live turn would write into the transcript the runner is appending to, so the
// rejection has to happen before anything is stored.
func TestAddCommand_BusyRejectLeavesSessionUntouched(t *testing.T) {
	var hits int32
	srv := unusedRuntime(t, &hits)
	defer srv.Close()

	b, spy, agentDir := setupCompactBot(t, runtimeJSON(srv.URL))
	seedSession(t, agentDir, 2)
	markGenerating(t, b, "chan_compact")

	b.handleAddCommand(b.Session, addInteraction("interleave me"))

	_, _, content, flags := spy.snapshot()
	if !strings.Contains(content, "currently generating") {
		t.Fatalf("expected the generating-busy rejection, got %q", content)
	}
	if flags != discordgo.MessageFlagsEphemeral {
		t.Errorf("expected an ephemeral refusal, flags %d", flags)
	}
	if n := sessionTurnCount(t, agentDir); n != 4 {
		t.Errorf("session grew to %d turns despite the rejection", n)
	}
}

func TestAddCommand_UnboundChannelIsRejected(t *testing.T) {
	b, spy, _ := setupCompactBot(t, "")
	if err := b.State.RemoveBinding("chan_compact"); err != nil {
		t.Fatalf("RemoveBinding: %v", err)
	}

	b.handleAddCommand(b.Session, addInteraction("anything"))

	_, _, content, flags := spy.snapshot()
	if !strings.Contains(content, "not bound") {
		t.Errorf("expected the unbound notice, got %q", content)
	}
	if flags != discordgo.MessageFlagsEphemeral {
		t.Errorf("expected an ephemeral refusal, flags %d", flags)
	}
}

func TestAddCommand_EmptyOrWrongTypeMessageIsRejected(t *testing.T) {
	cases := map[string][]*discordgo.ApplicationCommandInteractionDataOption{
		"missing":   nil,
		"blank":     {{Name: "message", Type: discordgo.ApplicationCommandOptionString, Value: any("   ")}},
		"wrongType": {{Name: "message", Type: discordgo.ApplicationCommandOptionInteger, Value: any(7)}},
	}
	for name, opts := range cases {
		t.Run(name, func(t *testing.T) {
			b, spy, agentDir := setupCompactBot(t, "")
			seedSession(t, agentDir, 1)

			b.handleAddCommand(b.Session, addInteractionOptions(opts))

			_, _, content, flags := spy.snapshot()
			if !strings.Contains(content, "needs a message") {
				t.Errorf("expected the usage notice, got %q", content)
			}
			if flags != discordgo.MessageFlagsEphemeral {
				t.Errorf("expected an ephemeral refusal, flags %d", flags)
			}
			if n := sessionTurnCount(t, agentDir); n != 2 {
				t.Errorf("session grew to %d turns on a rejected command", n)
			}
		})
	}
}

func TestSlashGate_AddBlockedForNonOwner(t *testing.T) {
	b, spy, agentDir := setupCompactBot(t, "")
	seedSession(t, agentDir, 1)
	if _, _, err := b.State.TryClaim("owner_1"); err != nil {
		t.Fatalf("TryClaim: %v", err)
	}

	inter := addInteraction("queue this for later")
	inter.Member = &discordgo.Member{User: &discordgo.User{ID: "random_lurker"}}
	b.HandleInteraction(b.Session, inter)

	_, reqType, content, _ := spy.snapshot()
	if reqType != discordgo.InteractionResponseChannelMessageWithSource {
		t.Errorf("expected a direct reply, got response type %d", reqType)
	}
	if !strings.Contains(content, "claimed by another user") {
		t.Errorf("expected the claim refusal, got %q", content)
	}
	if n := sessionTurnCount(t, agentDir); n != 2 {
		t.Errorf("a blocked command wrote to the session: %d turns", n)
	}
}

// A routed agent has to be reached through dispatch resolution, otherwise /add would quietly
// write into a local folder that no harness will ever read.
func TestAddCommand_RoutedAgentGoesToTheBridge(t *testing.T) {
	b, spy, agentDir := setupCompactBot(t, "")
	seedSession(t, agentDir, 2)
	_ = os.WriteFile(filepath.Join(b.WsDir, agent.RemoteManifestFile),
		[]byte("bob: /nonexistent/bridge-binary --agent-folder="+agentDir+"\n"), 0644)

	b.handleAddCommand(b.Session, addInteraction("anything the bridge knows"))

	_, _, content, _ := spy.snapshot()
	if strings.Contains(content, "Queued") {
		t.Errorf("the turn was reported queued while the routed agent was never reached: %q", content)
	}
	if !strings.Contains(content, "bob") {
		t.Errorf("expected the unreachable agent to be named, got %q", content)
	}
	// The failure has to be the routing failure: an error that says nothing about the bridge would
	// mean the append went somewhere local instead.
	if !strings.Contains(content, "bridge") {
		t.Errorf("expected the bridge routing failure, got %q", content)
	}
	if n := sessionTurnCount(t, agentDir); n != 4 {
		t.Errorf("the routed path wrote to the local session: %d turns, want 4", n)
	}
}

func TestQueueFailureNamesUnsupportedBridges(t *testing.T) {
	bridgeMsg := "AddUserTurn is not implemented by this harness"
	unimpl := status.Errorf(codes.Unimplemented, "%s", bridgeMsg)

	for name, err := range map[string]error{
		"status error":   unimpl,
		"wrapped status": fmt.Errorf("dispatch to bridge: %w", unimpl),
	} {
		t.Run(name, func(t *testing.T) {
			msg := queueFailure("bob", err)
			if !strings.Contains(msg, "Queuing input") || !strings.Contains(msg, "bob") {
				t.Errorf("refusal did not say what is unsupported and for whom: %q", msg)
			}
			if !strings.Contains(msg, bridgeMsg) {
				t.Errorf("the bridge's own explanation was dropped: %q", msg)
			}
		})
	}

	for name, err := range map[string]error{
		"other code":  status.Error(codes.InvalidArgument, "message cannot be empty"),
		"plain error": fmt.Errorf("connection reset by peer"),
		"nil":         nil,
	} {
		t.Run(name, func(t *testing.T) {
			msg := queueFailure("bob", err)
			if !strings.Contains(msg, "Could not queue") {
				t.Errorf("expected the generic failure wording, got %q", msg)
			}
		})
	}
}

func TestAddAck_SurfacesHookWarnings(t *testing.T) {
	plain := addAck("bob", &agentv1.AddUserTurnResponse{Text: "queued text"})
	if strings.Contains(plain, "⚠️") {
		t.Errorf("a clean queue reported warnings: %q", plain)
	}
	if !strings.Contains(plain, "Nothing was generated") {
		t.Errorf("the confirmation must say nothing was generated: %q", plain)
	}

	warned := addAck("bob", &agentv1.AddUserTurnResponse{
		Text:     "queued text",
		Warnings: []string{"hook: date preamble failed"},
	})
	if !strings.Contains(warned, "hook: date preamble failed") {
		t.Errorf("hook warning dropped: %q", warned)
	}
}

// The watcher renders session.jsonl as the single source of truth, so an added turn has to
// arrive as attributed user backfill and never as an assistant answer, and the sync pass must
// not spend a generation on it.
func TestAddCommand_WatcherBackfillsAddedTurnWithoutGenerating(t *testing.T) {
	var hits int32
	srv := unusedRuntime(t, &hits)
	defer srv.Close()

	b, spy, agentDir := setupCompactBot(t, runtimeJSON(srv.URL))
	seedSession(t, agentDir, 1)
	// Watermark the seeded conversation, so the only unsynced turn is the one /add appends.
	turns, err := agent.ReadSessionTurns(agentDir)
	if err != nil {
		t.Fatalf("ReadSessionTurns: %v", err)
	}
	binding := b.State.GetBinding("chan_compact")
	binding.LastTurnIndex = len(turns) - 1
	binding.LastTurnHash = ComputeTurnHash(turns[len(turns)-1])
	if err := b.State.SetBinding(binding); err != nil {
		t.Fatalf("SetBinding: %v", err)
	}

	b.handleAddCommand(b.Session, addInteraction("note for the next turn"))
	b.SyncAgentToChannels("bob")

	_, _, content, _ := spy.snapshot()
	if !strings.Contains(content, "[User Turn]") || !strings.Contains(content, "note for the next turn") {
		t.Errorf("the queued turn was not backfilled as an attributed user turn: %q", content)
	}
	roles := sessionRoles(t, agentDir)
	if got := countRole(roles, "model"); got != 1 {
		t.Errorf("session carries %d model turns, want the single seeded one", got)
	}
	if got := countRole(roles, "user"); got != 2 {
		t.Errorf("session carries %d user turns, want the seeded one plus the queued one", got)
	}
	if n := atomic.LoadInt32(&hits); n != 0 {
		t.Errorf("the sync pass called the model %d times", n)
	}
}

func countRole(roles []string, role string) int {
	n := 0
	for _, r := range roles {
		if r == role {
			n++
		}
	}
	return n
}
