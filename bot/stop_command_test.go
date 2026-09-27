package bot

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
	agentv1 "github.com/colinrgodsey/wackypub/pkg/agent/v1"
	"google.golang.org/genai"
)

// TestSlashGate_StopBlockedForNonOwner verifies /stop is gated by the D102
// allowlist like every non-/claim command: a claimed bot rejects /stop from a
// non-owner with an ephemeral message, so a random channel member cannot cancel
// someone else's generation.
func TestSlashGate_StopBlockedForNonOwner(t *testing.T) {
	b, capturedResp, capturedFlags := setupTestBotWithSpy(t)

	// Claim the bot as owner_1; non-owner "attacker" cannot stop the turn.
	ok, _, _ := b.State.TryClaim("owner_1")
	if !ok {
		t.Fatalf("TryClaim failed")
	}

	_ = b.State.SetBinding(&ChannelBinding{
		ChannelID: "chan_stop_gate",
		AgentID:   "bob",
	})

	i := &discordgo.InteractionCreate{
		Interaction: &discordgo.Interaction{
			ID:        "int_stop_1",
			Token:     "tok_stop_1",
			ChannelID: "chan_stop_gate",
			Type:      discordgo.InteractionApplicationCommand,
			Data: discordgo.ApplicationCommandInteractionData{
				Name: "stop",
			},
			Member: &discordgo.Member{
				User: &discordgo.User{ID: "attacker"},
			},
		},
	}

	b.HandleInteraction(b.Session, i)

	if !strings.Contains(*capturedResp, "This bot is claimed by another user") {
		t.Errorf("expected rejection message for non-owner /stop, got: %q", *capturedResp)
	}
	if *capturedFlags&discordgo.MessageFlagsEphemeral == 0 {
		t.Errorf("expected ephemeral flag on /stop rejection, got flags: %v", *capturedFlags)
	}
}

// TestStopCommand_PartialCommitSessionState pins the documented cancel
// semantics: /stop during a running turn keeps the user turn in session.jsonl
// (partial commit) and never writes a torn model turn, and the in-flight
// registration is released so a second /stop reports no in-flight turn.
func TestStopCommand_PartialCommitSessionState(t *testing.T) {
	tmpDir := t.TempDir()
	stateFile := filepath.Join(tmpDir, ".wackydiscord.json")
	st, err := NewState(stateFile)
	if err != nil {
		t.Fatalf("NewState failed: %v", err)
	}

	sdk := newFakeAgent(tmpDir)
	b := &Bot{
		WsDir:  tmpDir,
		State:  st,
		Client: sdk,
	}

	var capturedResponse string
	s, err := discordgo.New("Bot fake-token")
	if err != nil {
		t.Fatalf("discordgo.New failed: %v", err)
	}
	s.Client.Transport = fakeRoundTripper(func(req *http.Request) (*http.Response, error) {
		body, _ := io.ReadAll(req.Body)
		var resp discordgo.InteractionResponse
		_ = json.Unmarshal(body, &resp)
		if resp.Data != nil {
			capturedResponse = resp.Data.Content
		}
		return &http.Response{
			StatusCode: 200,
			Body:       io.NopCloser(strings.NewReader("{}")),
			Header:     make(http.Header),
		}, nil
	})

	bobDir := agentDirIn(tmpDir, "bob")
	if err := appendSessionContent(bobDir, genai.NewContentFromText("Hello from the partial-commit test", "user")); err != nil {
		t.Fatalf("appendSessionContent failed: %v", err)
	}

	_ = st.SetBinding(&ChannelBinding{
		ChannelID: "chan_partial",
		AgentID:   "bob",
	})

	sdk.hold = make(chan struct{})
	sdk.started = make(chan struct{})

	streamDone := make(chan struct{})
	go func() {
		defer close(streamDone)
		stream, err := sdk.GenerateTurnStream(context.Background(), &agentv1.GenerateTurnStreamRequest{
			AgentId:      "bob",
			WorkspaceDir: tmpDir,
		})
		if err != nil {
			return
		}
		for {
			_, err := stream.Recv()
			if err != nil {
				return
			}
		}
	}()

	select {
	case <-sdk.started:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for request to start")
	}

	iStop := &discordgo.InteractionCreate{
		Interaction: &discordgo.Interaction{
			ID:        "int_stop_2",
			Token:     "tok_stop_2",
			ChannelID: "chan_partial",
			Type:      discordgo.InteractionApplicationCommand,
			Data: discordgo.ApplicationCommandInteractionData{
				Name: "stop",
			},
			Member: &discordgo.Member{
				User: &discordgo.User{ID: "owner_1"},
			},
		},
	}
	b.handleStopCommand(s, iStop)

	select {
	case <-streamDone:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for stream to finish after stop")
	}

	if !strings.Contains(capturedResponse, "Turn cancelled for agent") {
		t.Errorf("expected turn cancelled message, got: %q", capturedResponse)
	}

	// Partial commit: user turn persisted, NO model turn written.
	turns, err := readSessionTurns(bobDir)
	if err != nil {
		t.Fatalf("ReadSessionTurns failed: %v", err)
	}
	if len(turns) != 1 {
		t.Fatalf("expected exactly 1 turn (user) after cancelled generation, got %d: %+v", len(turns), turns)
	}
	if turns[0].Role != "user" || !strings.Contains(contentText(turns[0]), "partial-commit test") {
		t.Errorf("expected user turn preserved with original message, got role=%s text=%s", turns[0].Role, contentText(turns[0]))
	}

	// Registration released: a second /stop reports no in-flight turn.
	capturedResponse = ""
	b.handleStopCommand(s, iStop)
	if !strings.Contains(capturedResponse, "No in-flight turn for agent") {
		t.Errorf("expected no in-flight turn message after release, got: %q", capturedResponse)
	}
}
