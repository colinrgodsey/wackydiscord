package bot

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/bwmarrin/discordgo"
	"github.com/colinrgodsey/wackypub/pkg/agent"
	agentv1 "github.com/colinrgodsey/wackypub/pkg/agent/v1"
)

func TestInteractionUserID_GuildAndDM(t *testing.T) {
	// 1. Guild interaction (Member.User populated)
	iGuild := &discordgo.InteractionCreate{
		Interaction: &discordgo.Interaction{
			Member: &discordgo.Member{
				User: &discordgo.User{ID: "guild_user_123"},
			},
		},
	}
	if uid := interactionUserID(iGuild); uid != "guild_user_123" {
		t.Errorf("expected guild_user_123, got %q", uid)
	}

	// 2. DM interaction (Member is nil, User is populated)
	iDM := &discordgo.InteractionCreate{
		Interaction: &discordgo.Interaction{
			User: &discordgo.User{ID: "dm_user_456"},
		},
	}
	if uid := interactionUserID(iDM); uid != "dm_user_456" {
		t.Errorf("expected dm_user_456, got %q", uid)
	}

	// 3. Both Member and User nil (does not panic, returns "")
	iEmpty := &discordgo.InteractionCreate{
		Interaction: &discordgo.Interaction{},
	}
	if uid := interactionUserID(iEmpty); uid != "" {
		t.Errorf("expected empty string for empty interaction, got %q", uid)
	}

	// 4. Member present but Member.User nil
	iMemberNilUser := &discordgo.InteractionCreate{
		Interaction: &discordgo.Interaction{
			Member: &discordgo.Member{},
		},
	}
	if uid := interactionUserID(iMemberNilUser); uid != "" {
		t.Errorf("expected empty string when Member.User is nil, got %q", uid)
	}

	// 5. InteractionCreate nil
	if uid := interactionUserID(nil); uid != "" {
		t.Errorf("expected empty string for nil interaction, got %q", uid)
	}
}

func setupTestBotWithSpy(t *testing.T) (*Bot, *string, *discordgo.MessageFlags) {
	t.Helper()
	tmpDir := t.TempDir()
	markerPath := filepath.Join(tmpDir, "WACKYPUB_ROOT")
	if err := os.WriteFile(markerPath, []byte(""), 0644); err != nil {
		t.Fatalf("failed to write WACKYPUB_ROOT: %v", err)
	}

	bobDir := filepath.Join(tmpDir, "bob")
	if err := os.MkdirAll(bobDir, 0755); err != nil {
		t.Fatalf("MkdirAll failed: %v", err)
	}
	_ = os.WriteFile(filepath.Join(bobDir, "AGENTS.md"), []byte("Prompt"), 0644)

	stateFile := filepath.Join(tmpDir, ".wackydiscord.json")
	st, err := NewState(stateFile)
	if err != nil {
		t.Fatalf("NewState failed: %v", err)
	}

	s, err := discordgo.New("Bot dummy_token")
	if err != nil {
		t.Fatalf("failed to create discord session: %v", err)
	}

	var capturedResponse string
	var capturedFlags discordgo.MessageFlags
	var mu sync.Mutex

	s.Client.Transport = fakeRoundTripper(func(req *http.Request) (*http.Response, error) {
		body, _ := io.ReadAll(req.Body)
		var resp discordgo.InteractionResponse
		_ = json.Unmarshal(body, &resp)
		mu.Lock()
		if resp.Data != nil {
			capturedResponse = resp.Data.Content
			capturedFlags = resp.Data.Flags
		}
		mu.Unlock()
		return &http.Response{
			StatusCode: 200,
			Body:       io.NopCloser(strings.NewReader("{}")),
			Header:     make(http.Header),
		}, nil
	})

	b := &Bot{
		Session: s,
		WsDir:   tmpDir,
		State:   st,
		SDK:     agent.NewSDK(tmpDir),
	}

	return b, &capturedResponse, &capturedFlags
}

func TestSlashGate_NonClaimCommandBlockedForNonOwner(t *testing.T) {
	b, capturedResp, capturedFlags := setupTestBotWithSpy(t)

	// Claim the bot as owner_1
	ok, _, _ := b.State.TryClaim("owner_1")
	if !ok {
		t.Fatalf("TryClaim failed")
	}

	// Non-owner "attacker" attempts /status
	i := &discordgo.InteractionCreate{
		Interaction: &discordgo.Interaction{
			ID:    "int_1",
			Token: "tok_1",
			Type:  discordgo.InteractionApplicationCommand,
			Data: discordgo.ApplicationCommandInteractionData{
				Name: "status",
			},
			Member: &discordgo.Member{
				User: &discordgo.User{ID: "attacker"},
			},
		},
	}

	b.HandleInteraction(b.Session, i)

	if !strings.Contains(*capturedResp, "This bot is claimed by another user") {
		t.Errorf("expected rejection message for non-owner, got: %q", *capturedResp)
	}
	if *capturedFlags&discordgo.MessageFlagsEphemeral == 0 {
		t.Errorf("expected ephemeral flag to be set on rejection, got flags: %v", *capturedFlags)
	}
}

func TestSlashGate_UnclaimedDefaultClosed(t *testing.T) {
	b, capturedResp, capturedFlags := setupTestBotWithSpy(t)
	b.State.SetDefaultOpen(false)

	// Unclaimed bot: user_1 tries /status -> rejected with unclaimed notice
	iStatus := &discordgo.InteractionCreate{
		Interaction: &discordgo.Interaction{
			ID:    "int_1",
			Token: "tok_1",
			Type:  discordgo.InteractionApplicationCommand,
			Data: discordgo.ApplicationCommandInteractionData{
				Name: "status",
			},
			Member: &discordgo.Member{
				User: &discordgo.User{ID: "user_1"},
			},
		},
	}

	b.HandleInteraction(b.Session, iStatus)

	if !strings.Contains(*capturedResp, "This bot is unclaimed — run `/claim` to take ownership.") {
		t.Errorf("expected unclaimed notice, got: %q", *capturedResp)
	}
	if *capturedFlags&discordgo.MessageFlagsEphemeral == 0 {
		t.Errorf("expected ephemeral response")
	}

	// /claim is permitted through the gate
	iClaim := &discordgo.InteractionCreate{
		Interaction: &discordgo.Interaction{
			ID:    "int_2",
			Token: "tok_2",
			Type:  discordgo.InteractionApplicationCommand,
			Data: discordgo.ApplicationCommandInteractionData{
				Name: "claim",
			},
			Member: &discordgo.Member{
				User: &discordgo.User{ID: "user_1"},
			},
		},
	}

	*capturedResp = ""
	b.HandleInteraction(b.Session, iClaim)

	if !strings.Contains(*capturedResp, "Bot claimed successfully") || !strings.Contains(*capturedResp, "user_1") {
		t.Errorf("expected claim success message, got: %q", *capturedResp)
	}
	if b.State.ClaimedUser() != "user_1" {
		t.Errorf("expected ClaimedUser to be user_1, got: %q", b.State.ClaimedUser())
	}
}

func TestSlashGate_HandleClaimSecondUserRejected(t *testing.T) {
	b, capturedResp, capturedFlags := setupTestBotWithSpy(t)

	// Claim as user_1
	ok, _, _ := b.State.TryClaim("user_1")
	if !ok {
		t.Fatalf("TryClaim failed")
	}

	// user_2 attempts to /claim
	iClaim2 := &discordgo.InteractionCreate{
		Interaction: &discordgo.Interaction{
			ID:    "int_claim_2",
			Token: "tok_2",
			Type:  discordgo.InteractionApplicationCommand,
			Data: discordgo.ApplicationCommandInteractionData{
				Name: "claim",
			},
			Member: &discordgo.Member{
				User: &discordgo.User{ID: "user_2"},
			},
		},
	}

	b.HandleInteraction(b.Session, iClaim2)

	if !strings.Contains(*capturedResp, "already claimed by <@user_1>") {
		t.Errorf("expected already claimed by user_1 message, got: %q", *capturedResp)
	}
	if *capturedFlags&discordgo.MessageFlagsEphemeral == 0 {
		t.Errorf("expected ephemeral flag")
	}
	if b.State.ClaimedUser() != "user_1" {
		t.Errorf("expected ClaimedUser to stay user_1, got %q", b.State.ClaimedUser())
	}

	// user_1 claims again -> idempotent success
	*capturedResp = ""
	iClaim1Again := &discordgo.InteractionCreate{
		Interaction: &discordgo.Interaction{
			ID:    "int_claim_1_again",
			Token: "tok_1_again",
			Type:  discordgo.InteractionApplicationCommand,
			Data: discordgo.ApplicationCommandInteractionData{
				Name: "claim",
			},
			Member: &discordgo.Member{
				User: &discordgo.User{ID: "user_1"},
			},
		},
	}

	b.HandleInteraction(b.Session, iClaim1Again)

	if !strings.Contains(*capturedResp, "Bot claimed successfully") {
		t.Errorf("expected idempotent claim success message, got: %q", *capturedResp)
	}
}

func TestSlashGate_HandleUnclaim(t *testing.T) {
	b, capturedResp, capturedFlags := setupTestBotWithSpy(t)

	ok, _, _ := b.State.TryClaim("user_1")
	if !ok {
		t.Fatalf("TryClaim failed")
	}

	// 1. Non-owner user_2 tries /unclaim -> blocked by slash gate
	iUnclaim2 := &discordgo.InteractionCreate{
		Interaction: &discordgo.Interaction{
			ID:    "int_unclaim_2",
			Token: "tok_2",
			Type:  discordgo.InteractionApplicationCommand,
			Data: discordgo.ApplicationCommandInteractionData{
				Name: "unclaim",
			},
			Member: &discordgo.Member{
				User: &discordgo.User{ID: "user_2"},
			},
		},
	}

	b.HandleInteraction(b.Session, iUnclaim2)

	if !strings.Contains(*capturedResp, "This bot is claimed by another user") {
		t.Errorf("expected non-owner unclaim to be rejected by gate, got: %q", *capturedResp)
	}
	if b.State.ClaimedUser() != "user_1" {
		t.Errorf("expected user_1 to still be owner")
	}

	// 2. Direct call to handleUnclaimCommand by non-owner (defense-in-depth)
	*capturedResp = ""
	b.handleUnclaimCommand(b.Session, iUnclaim2)
	if !strings.Contains(*capturedResp, "you are not the current owner") {
		t.Errorf("expected direct unclaim reject message, got: %q", *capturedResp)
	}

	// 3. Owner user_1 runs /unclaim -> succeeds
	*capturedResp = ""
	iUnclaim1 := &discordgo.InteractionCreate{
		Interaction: &discordgo.Interaction{
			ID:    "int_unclaim_1",
			Token: "tok_1",
			Type:  discordgo.InteractionApplicationCommand,
			Data: discordgo.ApplicationCommandInteractionData{
				Name: "unclaim",
			},
			Member: &discordgo.Member{
				User: &discordgo.User{ID: "user_1"},
			},
		},
	}

	b.HandleInteraction(b.Session, iUnclaim1)

	if !strings.Contains(*capturedResp, "Bot ownership released") {
		t.Errorf("expected ownership released message, got: %q", *capturedResp)
	}
	if *capturedFlags&discordgo.MessageFlagsEphemeral == 0 {
		t.Errorf("expected ephemeral unclaim confirmation")
	}
	if b.State.ClaimedUser() != "" {
		t.Errorf("expected bot to be unclaimed, got: %q", b.State.ClaimedUser())
	}
}

func TestMessage_NonOwnerSilentlyIgnored(t *testing.T) {
	b, _, _ := setupTestBotWithSpy(t)

	// Bind channel "c1" to agent "bob"
	_ = b.State.SetBinding(&ChannelBinding{
		ChannelID: "c1",
		AgentID:   "bob",
	})

	// Claim bot as user_1
	_, _, _ = b.State.TryClaim("user_1")

	var requestsMade int64
	b.Session.Client.Transport = fakeRoundTripper(func(req *http.Request) (*http.Response, error) {
		atomic.AddInt64(&requestsMade, 1)
		return &http.Response{
			StatusCode: 200,
			Body:       io.NopCloser(strings.NewReader("{}")),
			Header:     make(http.Header),
		}, nil
	})

	// Non-owner "user_2" sends a message to bound channel "c1"
	b.HandleMessageCreate(b.Session, &discordgo.MessageCreate{
		Message: &discordgo.Message{
			ChannelID: "c1",
			Content:   "Hello bot",
			Author: &discordgo.User{
				ID:  "user_2",
				Bot: false,
			},
		},
	})

	if atomic.LoadInt64(&requestsMade) != 0 {
		t.Fatalf("expected zero HTTP requests (silent ignore) for non-owner message, got %d", requestsMade)
	}

	// Verify no session turns were recorded
	res, _ := b.SDK.ReadSession(context.Background(), &agentv1.ReadSessionRequest{AgentId: "bob"})
	if res != nil && len(res.GetTurns()) != 0 {
		t.Fatalf("expected 0 session turns for agent bob, got %d", len(res.GetTurns()))
	}
}

func TestMessage_OwnerProcessed(t *testing.T) {
	b, _, _ := setupTestBotWithSpy(t)

	// Claim bot as user_1
	_, _, _ = b.State.TryClaim("user_1")

	// Channel is unbound
	// Non-owner is dropped before binding check:
	b.HandleMessageCreate(b.Session, &discordgo.MessageCreate{
		Message: &discordgo.Message{
			ChannelID: "unbound_chan",
			Content:   "Hello",
			Author:    &discordgo.User{ID: "user_2", Bot: false},
		},
	})

	// Owner passes allowlist gate and reaches binding check (cleanly exits without panic)
	b.HandleMessageCreate(b.Session, &discordgo.MessageCreate{
		Message: &discordgo.Message{
			ChannelID: "unbound_chan",
			Content:   "Hello",
			Author:    &discordgo.User{ID: "user_1", Bot: false},
		},
	})
}

func TestMessage_UnclaimedDefaultClosedIgnored(t *testing.T) {
	b, _, _ := setupTestBotWithSpy(t)
	b.State.SetDefaultOpen(false)

	_ = b.State.SetBinding(&ChannelBinding{
		ChannelID: "c1",
		AgentID:   "bob",
	})

	var requestsMade int64
	b.Session.Client.Transport = fakeRoundTripper(func(req *http.Request) (*http.Response, error) {
		atomic.AddInt64(&requestsMade, 1)
		return &http.Response{
			StatusCode: 200,
			Body:       io.NopCloser(strings.NewReader("{}")),
			Header:     make(http.Header),
		}, nil
	})

	b.HandleMessageCreate(b.Session, &discordgo.MessageCreate{
		Message: &discordgo.Message{
			ChannelID: "c1",
			Content:   "Hello",
			Author:    &discordgo.User{ID: "user_random", Bot: false},
		},
	})

	if atomic.LoadInt64(&requestsMade) != 0 {
		t.Fatalf("expected zero HTTP requests in default-closed unclaimed mode, got %d", requestsMade)
	}
}

func TestSlashGate_HandleClaim_SaveError(t *testing.T) {
	b, capturedResp, capturedFlags := setupTestBotWithSpy(t)

	// Make state persistence fail by pointing filePath into a regular file
	blockingFile := filepath.Join(b.WsDir, "blocker")
	if err := os.WriteFile(blockingFile, []byte("block"), 0644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}
	b.State.filePath = filepath.Join(blockingFile, "invalid_dir", "state.json")

	iClaim := &discordgo.InteractionCreate{
		Interaction: &discordgo.Interaction{
			ID:    "int_claim_err",
			Token: "tok_claim_err",
			Type:  discordgo.InteractionApplicationCommand,
			Data: discordgo.ApplicationCommandInteractionData{
				Name: "claim",
			},
			Member: &discordgo.Member{
				User: &discordgo.User{ID: "user_1"},
			},
		},
	}

	b.HandleInteraction(b.Session, iClaim)

	if !strings.Contains(strings.ToLower(*capturedResp), "failed to persist state, try again") {
		t.Errorf("expected error message mentioning failed to persist state, got: %q", *capturedResp)
	}
	if *capturedFlags&discordgo.MessageFlagsEphemeral == 0 {
		t.Errorf("expected ephemeral flag to be set")
	}
	if b.State.ClaimedUser() != "" {
		t.Errorf("expected ClaimedUser to remain empty after failed persistence")
	}
}

func TestSlashGate_HandleUnclaim_SaveError(t *testing.T) {
	b, capturedResp, capturedFlags := setupTestBotWithSpy(t)

	ok, _, err := b.State.TryClaim("user_1")
	if !ok || err != nil {
		t.Fatalf("TryClaim failed: ok=%v, err=%v", ok, err)
	}

	// Make state persistence fail
	blockingFile := filepath.Join(b.WsDir, "blocker")
	if err := os.WriteFile(blockingFile, []byte("block"), 0644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}
	b.State.filePath = filepath.Join(blockingFile, "invalid_dir", "state.json")

	iUnclaim := &discordgo.InteractionCreate{
		Interaction: &discordgo.Interaction{
			ID:    "int_unclaim_err",
			Token: "tok_unclaim_err",
			Type:  discordgo.InteractionApplicationCommand,
			Data: discordgo.ApplicationCommandInteractionData{
				Name: "unclaim",
			},
			Member: &discordgo.Member{
				User: &discordgo.User{ID: "user_1"},
			},
		},
	}

	b.HandleInteraction(b.Session, iUnclaim)

	if !strings.Contains(strings.ToLower(*capturedResp), "failed to persist state, try again") {
		t.Errorf("expected error message mentioning failed to persist state, got: %q", *capturedResp)
	}
	if *capturedFlags&discordgo.MessageFlagsEphemeral == 0 {
		t.Errorf("expected ephemeral flag to be set")
	}
	if b.State.ClaimedUser() != "user_1" {
		t.Errorf("expected ClaimedUser to remain user_1 after failed persistence")
	}
}
