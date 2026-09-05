package bot

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/colinrgodsey/wackypub/pkg/agent"
	"google.golang.org/genai"
)

// setupTestBot creates an isolated workspace with WACKYPUB_ROOT, an agent, and a Bot instance.
func setupTestBot(t *testing.T, agentID string) (*Bot, string, *State) {
	t.Helper()

	tmpDir := t.TempDir()
	markerPath := filepath.Join(tmpDir, "WACKYPUB_ROOT")
	if err := os.WriteFile(markerPath, []byte(""), 0644); err != nil {
		t.Fatalf("failed to write WACKYPUB_ROOT: %v", err)
	}

	agentDir := filepath.Join(tmpDir, agentID)
	if err := os.MkdirAll(agentDir, 0755); err != nil {
		t.Fatalf("failed to create agentDir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(agentDir, "AGENTS.md"), []byte("Prompt for "+agentID), 0644); err != nil {
		t.Fatalf("failed to write AGENTS.md: %v", err)
	}
	if err := os.WriteFile(filepath.Join(agentDir, agent.AllowedAgentsFile), []byte(agentID+"\n"), 0644); err != nil {
		t.Fatalf("failed to write allowed agents: %v", err)
	}

	stateFile := filepath.Join(tmpDir, ".wackydiscord.json")
	st, err := NewState(stateFile)
	if err != nil {
		t.Fatalf("NewState failed: %v", err)
	}

	origCwd, _ := os.Getwd()
	if err := os.Chdir(tmpDir); err != nil {
		t.Fatalf("failed to chdir to tmpDir: %v", err)
	}
	t.Cleanup(func() {
		_ = os.Chdir(origCwd)
	})

	sdk := agent.NewSDK(tmpDir)
	b := &Bot{
		WsDir: tmpDir,
		State: st,
		SDK:   sdk,
	}

	watcher, err := NewSessionWatcher(b)
	if err != nil {
		t.Fatalf("NewSessionWatcher failed: %v", err)
	}
	t.Cleanup(func() {
		_ = watcher.Close()
	})
	b.Watcher = watcher

	return b, agentDir, st
}

// createMockDiscordSession creates a discordgo.Session with recorded outbound messages.
func createMockDiscordSession(mu *sync.Mutex, sentMessages *[]string) *discordgo.Session {
	s, _ := discordgo.New("Bot fake-token")
	s.Client.Transport = fakeRoundTripper(func(req *http.Request) (*http.Response, error) {
		body, _ := io.ReadAll(req.Body)
		var payload struct {
			Content string `json:"content"`
		}
		if err := json.Unmarshal(body, &payload); err == nil && payload.Content != "" {
			mu.Lock()
			*sentMessages = append(*sentMessages, payload.Content)
			mu.Unlock()
		}

		return &http.Response{
			StatusCode: 200,
			Body:       io.NopCloser(strings.NewReader(`{"id":"msg_1"}`)),
			Header:     make(http.Header),
		}, nil
	})
	return s
}

// 1. Test that tool execution during generation renders via watcher without deadlocking.
func TestWatcher_ToolExecutionDuringGenerationRendersWithoutDeadlock(t *testing.T) {
	b, agentDir, st := setupTestBot(t, "builder")

	var mu sync.Mutex
	var sentMessages []string
	s := createMockDiscordSession(&mu, &sentMessages)
	b.Session = s

	// Initial user prompt that triggered the generation turn
	initTurn := genai.NewContentFromText("Build the target", "user")
	if err := agent.AppendSessionContent(agentDir, initTurn); err != nil {
		t.Fatalf("AppendSessionContent initTurn failed: %v", err)
	}
	initHash := ComputeTurnHash(initTurn)

	channelID := "chan_tool_render"
	binding := &ChannelBinding{
		ChannelID:     channelID,
		AgentID:       "builder",
		Verbose:       true,
		IsGenerating:  true, // Actively generating!
		LastTurnIndex: 0,
		LastTurnHash:  initHash,
	}
	if err := st.SetBinding(binding); err != nil {
		t.Fatalf("SetBinding failed: %v", err)
	}

	// Append tool call turn (role: model) and tool response turn (role: user)
	callTurn := &genai.Content{
		Role: "model",
		Parts: []*genai.Part{
			{
				FunctionCall: &genai.FunctionCall{
					Name: "run_build",
					Args: map[string]any{"target": "all"},
				},
			},
		},
	}
	respTurn := &genai.Content{
		Role: "user",
		Parts: []*genai.Part{
			{
				FunctionResponse: &genai.FunctionResponse{
					Name:     "run_build",
					Response: map[string]any{"output": "Build succeeded: 0 errors, 0 warnings"},
				},
			},
		},
	}
	if err := agent.AppendSessionContent(agentDir, callTurn); err != nil {
		t.Fatalf("AppendSessionContent callTurn failed: %v", err)
	}
	if err := agent.AppendSessionContent(agentDir, respTurn); err != nil {
		t.Fatalf("AppendSessionContent respTurn failed: %v", err)
	}

	// Trigger watcher sync while IsGenerating is true. Must not deadlock or drop turns.
	syncDone := make(chan struct{})
	go func() {
		b.SyncAgentToChannels("builder")
		close(syncDone)
	}()

	select {
	case <-syncDone:
		// Succeeded without deadlock
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for SyncAgentToChannels: possible deadlock")
	}

	mu.Lock()
	msgs := append([]string{}, sentMessages...)
	mu.Unlock()

	foundCall := false
	foundOutput := false
	for _, m := range msgs {
		if strings.Contains(m, "Tool Call") && strings.Contains(m, "run_build") {
			foundCall = true
		}
		if strings.Contains(m, "Tool Output") && strings.Contains(m, "Build succeeded") {
			foundOutput = true
		}
	}

	if !foundCall || !foundOutput {
		t.Errorf("expected batched tool call and output rendered, got messages: %v", msgs)
	}

	// Verify sync markers were committed
	finalBnd := st.GetBinding(channelID)
	if finalBnd == nil || finalBnd.LastTurnIndex != 2 {
		t.Errorf("expected LastTurnIndex to be updated to 2, got %+v", finalBnd)
	}
}

// 2. Test that user message is not echoed back by the watcher (atomic hash registration).
func TestWatcher_UserMessageNotEchoedBack_AtomicRegistration(t *testing.T) {
	b, agentDir, st := setupTestBot(t, "echo_test_agent")

	var mu sync.Mutex
	var sentMessages []string
	s := createMockDiscordSession(&mu, &sentMessages)
	b.Session = s

	channelID := "chan_echo_test"
	binding := &ChannelBinding{
		ChannelID:     channelID,
		AgentID:       "echo_test_agent",
		LastTurnIndex: -1,
	}
	if err := st.SetBinding(binding); err != nil {
		t.Fatalf("SetBinding failed: %v", err)
	}

	userMsgText := "Verify the reactor core temperature."

	// Simulate atomic registration under LockChannelSync
	func() {
		syncUnlock := st.LockChannelSync(channelID)
		defer syncUnlock()

		bnd := st.GetBinding(channelID)
		if bnd == nil {
			t.Fatal("binding missing")
		}

		bnd.IsGenerating = true
		_ = st.SetBinding(bnd)

		res, err := b.SDK.AddUserTurn(bnd.AgentID, userMsgText)
		if err != nil {
			t.Fatalf("AddUserTurn failed: %v", err)
		}

		freshBnd := st.GetBinding(channelID)
		if freshBnd != nil && freshBnd.AgentID == binding.AgentID {
			actualHash := ComputeTurnHash(res.Content)
			freshBnd.AddPendingUserHash(actualHash)
			freshBnd.IsGenerating = true
			if err := st.SetBinding(freshBnd); err != nil {
				t.Fatalf("SetBinding failed: %v", err)
			}
		}
	}()

	// Now watcher fires (leading edge)
	b.SyncAgentToChannels("echo_test_agent")

	mu.Lock()
	msgs := append([]string{}, sentMessages...)
	mu.Unlock()

	// Verify that the user message was NOT echoed back to Discord
	for _, m := range msgs {
		if strings.Contains(m, "User Turn") || strings.Contains(m, userMsgText) {
			t.Errorf("user message was echoed back to Discord: %q", m)
		}
	}

	// Verify the pending hash was consumed
	bnd := st.GetBinding(channelID)
	if len(bnd.PendingUserHashes) != 0 {
		t.Errorf("expected PendingUserHashes to be consumed, got: %v", bnd.PendingUserHashes)
	}
	if bnd.LastTurnIndex != 0 {
		t.Errorf("expected LastTurnIndex to be updated to 0, got %d", bnd.LastTurnIndex)
	}
	_ = agentDir
}

// 3. Test that FlushNow flushes pending turns immediately without waiting for debounce timers.
func TestWatcher_FlushNowFlushesPendingTurnsImmediately(t *testing.T) {
	b, agentDir, st := setupTestBot(t, "flush_agent")

	var mu sync.Mutex
	var sentMessages []string
	s := createMockDiscordSession(&mu, &sentMessages)
	b.Session = s

	initTurn := genai.NewContentFromText("User prompt", "user")
	if err := agent.AppendSessionContent(agentDir, initTurn); err != nil {
		t.Fatalf("AppendSessionContent initTurn failed: %v", err)
	}
	initHash := ComputeTurnHash(initTurn)

	channelID := "chan_flush"
	binding := &ChannelBinding{
		ChannelID:     channelID,
		AgentID:       "flush_agent",
		LastTurnIndex: 0,
		LastTurnHash:  initHash,
	}
	if err := st.SetBinding(binding); err != nil {
		t.Fatalf("SetBinding failed: %v", err)
	}

	// Append a model turn
	modelTurn := genai.NewContentFromText("Mission accomplished immediately.", "model")
	if err := agent.AppendSessionContent(agentDir, modelTurn); err != nil {
		t.Fatalf("AppendSessionContent failed: %v", err)
	}

	// Start debounce timers
	b.Watcher.debounceAgentSync("flush_agent")

	// Call FlushNow synchronously
	start := time.Now()
	b.Watcher.FlushNow("flush_agent")
	elapsed := time.Since(start)

	// Verify FlushNow executed immediately (well under the 400ms debounce timer)
	if elapsed > 250*time.Millisecond {
		t.Errorf("FlushNow took %v, expected near-instantaneous execution", elapsed)
	}

	// Verify turns were rendered
	mu.Lock()
	msgs := append([]string{}, sentMessages...)
	mu.Unlock()

	found := false
	for _, m := range msgs {
		if strings.Contains(m, "Mission accomplished immediately.") {
			found = true
		}
	}
	if !found {
		t.Errorf("expected model response flushed immediately, got: %v", msgs)
	}

	// Verify sync marker was committed
	finalBnd := st.GetBinding(channelID)
	if finalBnd == nil || finalBnd.LastTurnIndex != 1 {
		t.Errorf("expected LastTurnIndex to be 1, got %+v", finalBnd)
	}
	time.Sleep(20 * time.Millisecond)
}

// 4. Test that /unbind synchronizes cleanly with turn generation.
func TestWatcher_UnbindSynchronizesCleanlyWithTurnGeneration(t *testing.T) {
	b, _, st := setupTestBot(t, "unbind_agent")

	channelID := "chan_unbind_sync"
	_ = st.SetBinding(&ChannelBinding{
		ChannelID: channelID,
		AgentID:   "unbind_agent",
	})

	// Acquire LockChannelTurn to simulate an active in-flight generation turn
	turnUnlock := st.LockChannelTurn(channelID)

	unbindStarted := make(chan struct{})
	unbindDone := make(chan struct{})

	go func() {
		close(unbindStarted)
		b.handleUnbindCommand(nil, &discordgo.InteractionCreate{
			Interaction: &discordgo.Interaction{
				ChannelID: channelID,
				Type:      discordgo.InteractionApplicationCommand,
			},
		})
		close(unbindDone)
	}()

	<-unbindStarted

	// /unbind must be blocked while LockChannelTurn is held
	select {
	case <-unbindDone:
		t.Fatal("handleUnbindCommand should have blocked while generation held LockChannelTurn")
	case <-time.After(50 * time.Millisecond):
		// Expected to be blocked
	}

	// Generation completes: release LockChannelTurn
	turnUnlock()

	select {
	case <-unbindDone:
		// Succeeded
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for handleUnbindCommand after turn lock released")
	}

	// Verify channel is cleanly unbound
	if remaining := st.GetBinding(channelID); remaining != nil {
		t.Errorf("expected channel to be unbound, got %+v", remaining)
	}
}

// 5. Test tool turn coalescing into batched summaries <= 2000 chars.
func TestWatcher_ToolTurnCoalescingAndChunking(t *testing.T) {
	b, agentDir, st := setupTestBot(t, "batch_agent")

	var mu sync.Mutex
	var sentMessages []string
	s := createMockDiscordSession(&mu, &sentMessages)
	b.Session = s

	initTurn := genai.NewContentFromText("Batch user prompt", "user")
	if err := agent.AppendSessionContent(agentDir, initTurn); err != nil {
		t.Fatalf("AppendSessionContent initTurn failed: %v", err)
	}
	initHash := ComputeTurnHash(initTurn)

	channelID := "chan_batch"
	_ = st.SetBinding(&ChannelBinding{
		ChannelID:     channelID,
		AgentID:       "batch_agent",
		Verbose:       true,
		LastTurnIndex: 0,
		LastTurnHash:  initHash,
	})

	// Append 3 contiguous tool calls and 3 tool outputs
	for k := 1; k <= 3; k++ {
		call := &genai.Content{
			Role: "model",
			Parts: []*genai.Part{
				{
					FunctionCall: &genai.FunctionCall{
						Name: fmt.Sprintf("tool_%d", k),
						Args: map[string]any{"arg": fmt.Sprintf("val_%d", k)},
					},
				},
			},
		}
		resp := &genai.Content{
			Role: "user",
			Parts: []*genai.Part{
				{
					FunctionResponse: &genai.FunctionResponse{
						Name:     fmt.Sprintf("tool_%d", k),
						Response: map[string]any{"output": fmt.Sprintf("result_%d", k)},
					},
				},
			},
		}
		_ = agent.AppendSessionContent(agentDir, call)
		_ = agent.AppendSessionContent(agentDir, resp)
	}

	// Add final assistant text turn
	_ = agent.AppendSessionContent(agentDir, genai.NewContentFromText("All tools finished.", "model"))

	// Sync turns
	count, err := b.autoFillUnsyncedTurns(s, nil, channelID, 0)
	if err != nil {
		t.Fatalf("autoFillUnsyncedTurns failed: %v", err)
	}
	if count != 7 {
		t.Errorf("expected 7 turns synced, got %d", count)
	}

	mu.Lock()
	msgs := append([]string{}, sentMessages...)
	mu.Unlock()

	// Contiguous tool turns should be coalesced into 1 message, followed by assistant text
	if len(msgs) != 2 {
		t.Errorf("expected 2 messages (1 batched tools + 1 text), got %d: %v", len(msgs), msgs)
	}

	// Enforce 2000-char limit on all messages
	for i, m := range msgs {
		if len(m) > 2000 {
			t.Errorf("message %d exceeded 2000 chars: len %d", i, len(m))
		}
	}
}

// 6. Test synthetic sentinel turn handling (D88 continuations).
func TestWatcher_SyntheticContinuationTurns(t *testing.T) {
	b, agentDir, st := setupTestBot(t, "continuation_agent")

	var mu sync.Mutex
	var sentMessages []string
	s := createMockDiscordSession(&mu, &sentMessages)
	b.Session = s

	initTurn := genai.NewContentFromText("Initial task prompt", "user")
	if err := agent.AppendSessionContent(agentDir, initTurn); err != nil {
		t.Fatalf("AppendSessionContent initTurn failed: %v", err)
	}
	initHash := ComputeTurnHash(initTurn)

	channelID := "chan_continuation"
	_ = st.SetBinding(&ChannelBinding{
		ChannelID:     channelID,
		AgentID:       "continuation_agent",
		Verbose:       true,
		LastTurnIndex: 0,
		LastTurnHash:  initHash,
	})

	// Inject synthetic continuation user turn
	continuationTurn := genai.NewContentFromText(
		`<CONTINUATION reason="post-compaction">Session context was compacted. Resume your task.</CONTINUATION>`,
		"user",
	)
	_ = agent.AppendSessionContent(agentDir, continuationTurn)

	// Sync turns
	count, err := b.autoFillUnsyncedTurns(s, nil, channelID, 0)
	if err != nil {
		t.Fatalf("autoFillUnsyncedTurns failed: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 turn synced, got %d", count)
	}

	mu.Lock()
	msgs := append([]string{}, sentMessages...)
	mu.Unlock()

	if len(msgs) != 1 {
		t.Fatalf("expected 1 message for verbose synthetic turn, got %d: %v", len(msgs), msgs)
	}
	if !strings.Contains(msgs[0], "[Auto-Continuation]") || !strings.Contains(msgs[0], "Post-compaction resume") {
		t.Errorf("unexpected synthetic status badge: %q", msgs[0])
	}
}

// 7. Test that concurrent sync (e.g. FlushNow while leading-edge sync is sending HTTP messages)
// does not echo the user message or double-render assistant output.
func TestWatcher_ConcurrentSyncDoesNotEchoOrDoubleRender(t *testing.T) {
	b, agentDir, st := setupTestBot(t, "concurrent_agent")

	var mu sync.Mutex
	var sentMessages []string
	sendStarted := make(chan struct{})
	allowSendFinish := make(chan struct{})

	s, _ := discordgo.New("Bot fake-token")
	s.Client.Transport = fakeRoundTripper(func(req *http.Request) (*http.Response, error) {
		body, _ := io.ReadAll(req.Body)
		var payload struct {
			Content string `json:"content"`
		}
		if err := json.Unmarshal(body, &payload); err == nil && payload.Content != "" {
			mu.Lock()
			sentMessages = append(sentMessages, payload.Content)
			mu.Unlock()
		}

		select {
		case sendStarted <- struct{}{}:
		default:
		}
		<-allowSendFinish

		return &http.Response{
			StatusCode: 200,
			Body:       io.NopCloser(strings.NewReader(`{"id":"msg_1"}`)),
			Header:     make(http.Header),
		}, nil
	})
	b.Session = s

	channelID := "chan_concurrent"
	initTurn := genai.NewContentFromText("System init", "user")
	if err := agent.AppendSessionContent(agentDir, initTurn); err != nil {
		t.Fatalf("AppendSessionContent initTurn failed: %v", err)
	}
	initHash := ComputeTurnHash(initTurn)

	userMsgText := "Calculate the warp trajectory."
	userTurn := genai.NewContentFromText(userMsgText, "user")
	if err := agent.AppendSessionContent(agentDir, userTurn); err != nil {
		t.Fatalf("AppendSessionContent userTurn failed: %v", err)
	}
	userHash := ComputeTurnHash(userTurn)

	modelTurn := genai.NewContentFromText("Trajectory calculated.", "model")
	if err := agent.AppendSessionContent(agentDir, modelTurn); err != nil {
		t.Fatalf("AppendSessionContent modelTurn failed: %v", err)
	}

	binding := &ChannelBinding{
		ChannelID:     channelID,
		AgentID:       "concurrent_agent",
		LastTurnIndex: 0,
		LastTurnHash:  initHash,
	}
	binding.AddPendingUserHash(userHash)
	if err := st.SetBinding(binding); err != nil {
		t.Fatalf("SetBinding failed: %v", err)
	}

	// Start first sync in background (simulating leading edge)
	firstSyncDone := make(chan struct{})
	go func() {
		b.SyncAgentToChannels("concurrent_agent")
		close(firstSyncDone)
	}()

	// Wait until first sync is in HTTP send phase
	select {
	case <-sendStarted:
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for first sync to reach send phase")
	}

	// While first sync is sending, trigger a concurrent sync (e.g. FlushNow)
	secondSyncDone := make(chan struct{})
	go func() {
		b.SyncAgentToChannels("concurrent_agent")
		close(secondSyncDone)
	}()

	// Release the HTTP send
	close(allowSendFinish)

	<-firstSyncDone
	<-secondSyncDone

	mu.Lock()
	msgs := append([]string{}, sentMessages...)
	mu.Unlock()

	// Verify no user turn echo
	for _, m := range msgs {
		if strings.Contains(m, userMsgText) {
			t.Errorf("user message was echoed back to Discord: %q", m)
		}
	}

	// Verify assistant output was sent exactly once
	modelMsgCount := 0
	for _, m := range msgs {
		if strings.Contains(m, "Trajectory calculated.") {
			modelMsgCount++
		}
	}
	if modelMsgCount != 1 {
		t.Errorf("expected assistant output to be rendered exactly once, got %d times (msgs: %v)", modelMsgCount, msgs)
	}
}
