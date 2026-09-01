package bot

import (
	"fmt"
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

func TestSplitDiscordMessage(t *testing.T) {
	t.Run("empty or whitespace", func(t *testing.T) {
		if chunks := SplitDiscordMessage(""); len(chunks) != 0 {
			t.Errorf("expected 0 chunks for empty string, got %d", len(chunks))
		}
		if chunks := SplitDiscordMessage("   \n\n  "); len(chunks) != 0 {
			t.Errorf("expected 0 chunks for whitespace, got %d", len(chunks))
		}
	})

	t.Run("short message", func(t *testing.T) {
		msg := "Hello world"
		chunks := SplitDiscordMessage(msg)
		if len(chunks) != 1 || chunks[0] != msg {
			t.Errorf("expected 1 chunk %q, got %+v", msg, chunks)
		}
	})

	t.Run("splits on paragraph break", func(t *testing.T) {
		p1 := strings.Repeat("A", 1200)
		p2 := strings.Repeat("B", 1200)
		full := p1 + "\n\n" + p2
		chunks := SplitDiscordMessage(full, 2000)
		if len(chunks) != 2 {
			t.Fatalf("expected 2 chunks, got %d", len(chunks))
		}
		if chunks[0] != p1 {
			t.Errorf("chunk 0 mismatch: len %d vs %d", len(chunks[0]), len(p1))
		}
		if chunks[1] != p2 {
			t.Errorf("chunk 1 mismatch: len %d vs %d", len(chunks[1]), len(p2))
		}
	})

	t.Run("splits on line break", func(t *testing.T) {
		l1 := strings.Repeat("X", 1500)
		l2 := strings.Repeat("Y", 1000)
		full := l1 + "\n" + l2
		chunks := SplitDiscordMessage(full, 2000)
		if len(chunks) != 2 {
			t.Fatalf("expected 2 chunks, got %d", len(chunks))
		}
		if chunks[0] != l1 {
			t.Errorf("chunk 0 mismatch")
		}
		if chunks[1] != l2 {
			t.Errorf("chunk 1 mismatch")
		}
	})

	t.Run("splits continuous word when exceeding limit", func(t *testing.T) {
		huge := strings.Repeat("Z", 4500)
		chunks := SplitDiscordMessage(huge, 2000)
		if len(chunks) != 3 {
			t.Fatalf("expected 3 chunks, got %d", len(chunks))
		}
		for i, c := range chunks {
			if len(c) > 2000 {
				t.Errorf("chunk %d exceeded limit: len %d", i, len(c))
			}
		}
	})
}

func TestStatePersistenceAndConcurrency(t *testing.T) {
	tmpDir := t.TempDir()
	stateFile := filepath.Join(tmpDir, ".wackydiscord.json")

	st, err := NewState(stateFile)
	if err != nil {
		t.Fatalf("NewState failed: %v", err)
	}

	b1 := &ChannelBinding{
		ChannelID:     "chan_100",
		AgentID:       "bob",
		GuildID:       "guild_1",
		Verbose:       true,
		LastTurnIndex: 5,
		LastTurnHash:  "hash_abc",
		WebhookID:     "wh_1",
		WebhookToken:  "tok_1",
	}

	if err := st.SetBinding(b1); err != nil {
		t.Fatalf("SetBinding failed: %v", err)
	}

	// Verify lookup
	got := st.GetBinding("chan_100")
	if got == nil || got.AgentID != "bob" || !got.Verbose {
		t.Fatalf("unexpected binding retrieved: %+v", got)
	}

	// Verify persistence to disk by reading with new instance
	st2, err := NewState(stateFile)
	if err != nil {
		t.Fatalf("NewState re-read failed: %v", err)
	}
	got2 := st2.GetBinding("chan_100")
	if got2 == nil || got2.AgentID != "bob" || got2.LastTurnHash != "hash_abc" {
		t.Fatalf("re-read state mismatch: %+v", got2)
	}

	// Concurrent writes
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			chanID := filepath.Join("chan", string(rune('0'+idx)))
			_ = st.SetBinding(&ChannelBinding{
				ChannelID: chanID,
				AgentID:   "bob",
			})
		}(i)
	}
	wg.Wait()

	if len(st.GetAllBindings()) < 20 {
		t.Errorf("expected at least 20 bindings, got %d", len(st.GetAllBindings()))
	}

	// Remove binding
	if err := st.RemoveBinding("chan_100"); err != nil {
		t.Fatalf("RemoveBinding failed: %v", err)
	}
	if st.GetBinding("chan_100") != nil {
		t.Errorf("expected chan_100 to be removed")
	}
}

func TestState_LockChannel(t *testing.T) {
	st, err := NewState(filepath.Join(t.TempDir(), ".wackydiscord.json"))
	if err != nil {
		t.Fatalf("NewState failed: %v", err)
	}

	// 1. Lazy creation & unlock execution
	unlock1 := st.LockChannel("chan_a")
	if unlock1 == nil {
		t.Fatal("expected non-nil unlock function")
	}
	unlock1()

	// 2. Different channels get independent locks and can be acquired concurrently
	unlockA := st.LockChannel("chan_a")
	unlockB := st.LockChannel("chan_b")

	// Both chan_a and chan_b held simultaneously without deadlock
	unlockA()
	unlockB()

	// 3. Same channel blocks until unlocked
	locked := make(chan struct{})
	unlocked := make(chan struct{})
	done := make(chan struct{})

	unlockMain := st.LockChannel("chan_c")
	go func() {
		close(locked)
		unlockSecond := st.LockChannel("chan_c")
		close(unlocked)
		unlockSecond()
		close(done)
	}()

	<-locked
	select {
	case <-unlocked:
		t.Fatal("second LockChannel should have blocked while chan_c was held")
	case <-time.After(50 * time.Millisecond):
		// Expected to still be blocked
	}

	unlockMain()

	select {
	case <-done:
		// Succeeded
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for second LockChannel to acquire lock after unlock")
	}
}

func TestComputeTurnHash(t *testing.T) {
	t1 := &genai.Content{
		Role: "user",
		Parts: []*genai.Part{
			{Text: "hello world"},
		},
	}
	t2 := &genai.Content{
		Role: "user",
		Parts: []*genai.Part{
			{Text: "hello world"},
		},
	}
	t3 := &genai.Content{
		Role: "model",
		Parts: []*genai.Part{
			{Text: "hello world"},
		},
	}

	h1 := ComputeTurnHash(t1)
	h2 := ComputeTurnHash(t2)
	h3 := ComputeTurnHash(t3)

	if h1 == "" || h2 == "" || h3 == "" {
		t.Fatalf("hashes should not be empty")
	}
	if h1 != h2 {
		t.Errorf("identical turns should have identical hashes: %q vs %q", h1, h2)
	}
	if h1 == h3 {
		t.Errorf("different roles should produce different hashes")
	}
}

func TestDiffUnsyncedTurns(t *testing.T) {
	turn0 := &genai.Content{Role: "user", Parts: []*genai.Part{{Text: "turn 0"}}}
	turn1 := &genai.Content{Role: "model", Parts: []*genai.Part{{Text: "turn 1"}}}
	turn2 := &genai.Content{Role: "user", Parts: []*genai.Part{{Text: "turn 2"}}}
	turn3 := &genai.Content{Role: "model", Parts: []*genai.Part{{Text: "turn 3"}}}

	turns := []*genai.Content{turn0, turn1, turn2, turn3}
	h1 := ComputeTurnHash(turn1)
	h3 := ComputeTurnHash(turn3)

	t.Run("empty session", func(t *testing.T) {
		unsynced, idx, hash := DiffUnsyncedTurns(nil, "", 0)
		if len(unsynced) != 0 || idx != 0 || hash != "" {
			t.Errorf("unexpected diff for empty session")
		}
	})

	t.Run("brand new binding (lastHash empty)", func(t *testing.T) {
		unsynced, idx, hash := DiffUnsyncedTurns(turns, "", -1)
		if len(unsynced) != 0 {
			t.Errorf("brand new binding should return empty unsynced by default, got %d", len(unsynced))
		}
		if idx != 3 || hash != h3 {
			t.Errorf("expected idx=3 hash=%s, got idx=%d hash=%s", h3, idx, hash)
		}
	})

	t.Run("fast path match at turn 1", func(t *testing.T) {
		unsynced, idx, hash := DiffUnsyncedTurns(turns, h1, 1)
		if len(unsynced) != 2 {
			t.Fatalf("expected 2 unsynced turns (turn2, turn3), got %d", len(unsynced))
		}
		if unsynced[0] != turn2 || unsynced[1] != turn3 {
			t.Errorf("wrong unsynced turns returned")
		}
		if idx != 3 || hash != h3 {
			t.Errorf("expected idx=3 hash=%s, got idx=%d hash=%s", h3, idx, hash)
		}
	})

	t.Run("compaction index shift (hash scan)", func(t *testing.T) {
		// Suppose turn0 was pruned by compaction, so turns are now [turn1, turn2, turn3]
		compactedTurns := []*genai.Content{turn1, turn2, turn3}
		// old index was 1, but in compacted array turn1 is at index 0
		unsynced, idx, hash := DiffUnsyncedTurns(compactedTurns, h1, 1)
		if len(unsynced) != 2 {
			t.Fatalf("expected 2 unsynced turns, got %d", len(unsynced))
		}
		if unsynced[0] != turn2 || unsynced[1] != turn3 {
			t.Errorf("wrong unsynced turns returned")
		}
		if idx != 2 || hash != h3 {
			t.Errorf("expected idx=2 hash=%s, got idx=%d hash=%s", h3, idx, hash)
		}
	})

	t.Run("pruned turns from end (hash not found / session shrank)", func(t *testing.T) {
		// Suppose turns were [turn0, turn1, turn2, turn3] and last synced was turn3 (idx 3, h3).
		// Now user pruned turn3 and turn2, so only [turn0, turn1] remains.
		prunedTurns := []*genai.Content{turn0, turn1}
		unsynced, idx, hash := DiffUnsyncedTurns(prunedTurns, h3, 3)
		// Must NOT replay turn0 or turn1! Must return 0 unsynced turns and update marker to turn1 (idx 1).
		if len(unsynced) != 0 {
			t.Fatalf("expected 0 unsynced turns on pruned session, got %d", len(unsynced))
		}
		if idx != 1 || hash != h1 {
			t.Errorf("expected idx=1 hash=%s, got idx=%d hash=%s", h1, idx, hash)
		}
	})

	t.Run("fully synced session", func(t *testing.T) {
		unsynced, idx, hash := DiffUnsyncedTurns(turns, h3, 3)
		if len(unsynced) != 0 {
			t.Fatalf("expected 0 unsynced turns, got %d", len(unsynced))
		}
		if idx != 3 || hash != h3 {
			t.Errorf("expected idx=3 hash=%s, got idx=%d hash=%s", h3, idx, hash)
		}
	})
}

func TestFormattingHelpers(t *testing.T) {
	t.Run("FormatUserBackfillMessage", func(t *testing.T) {
		turn := &genai.Content{
			Role: "user",
			Parts: []*genai.Part{
				{Text: "Line 1\nLine 2"},
			},
		}
		formatted := FormatUserBackfillMessage(turn)
		expected := "> 👤 **[User Turn]**\n> Line 1\n> Line 2"
		if formatted != expected {
			t.Errorf("expected %q, got %q", expected, formatted)
		}
	})

	t.Run("FormatAssistantBackfillMessage ignores thoughts", func(t *testing.T) {
		turn := &genai.Content{
			Role: "model",
			Parts: []*genai.Part{
				{Text: "Internal thought process", Thought: true},
				{Text: "Hello there!"},
			},
		}
		formatted := FormatAssistantBackfillMessage(turn)
		if formatted != "Hello there!" {
			t.Errorf("expected %q, got %q", "Hello there!", formatted)
		}
	})

	t.Run("FormatToolTurnSummary", func(t *testing.T) {
		// Model turn with FunctionCall
		callTurn := &genai.Content{
			Role: "model",
			Parts: []*genai.Part{
				{
					FunctionCall: &genai.FunctionCall{
						Name: "run_command",
						Args: map[string]any{"command": "files-rw", "args": []any{"list", "."}},
					},
				},
			},
		}
		callSummary := FormatToolTurnSummary(callTurn)
		if !strings.Contains(callSummary, "🔧 **Tool Call:** `run_command`") || !strings.Contains(callSummary, `"command": "files-rw"`) {
			t.Errorf("unexpected tool call summary: %s", callSummary)
		}

		// User turn with FunctionResponse and output field
		respTurn := &genai.Content{
			Role: "user",
			Parts: []*genai.Part{
				{
					FunctionResponse: &genai.FunctionResponse{
						Name:     "run_command",
						Response: map[string]any{"output": "<STDOUT>file1.txt\nfile2.txt</STDOUT>"},
					},
				},
			},
		}
		respSummary := FormatToolTurnSummary(respTurn)
		if !strings.Contains(respSummary, "⚡ **Tool Output:** `run_command`") || !strings.Contains(respSummary, "file1.txt") {
			t.Errorf("unexpected tool response summary: %s", respSummary)
		}

		// Plain user text turn returns empty
		userTurn := &genai.Content{
			Role: "user",
			Parts: []*genai.Part{
				{Text: "just a text message"},
			},
		}
		if s := FormatToolTurnSummary(userTurn); s != "" {
			t.Errorf("expected empty tool summary for plain text turn, got %q", s)
		}
	})

	t.Run("ExpandScratchpadSentinels", func(t *testing.T) {
		wsDir := t.TempDir()
		if err := os.WriteFile(filepath.Join(wsDir, agent.RootMarkerFile), []byte(""), 0644); err != nil {
			t.Fatalf("failed writing root marker: %v", err)
		}
		bobDir := filepath.Join(wsDir, "bob")
		if err := os.MkdirAll(bobDir, 0755); err != nil {
			t.Fatalf("failed creating bobDir: %v", err)
		}
		if err := os.WriteFile(filepath.Join(bobDir, "AGENTS.md"), []byte("Prompt"), 0644); err != nil {
			t.Fatalf("failed writing AGENTS.md: %v", err)
		}

		origCwd, _ := os.Getwd()
		if err := os.Chdir(wsDir); err != nil {
			t.Fatalf("failed to chdir to wsDir: %v", err)
		}
		defer os.Chdir(origCwd)

		sdk := agent.NewSDK(wsDir)

		// Create scratchpad entries
		entry1, err := sdk.CreateScratchpad("bob", "The rain fell heavily across Neo-Tokyo.", "test")
		if err != nil {
			t.Fatalf("CreateScratchpad failed: %v", err)
		}
		entry2, err := sdk.CreateScratchpad("bob", "A shadow stepped out from the alley.", "test")
		if err != nil {
			t.Fatalf("CreateScratchpad failed: %v", err)
		}

		// 1. Single sentinel expansion
		rawText1 := fmt.Sprintf("Prologue:\n<SCRATCHPAD_EXPAND id=%q />", entry1.ID)
		expanded1 := ExpandScratchpadSentinels(sdk, "bob", rawText1)
		expected1 := fmt.Sprintf("Prologue:\n%s", "The rain fell heavily across Neo-Tokyo.")
		if expanded1 != expected1 {
			t.Errorf("expected %q, got %q", expected1, expanded1)
		}

		// 2. Multiple sentinels with mixed case and quote styles
		rawText2 := fmt.Sprintf("Chapter 1:\n<SCRATCHPAD_EXPAND id=%q/>\nChapter 2:\n<scratchpad_expand id='%s' />", entry1.ID, entry2.ID)
		expanded2 := ExpandScratchpadSentinels(sdk, "bob", rawText2)
		expected2 := "Chapter 1:\nThe rain fell heavily across Neo-Tokyo.\nChapter 2:\nA shadow stepped out from the alley."
		if expanded2 != expected2 {
			t.Errorf("expected %q, got %q", expected2, expanded2)
		}

		// 3. Graceful fallback on missing/invalid ID
		rawText3 := "Unknown entry: <SCRATCHPAD_EXPAND id=\"nonexistent\" />"
		expanded3 := ExpandScratchpadSentinels(sdk, "bob", rawText3)
		if expanded3 != rawText3 {
			t.Errorf("expected missing ID to remain unexpanded %q, got %q", rawText3, expanded3)
		}

		// 4. Pass-through for text without sentinels
		plainText := "Just a normal conversational turn."
		if s := ExpandScratchpadSentinels(sdk, "bob", plainText); s != plainText {
			t.Errorf("expected unchanged text, got %q", s)
		}
	})
}

func TestSessionWatcher(t *testing.T) {
	tmpDir := t.TempDir()
	agentDir := filepath.Join(tmpDir, "bob")
	if err := os.MkdirAll(agentDir, 0755); err != nil {
		t.Fatalf("MkdirAll failed: %v", err)
	}

	stateFile := filepath.Join(tmpDir, ".wackydiscord.json")
	st, err := NewState(stateFile)
	if err != nil {
		t.Fatalf("NewState failed: %v", err)
	}

	_ = st.SetBinding(&ChannelBinding{
		ChannelID: "chan_1",
		AgentID:   "bob",
	})

	b := &Bot{
		WsDir: tmpDir,
		State: st,
	}

	watcher, err := NewSessionWatcher(b)
	if err != nil {
		t.Fatalf("NewSessionWatcher failed: %v", err)
	}
	defer watcher.Close()

	if err := watcher.WatchAgent("bob"); err != nil {
		t.Fatalf("WatchAgent failed: %v", err)
	}

	// Verify unwatch
	watcher.UnwatchAgent("nonexistent")
	if !watcher.watchedDirs[agentDir] {
		t.Errorf("expected bob to still be watched")
	}

	_ = st.RemoveBinding("chan_1")
	watcher.UnwatchAgent("bob")
	if watcher.watchedDirs[agentDir] {
		t.Errorf("expected bob to be unwatched after removing binding")
	}
}

func TestPendingUserEchoSuppression(t *testing.T) {
	prevTurn := genai.NewContentFromText("prev message", "model")
	prevHash := ComputeTurnHash(prevTurn)

	userMsg := "What is the status of sector 7?"
	userTurn := genai.NewContentFromText(userMsg, "user")
	userHash := ComputeTurnHash(userTurn)

	binding := &ChannelBinding{
		ChannelID:       "chan_10",
		AgentID:         "bob",
		PendingUserHash: userHash,
		PendingUserText: userMsg,
		LastTurnIndex:   0,
		LastTurnHash:    prevHash,
	}

	turns := []*genai.Content{prevTurn, userTurn}
	unsynced, _, _ := DiffUnsyncedTurns(turns, binding.LastTurnHash, binding.LastTurnIndex)
	if len(unsynced) != 1 {
		t.Fatalf("expected 1 unsynced turn, got %d", len(unsynced))
	}

	// Verify matching
	turnHash := ComputeTurnHash(unsynced[0])
	if turnHash != binding.PendingUserHash {
		t.Errorf("expected turnHash to match PendingUserHash")
	}
}

func TestNewBot_WorkspaceValidation(t *testing.T) {
	tmpDir := t.TempDir()

	// 1. Missing WACKYPUB_ROOT -> should fail
	_, err := NewBot(Config{
		Token:        "dummy_token",
		WorkspaceDir: tmpDir,
	})
	if err == nil {
		t.Fatalf("expected NewBot to fail on workspace missing WACKYPUB_ROOT marker file")
	}
	if !strings.Contains(err.Error(), "missing WACKYPUB_ROOT marker file") {
		t.Errorf("unexpected error message: %v", err)
	}

	// 2. Create WACKYPUB_ROOT marker file -> should succeed
	markerPath := filepath.Join(tmpDir, "WACKYPUB_ROOT")
	if err := os.WriteFile(markerPath, []byte(""), 0644); err != nil {
		t.Fatalf("failed to write WACKYPUB_ROOT marker file: %v", err)
	}

	b, err := NewBot(Config{
		Token:        "dummy_token",
		WorkspaceDir: tmpDir,
	})
	if err != nil {
		t.Fatalf("expected NewBot to succeed with WACKYPUB_ROOT marker file, got: %v", err)
	}
	if b == nil || b.WsDir != tmpDir {
		t.Errorf("unexpected bot instance: %+v", b)
	}
}

func TestValidateBindings(t *testing.T) {
	tmpDir := t.TempDir()
	markerPath := filepath.Join(tmpDir, "WACKYPUB_ROOT")
	if err := os.WriteFile(markerPath, []byte(""), 0644); err != nil {
		t.Fatalf("failed to write WACKYPUB_ROOT: %v", err)
	}

	// Create valid agent 'charlie'
	charlieDir := filepath.Join(tmpDir, "charlie")
	_ = os.MkdirAll(charlieDir, 0755)
	_ = os.WriteFile(filepath.Join(charlieDir, "AGENTS.md"), []byte("I am Charlie"), 0644)
	_ = os.WriteFile(filepath.Join(charlieDir, "runtime.json"), []byte(`{"model":"dummy"}`), 0644)

	// Create broken agent 'bob' with invalid runtime.json
	bobDir := filepath.Join(tmpDir, "bob")
	_ = os.MkdirAll(bobDir, 0755)
	_ = os.WriteFile(filepath.Join(bobDir, "AGENTS.md"), []byte("I am Bob"), 0644)
	_ = os.WriteFile(filepath.Join(bobDir, "runtime.json"), []byte(`{invalid json`), 0644)

	b, err := NewBot(Config{
		Token:        "dummy_token",
		WorkspaceDir: tmpDir,
	})
	if err != nil {
		t.Fatalf("NewBot failed: %v", err)
	}

	// 1. Bind to missing agent 'alice'
	_ = b.State.SetBinding(&ChannelBinding{ChannelID: "c1", AgentID: "alice"})
	// 2. Bind to broken agent 'bob'
	_ = b.State.SetBinding(&ChannelBinding{ChannelID: "c2", AgentID: "bob"})
	// 3. Bind to valid agent 'charlie'
	_ = b.State.SetBinding(&ChannelBinding{ChannelID: "c3", AgentID: "charlie"})

	warnings := b.ValidateBindings()
	if len(warnings) != 2 {
		t.Fatalf("expected 2 warnings, got %d: %v", len(warnings), warnings)
	}

	var foundMissing, foundInvalid bool
	for _, w := range warnings {
		if strings.Contains(w, "missing agent \"alice\"") {
			foundMissing = true
		}
		if strings.Contains(w, "invalid runtime.json") && strings.Contains(w, "bob") {
			foundInvalid = true
		}
	}
	if !foundMissing {
		t.Errorf("expected warning for missing agent alice, got: %v", warnings)
	}
	if !foundInvalid {
		t.Errorf("expected warning for invalid runtime.json bob, got: %v", warnings)
	}
}

func TestHandleMessageCreate_UnbindingRaceAndNilCheck(t *testing.T) {
	tmpDir := t.TempDir()
	markerPath := filepath.Join(tmpDir, "WACKYPUB_ROOT")
	if err := os.WriteFile(markerPath, []byte(""), 0644); err != nil {
		t.Fatalf("failed to write WACKYPUB_ROOT: %v", err)
	}

	bobDir := filepath.Join(tmpDir, "bob")
	if err := os.MkdirAll(bobDir, 0755); err != nil {
		t.Fatalf("MkdirAll failed: %v", err)
	}
	if err := os.WriteFile(filepath.Join(bobDir, "AGENTS.md"), []byte("Prompt"), 0644); err != nil {
		t.Fatalf("failed writing AGENTS.md: %v", err)
	}

	stateFile := filepath.Join(tmpDir, ".wackydiscord.json")
	st, err := NewState(stateFile)
	if err != nil {
		t.Fatalf("NewState failed: %v", err)
	}

	b := &Bot{
		WsDir: tmpDir,
		State: st,
		SDK:   agent.NewSDK(tmpDir),
	}

	// 1. Unbound channel -> fast pre-lock check returns cleanly
	b.HandleMessageCreate(nil, &discordgo.MessageCreate{
		Message: &discordgo.Message{
			ChannelID: "unbound_chan",
			Content:   "hello",
			Author:    &discordgo.User{ID: "user1", Bot: false},
		},
	})

	// 2. Bound channel: hold channel lock, launch HandleMessageCreate on background goroutine.
	// It passes the pre-lock check (binding exists), then blocks on LockChannel.
	// Remove the binding while it's blocked, then release the lock.
	// Assert it unblocks, sees nil binding post-lock, and exits cleanly without panicking or recreating the binding.
	_ = st.SetBinding(&ChannelBinding{
		ChannelID: "chan_race",
		AgentID:   "bob",
	})

	unlock := st.LockChannel("chan_race")
	done := make(chan struct{})

	go func() {
		b.HandleMessageCreate(nil, &discordgo.MessageCreate{
			Message: &discordgo.Message{
				ChannelID: "chan_race",
				Content:   "hello",
				Author:    &discordgo.User{ID: "user1", Bot: false},
			},
		})
		close(done)
	}()

	// Ensure HandleMessageCreate has started and is blocked on LockChannel
	select {
	case <-done:
		t.Fatal("HandleMessageCreate should have blocked while channel lock was held")
	case <-time.After(50 * time.Millisecond):
		// Expected to be blocked
	}

	// Remove binding while HandleMessageCreate is waiting on the lock
	if err := st.RemoveBinding("chan_race"); err != nil {
		t.Fatalf("RemoveBinding failed: %v", err)
	}

	// Release channel lock
	unlock()

	select {
	case <-done:
		// Succeeded
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for HandleMessageCreate to return after unbinding")
	}

	// Confirm channel remains unbound (no resurrection write)
	if remaining := st.GetBinding("chan_race"); remaining != nil {
		t.Errorf("expected chan_race to remain unbound, got: %+v", remaining)
	}
}

func TestAutoFillUnsyncedTurns_Limit(t *testing.T) {
	tmpDir := t.TempDir()
	markerPath := filepath.Join(tmpDir, "WACKYPUB_ROOT")
	if err := os.WriteFile(markerPath, []byte(""), 0644); err != nil {
		t.Fatalf("failed to write WACKYPUB_ROOT: %v", err)
	}

	bobDir := filepath.Join(tmpDir, "bob")
	if err := os.MkdirAll(bobDir, 0755); err != nil {
		t.Fatalf("MkdirAll failed: %v", err)
	}
	if err := os.WriteFile(filepath.Join(bobDir, "AGENTS.md"), []byte("Prompt"), 0644); err != nil {
		t.Fatalf("failed writing AGENTS.md: %v", err)
	}

	origCwd, _ := os.Getwd()
	if err := os.Chdir(tmpDir); err != nil {
		t.Fatalf("failed to chdir to tmpDir: %v", err)
	}
	defer os.Chdir(origCwd)

	sdk := agent.NewSDK(tmpDir)

	// Append 4 turns: user1, model1, user2, model2
	t1 := genai.NewContentFromText("user msg 1", "user")
	t2 := genai.NewContentFromText("model resp 1", "model")
	t3 := genai.NewContentFromText("user msg 2", "user")
	t4 := genai.NewContentFromText("model resp 2", "model")

	_ = agent.AppendSessionContent(bobDir, t1)
	_ = agent.AppendSessionContent(bobDir, t2)
	_ = agent.AppendSessionContent(bobDir, t3)
	_ = agent.AppendSessionContent(bobDir, t4)

	stateFile := filepath.Join(tmpDir, ".wackydiscord.json")
	st, err := NewState(stateFile)
	if err != nil {
		t.Fatalf("NewState failed: %v", err)
	}

	b := &Bot{
		WsDir: tmpDir,
		State: st,
		SDK:   sdk,
	}

	// 1. limit = 2 on a fully synced session (simulating /fill limit:2)
	binding := &ChannelBinding{
		ChannelID:     "chan_test",
		AgentID:       "bob",
		LastTurnIndex: 3,
		LastTurnHash:  ComputeTurnHash(t4),
	}
	_ = st.SetBinding(binding)

	unlock := st.LockChannel("chan_test")
	count, err := b.autoFillUnsyncedTurns(nil, binding, "chan_test", 2)
	unlock()

	if err != nil {
		t.Fatalf("autoFillUnsyncedTurns failed: %v", err)
	}
	if count != 2 {
		t.Errorf("expected 2 turns backfilled, got %d", count)
	}

	// 2. limit = 10 on unsynced session (caps at total 4 turns)
	binding.LastTurnIndex = -1
	binding.LastTurnHash = ""
	_ = st.SetBinding(binding)

	unlock = st.LockChannel("chan_test")
	count, err = b.autoFillUnsyncedTurns(nil, binding, "chan_test", 10)
	unlock()

	if err != nil {
		t.Fatalf("autoFillUnsyncedTurns failed: %v", err)
	}
	if count != 4 {
		t.Errorf("expected 4 turns backfilled when limit exceeds count, got %d", count)
	}

	// 3. Skip when IsGenerating is true
	binding.IsGenerating = true
	_ = st.SetBinding(binding)

	unlock = st.LockChannel("chan_test")
	count, err = b.autoFillUnsyncedTurns(nil, binding, "chan_test", 0)
	unlock()

	if err != nil {
		t.Fatalf("autoFillUnsyncedTurns with IsGenerating true failed: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0 turns backfilled when IsGenerating is true, got %d", count)
	}
}

func TestHandleFillCommand_Locking(t *testing.T) {
	tmpDir := t.TempDir()
	markerPath := filepath.Join(tmpDir, "WACKYPUB_ROOT")
	if err := os.WriteFile(markerPath, []byte(""), 0644); err != nil {
		t.Fatalf("failed to write WACKYPUB_ROOT: %v", err)
	}

	bobDir := filepath.Join(tmpDir, "bob")
	if err := os.MkdirAll(bobDir, 0755); err != nil {
		t.Fatalf("MkdirAll failed: %v", err)
	}
	if err := os.WriteFile(filepath.Join(bobDir, "AGENTS.md"), []byte("Prompt"), 0644); err != nil {
		t.Fatalf("failed writing AGENTS.md: %v", err)
	}

	stateFile := filepath.Join(tmpDir, ".wackydiscord.json")
	st, err := NewState(stateFile)
	if err != nil {
		t.Fatalf("NewState failed: %v", err)
	}

	b := &Bot{
		WsDir: tmpDir,
		State: st,
		SDK:   agent.NewSDK(tmpDir),
	}

	// 1. /fill on unbound channel
	iUnbound := &discordgo.InteractionCreate{
		Interaction: &discordgo.Interaction{
			ChannelID: "unbound_chan",
			Type:      discordgo.InteractionApplicationCommand,
		},
	}
	b.handleFillCommand(nil, iUnbound)

	// 2. /fill on bound channel while generating
	_ = st.SetBinding(&ChannelBinding{
		ChannelID:    "chan_gen",
		AgentID:      "bob",
		IsGenerating: true,
	})

	iGen := &discordgo.InteractionCreate{
		Interaction: &discordgo.Interaction{
			ChannelID: "chan_gen",
			Type:      discordgo.InteractionApplicationCommand,
		},
	}
	b.handleFillCommand(nil, iGen)

	// 3. /fill acquires channel lock during operation
	_ = st.SetBinding(&ChannelBinding{
		ChannelID: "chan_lock_test",
		AgentID:   "bob",
	})

	unlock := st.LockChannel("chan_lock_test")
	lockAcquiredByFill := make(chan struct{})
	go func() {
		b.handleFillCommand(nil, &discordgo.InteractionCreate{
			Interaction: &discordgo.Interaction{
				ChannelID: "chan_lock_test",
				Type:      discordgo.InteractionApplicationCommand,
			},
		})
		close(lockAcquiredByFill)
	}()

	// Since we hold the lock, handleFillCommand must be blocked
	select {
	case <-lockAcquiredByFill:
		t.Fatal("handleFillCommand should have blocked while channel lock was held")
	case <-time.After(50 * time.Millisecond):
		// Expected
	}

	unlock()

	select {
	case <-lockAcquiredByFill:
		// Succeeded
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for handleFillCommand after unlocking channel")
	}
}

func TestHandleUnbindCommand_Locking(t *testing.T) {
	tmpDir := t.TempDir()
	markerPath := filepath.Join(tmpDir, "WACKYPUB_ROOT")
	if err := os.WriteFile(markerPath, []byte(""), 0644); err != nil {
		t.Fatalf("failed to write WACKYPUB_ROOT: %v", err)
	}

	bobDir := filepath.Join(tmpDir, "bob")
	if err := os.MkdirAll(bobDir, 0755); err != nil {
		t.Fatalf("MkdirAll failed: %v", err)
	}
	if err := os.WriteFile(filepath.Join(bobDir, "AGENTS.md"), []byte("Prompt"), 0644); err != nil {
		t.Fatalf("failed writing AGENTS.md: %v", err)
	}

	stateFile := filepath.Join(tmpDir, ".wackydiscord.json")
	st, err := NewState(stateFile)
	if err != nil {
		t.Fatalf("NewState failed: %v", err)
	}

	b := &Bot{
		WsDir: tmpDir,
		State: st,
		SDK:   agent.NewSDK(tmpDir),
	}

	// 1. /unbind on unbound channel
	iUnbound := &discordgo.InteractionCreate{
		Interaction: &discordgo.Interaction{
			ChannelID: "unbound_chan",
			Type:      discordgo.InteractionApplicationCommand,
		},
	}
	b.handleUnbindCommand(nil, iUnbound)

	// 2. /unbind acquires channel lock during operation
	_ = st.SetBinding(&ChannelBinding{
		ChannelID: "chan_unbind_lock",
		AgentID:   "bob",
	})

	unlock := st.LockChannel("chan_unbind_lock")
	lockAcquiredByUnbind := make(chan struct{})
	go func() {
		b.handleUnbindCommand(nil, &discordgo.InteractionCreate{
			Interaction: &discordgo.Interaction{
				ChannelID: "chan_unbind_lock",
				Type:      discordgo.InteractionApplicationCommand,
			},
		})
		close(lockAcquiredByUnbind)
	}()

	// Since we hold the lock, handleUnbindCommand must be blocked
	select {
	case <-lockAcquiredByUnbind:
		t.Fatal("handleUnbindCommand should have blocked while channel lock was held")
	case <-time.After(50 * time.Millisecond):
		// Expected
	}

	unlock()

	select {
	case <-lockAcquiredByUnbind:
		// Succeeded
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for handleUnbindCommand after unlocking channel")
	}

	// Verify binding was removed
	if st.GetBinding("chan_unbind_lock") != nil {
		t.Errorf("expected chan_unbind_lock to be removed")
	}
}

func TestHandleMessageCreate_AgentIDGuardMidGeneration(t *testing.T) {
	tmpDir := t.TempDir()
	markerPath := filepath.Join(tmpDir, "WACKYPUB_ROOT")
	if err := os.WriteFile(markerPath, []byte(""), 0644); err != nil {
		t.Fatalf("failed to write WACKYPUB_ROOT: %v", err)
	}

	// 1. Create agent 'bob' with 2 session turns
	bobDir := filepath.Join(tmpDir, "bob")
	if err := os.MkdirAll(bobDir, 0755); err != nil {
		t.Fatalf("MkdirAll failed: %v", err)
	}
	if err := os.WriteFile(filepath.Join(bobDir, "AGENTS.md"), []byte("I am Bob"), 0644); err != nil {
		t.Fatalf("failed writing bob AGENTS.md: %v", err)
	}
	_ = agent.AppendSessionContent(bobDir, genai.NewContentFromText("bob turn 1", "user"))
	_ = agent.AppendSessionContent(bobDir, genai.NewContentFromText("bob turn 2", "model"))

	// 2. Create agent 'alice' with 5 session turns
	aliceDir := filepath.Join(tmpDir, "alice")
	if err := os.MkdirAll(aliceDir, 0755); err != nil {
		t.Fatalf("MkdirAll failed: %v", err)
	}
	if err := os.WriteFile(filepath.Join(aliceDir, "AGENTS.md"), []byte("I am Alice"), 0644); err != nil {
		t.Fatalf("failed writing alice AGENTS.md: %v", err)
	}
	for j := 0; j < 5; j++ {
		_ = agent.AppendSessionContent(aliceDir, genai.NewContentFromText(fmt.Sprintf("alice turn %d", j), "user"))
	}

	origCwd, _ := os.Getwd()
	if err := os.Chdir(tmpDir); err != nil {
		t.Fatalf("failed to chdir to tmpDir: %v", err)
	}
	defer os.Chdir(origCwd)

	stateFile := filepath.Join(tmpDir, ".wackydiscord.json")
	st, err := NewState(stateFile)
	if err != nil {
		t.Fatalf("NewState failed: %v", err)
	}

	b := &Bot{
		WsDir: tmpDir,
		State: st,
		SDK:   agent.NewSDK(tmpDir),
	}

	// Channel initially bound to bob
	_ = st.SetBinding(&ChannelBinding{
		ChannelID:     "chan_rebind",
		AgentID:       "bob",
		LastTurnIndex: 1,
		LastTurnHash:  "bob_hash_1",
	})

	// Hold session lock on bobDir so HandleMessageCreate pauses while IsGenerating is true,
	// allowing us to deterministically simulate a concurrent rebind to alice mid-generation.
	lock, err := agent.AcquireSessionLock(bobDir)
	if err != nil {
		t.Fatalf("AcquireSessionLock failed: %v", err)
	}

	done := make(chan struct{})
	go func() {
		b.HandleMessageCreate(nil, &discordgo.MessageCreate{
			Message: &discordgo.Message{
				ChannelID: "chan_rebind",
				Content:   "hello bob",
				Author:    &discordgo.User{ID: "user1", Bot: false},
			},
		})
		close(done)
	}()

	// Wait for HandleMessageCreate to mark the channel as actively generating
	for {
		bnd := st.GetBinding("chan_rebind")
		if bnd != nil && bnd.IsGenerating && bnd.AgentID == "bob" {
			break
		}
		time.Sleep(1 * time.Millisecond)
	}

	// Rebind channel to alice while bob is mid-generation
	_ = st.SetBinding(&ChannelBinding{
		ChannelID:     "chan_rebind",
		AgentID:       "alice",
		LastTurnIndex: 4,
		LastTurnHash:  "alice_hash_4",
	})

	// Release bob's lock so HandleMessageCreate can proceed and finish
	lock.Release()

	select {
	case <-done:
		// Succeeded
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for HandleMessageCreate to complete")
	}

	// Verify alice's binding was NOT corrupted with bob's sync markers
	finalBnd := st.GetBinding("chan_rebind")
	if finalBnd == nil {
		t.Fatalf("expected chan_rebind to still be bound")
	}
	if finalBnd.AgentID != "alice" {
		t.Errorf("expected AgentID to be alice, got %q", finalBnd.AgentID)
	}
	if finalBnd.LastTurnIndex != 4 {
		t.Errorf("expected LastTurnIndex to remain 4 (alice), got %d (corrupted by bob's generation)", finalBnd.LastTurnIndex)
	}
	if finalBnd.LastTurnHash != "alice_hash_4" {
		t.Errorf("expected LastTurnHash to remain alice_hash_4, got %q", finalBnd.LastTurnHash)
	}
}
