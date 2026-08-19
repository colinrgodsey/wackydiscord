package bot

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

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

	t.Run("compaction pruned past last synced turn (hash not found)", func(t *testing.T) {
		// Suppose compaction pruned turn0, turn1, turn2, so only [turn3] remains
		compactedTurns := []*genai.Content{turn3}
		unsynced, idx, hash := DiffUnsyncedTurns(compactedTurns, h1, 1)
		if len(unsynced) != 1 || unsynced[0] != turn3 {
			t.Fatalf("expected all surviving turns to be returned, got %d", len(unsynced))
		}
		if idx != 0 || hash != h3 {
			t.Errorf("expected idx=0 hash=%s, got idx=%d hash=%s", h3, idx, hash)
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
