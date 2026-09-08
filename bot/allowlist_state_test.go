package bot

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
)

func TestClaim_FirstClaimWins(t *testing.T) {
	tmpDir := t.TempDir()
	st, err := NewState(filepath.Join(tmpDir, ".wackydiscord.json"))
	if err != nil {
		t.Fatalf("NewState failed: %v", err)
	}

	ok, owner, err := st.TryClaim("user_A")
	if err != nil {
		t.Fatalf("unexpected error from TryClaim: %v", err)
	}
	if !ok || owner != "user_A" {
		t.Fatalf("expected user_A claim to succeed, got ok=%v, owner=%q", ok, owner)
	}
	if st.ClaimedUser() != "user_A" {
		t.Fatalf("expected ClaimedUser to be user_A, got %q", st.ClaimedUser())
	}

	ok, owner, err = st.TryClaim("user_B")
	if err != nil {
		t.Fatalf("unexpected error from TryClaim: %v", err)
	}
	if ok || owner != "user_A" {
		t.Fatalf("expected user_B claim to fail with owner=user_A, got ok=%v, owner=%q", ok, owner)
	}
	if st.ClaimedUser() != "user_A" {
		t.Fatalf("expected ClaimedUser to still be user_A, got %q", st.ClaimedUser())
	}
}

func TestClaim_Idempotent(t *testing.T) {
	tmpDir := t.TempDir()
	st, err := NewState(filepath.Join(tmpDir, ".wackydiscord.json"))
	if err != nil {
		t.Fatalf("NewState failed: %v", err)
	}

	ok1, owner1, err1 := st.TryClaim("user_A")
	if err1 != nil {
		t.Fatalf("first claim error: %v", err1)
	}
	if !ok1 || owner1 != "user_A" {
		t.Fatalf("first claim failed: ok=%v, owner=%q", ok1, owner1)
	}

	ok2, owner2, err2 := st.TryClaim("user_A")
	if err2 != nil {
		t.Fatalf("second claim error: %v", err2)
	}
	if !ok2 || owner2 != "user_A" {
		t.Fatalf("second claim failed: ok=%v, owner=%q", ok2, owner2)
	}

	if st.ClaimedUser() != "user_A" {
		t.Fatalf("expected ClaimedUser user_A, got %q", st.ClaimedUser())
	}
}

func TestClaim_ConcurrentRaceSingleWinner(t *testing.T) {
	tmpDir := t.TempDir()
	st, err := NewState(filepath.Join(tmpDir, ".wackydiscord.json"))
	if err != nil {
		t.Fatalf("NewState failed: %v", err)
	}

	const numGoroutines = 50
	var wg sync.WaitGroup
	startBarrier := make(chan struct{})
	var successCount int64
	var winningUser string
	var winMu sync.Mutex

	for i := 0; i < numGoroutines; i++ {
		uid := fmt.Sprintf("user_%d", i)
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			<-startBarrier
			ok, _, _ := st.TryClaim(id)
			if ok {
				atomic.AddInt64(&successCount, 1)
				winMu.Lock()
				winningUser = id
				winMu.Unlock()
			}
		}(uid)
	}

	close(startBarrier)
	wg.Wait()

	if successCount != 1 {
		t.Fatalf("expected exactly 1 winner, got %d", successCount)
	}

	winMu.Lock()
	winner := winningUser
	winMu.Unlock()

	if st.ClaimedUser() != winner {
		t.Fatalf("expected ClaimedUser to match winning user %q, got %q", winner, st.ClaimedUser())
	}
}

func TestUnclaim_OwnerReleases(t *testing.T) {
	tmpDir := t.TempDir()
	st, err := NewState(filepath.Join(tmpDir, ".wackydiscord.json"))
	if err != nil {
		t.Fatalf("NewState failed: %v", err)
	}

	ok, _, err := st.TryClaim("user_A")
	if !ok || err != nil {
		t.Fatalf("TryClaim user_A failed: ok=%v, err=%v", ok, err)
	}

	if ok, err := st.Unclaim("user_A"); !ok || err != nil {
		t.Fatalf("expected Unclaim by owner to return true, err=nil, got ok=%v, err=%v", ok, err)
	}

	if st.ClaimedUser() != "" {
		t.Fatalf("expected ClaimedUser to be empty after unclaim, got %q", st.ClaimedUser())
	}

	// Now user_B can claim
	ok, owner, err := st.TryClaim("user_B")
	if !ok || owner != "user_B" || err != nil {
		t.Fatalf("expected user_B to successfully claim after unclaim, got ok=%v, owner=%q, err=%v", ok, owner, err)
	}
	if st.ClaimedUser() != "user_B" {
		t.Fatalf("expected ClaimedUser to be user_B, got %q", st.ClaimedUser())
	}
}

func TestUnclaim_NonOwnerRejected(t *testing.T) {
	tmpDir := t.TempDir()
	st, err := NewState(filepath.Join(tmpDir, ".wackydiscord.json"))
	if err != nil {
		t.Fatalf("NewState failed: %v", err)
	}

	_, _, _ = st.TryClaim("user_A")

	if ok, err := st.Unclaim("user_B"); ok || err != nil {
		t.Fatalf("expected Unclaim by non-owner to return false, err=nil, got ok=%v, err=%v", ok, err)
	}

	if st.ClaimedUser() != "user_A" {
		t.Fatalf("expected ClaimedUser to remain user_A, got %q", st.ClaimedUser())
	}
}

func TestClaim_SurvivesRestart(t *testing.T) {
	tmpDir := t.TempDir()
	stateFile := filepath.Join(tmpDir, ".wackydiscord.json")

	st1, err := NewState(stateFile)
	if err != nil {
		t.Fatalf("NewState failed: %v", err)
	}

	ok, _, err := st1.TryClaim("123456789012345678")
	if !ok || err != nil {
		t.Fatalf("TryClaim failed: ok=%v, err=%v", ok, err)
	}

	// Reload from disk
	st2, err := NewState(stateFile)
	if err != nil {
		t.Fatalf("NewState reload failed: %v", err)
	}

	if st2.ClaimedUser() != "123456789012345678" {
		t.Fatalf("expected reloaded ClaimedUser to be '123456789012345678', got %q", st2.ClaimedUser())
	}
	if !st2.IsAllowedUser("123456789012345678") {
		t.Fatalf("expected IsAllowedUser('123456789012345678') to be true")
	}
	if st2.IsAllowedUser("other_user") {
		t.Fatalf("expected IsAllowedUser('other_user') to be false")
	}
}

func TestPersistence_ClaimedUserIDShape(t *testing.T) {
	tmpDir := t.TempDir()
	stateFile := filepath.Join(tmpDir, ".wackydiscord.json")

	st, err := NewState(stateFile)
	if err != nil {
		t.Fatalf("NewState failed: %v", err)
	}

	snowflake := "987654321098765432"
	ok, _, err := st.TryClaim(snowflake)
	if !ok || err != nil {
		t.Fatalf("TryClaim failed: ok=%v, err=%v", ok, err)
	}

	rawBytes, err := os.ReadFile(stateFile)
	if err != nil {
		t.Fatalf("failed reading state file: %v", err)
	}

	var rawMap map[string]interface{}
	if err := json.Unmarshal(rawBytes, &rawMap); err != nil {
		t.Fatalf("failed unmarshaling json: %v", err)
	}

	val, exists := rawMap["claimed_user_id"]
	if !exists {
		t.Fatalf("claimed_user_id not found in marshaled JSON: %s", string(rawBytes))
	}

	strVal, isString := val.(string)
	if !isString {
		t.Fatalf("expected claimed_user_id to be a string literal, got type %T: %v", val, val)
	}
	if strVal != snowflake {
		t.Fatalf("expected claimed_user_id to match %q, got %q", snowflake, strVal)
	}
}

func TestLoad_ClaimWithoutBindings(t *testing.T) {
	tmpDir := t.TempDir()
	stateFile := filepath.Join(tmpDir, ".wackydiscord.json")

	// State file with claimed_user_id but completely omitted or null bindings
	rawJSON := `{"claimed_user_id": "112233445566778899"}`
	if err := os.WriteFile(stateFile, []byte(rawJSON), 0644); err != nil {
		t.Fatalf("failed writing state file: %v", err)
	}

	st, err := NewState(stateFile)
	if err != nil {
		t.Fatalf("NewState failed: %v", err)
	}

	if st.ClaimedUser() != "112233445566778899" {
		t.Fatalf("expected ClaimedUser to be '112233445566778899', got %q", st.ClaimedUser())
	}
	if !st.IsAllowedUser("112233445566778899") {
		t.Fatalf("expected IsAllowedUser to be true for owner")
	}
	if st.IsAllowedUser("random_user") {
		t.Fatalf("expected IsAllowedUser to be false for non-owner")
	}
}

func TestDefaultPolicy_OpenAndClosed(t *testing.T) {
	tmpDir := t.TempDir()
	st, err := NewState(filepath.Join(tmpDir, ".wackydiscord.json"))
	if err != nil {
		t.Fatalf("NewState failed: %v", err)
	}

	// 1. By default, starting policy is OPEN
	if !st.DefaultOpen() {
		t.Fatalf("expected DefaultOpen to be true by default")
	}
	if !st.IsAllowedUser("any_user") {
		t.Fatalf("expected unclaimed bot in default-open mode to allow any user")
	}
	if st.IsAllowedUser("") {
		t.Fatalf("expected empty user ID to never be allowed")
	}

	// 2. Set policy to CLOSED
	st.SetDefaultOpen(false)
	if st.DefaultOpen() {
		t.Fatalf("expected DefaultOpen to be false after SetDefaultOpen(false)")
	}
	if st.IsAllowedUser("any_user") {
		t.Fatalf("expected unclaimed bot in default-closed mode to deny any user")
	}

	// 3. Once claimed, defaultOpen setting is superseded by owner check
	ok, _, err := st.TryClaim("owner_user")
	if !ok || err != nil {
		t.Fatalf("TryClaim failed: ok=%v, err=%v", ok, err)
	}
	if !st.IsAllowedUser("owner_user") {
		t.Fatalf("expected claimed bot to allow owner")
	}
	if st.IsAllowedUser("any_user") {
		t.Fatalf("expected claimed bot to deny non-owner regardless of defaultOpen setting")
	}

	// Even if defaultOpen is flipped back to true, non-owner is still denied when claimed
	st.SetDefaultOpen(true)
	if st.IsAllowedUser("any_user") {
		t.Fatalf("expected claimed bot to deny non-owner even when defaultOpen is true")
	}
}

func TestDefaultOpen_EnvEscapeHatch(t *testing.T) {
	tmpDir := t.TempDir()
	stateFile := filepath.Join(tmpDir, ".wackydiscord.json")

	// Test WACKYDISCORD_DEFAULT_OPEN=0 -> closed
	t.Setenv("WACKYDISCORD_DEFAULT_OPEN", "0")
	stClosed, err := NewState(stateFile)
	if err != nil {
		t.Fatalf("NewState failed: %v", err)
	}
	if stClosed.DefaultOpen() {
		t.Fatalf("expected DefaultOpen to be false when WACKYDISCORD_DEFAULT_OPEN=0")
	}
	if stClosed.IsAllowedUser("any_user") {
		t.Fatalf("expected IsAllowedUser to be false when WACKYDISCORD_DEFAULT_OPEN=0")
	}

	// Test WACKYDISCORD_DEFAULT_OPEN=1 -> open
	t.Setenv("WACKYDISCORD_DEFAULT_OPEN", "1")
	stOpen, err := NewState(stateFile)
	if err != nil {
		t.Fatalf("NewState failed: %v", err)
	}
	if !stOpen.DefaultOpen() {
		t.Fatalf("expected DefaultOpen to be true when WACKYDISCORD_DEFAULT_OPEN=1")
	}
	if !stOpen.IsAllowedUser("any_user") {
		t.Fatalf("expected IsAllowedUser to be true when WACKYDISCORD_DEFAULT_OPEN=1")
	}

	// Test WACKYDISCORD_DEFAULT_POLICY=closed
	t.Setenv("WACKYDISCORD_DEFAULT_OPEN", "")
	t.Setenv("WACKYDISCORD_DEFAULT_POLICY", "closed")
	stPolicyClosed, err := NewState(stateFile)
	if err != nil {
		t.Fatalf("NewState failed: %v", err)
	}
	if stPolicyClosed.DefaultOpen() {
		t.Fatalf("expected DefaultOpen to be false when WACKYDISCORD_DEFAULT_POLICY=closed")
	}
}

func TestTryClaim_ReturnsErrorWhenSaveLockedFails(t *testing.T) {
	tmpDir := t.TempDir()
	stateFile := filepath.Join(tmpDir, ".wackydiscord.json")
	st, err := NewState(stateFile)
	if err != nil {
		t.Fatalf("NewState failed: %v", err)
	}

	// Make saveLocked fail by pointing filePath inside a regular file (causing ENOTDIR on MkdirAll)
	blockingFile := filepath.Join(tmpDir, "blocker")
	if err := os.WriteFile(blockingFile, []byte("block"), 0644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}
	st.filePath = filepath.Join(blockingFile, "invalid_dir", "state.json")

	ok, owner, err := st.TryClaim("user_A")
	if err == nil {
		t.Fatalf("expected error from TryClaim when saveLocked fails, got nil")
	}
	if ok {
		t.Fatalf("expected ok=false when saveLocked fails, got true")
	}
	if owner != "" {
		t.Fatalf("expected owner to be empty when claim fails, got %q", owner)
	}
	if st.ClaimedUser() != "" {
		t.Fatalf("expected ClaimedUser to be rolled back to empty, got %q", st.ClaimedUser())
	}
}

func TestUnclaim_ReturnsErrorWhenSaveLockedFails(t *testing.T) {
	tmpDir := t.TempDir()
	stateFile := filepath.Join(tmpDir, ".wackydiscord.json")
	st, err := NewState(stateFile)
	if err != nil {
		t.Fatalf("NewState failed: %v", err)
	}

	ok, _, err := st.TryClaim("user_A")
	if !ok || err != nil {
		t.Fatalf("TryClaim user_A failed: ok=%v, err=%v", ok, err)
	}

	// Make saveLocked fail
	blockingFile := filepath.Join(tmpDir, "blocker")
	if err := os.WriteFile(blockingFile, []byte("block"), 0644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}
	st.filePath = filepath.Join(blockingFile, "invalid_dir", "state.json")

	ok, err = st.Unclaim("user_A")
	if err == nil {
		t.Fatalf("expected error from Unclaim when saveLocked fails, got nil")
	}
	if ok {
		t.Fatalf("expected ok=false when saveLocked fails, got true")
	}
	if st.ClaimedUser() != "user_A" {
		t.Fatalf("expected ClaimedUser to remain user_A after rollback on failed save, got %q", st.ClaimedUser())
	}
}
