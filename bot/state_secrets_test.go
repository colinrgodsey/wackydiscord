package bot

import (
	"os"
	"path/filepath"
	"testing"
)

// The state file holds Discord webhook tokens, which authorize posting into a channel as the
// bot's persona. It used to be written 0644.
func TestStateSaveRestrictsFileMode(t *testing.T) {
	path := filepath.Join(t.TempDir(), ".wackydiscord.json")
	st, err := NewState(path)
	if err != nil {
		t.Fatalf("NewState: %v", err)
	}
	if err := st.SetBinding(&ChannelBinding{ChannelID: "chan_1", AgentID: "bob", WebhookToken: "secret-webhook-token"}); err != nil {
		t.Fatalf("SetBinding: %v", err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if mode := info.Mode().Perm(); mode&0077 != 0 {
		t.Errorf("state file mode is %o; webhook tokens must not be group- or world-readable", mode)
	}
}

// Upgrading does not help unless the file that is already sitting world-readable on disk gets
// tightened, since a quiet instance may never write state again.
func TestNewStateTightensExistingFileMode(t *testing.T) {
	path := filepath.Join(t.TempDir(), ".wackydiscord.json")
	if err := os.WriteFile(path, []byte("{\"bindings\":{}}"), 0644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	if _, err := NewState(path); err != nil {
		t.Fatalf("NewState: %v", err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if mode := info.Mode().Perm(); mode&0077 != 0 {
		t.Errorf("a pre-existing 0644 state file is still %o after loading", mode)
	}
}
