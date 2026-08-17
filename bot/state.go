package bot

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

const (
	// DefaultStateFileName is the default filename for persisting channel bindings in the workspace root.
	DefaultStateFileName = ".wackydiscord.json"
)

// ChannelBinding records the association between a Discord channel and a WackyPub agent.
type ChannelBinding struct {
	ChannelID       string `json:"channel_id"`
	AgentID         string `json:"agent_id"`
	GuildID         string `json:"guild_id,omitempty"`
	Verbose         bool   `json:"verbose"`
	IsGenerating    bool   `json:"is_generating,omitempty"`
	LastTurnHash    string `json:"last_turn_hash,omitempty"`
	LastTurnIndex   int    `json:"last_turn_index"`
	PendingUserHash string `json:"pending_user_hash,omitempty"`
	PendingUserText string `json:"pending_user_text,omitempty"`
	WebhookID       string `json:"webhook_id,omitempty"`
	WebhookToken    string `json:"webhook_token,omitempty"`
}

// State manages channel-to-agent bindings across bot restarts.
type State struct {
	mu       sync.RWMutex
	filePath string
	Bindings map[string]*ChannelBinding `json:"bindings"`
}

// NewState loads or initializes state from the specified JSON file.
func NewState(filePath string) (*State, error) {
	s := &State{
		filePath: filePath,
		Bindings: make(map[string]*ChannelBinding),
	}

	if data, err := os.ReadFile(filePath); err == nil {
		var loaded struct {
			Bindings map[string]*ChannelBinding `json:"bindings"`
		}
		if err := json.Unmarshal(data, &loaded); err == nil && loaded.Bindings != nil {
			s.Bindings = loaded.Bindings
			for _, b := range s.Bindings {
				if b != nil {
					b.IsGenerating = false
				}
			}
		}
	} else if !os.IsNotExist(err) {
		return nil, fmt.Errorf("failed to read state file %s: %w", filePath, err)
	}

	return s, nil
}

// GetBinding retrieves the binding for a channel, returning nil if unbound.
func (s *State) GetBinding(channelID string) *ChannelBinding {
	s.mu.RLock()
	defer s.mu.RUnlock()

	b, ok := s.Bindings[channelID]
	if !ok || b == nil {
		return nil
	}
	// Return a copy to avoid race conditions
	copied := *b
	return &copied
}

// SetBinding adds or updates a channel binding and persists state to disk.
func (s *State) SetBinding(b *ChannelBinding) error {
	if b == nil || b.ChannelID == "" {
		return fmt.Errorf("invalid channel binding: channel ID is required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	copied := *b
	s.Bindings[b.ChannelID] = &copied
	return s.saveLocked()
}

// RemoveBinding unbinds a channel and persists state to disk.
func (s *State) RemoveBinding(channelID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.Bindings, channelID)
	return s.saveLocked()
}

// GetAllBindings returns a snapshot of all active bindings.
func (s *State) GetAllBindings() map[string]ChannelBinding {
	s.mu.RLock()
	defer s.mu.RUnlock()

	res := make(map[string]ChannelBinding, len(s.Bindings))
	for k, v := range s.Bindings {
		if v != nil {
			res[k] = *v
		}
	}
	return res
}

// Save persists state to disk.
func (s *State) Save() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.saveLocked()
}

func (s *State) saveLocked() error {
	if s.filePath == "" {
		return nil
	}

	data, err := json.MarshalIndent(struct {
		Bindings map[string]*ChannelBinding `json:"bindings"`
	}{
		Bindings: s.Bindings,
	}, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal state: %w", err)
	}

	dir := filepath.Dir(s.filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create state directory: %w", err)
	}

	tmpFile := fmt.Sprintf("%s.tmp.%d", s.filePath, os.Getpid())
	if err := os.WriteFile(tmpFile, data, 0644); err != nil {
		return fmt.Errorf("failed to write temp state file: %w", err)
	}

	if err := os.Rename(tmpFile, s.filePath); err != nil {
		_ = os.Remove(tmpFile)
		return fmt.Errorf("failed to commit state file: %w", err)
	}

	return nil
}
