package bot

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

const (
	// DefaultStateFileName is the default filename for persisting channel bindings in the workspace root.
	DefaultStateFileName = ".wackydiscord.json"

	// MaxPendingUserHashes is the maximum capacity of the FIFO ring buffer for deduplicating user turns.
	MaxPendingUserHashes = 20

	// EnvDefaultOpen is the environment variable for controlling allowlist policy when unclaimed.
	EnvDefaultOpen = "WACKYDISCORD_DEFAULT_OPEN"

	// EnvDefaultPolicy is the environment variable for allowlist default policy ("open" or "closed").
	EnvDefaultPolicy = "WACKYDISCORD_DEFAULT_POLICY"
)

// ChannelBinding records the association between a Discord channel and a WackyPub agent.
type ChannelBinding struct {
	ChannelID         string   `json:"channel_id"`
	AgentID           string   `json:"agent_id"`
	GuildID           string   `json:"guild_id,omitempty"`
	Verbose           bool     `json:"verbose"`
	IsGenerating      bool     `json:"is_generating,omitempty"`
	LastTurnHash      string   `json:"last_turn_hash,omitempty"`
	LastTurnIndex     int      `json:"last_turn_index"`
	PendingUserHashes []string `json:"pending_user_hashes,omitempty"`
	WebhookID         string   `json:"webhook_id,omitempty"`
	WebhookToken      string   `json:"webhook_token,omitempty"`
}

// AddPendingUserHash registers a user turn hash in the FIFO ring buffer.
func (b *ChannelBinding) AddPendingUserHash(hash string) {
	if hash == "" {
		return
	}
	for _, h := range b.PendingUserHashes {
		if h == hash {
			return
		}
	}
	if len(b.PendingUserHashes) >= MaxPendingUserHashes {
		b.PendingUserHashes = b.PendingUserHashes[1:]
	}
	b.PendingUserHashes = append(b.PendingUserHashes, hash)
}

// ConsumePendingUserHash checks if the given hash is in the FIFO ring buffer,
// removes it if present, and returns true. Returns false otherwise.
func (b *ChannelBinding) ConsumePendingUserHash(hash string) bool {
	for i, h := range b.PendingUserHashes {
		if h == hash {
			b.PendingUserHashes = append(b.PendingUserHashes[:i], b.PendingUserHashes[i+1:]...)
			return true
		}
	}
	return false
}

// State manages channel-to-agent bindings across bot restarts.
type State struct {
	mu            sync.RWMutex
	chanLocksMu   sync.Mutex
	chanSyncMu    map[string]*sync.Mutex
	chanTurnMu    map[string]*sync.Mutex
	chanDrainMu   map[string]*sync.Mutex
	filePath      string
	claimedUserID string
	defaultOpen   bool
	Bindings      map[string]*ChannelBinding `json:"bindings"`
}

// NewState loads or initializes state from the specified JSON file.
func NewState(filePath string) (*State, error) {
	defaultOpen := true
	if v := os.Getenv(EnvDefaultOpen); v != "" {
		v = strings.ToLower(strings.TrimSpace(v))
		if v == "0" || v == "false" || v == "f" || v == "closed" || v == "no" {
			defaultOpen = false
		} else if v == "1" || v == "true" || v == "t" || v == "open" || v == "yes" {
			defaultOpen = true
		}
	} else if v := os.Getenv(EnvDefaultPolicy); v != "" {
		v = strings.ToLower(strings.TrimSpace(v))
		if v == "closed" {
			defaultOpen = false
		} else if v == "open" {
			defaultOpen = true
		}
	}

	s := &State{
		filePath:    filePath,
		defaultOpen: defaultOpen,
		Bindings:    make(map[string]*ChannelBinding),
		chanSyncMu:  make(map[string]*sync.Mutex),
		chanTurnMu:  make(map[string]*sync.Mutex),
		chanDrainMu: make(map[string]*sync.Mutex),
	}

	if data, err := os.ReadFile(filePath); err == nil {
		var loaded struct {
			ClaimedUserID string                     `json:"claimed_user_id,omitempty"`
			Bindings      map[string]*ChannelBinding `json:"bindings"`
		}
		if err := json.Unmarshal(data, &loaded); err == nil {
			s.claimedUserID = loaded.ClaimedUserID
			if loaded.Bindings != nil {
				s.Bindings = loaded.Bindings
				for _, b := range s.Bindings {
					if b != nil {
						b.IsGenerating = false
					}
				}
			}
		}
	} else if !os.IsNotExist(err) {
		return nil, fmt.Errorf("failed to read state file %s: %w", filePath, err)
	}

	return s, nil
}

// LockChannelTurn acquires the generation-turn mutex for the specified channelID.
// It serializes user prompt generation dispatches so multiple turns don't collide.
func (s *State) LockChannelTurn(channelID string) func() {
	s.chanLocksMu.Lock()
	if s.chanTurnMu == nil {
		s.chanTurnMu = make(map[string]*sync.Mutex)
	}
	mu, ok := s.chanTurnMu[channelID]
	if !ok {
		mu = &sync.Mutex{}
		s.chanTurnMu[channelID] = mu
	}
	s.chanLocksMu.Unlock()

	mu.Lock()
	return func() {
		mu.Unlock()
	}
}

// LockChannelSync acquires the short-lived sync mutex for the specified channelID.
// Held only for brief memory updates (<= 1ms) during ChannelBinding read-modify-write sequences.
func (s *State) LockChannelSync(channelID string) func() {
	s.chanLocksMu.Lock()
	if s.chanSyncMu == nil {
		s.chanSyncMu = make(map[string]*sync.Mutex)
	}
	mu, ok := s.chanSyncMu[channelID]
	if !ok {
		mu = &sync.Mutex{}
		s.chanSyncMu[channelID] = mu
	}
	s.chanLocksMu.Unlock()

	mu.Lock()
	return func() {
		mu.Unlock()
	}
}

// LockChannelDrain acquires the send-drain mutex for the specified channelID.
// It serializes Discord message sending across concurrent sync passes.
func (s *State) LockChannelDrain(channelID string) func() {
	s.chanLocksMu.Lock()
	if s.chanDrainMu == nil {
		s.chanDrainMu = make(map[string]*sync.Mutex)
	}
	mu, ok := s.chanDrainMu[channelID]
	if !ok {
		mu = &sync.Mutex{}
		s.chanDrainMu[channelID] = mu
	}
	s.chanLocksMu.Unlock()

	mu.Lock()
	return func() {
		mu.Unlock()
	}
}

// LockChannel is deprecated: use LockChannelSync or LockChannelTurn directly.
// It forwards to LockChannelSync for backwards compatibility.
func (s *State) LockChannel(channelID string) func() {
	return s.LockChannelSync(channelID)
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
	if b.PendingUserHashes != nil {
		copied.PendingUserHashes = make([]string, len(b.PendingUserHashes))
		copy(copied.PendingUserHashes, b.PendingUserHashes)
	}
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
	if b.PendingUserHashes != nil {
		copied.PendingUserHashes = make([]string, len(b.PendingUserHashes))
		copy(copied.PendingUserHashes, b.PendingUserHashes)
	}
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
			copied := *v
			if v.PendingUserHashes != nil {
				copied.PendingUserHashes = make([]string, len(v.PendingUserHashes))
				copy(copied.PendingUserHashes, v.PendingUserHashes)
			}
			res[k] = copied
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
		ClaimedUserID string                     `json:"claimed_user_id,omitempty"`
		Bindings      map[string]*ChannelBinding `json:"bindings"`
	}{
		ClaimedUserID: s.claimedUserID,
		Bindings:      s.Bindings,
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

// SetDefaultOpen sets the default policy when the bot is unclaimed.
func (s *State) SetDefaultOpen(open bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.defaultOpen = open
}

// DefaultOpen returns the default policy when the bot is unclaimed.
func (s *State) DefaultOpen() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.defaultOpen
}

// TryClaim attempts to claim ownership of the bot instance for the given user ID.
// If the bot is unclaimed, the claim succeeds and is persisted to disk.
// If the bot is already claimed by userID, it returns (true, userID, nil) idempotently.
// If the bot is claimed by another user, it returns (false, owner, nil) without mutating state.
// If persisting to disk fails, the claim is rolled back and an error is returned.
func (s *State) TryClaim(userID string) (ok bool, owner string, err error) {
	if userID == "" {
		s.mu.RLock()
		defer s.mu.RUnlock()
		return false, s.claimedUserID, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.claimedUserID == "" {
		s.claimedUserID = userID
		if err := s.saveLocked(); err != nil {
			s.claimedUserID = ""
			return false, "", err
		}
		return true, userID, nil
	}

	if s.claimedUserID == userID {
		return true, userID, nil
	}

	return false, s.claimedUserID, nil
}

// Unclaim releases ownership of the bot instance if userID matches the current owner.
// Returns (true, nil) if successfully unclaimed, (false, nil) if userID is not the owner,
// or (false, err) if persisting to disk fails.
func (s *State) Unclaim(userID string) (ok bool, err error) {
	if userID == "" {
		return false, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.claimedUserID != "" && s.claimedUserID == userID {
		s.claimedUserID = ""
		if err := s.saveLocked(); err != nil {
			s.claimedUserID = userID
			return false, err
		}
		return true, nil
	}

	return false, nil
}

// IsAllowedUser checks whether a Discord user ID is permitted to interact with the bot.
// If the bot is unclaimed, it returns the default policy (defaultOpen).
// If the bot is claimed, only the claimed user ID is permitted.
func (s *State) IsAllowedUser(userID string) bool {
	if userID == "" {
		return false
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.claimedUserID == "" {
		return s.defaultOpen
	}

	return userID == s.claimedUserID
}

// ClaimedUser returns the Discord user ID of the current owner, or empty string if unclaimed.
func (s *State) ClaimedUser() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.claimedUserID
}
