package bot

import (
	"context"
	"log"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
)

const (
	// TrailingDebounceDuration is the quiet window before syncing session turns to Discord.
	TrailingDebounceDuration = 400 * time.Millisecond

	// MaxWaitDebounceDuration is the hard ceiling before forcing a sync pass.
	MaxWaitDebounceDuration = 1200 * time.Millisecond

	// DefaultDebounceDuration is kept for backwards compatibility.
	DefaultDebounceDuration = TrailingDebounceDuration
)

type debounceTracker struct {
	timer        *time.Timer // Trailing quiet-window timer (400ms)
	maxTimer     *time.Timer // Hard max-wait ceiling timer (1200ms)
	firstEventAt time.Time
}

// SessionWatcher monitors agent directories for session.jsonl modifications and triggers real-time backfill.
type SessionWatcher struct {
	mu          sync.Mutex
	bot         *Bot
	watcher     *fsnotify.Watcher
	watchedDirs map[string]bool
	trackers    map[string]*debounceTracker
	stopCh      chan struct{}
}

// NewSessionWatcher creates a new SessionWatcher for the provided bot.
func NewSessionWatcher(b *Bot) (*SessionWatcher, error) {
	fw, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}

	sw := &SessionWatcher{
		bot:         b,
		watcher:     fw,
		watchedDirs: make(map[string]bool),
		trackers:    make(map[string]*debounceTracker),
		stopCh:      make(chan struct{}),
	}

	return sw, nil
}

// Start begins processing filesystem events in a background goroutine.
func (sw *SessionWatcher) Start(ctx context.Context) {
	// Initialize watches for all currently bound agents
	bindings := sw.bot.State.GetAllBindings()
	for _, b := range bindings {
		if b.AgentID != "" {
			_ = sw.WatchAgent(b.AgentID)
		}
	}

	go sw.eventLoop(ctx)
}

// WatchAgent registers a directory watch on <ws_dir>/<agentID>.
func (sw *SessionWatcher) WatchAgent(agentID string) error {
	if agentID == "" {
		return nil
	}

	sw.mu.Lock()
	defer sw.mu.Unlock()

	agentDir := filepath.Join(sw.bot.WsDir, agentID)
	if sw.watchedDirs[agentDir] {
		return nil
	}

	if err := sw.watcher.Add(agentDir); err != nil {
		return err
	}

	sw.watchedDirs[agentDir] = true
	log.Printf("👀 Watching agent directory for live updates: %s", agentDir)
	return nil
}

// UnwatchAgent removes a directory watch if no other channels are bound to this agent.
func (sw *SessionWatcher) UnwatchAgent(agentID string) {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	agentDir := filepath.Join(sw.bot.WsDir, agentID)
	if !sw.watchedDirs[agentDir] {
		return
	}

	// Check if any active bindings still use this agent
	bindings := sw.bot.State.GetAllBindings()
	for _, b := range bindings {
		if b.AgentID == agentID {
			return // Still in use
		}
	}

	if tr, exists := sw.trackers[agentID]; exists {
		if tr.timer != nil {
			tr.timer.Stop()
		}
		if tr.maxTimer != nil {
			tr.maxTimer.Stop()
		}
		delete(sw.trackers, agentID)
	}

	_ = sw.watcher.Remove(agentDir)
	delete(sw.watchedDirs, agentDir)
	log.Printf("Unwatched agent directory: %s", agentDir)
}

// Close terminates the filesystem watcher and cleans up all active timers.
func (sw *SessionWatcher) Close() error {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	select {
	case <-sw.stopCh:
		// already closed
	default:
		close(sw.stopCh)
	}

	for _, tr := range sw.trackers {
		if tr.timer != nil {
			tr.timer.Stop()
		}
		if tr.maxTimer != nil {
			tr.maxTimer.Stop()
		}
	}

	return sw.watcher.Close()
}

func (sw *SessionWatcher) eventLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-sw.stopCh:
			return
		case err, ok := <-sw.watcher.Errors:
			if !ok {
				return
			}
			log.Printf("⚠️ File watcher error: %v", err)
		case event, ok := <-sw.watcher.Events:
			if !ok {
				return
			}

			// Only react to session.jsonl modifications, creations, or renames (compaction atomic commits)
			baseName := filepath.Base(event.Name)
			if baseName != "session.jsonl" && !strings.HasPrefix(baseName, "session.jsonl.tmp") {
				continue
			}

			if event.Has(fsnotify.Write) || event.Has(fsnotify.Create) || event.Has(fsnotify.Rename) {
				agentDir := filepath.Dir(event.Name)
				agentID := filepath.Base(agentDir)
				sw.debounceAgentSync(agentID)
			}
		}
	}
}

func (sw *SessionWatcher) debounceAgentSync(agentID string) {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	tr, exists := sw.trackers[agentID]
	if !exists {
		// Leading edge: Execute sync immediately for the first event
		go sw.bot.SyncAgentToChannels(agentID)

		tr = &debounceTracker{
			firstEventAt: time.Now(),
		}
		// Start hard ceiling timer (1200ms)
		tr.maxTimer = time.AfterFunc(MaxWaitDebounceDuration, func() {
			sw.flushAgentSync(agentID)
		})
		sw.trackers[agentID] = tr
	}

	// Reset trailing quiet timer (400ms)
	if tr.timer != nil {
		tr.timer.Stop()
	}
	tr.timer = time.AfterFunc(TrailingDebounceDuration, func() {
		sw.flushAgentSync(agentID)
	})
}

func (sw *SessionWatcher) flushAgentSync(agentID string) {
	sw.mu.Lock()
	if tr, exists := sw.trackers[agentID]; exists {
		if tr.timer != nil {
			tr.timer.Stop()
		}
		if tr.maxTimer != nil {
			tr.maxTimer.Stop()
		}
		delete(sw.trackers, agentID)
	}
	sw.mu.Unlock()

	sw.bot.SyncAgentToChannels(agentID)
}

// FlushNow cancels pending debounce timers for agentID and synchronously calls SyncAgentToChannels immediately.
func (sw *SessionWatcher) FlushNow(agentID string) {
	sw.mu.Lock()
	if tr, exists := sw.trackers[agentID]; exists {
		if tr.timer != nil {
			tr.timer.Stop()
		}
		if tr.maxTimer != nil {
			tr.maxTimer.Stop()
		}
		delete(sw.trackers, agentID)
	}
	sw.mu.Unlock()

	sw.bot.SyncAgentToChannels(agentID)
}
