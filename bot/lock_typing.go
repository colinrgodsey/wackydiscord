package bot

import (
	"context"
	"errors"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/fsnotify/fsnotify"
)

// typingRefreshInterval is the minimum gap between two typing assertions on the same
// channel. It matches withTyping's 6s ticker and stays under Discord's ~10s indicator
// expiry, so a mid-turn agent (lock held by a live PID, refreshes keep coming) never
// lets the indicator fade, and an ended turn's last assertion expires on its own.
const typingRefreshInterval = 6 * time.Second

// lockTypingPollInterval is the backstop poll for the per-agent lock watchers. The
// primary signal is an inotify event on the agent's session.lock (Truncate and Write
// both fire events); the poll covers what a file watch cannot: the file not existing
// yet (an agent that has never had a turn has no lock file until its first acquire)
// and rare missed events. Steady state is one read of a 7-byte file per agent per
// interval.
const lockTypingPollInterval = 5 * time.Second

// sessionLockName is the per-agent lock file wackypub's turns hold for the whole turn
// (AcquireSessionLock/Release, pkg/agent/lock.go), regardless of what dispatched the
// turn. While a turn is in flight the file holds the live holder's PID as one line;
// a clean release truncates it to 0, and a killed holder leaves a stale PID behind.
const sessionLockName = "session.lock"

// sessionLockTurnActive reports whether the agent whose lock file is at lockPath is
// mid-turn: the file is non-empty AND the PID recorded in it is alive right now. A
// missing file (never had a turn) and an empty file (cleanly released) are both idle;
// a stale PID from a killed holder is idle too. The liveness check is the whole point:
// a bare non-empty check shows typing forever for a killed holder's leftover file.
// Mirrors wackypub's lockFilePidHint (pkg/agent/lock_holder.go) plus processIsLive
// (pkg/agent/cancel.go): the PID hint is advisory, liveness is the signal.
func sessionLockTurnActive(lockPath string) bool {
	data, err := os.ReadFile(lockPath)
	if err != nil {
		return false
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil || pid <= 0 {
		return false
	}
	return processIsLive(pid)
}

// processIsLive reports whether a process exists. Signal 0 runs the existence and
// permission checks without delivering anything, so EPERM also means alive.
func processIsLive(pid int) bool {
	if pid <= 0 {
		return false
	}
	err := syscall.Kill(pid, 0)
	if err == nil {
		return true
	}
	return errors.Is(err, syscall.EPERM)
}

// lockTyping drives the Discord typing indicator for wackypub-native bound agents from
// their session.lock files. A turn holds the lock for its whole duration no matter who
// dispatched it (bot dispatch, idle wake, piggyback, self-dispatch), so the lock is one
// mid-turn signal covering every origin - and one watcher per agent. Routed
// (bridged/SDK-direct) agents have no session.lock; withTyping keeps covering them on
// the synchronous handler path.
type lockTyping struct {
	bot       *Bot
	mu        sync.Mutex
	ctx       context.Context
	cancel    context.CancelFunc
	watchers  map[string]*lockWatcher
	started   bool
	pollEvery time.Duration
}

// lockWatcher is the per-agent lock file follower.
type lockWatcher struct {
	agentID  string
	lockPath string
	cancel   context.CancelFunc
	turning  atomic.Bool // last evaluation: lock held by a live PID
}

func newLockTyping(b *Bot) *lockTyping {
	return &lockTyping{bot: b, watchers: map[string]*lockWatcher{}, pollEvery: lockTypingPollInterval}
}

// start launches the subsystem and re-arms a watcher for every persisted native
// binding, mirroring startFeed/watchExistingBindings: a binding loaded from state gets
// no watcher until re-bound otherwise. Idempotent.
func (l *lockTyping) start(ctx context.Context) {
	if l == nil || l.bot == nil {
		return
	}
	l.mu.Lock()
	if l.started {
		l.mu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(ctx)
	l.ctx = ctx
	l.cancel = cancel
	l.started = true
	agents := []string{}
	for _, binding := range l.bot.State.GetAllBindings() {
		if binding.AgentID != "" && !l.bot.isBridged(binding.AgentID) {
			agents = append(agents, binding.AgentID)
		}
	}
	l.mu.Unlock()
	for _, agentID := range agents {
		l.watch(agentID)
	}
}

func (l *lockTyping) close() {
	if l == nil {
		return
	}
	l.mu.Lock()
	cancel := l.cancel
	l.cancel = nil
	l.started = false
	l.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// watch starts following an agent's lock file. Called when a channel binds to the
// agent; no-op for routed agents (they have no session.lock).
func (l *lockTyping) watch(agentID string) {
	if l == nil || agentID == "" {
		return
	}
	if l.bot.isBridged(agentID) {
		return
	}
	l.mu.Lock()
	if _, exists := l.watchers[agentID]; exists {
		l.mu.Unlock()
		return
	}
	if l.ctx == nil {
		l.mu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(l.ctx)
	l.watchers[agentID] = &lockWatcher{
		agentID:  agentID,
		lockPath: filepath.Join(l.bot.WsDir, agentID, sessionLockName),
		cancel:   cancel,
	}
	w := l.watchers[agentID]
	l.mu.Unlock()
	go l.run(ctx, w)
}

// unwatch stops following an agent. Called when its last channel unbinds; safe to call
// for agents that have no watcher.
func (l *lockTyping) unwatch(agentID string) {
	if l == nil {
		return
	}
	l.mu.Lock()
	w := l.watchers[agentID]
	delete(l.watchers, agentID)
	l.mu.Unlock()
	if w != nil {
		w.cancel()
	}
}

// watcherFor is a test seam: the live watcher for agentID, or nil.
func (l *lockTyping) watcherFor(agentID string) *lockWatcher {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.watchers[agentID]
}

// run follows one agent's lock file: an inotify watch for instant transitions plus a
// poll backstop, both funneling into evaluate. The file watch is armed once the file
// exists (it may not yet) and re-armed if the watch dies; wackypub truncates and writes
// in place and never replaces the inode, so a live watch stays valid.
func (l *lockTyping) run(ctx context.Context, w *lockWatcher) {
	var events <-chan fsnotify.Event
	var fswErr <-chan error
	fsw, err := fsnotify.NewWatcher()
	if err != nil {
		log.Printf("\u26a0\ufe0f lock typing for %s: no file watcher (%v); poll-only", w.agentID, err)
	} else {
		defer fsw.Close()
		events = fsw.Events
		fswErr = fsw.Errors
	}
	autoArmed := false
	arm := func() bool {
		if fsw == nil || autoArmed {
			return false
		}
		if _, statErr := os.Stat(w.lockPath); statErr != nil {
			return false // no lock file yet: never had a turn; retry on the next tick
		}
		if addErr := fsw.Add(w.lockPath); addErr != nil {
			return false
		}
		autoArmed = true
		return true
	}
	if arm() {
		l.evaluate(w)
	}
	tick := time.NewTicker(l.pollEvery)
	defer tick.Stop()
	closed := false
	for {
		if closed {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				arm()
				l.evaluate(w)
			}
			continue
		}
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-events:
			if !ok {
				closed = true
				continue
			}
			_ = ev
			l.evaluate(w)
		case err, ok := <-fswErr:
			if !ok {
				closed = true
				continue
			}
			log.Printf("\u26a0\ufe0f lock typing for %s: watcher error: %v", w.agentID, err)
		case <-tick.C:
			arm()
			l.evaluate(w)
		}
	}
}

// evaluate re-reads the lock and, while the agent is mid-turn, re-asserts the typing
// indicator on its bound channels (rate-limited per channel). When the turn ends the
// evaluations stop reporting active and the last assertion expires on its own; there
// is no stop-typing API to call.
func (l *lockTyping) evaluate(w *lockWatcher) {
	active := sessionLockTurnActive(w.lockPath)
	w.turning.Store(active)
	if active {
		l.bot.refreshAgentTyping(w.agentID)
	}
}

// refreshAgentTyping re-asserts the typing indicator on every channel bound to
// agentID, rate-limited per channel. Discord typing is channel-scoped, so two bound
// agents sharing a channel collapse into one indicator by the per-channel rate limit.
func (b *Bot) refreshAgentTyping(agentID string) {
	if b.Session == nil || !b.Session.DataReady {
		return
	}
	for _, binding := range b.State.GetAllBindings() {
		if binding.AgentID == agentID {
			b.refreshChannelTyping(binding.ChannelID)
		}
	}
}

// refreshChannelTyping asserts the typing indicator on channelID at most once per
// typingRefreshInterval.
func (b *Bot) refreshChannelTyping(channelID string) {
	if b.Session == nil || !b.Session.DataReady || channelID == "" {
		return
	}
	if b.typingDue(channelID, time.Now()) {
		_ = b.Session.ChannelTyping(channelID)
	}
}

// typingDue reports whether a typing assertion is due for channelID at now and records
// the send when it is, so a mid-turn agent's stream of lock events refreshes the
// indicator at the established cadence rather than once per event.
func (b *Bot) typingDue(channelID string, now time.Time) bool {
	b.typingMu.Lock()
	defer b.typingMu.Unlock()
	if b.lastTyping == nil {
		b.lastTyping = map[string]time.Time{}
	}
	if now.Sub(b.lastTyping[channelID]) < typingRefreshInterval {
		return false
	}
	b.lastTyping[channelID] = now
	return true
}
