package bot

import (
	"context"
	"log"
	"sync"
	"time"

	agentv1 "github.com/colinrgodsey/wackypub/pkg/agent/v1"
)

// coalesceWindow merges the burst of events a single turn produces into one render pass.
// It replaces the watcher's fsnotify debounce; there is no trailing timer because the
// subscription tells us when work exists rather than us inferring it from file writes.
const coalesceWindow = 200 * time.Millisecond

// resubscribeBackoff is how long the feed waits before retrying a failed subscription, so a
// restarted server cannot make the bot spin.
const resubscribeBackoff = 2 * time.Second

// sessionFeed keeps Discord channels current by consuming the watch protocol instead of
// watching files. One subscription per watched agent pushes change signals; a single worker
// coalesces them and runs the existing read-and-render pass over ReadSession. The events
// themselves are never rendered: they say that something changed, and the session is the
// source of what to post.
type sessionFeed struct {
	bot     *Bot
	mu      sync.Mutex
	ctx     context.Context
	want    map[string]bool
	pending map[string]bool
	wake    chan struct{}
	cancel  context.CancelFunc
	started bool
}

func newSessionFeed(b *Bot) *sessionFeed {
	return &sessionFeed{
		bot:     b,
		want:    map[string]bool{},
		pending: map[string]bool{},
		wake:    make(chan struct{}, 1),
	}
}

// start launches the subscription supervisors and the coalescing worker. It is idempotent
// so a test harness can call it after construction.
func (f *sessionFeed) start(ctx context.Context) {
	if f == nil {
		return
	}
	f.mu.Lock()
	if f.started {
		f.mu.Unlock()
		return
	}
	f.started = true
	ctx, cancel := context.WithCancel(ctx)
	f.ctx, f.cancel = ctx, cancel
	agents := make([]string, 0, len(f.want))
	for agentID := range f.want {
		agents = append(agents, agentID)
	}
	f.mu.Unlock()

	go f.worker(ctx)
	for _, agentID := range agents {
		go f.supervise(ctx, agentID)
	}
}

func (f *sessionFeed) close() {
	if f == nil {
		return
	}
	f.mu.Lock()
	cancel := f.cancel
	f.cancel = nil
	f.started = false
	f.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// watch begins following an agent, called when a channel binds to it.
func (f *sessionFeed) watch(agentID string) {
	if f == nil || agentID == "" {
		return
	}
	f.mu.Lock()
	already := f.want[agentID]
	f.want[agentID] = true
	started := f.started
	ctx := f.ctx
	f.mu.Unlock()
	if already || !started || ctx == nil {
		return
	}
	go f.supervise(ctx, agentID)
}

// unwatch stops following an agent, called when its last channel unbinds.
func (f *sessionFeed) unwatch(agentID string) {
	if f == nil {
		return
	}
	f.mu.Lock()
	delete(f.want, agentID)
	delete(f.pending, agentID)
	f.mu.Unlock()
}

// trigger schedules a sync pass for agentID without waiting for a change signal, which is
// what the turn handler needs after it finishes generating: the session has grown and the
// next push may already have been consumed.
func (f *sessionFeed) trigger(agentID string) {
	if f == nil || agentID == "" {
		return
	}
	f.mu.Lock()
	f.pending[agentID] = true
	f.mu.Unlock()
	f.signal()
}

func (f *sessionFeed) signal() {
	select {
	case f.wake <- struct{}{}:
	default:
	}
}

// supervise keeps exactly one subscription alive per agent for as long as it is wanted.
func (f *sessionFeed) supervise(ctx context.Context, agentID string) {
	for {
		f.mu.Lock()
		wanted := f.want[agentID]
		f.mu.Unlock()
		if !wanted || ctx.Err() != nil {
			return
		}

		stream, err := f.bot.activeClient().SubscribeSession(ctx, &agentv1.SubscribeSessionRequest{
			AgentId:      agentID,
			WorkspaceDir: f.bot.WsDir,
		})
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("\u26a0\ufe0f session subscription failed for %s: %v", agentID, err)
		} else {
			f.consume(ctx, agentID, stream)
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(resubscribeBackoff):
		}
	}
}

// consume reads change signals until the stream ends, noting rewind and drop reports so a
// consumer that lost events says so instead of pretending continuity.
func (f *sessionFeed) consume(ctx context.Context, agentID string, stream agentv1.AgentService_SubscribeSessionClient) {
	for {
		if ctx.Err() != nil {
			return
		}
		resp, err := stream.Recv()
		if err != nil {
			if ctx.Err() == nil {
				log.Printf("\u26a0\ufe0f session stream ended for %s: %v", agentID, err)
			}
			return
		}
		if resp.GetDroppedEvents() > 0 {
			log.Printf("\u26a0\ufe0f %s: %d session events dropped since the last render; resyncing from the session",
				agentID, resp.GetDroppedEvents())
		}
		if resp.GetRewound() || resp.GetRolledBack() {
			// The session's sequence moved backwards or was replaced. The cursor is adopted
			// from the new session rather than compared against a history that no longer exists.
			f.bot.adoptSeqCursor(agentID)
		}
		f.trigger(agentID)
	}
}

// flushNow runs the sync pass for agentID immediately, bypassing the coalescing window. The
// turn handler uses it because it just grew the session itself and the next push may already
// have been consumed before the write landed.
func (f *sessionFeed) flushNow(agentID string) {
	if f == nil || agentID == "" {
		return
	}
	f.bot.SyncAgentToChannels(agentID)
}

// worker is the single renderer: it waits for a change, sleeps out the coalescing window so
// a turn's events collapse into one pass, then syncs every channel of every pending agent.
func (f *sessionFeed) worker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-f.wake:
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(coalesceWindow):
		}

		f.mu.Lock()
		batch := f.pending
		f.pending = map[string]bool{}
		f.mu.Unlock()

		for agentID := range batch {
			if ctx.Err() != nil {
				return
			}
			f.bot.SyncAgentToChannels(agentID)
		}
	}
}
