package bot

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	agentv1 "github.com/colinrgodsey/wackypub/pkg/agent/v1"
)

// TestFeed_WatchesExistingBindingsAtStartup pins the async-watch regression
// (bugs/wackydiscord/async-watch-no-cross-client-updates): the retired fsnotify
// watcher watched every persisted binding when it started, but feed.start() only
// launches the worker and supervisors. Without re-arming the subscription for
// bindings loaded from state, a bound agent gets NO async updates until it is
// bound again - cross-client turns never surface. The startup pass restores the
// old watcher's Start() behavior.
func TestFeed_WatchesExistingBindingsAtStartup(t *testing.T) {
	tmpDir := t.TempDir()
	agentDir := filepath.Join(tmpDir, "bob")
	if err := os.MkdirAll(agentDir, 0755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	st, err := NewState(filepath.Join(tmpDir, ".wackydiscord.json"))
	if err != nil {
		t.Fatalf("NewState: %v", err)
	}
	if err := st.SetBinding(&ChannelBinding{ChannelID: "chan_1", AgentID: "bob"}); err != nil {
		t.Fatalf("SetBinding: %v", err)
	}

	b := &Bot{WsDir: tmpDir, State: st, Client: newFakeAgent(tmpDir)}
	b.feed = newSessionFeed(b)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b.startFeed(ctx)

	b.feed.mu.Lock()
	watched := b.feed.want["bob"]
	b.feed.mu.Unlock()
	if !watched {
		t.Fatalf("startup must subscribe the session feed to persisted bindings (cross-client turns never surface otherwise)")
	}
}

// TestFeed_CrossClientEventSurfacesInPendingChannel is the delivery half of the
// regression: a session event produced by another client (not the bot's own turn
// handler) must schedule a sync pass for the bound agent. It drives the fake
// agent's SubscribeSession stream directly with a synthetic event, exactly as
// the wackypub side would emit one for a turn written by an A2A dispatch or a
// CLI prompt.
func TestFeed_CrossClientEventSurfacesInPendingChannel(t *testing.T) {
	tmpDir := t.TempDir()
	agentDir := filepath.Join(tmpDir, "bob")
	if err := os.MkdirAll(agentDir, 0755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	st, err := NewState(filepath.Join(tmpDir, ".wackydiscord.json"))
	if err != nil {
		t.Fatalf("NewState: %v", err)
	}
	if err := st.SetBinding(&ChannelBinding{ChannelID: "chan_1", AgentID: "bob"}); err != nil {
		t.Fatalf("SetBinding: %v", err)
	}

	fake := newFakeAgent(tmpDir)
	b := &Bot{WsDir: tmpDir, State: st, Client: fake}
	b.feed = newSessionFeed(b)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b.startFeed(ctx)

	// Wait for the supervisor to subscribe (the fake's stream is created lazily by
	// the consume goroutine; the channel exists from construction).
	time.Sleep(50 * time.Millisecond)

	// A cross-client turn event: the session grew because ANOTHER client drove it.
	fake.subEvents <- &agentv1.SubscribeSessionResponse{
		Event: &agentv1.SessionEvent{Seq: 7},
	}

	// The event must land in the pending set (the worker renders it; the pending
	// membership is the observable that a sync is scheduled).
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		b.feed.mu.Lock()
		pending := b.feed.pending["bob"]
		b.feed.mu.Unlock()
		if pending {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("cross-client session event did not schedule a sync for bob")
}
