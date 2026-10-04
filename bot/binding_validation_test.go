package bot

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// The bug these pin: an unreachable agent service and an agent folder that really is gone both
// produced "Bound agent X does not exist in workspace", so a bot restart looked like binding
// corruption and a restart appeared to fix a binding that was never broken.

func newValidationBot(t *testing.T) (*Bot, *fakeAgent, *sync.Mutex, *[]string) {
	t.Helper()
	ws := t.TempDir()
	st, err := NewState(filepath.Join(ws, ".wackydiscord.json"))
	if err != nil {
		t.Fatalf("NewState: %v", err)
	}
	fake := newFakeAgent(ws)
	var mu sync.Mutex
	var msgs []string
	b := &Bot{WsDir: ws, State: st, Client: fake}
	if err := st.SetBinding(&ChannelBinding{ChannelID: "chan_v", AgentID: "bob"}); err != nil {
		t.Fatalf("SetBinding: %v", err)
	}
	return b, fake, &mu, &msgs
}

func postedMessages(mu *sync.Mutex, msgs []string) string {
	mu.Lock()
	defer mu.Unlock()
	return strings.Join(append([]string{}, msgs...), " | ")
}

func TestClassifyAgentAbsentVersusUnreachable(t *testing.T) {
	b, fake, _, _ := newValidationBot(t)

	if presence, _, err := b.classifyAgentWithRetry(context.Background(), "bob"); presence != agentAbsent {
		t.Errorf("absent agent: presence=%v err=%v", presence, err)
	}

	fake.queueInspectFailures(errors.New("reset"), errors.New("reset"), errors.New("reset"))
	if presence, _, err := b.classifyAgentWithRetry(context.Background(), "bob"); presence != agentServiceUnreachable {
		t.Errorf("unreachable service must never read as absence: presence=%v err=%v", presence, err)
	}

	if err := os.MkdirAll(filepath.Join(b.WsDir, "bob"), 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if presence, _, err := b.classifyAgentWithRetry(context.Background(), "bob"); presence != agentAvailable {
		t.Errorf("available agent: presence=%v err=%v", presence, err)
	}
}

func TestTransientUnavailabilityRecoversWithinRetryBudget(t *testing.T) {
	b, fake, _, _ := newValidationBot(t)
	if err := os.MkdirAll(filepath.Join(b.WsDir, "bob"), 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	fake.queueInspectFailures(errors.New("transport is closing"))
	presence, _, err := b.classifyAgentWithRetry(context.Background(), "bob")
	if presence != agentAvailable {
		t.Fatalf("one transient failure must be retried away: presence=%v err=%v", presence, err)
	}
}

func TestResolveMessageContextUnreachableDoesNotBlameBinding(t *testing.T) {
	b, fake, mu, msgs := newValidationBot(t)
	fake.queueInspectFailures(errors.New("connection refused"), errors.New("connection refused"), errors.New("connection refused"))

	s := createMockDiscordSession(mu, msgs)
	if _, ok := b.resolveMessageContext(s, "chan_v"); ok {
		t.Fatalf("turn must not proceed while the service is unreachable")
	}
	got := postedMessages(mu, *msgs)
	if strings.Contains(got, "does not exist") {
		t.Errorf("unreachable service must not produce a does-not-exist claim: %q", got)
	}
	if !strings.Contains(got, "Not ready") {
		t.Errorf("expected a readiness message, got %q", got)
	}
	if b.State.GetBinding("chan_v") == nil {
		t.Errorf("a transient failure must not clear the binding")
	}
}

func TestResolveMessageContextAbsentAgentStillSaysDoesNotExist(t *testing.T) {
	b, _, mu, msgs := newValidationBot(t)
	s := createMockDiscordSession(mu, msgs)
	if _, ok := b.resolveMessageContext(s, "chan_v"); ok {
		t.Fatalf("absent agent must stop the turn")
	}
	if got := postedMessages(mu, *msgs); !strings.Contains(got, "does not exist") {
		t.Errorf("a service that answered no-such-agent should still say so: %q", got)
	}
}

func TestResolveMessageContextBridgedError(t *testing.T) {
	b, fake, mu, msgs := newValidationBot(t)
	_ = os.WriteFile(filepath.Join(b.WsDir, RemoteManifestFile), []byte("bridged_bob: /bin/test\n"), 0o644)
	_ = b.State.SetBinding(&ChannelBinding{ChannelID: "chan_bridged", AgentID: "bridged_bob"})

	fake.queueInspectFailures(errors.New("bridge startup timeout"), errors.New("bridge startup timeout"), errors.New("bridge startup timeout"))
	s := createMockDiscordSession(mu, msgs)
	if _, ok := b.resolveMessageContext(s, "chan_bridged"); ok {
		t.Fatalf("turn must not proceed on bridge failure")
	}
	got := postedMessages(mu, *msgs)
	if !strings.Contains(got, "Bridge error") {
		t.Errorf("expected Bridge error message, got %q", got)
	}
	if strings.Contains(got, "does not exist") {
		t.Errorf("bridge error must not claim agent does not exist: %q", got)
	}
}

func TestValidateBindingsMissingAndUnansweredAreDistinct(t *testing.T) {
	// Two separate bots, because GetAllBindings iterates a map and a shared queue of refusals
	// would be consumed in an order the test cannot control.
	run := func(agent string, mkdir bool, failures ...error) string {
		ws := t.TempDir()
		st, err := NewState(filepath.Join(ws, ".wackydiscord.json"))
		if err != nil {
			t.Fatalf("NewState: %v", err)
		}
		if mkdir {
			if err := os.MkdirAll(filepath.Join(ws, agent), 0o755); err != nil {
				t.Fatalf("MkdirAll: %v", err)
			}
		}
		if err := st.SetBinding(&ChannelBinding{ChannelID: "chan_1", AgentID: agent}); err != nil {
			t.Fatalf("SetBinding: %v", err)
		}
		fake := newFakeAgent(ws)
		fake.queueInspectFailures(failures...)
		b := &Bot{WsDir: ws, State: st, Client: fake}
		return strings.Join(b.ValidateBindings(), " | ")
	}

	missing := run("ghost", false)
	if !strings.Contains(missing, "missing agent") || strings.Contains(missing, "did not answer") {
		t.Errorf("a service that answered should report a missing agent, got %q", missing)
	}

	unanswered := run("bob", true, errors.New("connection refused"))
	if !strings.Contains(unanswered, "did not answer") || strings.Contains(unanswered, "missing agent") {
		t.Errorf("a refused call must not be reported as a missing agent, got %q", unanswered)
	}
}

func TestAwaitAgentServiceWaitsForFirstAnswer(t *testing.T) {
	ws := t.TempDir()
	fake := newFakeAgent(ws)
	fake.mu.Lock()
	fake.listErrs = []error{errors.New("transport is closing"), errors.New("transport is closing")}
	fake.mu.Unlock()

	start := time.Now()
	if err := awaitAgentService(context.Background(), fake, ws, 2*time.Second, 20*time.Millisecond); err != nil {
		t.Fatalf("expected readiness after two refusals, got %v", err)
	}
	if time.Since(start) < 40*time.Millisecond {
		t.Errorf("readiness returned before polling twice could happen")
	}
}

func TestAwaitAgentServiceTimesOutWithAccurateError(t *testing.T) {
	ws := t.TempDir()
	fake := newFakeAgent(ws)
	fake.mu.Lock()
	for i := 0; i < 64; i++ {
		fake.listErrs = append(fake.listErrs, errors.New("connection refused"))
	}
	fake.mu.Unlock()

	err := awaitAgentService(context.Background(), fake, ws, 150*time.Millisecond, 20*time.Millisecond)
	if err == nil {
		t.Fatalf("expected a timeout error")
	}
	if !strings.Contains(err.Error(), "did not become ready") {
		t.Errorf("error should name the readiness failure, got %v", err)
	}
}
