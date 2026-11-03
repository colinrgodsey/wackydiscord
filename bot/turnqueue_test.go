package bot

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
	agentv1 "github.com/colinrgodsey/wackypub/pkg/agent/v1"
	"google.golang.org/genai"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// waitQueueIdle blocks until the channel's turn queue has fully drained (worker
// retired), i.e. every enqueued message has been processed into turn(s).
// Returns immediately when nothing was enqueued: silently ignored messages
// never reach the queue.
func waitQueueIdle(t *testing.T, b *Bot, channelID string) {
	t.Helper()
	q := b.queueLookup(channelID)
	if q == nil {
		return
	}
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		q.mu.Lock()
		idle := len(q.items) == 0 && !q.workerOn
		q.mu.Unlock()
		if idle {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("turn queue for channel %s still active after 15s", channelID)
}

func testMsg(channelID, id, text string) *discordgo.MessageCreate {
	return &discordgo.MessageCreate{
		Message: &discordgo.Message{
			ID:        id,
			ChannelID: channelID,
			Content:   text,
			Author:    &discordgo.User{ID: "owner_1"},
		},
	}
}

func turnContentText(c *genai.Content) string {
	var sb strings.Builder
	for _, p := range c.Parts {
		if p != nil {
			sb.WriteString(p.Text)
		}
	}
	return sb.String()
}

func sessionUserTurns(t *testing.T, b *Bot, agentID string) []*genai.Content {
	t.Helper()
	res, err := b.Client.ReadSession(context.Background(), &agentv1.ReadSessionRequest{AgentId: agentID})
	if err != nil {
		t.Fatalf("ReadSession: %v", err)
	}
	var out []*genai.Content
	for _, tr := range res.GetTurns() {
		if tr.GetRole() == "user" {
			out = append(out, SessionTurnToContent(tr))
		}
	}
	return out
}

// TestTurnQueue_ConcatenatesBurstIntoOneTurn pins the coalescing contract:
// a burst of messages queued before the worker drains becomes exactly ONE
// user turn carrying the joined text, in FIFO order.
func TestTurnQueue_ConcatenatesBurstIntoOneTurn(t *testing.T) {
	b, _, _ := setupTestBotWithSpy(t)
	_ = b.State.SetBinding(&ChannelBinding{ChannelID: "c1", AgentID: "bob"})

	// Hold the worker at the drain gate until the whole burst is enqueued,
	// so the drain - and the assertion - are deterministic.
	released := make(chan struct{})
	testHookBeforeDrain = func(string) { <-released }
	defer func() { testHookBeforeDrain = nil }()

	for i, text := range []string{"first", "second", "third"} {
		b.HandleMessageCreate(b.Session, testMsg("c1", "m"+string(rune('0'+i)), text))
	}
	close(released)
	waitQueueIdle(t, b, "c1")

	turns := sessionUserTurns(t, b, "bob")
	if len(turns) != 1 {
		t.Fatalf("expected exactly 1 user turn (coalesced burst), got %d", len(turns))
	}
	want := "first" + batchSeparator + "second" + batchSeparator + "third"
	if got := turnContentText(turns[0]); got != want {
		t.Errorf("coalesced turn text mismatch:\ngot  %q\nwant %q", got, want)
	}
}

// TestTurnQueue_HandlerBurstPreservesOrder pins what holds without the drain
// gate: however the worker interleaves with the handler, every message lands
// in the session in arrival order (the burst may split into 1..3 turns).
func TestTurnQueue_HandlerBurstPreservesOrder(t *testing.T) {
	b, _, _ := setupTestBotWithSpy(t)
	_ = b.State.SetBinding(&ChannelBinding{ChannelID: "c1", AgentID: "bob"})

	for i, text := range []string{"first", "second", "third"} {
		b.HandleMessageCreate(b.Session, testMsg("c1", "m"+string(rune('0'+i)), text))
	}
	waitQueueIdle(t, b, "c1")

	var parts []string
	for _, c := range sessionUserTurns(t, b, "bob") {
		parts = append(parts, strings.Split(turnContentText(c), batchSeparator)...)
	}
	want := []string{"first", "second", "third"}
	if len(parts) != len(want) {
		t.Fatalf("expected %d queued messages delivered, got %d: %v", len(want), len(parts), parts)
	}
	for i := range want {
		if parts[i] != want[i] {
			t.Fatalf("message order broken: got %v, want %v", parts, want)
		}
	}
}

// TestTurnQueue_MidTurnMessagesFormNextConcatenatedTurn pins the second half
// of the design: messages that arrive while a turn is in flight are not lost
// and form the NEXT single concatenated turn.
func TestTurnQueue_MidTurnMessagesFormNextConcatenatedTurn(t *testing.T) {
	b, _, _ := setupTestBotWithSpy(t)
	_ = b.State.SetBinding(&ChannelBinding{ChannelID: "c1", AgentID: "bob"})
	fa := b.Client.(*fakeAgent)
	fa.mu.Lock()
	fa.hold = make(chan struct{})
	fa.started = make(chan struct{})
	fa.mu.Unlock()

	b.HandleMessageCreate(b.Session, testMsg("c1", "m0", "first"))
	select {
	case <-fa.started:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for first turn to start")
	}

	// Queue two more while the first turn is held in flight.
	b.HandleMessageCreate(b.Session, testMsg("c1", "m1", "second"))
	b.HandleMessageCreate(b.Session, testMsg("c1", "m2", "third"))

	fa.mu.Lock()
	close(fa.hold)
	fa.mu.Unlock()
	waitQueueIdle(t, b, "c1")

	turns := sessionUserTurns(t, b, "bob")
	if len(turns) != 2 {
		t.Fatalf("expected 2 user turns (1 + coalesced mid-turn batch), got %d", len(turns))
	}
	if got := turnContentText(turns[0]); got != "first" {
		t.Errorf("first turn mismatch: %q", got)
	}
	want := "second" + batchSeparator + "third"
	if got := turnContentText(turns[1]); got != want {
		t.Errorf("mid-turn batch mismatch: got %q, want %q", got, want)
	}
}

// TestTurnQueue_StopCancelsAndWorkerContinues pins /stop against the worker:
// the cancel reaches the in-flight turn and the worker then drains the next
// batch instead of dying.
func TestTurnQueue_StopCancelsAndWorkerContinues(t *testing.T) {
	b, _, _ := setupTestBotWithSpy(t)
	if ok, _, _ := b.State.TryClaim("owner_1"); !ok {
		t.Fatalf("TryClaim failed")
	}
	_ = b.State.SetBinding(&ChannelBinding{ChannelID: "c1", AgentID: "bob"})
	fa := b.Client.(*fakeAgent)
	fa.mu.Lock()
	fa.hold = make(chan struct{})
	fa.started = make(chan struct{})
	fa.mu.Unlock()

	b.HandleMessageCreate(b.Session, testMsg("c1", "m0", "one"))
	select {
	case <-fa.started:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for turn to start")
	}

	i := &discordgo.InteractionCreate{
		Interaction: &discordgo.Interaction{
			ID:        "int_stop_tq",
			Token:     "tok_stop_tq",
			ChannelID: "c1",
			Type:      discordgo.InteractionApplicationCommand,
			Data:      discordgo.ApplicationCommandInteractionData{Name: "stop"},
			Member:    &discordgo.Member{User: &discordgo.User{ID: "owner_1"}},
		},
	}
	b.HandleInteraction(b.Session, i)

	deadline := time.Now().Add(5 * time.Second)
	for {
		fa.mu.Lock()
		c := fa.cancelCalls
		fa.mu.Unlock()
		if c >= 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected CancelTurn to be called, cancelCalls=%d", c)
		}
		time.Sleep(5 * time.Millisecond)
	}

	// Release the hold so the follow-up turn can stream.
	fa.mu.Lock()
	close(fa.hold)
	fa.mu.Unlock()

	// The worker survives the cancel and processes the next message.
	b.HandleMessageCreate(b.Session, testMsg("c1", "m1", "two"))
	waitQueueIdle(t, b, "c1")

	turns := sessionUserTurns(t, b, "bob")
	if len(turns) != 2 {
		t.Fatalf("expected 2 user turns after stop + follow-up, got %d", len(turns))
	}
	if got := turnContentText(turns[1]); got != "two" {
		t.Errorf("post-stop turn mismatch: %q", got)
	}
}

// TestTurnQueue_ConcatenatedTurnRegistersCompositeHash pins the dedup
// integration: the session feed's ConsumePendingUserHash (keyed on the turn
// content hash) must find a registration for the coalesced turn, or the feed
// would double-post it.
func TestTurnQueue_ConcatenatedTurnRegistersCompositeHash(t *testing.T) {
	b, _, _ := setupTestBotWithSpy(t)
	_ = b.State.SetBinding(&ChannelBinding{ChannelID: "c1", AgentID: "bob"})

	released := make(chan struct{})
	testHookBeforeDrain = func(string) { <-released }
	defer func() { testHookBeforeDrain = nil }()

	b.queueForChannel("c1").enqueue(&turnItem{s: b.Session, userText: "alpha", message: testMsg("c1", "a", "alpha"), agentID: "bob"})
	b.queueForChannel("c1").enqueue(&turnItem{s: b.Session, userText: "beta", message: testMsg("c1", "b", "beta"), agentID: "bob"})
	close(released)
	waitQueueIdle(t, b, "c1")

	turns := sessionUserTurns(t, b, "bob")
	if len(turns) != 1 {
		t.Fatalf("expected 1 coalesced turn, got %d", len(turns))
	}
	hash := ComputeTurnHash(turns[0])

	syncUnlock := b.State.LockChannelSync("c1")
	bnd := b.State.GetBinding("c1")
	consumed := bnd.ConsumePendingUserHash(hash)
	b.State.SetBinding(bnd)
	syncUnlock()
	if !consumed {
		t.Errorf("composite hash for the coalesced turn was not registered in PendingUserHashes")
	}
}

// TestTurnQueue_WorkerRetiresAndRestarts pins the worker lifecycle: an idle
// queue retires its goroutine, and a later message starts a fresh worker
// instead of leaking or stranding the message.
func TestTurnQueue_WorkerRetiresAndRestarts(t *testing.T) {
	b, _, _ := setupTestBotWithSpy(t)
	_ = b.State.SetBinding(&ChannelBinding{ChannelID: "c1", AgentID: "bob"})
	q := b.queueForChannel("c1")

	q.enqueue(&turnItem{s: b.Session, userText: "one", message: testMsg("c1", "o", "one"), agentID: "bob"})
	waitQueueIdle(t, b, "c1")

	q.mu.Lock()
	retired := !q.workerOn && len(q.items) == 0
	q.mu.Unlock()
	if !retired {
		t.Fatalf("expected worker retired on idle queue")
	}

	q.enqueue(&turnItem{s: b.Session, userText: "two", message: testMsg("c1", "t", "two"), agentID: "bob"})
	waitQueueIdle(t, b, "c1")

	turns := sessionUserTurns(t, b, "bob")
	if len(turns) != 2 {
		t.Fatalf("expected 2 user turns across retire/restart, got %d", len(turns))
	}
}

// TestTurnQueue_BridgedMessagesDuringTurnAreNotLost pins the bridged no-loss
// fix: a message arriving mid-turn on a bridged channel is queued, not
// rejected - both messages end up generating turns.
func TestTurnQueue_BridgedMessagesDuringTurnAreNotLost(t *testing.T) {
	b, sdk, spy, _ := setupBridgedBot(t)
	_ = b.State.SetBinding(&ChannelBinding{ChannelID: "chan_bridged", AgentID: "agy"})

	sdk.mu.Lock()
	sdk.addAndGenStreams = []func(*agentv1.AddAndGenerateTurnStreamResponse){
		func(r *agentv1.AddAndGenerateTurnStreamResponse) { r.Text = "reply one" },
	}
	sdk.hold = make(chan struct{})
	sdk.started = make(chan struct{})
	sdk.mu.Unlock()

	b.HandleMessageCreate(b.Session, makeMessage("ping"))
	select {
	case <-sdk.started:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for bridged turn to start")
	}

	b.HandleMessageCreate(b.Session, makeMessage("pong"))

	sdk.mu.Lock()
	close(sdk.hold)
	sdk.mu.Unlock()
	waitQueueIdle(t, b, "chan_bridged")

	sdk.mu.Lock()
	calls := sdk.addAndGenCalls
	sdk.mu.Unlock()
	if calls != 2 {
		t.Fatalf("expected 2 add-and-generate calls (second message must not be lost), got %d", calls)
	}
	output := spy.allOutput()
	if !strings.Contains(output, "reply one") {
		t.Errorf("expected bridged reply in output, got: %q", output)
	}
}

// TestTurnQueue_BridgedBusyRejectsAndDrains pins the safety net: a bridge
// that rejects anyway (externally triggered busy turn) surfaces the friendly
// busy message and the worker still retires cleanly, no requeue loop.
func TestTurnQueue_BridgedBusyRejectsAndDrains(t *testing.T) {
	b, sdk, spy, _ := setupBridgedBot(t)
	_ = b.State.SetBinding(&ChannelBinding{ChannelID: "chan_bridged", AgentID: "agy"})

	sdk.mu.Lock()
	sdk.addAndGenErr = status.Error(codes.ResourceExhausted, "another prompt turn is already in flight for this agent")
	sdk.mu.Unlock()

	b.HandleMessageCreate(b.Session, makeMessage("hello"))
	waitQueueIdle(t, b, "chan_bridged")

	output := spy.allOutput()
	if !strings.Contains(output, "busy") {
		t.Fatalf("expected busy message, got: %q", output)
	}

	q := b.queueLookup("chan_bridged")
	q.mu.Lock()
	idle := len(q.items) == 0 && !q.workerOn
	q.mu.Unlock()
	if !idle {
		t.Fatalf("expected queue drained and worker retired after busy rejection")
	}
}
