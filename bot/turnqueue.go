package bot

import (
	"log"
	"strings"
	"sync"

	"github.com/bwmarrin/discordgo"
)

// batchSeparator joins queued messages coalesced into one turn. Plain
// separators, not numbered: every item in a batch shares one channel author,
// and the model needs the item boundary, not an index.
const batchSeparator = "\n\n---\n\n"

// turnItem is one queued user message: its prepared prompt text plus the
// originating event (session and message identity for delivery) and the agent
// the text was prepared against. Attachment prompt text may reference files in
// that agent's scratchpad, so a rebound channel must not redirect the batch
// to a different agent.
type turnItem struct {
	s        *discordgo.Session
	userText string
	message  *discordgo.MessageCreate
	agentID  string
}

// turnQueue is a per-channel FIFO of user messages coalesced into turns. One
// lazy worker goroutine per channel: the first enqueue after an empty queue
// starts it, and it retires when a drain comes up empty. The worker owns the
// runTurn call, so HandleMessageCreate (which runs on the Discord event loop)
// enqueues and returns without blocking on generation - a long turn no longer
// stalls every other channel and slash command on the session.
type turnQueue struct {
	bot       *Bot
	channelID string

	mu       sync.Mutex
	items    []*turnItem
	workerOn bool
}

// queueForChannel returns the channel's queue, creating it on first use.
func (b *Bot) queueForChannel(channelID string) *turnQueue {
	b.turnQueuesMu.Lock()
	defer b.turnQueuesMu.Unlock()
	if b.turnQueues == nil {
		b.turnQueues = make(map[string]*turnQueue)
	}
	q, ok := b.turnQueues[channelID]
	if !ok {
		q = &turnQueue{bot: b, channelID: channelID}
		b.turnQueues[channelID] = q
	}
	return q
}

// queueLookup is the non-creating variant for observers (tests).
func (b *Bot) queueLookup(channelID string) *turnQueue {
	b.turnQueuesMu.Lock()
	defer b.turnQueuesMu.Unlock()
	if b.turnQueues == nil {
		return nil
	}
	return b.turnQueues[channelID]
}

// testHookBeforeDrain, when non-nil, is called before every drain. Tests use
// it to hold the worker until a burst of messages is fully enqueued.
var testHookBeforeDrain func(channelID string)

func (q *turnQueue) enqueue(item *turnItem) {
	q.mu.Lock()
	q.items = append(q.items, item)
	start := !q.workerOn
	q.workerOn = true
	q.mu.Unlock()
	if start {
		go q.run()
	}
}

// run is the worker loop: drain everything currently queued, run it as ONE
// turn, repeat. It retires only when a drain comes up empty, and retirement
// is the same lock-protected step that starts a fresh worker, so a message
// can never strand: it is either in the drained batch, or it arrives after
// retirement and starts a new worker.
func (q *turnQueue) run() {
	for {
		if testHookBeforeDrain != nil {
			testHookBeforeDrain(q.channelID)
		}
		q.mu.Lock()
		if len(q.items) == 0 {
			q.workerOn = false
			q.mu.Unlock()
			return
		}
		batch := q.items
		q.items = nil
		q.mu.Unlock()

		q.bot.processBatch(q.channelID, batch)

		// Messages that arrived during the turn form the next single
		// concatenated turn; the loop picks them up.
	}
}

// processBatch coalesces a drained batch into one turn: FIFO order, joined
// with batchSeparator. The binding is re-read fresh rather than trusting the
// handler's snapshot: a rebind or unbind between enqueue and drain must not
// send the batch to a different agent.
func (b *Bot) processBatch(channelID string, batch []*turnItem) {
	if len(batch) == 0 {
		return
	}
	binding := b.State.GetBinding(channelID)
	if binding == nil {
		log.Printf("⚠️ %d queued message(s) for channel %s dropped: channel no longer bound", len(batch), channelID)
		return
	}
	if binding.AgentID != batch[0].agentID {
		log.Printf("⚠️ %d queued message(s) for channel %s dropped: channel rebound to %q before the batch drained (prepared for %q)", len(batch), channelID, binding.AgentID, batch[0].agentID)
		return
	}
	texts := make([]string, len(batch))
	for i, item := range batch {
		texts[i] = item.userText
	}
	b.runTurn(batch[0].s, channelID, binding, strings.Join(texts, batchSeparator))
}
