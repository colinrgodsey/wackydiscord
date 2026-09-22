package bot

import (
	"fmt"
	"log"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/bwmarrin/discordgo"
	agentv1 "github.com/colinrgodsey/wackypub/pkg/agent/v1"
)

const (
	// ToolActivityMaxLines caps the tool lines shown in one activity message.
	ToolActivityMaxLines = 12

	// ToolActivityFlushInterval is the minimum spacing between edits of the activity
	// message. Discord allows roughly five channel writes per second, so a burst of
	// tool events must not outrun that.
	ToolActivityFlushInterval = 1200 * time.Millisecond

	// ToolActivityPreviewLimit caps a rendered args or error preview.
	ToolActivityPreviewLimit = 120

	// ToolActivityContentLimit keeps the whole message under Discord's cap with room
	// left for the overflow tail.
	ToolActivityContentLimit = 1900

	// ToolActivitySpeaker is the webhook username used for the activity message.
	ToolActivitySpeaker = "Tool activity"
)

// Tool status display markers, keyed by the D112 status values.
const (
	toolIconRunning   = "🔧"
	toolIconCompleted = "✅"
	toolIconError     = "❌"
	toolIconDenied    = "🚫"
)

// toolStatus* mirror the D112 ToolCallUpdate status values. The announce half of the
// pair carries no status at all, so the empty string is a meaningful state here.
const (
	toolStatusCompleted = "completed"
	toolStatusError     = "error"
	toolStatusDenied    = "denied"
)

// secretValuePatterns mask credential-shaped substrings wherever they appear in rendered
// text. The upstream D112 payload redacts by argument KEY only, so a token inside a shell
// command string, or inside a tool result head, arrives verbatim. This is the last
// checkpoint before it reaches a public channel.
var secretValuePatterns = []*regexp.Regexp{
	regexp.MustCompile(`\bsk-[A-Za-z0-9_\-]{12,}\b`),
	regexp.MustCompile(`\bsk_(?:live|test)_[A-Za-z0-9]{8,}\b`),
	regexp.MustCompile(`\bgh[pousr]_[A-Za-z0-9]{16,}\b`),
	regexp.MustCompile(`\bxox[abprs]-[A-Za-z0-9\-]{8,}\b`),
	regexp.MustCompile(`\bAKIA[0-9A-Z]{12,}\b`),
	regexp.MustCompile(`(?i)\b(?:Bearer|Basic|Token)\s+[A-Za-z0-9._~+/]{8,}\b`),
	regexp.MustCompile(`(?i)-----BEGIN[ A-Z]*PRIVATE KEY-----`),
}

// ActivityPoster delivers the activity message and then edits it in place. Splitting it
// out keeps the renderer testable without a live Discord session.
type ActivityPoster interface {
	Post(content string) (string, error)
	Edit(id string, content string) error
}

// DiscordPoster delivers tool activity through the channel webhook, falling back to a
// plain bot message when no webhook is available (DMs, missing permissions).
type DiscordPoster struct {
	Session     *discordgo.Session
	ChannelID   string
	Webhook     *discordgo.Webhook
	SpeakerName string
}

// Post sends a new activity message and returns its ID so later states can edit it.
func (p *DiscordPoster) Post(content string) (string, error) {
	if p == nil || p.Session == nil {
		return "", nil
	}
	noMentions := &discordgo.MessageAllowedMentions{Parse: []discordgo.AllowedMentionType{}}
	if p.Webhook != nil && p.Webhook.ID != "" && p.Webhook.Token != "" {
		msg, err := p.Session.WebhookExecute(p.Webhook.ID, p.Webhook.Token, true, &discordgo.WebhookParams{
			Content:         content,
			Username:        p.SpeakerName,
			AllowedMentions: noMentions,
		})
		if err == nil && msg != nil {
			return msg.ID, nil
		}
		// Webhook delivery failed; fall through to plain bot delivery.
		// The persona username is lost but the activity still shows, which beats
		// showing nothing.
	}
	msg, err := p.Session.ChannelMessageSendComplex(p.ChannelID, &discordgo.MessageSend{
		Content:         content,
		AllowedMentions: noMentions,
	})
	if err != nil {
		return "", fmt.Errorf("failed to post tool activity to channel %s: %w", p.ChannelID, err)
	}
	if msg == nil {
		return "", nil
	}
	return msg.ID, nil
}

// Edit replaces the body of a previously posted activity message.
func (p *DiscordPoster) Edit(id string, content string) error {
	if p == nil || p.Session == nil || id == "" {
		return nil
	}
	if _, err := p.Session.ChannelMessageEditComplex(&discordgo.MessageEdit{
		ID:      id,
		Channel: p.ChannelID,
		Content: &content,
	}); err != nil {
		return fmt.Errorf("failed to update tool activity in channel %s: %w", p.ChannelID, err)
	}
	return nil
}

type toolActivityEntry struct {
	callID   string
	toolName string
	args     string
	status   string
	bytes    int64
	// detail is the error head, shown for status=error.
	detail string
	// resultHead is the completed-result head, rendered only when showResultHead is set.
	resultHead string
}

// ToolActivity folds D112 tool_call and tool_call_update events into one live activity
// message per turn. An announce opens a line in the running state and the matching update
// overwrites that same line with the outcome, so a turn with many calls stays one message
// instead of a flood.
//
// Delivery happens under the same mutex that guards the view. The payloads are small and
// the only other contender is the throttle timer for this same turn, so the lock is held
// across a short Discord write rather than juggling unlock/relock around it.
type ToolActivity struct {
	poster   ActivityPoster
	interval time.Duration
	maxLines int
	// showResultHead renders the result head on completed lines. Off by default: the
	// head is a raw slice of the tool's marshaled result and can contain file
	// contents, which is not something to volunteer into a public channel.
	showResultHead bool

	mu         sync.Mutex
	entries    map[string]*toolActivityEntry
	order      []string
	dirty      bool
	messageID  string
	lastFlush  time.Time
	timer      *time.Timer
	closed     bool
	deliveries []string
}

// NewToolActivity returns a renderer that posts through poster. A nil poster keeps the
// view in memory only.
func NewToolActivity(poster ActivityPoster) *ToolActivity {
	return &ToolActivity{
		poster:   poster,
		interval: ToolActivityFlushInterval,
		maxLines: ToolActivityMaxLines,
		entries:  make(map[string]*toolActivityEntry),
	}
}

// SetFlushInterval overrides the edit throttle. Zero flushes synchronously on each event.
func (a *ToolActivity) SetFlushInterval(d time.Duration) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.interval = d
}

// SetShowResultHead turns rendering of the completed result head on or off.
func (a *ToolActivity) SetShowResultHead(show bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.showResultHead = show
}

// SetMaxLines overrides how many tool lines the activity message shows.
func (a *ToolActivity) SetMaxLines(n int) {
	if n <= 0 {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.maxLines = n
}

// Observe folds one streamed response into the activity view. Text-only chunks are
// ignored: the session watcher owns message text, this owns tool activity.
func (a *ToolActivity) Observe(resp *agentv1.GenerateTurnStreamResponse) {
	if a == nil || resp == nil {
		return
	}
	if tc := resp.GetToolCall(); tc != nil {
		a.foldAnnounce(tc)
		return
	}
	if tu := resp.GetToolCallUpdate(); tu != nil {
		a.foldUpdate(tu)
	}
}

func (a *ToolActivity) foldAnnounce(tc *agentv1.ToolCall) {
	status := ""
	if tc.GetDenied() {
		status = toolStatusDenied
	}
	a.upsert(tc.GetCallId(), tc.GetToolName(), func(e *toolActivityEntry) {
		e.args = scrubSecrets(tc.GetArgsSummary())
		e.status = status
	})
}

func (a *ToolActivity) foldUpdate(tu *agentv1.ToolCallUpdate) {
	a.upsert(tu.GetCallId(), tu.GetToolName(), func(e *toolActivityEntry) {
		e.status = tu.GetStatus()
		e.bytes = tu.GetResultBytes()
		if tu.GetStatus() == toolStatusError {
			e.detail = scrubSecrets(tu.GetResultHead())
			return
		}
		e.resultHead = scrubSecrets(tu.GetResultHead())
	})
}

// upsert mutates the entry for callID, registering it on first sight, then schedules a
// delivery.
func (a *ToolActivity) upsert(callID, toolName string, mutate func(*toolActivityEntry)) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if callID == "" {
		// Without an id there is no way to pair announce with update, so keep them as
		// separate lines rather than silently merging unrelated calls.
		callID = fmt.Sprintf("name:%s:%d", toolName, len(a.order))
	}
	e, ok := a.entries[callID]
	if !ok {
		e = &toolActivityEntry{callID: callID, toolName: toolName}
		a.entries[callID] = e
		a.order = append(a.order, callID)
	}
	if e.toolName == "" {
		e.toolName = toolName
	}
	mutate(e)
	a.dirty = true
	a.scheduleLocked()
}

// scheduleLocked chooses between an immediate delivery and a throttled one. Callers hold mu.
func (a *ToolActivity) scheduleLocked() {
	if a.poster == nil {
		return
	}
	if a.interval <= 0 || a.lastFlush.IsZero() || time.Since(a.lastFlush) >= a.interval {
		a.deliverLocked()
		return
	}
	if a.timer != nil {
		return
	}
	wait := a.interval - time.Since(a.lastFlush)
	a.timer = time.AfterFunc(wait, func() {
		a.mu.Lock()
		defer a.mu.Unlock()
		a.timer = nil
		if a.closed {
			return
		}
		a.deliverLocked()
	})
}

// deliverLocked renders the current view and posts or edits it. Callers hold mu.
func (a *ToolActivity) deliverLocked() {
	if a.poster == nil {
		return
	}
	content := a.renderLocked()
	if content == "" {
		return
	}
	if a.messageID == "" {
		id, err := a.poster.Post(content)
		a.lastFlush = time.Now()
		if err != nil {
			// Leave the view dirty so the next flush retries; log so a
			// delivery failure is not invisible.
			log.Printf("⚠️ tool activity post failed: %v", err)
			return
		}
		a.messageID = id
		a.dirty = false
		a.deliveries = append(a.deliveries, content)
		return
	}
	if err := a.poster.Edit(a.messageID, content); err != nil {
		log.Printf("⚠️ tool activity edit failed: %v", err)
		return
	}
	a.lastFlush = time.Now()
	a.dirty = false
	a.deliveries = append(a.deliveries, content)
}

// Flush delivers any pending change immediately, bypassing the throttle.
func (a *ToolActivity) Flush() {
	if a == nil {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.stopTimerLocked()
	if a.dirty {
		a.deliverLocked()
	}
}

// Finish flushes the final state and stops scheduling. Safe to call more than once.
func (a *ToolActivity) Finish() {
	if a == nil {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.stopTimerLocked()
	if a.dirty {
		a.deliverLocked()
	}
	a.closed = true
}

func (a *ToolActivity) stopTimerLocked() {
	if a.timer != nil {
		a.timer.Stop()
		a.timer = nil
	}
}

// Render returns the activity message body for the current state.
func (a *ToolActivity) Render() string {
	if a == nil {
		return ""
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.renderLocked()
}

// Deliveries returns every payload actually delivered, oldest first.
func (a *ToolActivity) Deliveries() []string {
	if a == nil {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]string, len(a.deliveries))
	copy(out, a.deliveries)
	return out
}

// Len returns how many distinct tool calls the view tracks.
func (a *ToolActivity) Len() int {
	if a == nil {
		return 0
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.order)
}

func (a *ToolActivity) renderLocked() string {
	if len(a.order) == 0 {
		return ""
	}
	shown := a.maxLines
	if shown <= 0 {
		shown = ToolActivityMaxLines
	}
	lines := make([]string, 0, shown+1)
	for i, id := range a.order {
		if i >= shown {
			break
		}
		lines = append(lines, a.entryLineLocked(a.entries[id]))
	}
	if hidden := len(a.order) - shown; hidden > 0 {
		lines = append(lines, fmt.Sprintf("… and %d more", hidden))
	}
	return truncateRunes(strings.Join(lines, "\n"), ToolActivityContentLimit)
}

func (a *ToolActivity) entryLineLocked(e *toolActivityEntry) string {
	name := e.toolName
	if name == "" {
		name = "tool"
	}
	switch e.status {
	case toolStatusCompleted:
		suffix := formatToolBytes(e.bytes)
		if a.showResultHead && e.resultHead != "" {
			suffix += " — " + truncateRunes(e.resultHead, ToolActivityPreviewLimit)
		}
		return fmt.Sprintf("%s `%s` — %s", toolIconCompleted, name, suffix)
	case toolStatusError:
		if e.detail != "" {
			return fmt.Sprintf("%s `%s` — %s", toolIconError, name, truncateRunes(e.detail, ToolActivityPreviewLimit))
		}
		return fmt.Sprintf("%s `%s` — failed", toolIconError, name)
	case toolStatusDenied:
		return fmt.Sprintf("%s `%s` — denied", toolIconDenied, name)
	default:
		if e.args != "" {
			return fmt.Sprintf("%s `%s` — %s", toolIconRunning, name, truncateRunes(e.args, ToolActivityPreviewLimit))
		}
		return fmt.Sprintf("%s `%s`", toolIconRunning, name)
	}
}

// scrubSecrets masks credential-shaped substrings in any text headed for the channel and
// normalises bytes that are not valid UTF-8, so a mid-rune truncation upstream cannot make
// the renderer emit replacement garbage.
func scrubSecrets(s string) string {
	if s == "" {
		return ""
	}
	s = strings.ToValidUTF8(s, "?")
	for _, re := range secretValuePatterns {
		s = re.ReplaceAllString(s, "[REDACTED]")
	}
	return collapseWhitespace(s)
}

// collapseWhitespace squeezes newlines and space runs so one tool call stays one line.
func collapseWhitespace(s string) string {
	s = strings.ReplaceAll(s, "\r\n", " ")
	s = strings.ReplaceAll(s, "\n", " ")
	s = strings.ReplaceAll(s, "\t", " ")
	for strings.Contains(s, "  ") {
		s = strings.ReplaceAll(s, "  ", " ")
	}
	return strings.TrimSpace(s)
}

// truncateRunes cuts s to at most max runes without splitting a multibyte character.
func truncateRunes(s string, max int) string {
	if max <= 0 {
		return s
	}
	r := []rune(s)
	if len(r) <= max {
		return s
	}
	return string(r[:max])
}

// formatToolBytes renders a byte count for display.
func formatToolBytes(n int64) string {
	switch {
	case n < 1024:
		return fmt.Sprintf("%d B", n)
	case n < 1024*1024:
		return fmt.Sprintf("%.1f KB", float64(n)/1024)
	default:
		return fmt.Sprintf("%.1f MB", float64(n)/(1024*1024))
	}
}

// newChannelPoster builds a poster that delivers tool activity through the bound
// channel's webhook, falling back to plain bot delivery when it has none.
func newChannelPoster(s *discordgo.Session, channelID string, bnd *ChannelBinding) *DiscordPoster {
	var wh *discordgo.Webhook
	if bnd != nil && bnd.WebhookID != "" && bnd.WebhookToken != "" {
		wh = &discordgo.Webhook{ID: bnd.WebhookID, Token: bnd.WebhookToken}
	}
	return &DiscordPoster{
		Session:     s,
		ChannelID:   channelID,
		Webhook:     wh,
		SpeakerName: ToolActivitySpeaker,
	}
}
