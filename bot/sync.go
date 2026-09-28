package bot

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	agentv1 "github.com/colinrgodsey/wackypub/pkg/agent/v1"
	"google.golang.org/genai"
)

// SessionTurnToContent converts a proto SessionTurn into a *genai.Content struct.
func SessionTurnToContent(st *agentv1.SessionTurn) *genai.Content {
	if st == nil {
		return nil
	}
	c := &genai.Content{
		Role: st.Role,
	}
	for _, p := range st.Parts {
		if p == nil {
			continue
		}
		part := &genai.Part{
			Text:    p.Text,
			Thought: p.GetThought(),
		}
		if len(p.InlineData) > 0 || p.MimeType != "" {
			part.InlineData = &genai.Blob{
				Data:     p.InlineData,
				MIMEType: p.MimeType,
			}
		}
		c.Parts = append(c.Parts, part)
	}
	return c
}

// ComputeTurnHash generates a deterministic SHA-256 hash of a turn's role and content parts.
// Used for deduplicating messages across Discord channel backfills and compaction events.
func ComputeTurnHash(turn *genai.Content) string {
	if turn == nil {
		return ""
	}

	payload, err := json.Marshal(struct {
		Role  string        `json:"role"`
		Parts []*genai.Part `json:"parts"`
	}{
		Role:  string(turn.Role),
		Parts: turn.Parts,
	})
	if err != nil {
		// Fallback to text hash
		h := sha256.Sum256([]byte(fmt.Sprintf("%s:%s", turn.Role, contentText(turn))))
		return hex.EncodeToString(h[:])
	}

	h := sha256.Sum256(payload)
	return hex.EncodeToString(h[:])
}

// TurnWithSeq pairs a rendered turn with the durable sequence number the server stamped on
// it. Only the pair carries enough information to decide what is new.
type TurnWithSeq struct {
	Content *genai.Content
	Seq     int64
}

// DiffUnsyncedTurns returns the turns not yet rendered into the channel, oldest first.
// A cursor of zero means no cursor exists yet: whatever is already on screen in Discord is
// treated as seen and nothing is posted, because replaying it would duplicate messages users
// have read. That is simultaneously the migration rule for bindings written before seq.
//
// gap counts seq numbers below the oldest retained turn that the cursor cannot speak to,
// which is what a rewritten or truncated prefix looks like. It is reported, never replayed:
// the content behind those numbers is no longer in the session.
func DiffUnsyncedTurns(turns []TurnWithSeq, lastSeq int64) (unsynced []TurnWithSeq, newLastSeq int64, gap int64) {
	if len(turns) == 0 {
		return nil, lastSeq, 0
	}

	newest := turns[len(turns)-1].Seq
	if lastSeq <= 0 {
		return nil, newest, 0
	}

	if newest < lastSeq {
		// The session got shorter than the cursor, which means a rollback or a restored
		// directory. Holding the cursor is the safe direction: lowering it would queue up
		// turns the channel has already shown.
		return nil, lastSeq, 0
	}

	if oldest := turns[0].Seq; oldest > lastSeq+1 {
		gap = oldest - lastSeq - 1
	}

	for _, t := range turns {
		if t.Seq > lastSeq {
			unsynced = append(unsynced, t)
		}
	}
	return unsynced, newest, gap
}

// contentText flattens a turn to the text a reader would see, skipping reasoning parts.
// Nothing about it needs a server, and asking for one per turn would be a round trip to do
// a string operation.
func contentText(c *genai.Content) string {
	if c == nil {
		return ""
	}
	var sb strings.Builder
	for _, p := range c.Parts {
		if p != nil && p.Text != "" && !p.Thought {
			sb.WriteString(p.Text)
		}
	}
	return sb.String()
}

// SessionTurnsWithSeq converts protocol turns into renderable pairs, dropping any the
// server did not number so an unnumbered turn can never advance the cursor.
func SessionTurnsWithSeq(sts []*agentv1.SessionTurn) []TurnWithSeq {
	out := make([]TurnWithSeq, 0, len(sts))
	for _, st := range sts {
		if st == nil || st.GetSeq() <= 0 {
			continue
		}
		out = append(out, TurnWithSeq{Content: SessionTurnToContent(st), Seq: st.GetSeq()})
	}
	return out
}

// IsSyntheticHarnessTurn checks if a turn is a synthetic harness sentinel turn injected by D88
// (such as post-compaction continuation, scratchpad image injection, or compaction notice).
func IsSyntheticHarnessTurn(turn *genai.Content) bool {
	if turn == nil || turn.Role != "user" {
		return false
	}
	text := strings.TrimSpace(contentText(turn))
	return strings.HasPrefix(text, "<CONTINUATION") ||
		strings.HasPrefix(text, "<IMAGE") ||
		strings.HasPrefix(text, "<COMPACTION_NOTICE")
}

// FormatSyntheticHarnessTurn renders synthetic harness turns as subtle status badges in verbose mode.
func FormatSyntheticHarnessTurn(turn *genai.Content) string {
	if turn == nil {
		return ""
	}
	text := strings.TrimSpace(contentText(turn))
	if strings.HasPrefix(text, "<CONTINUATION") {
		re := regexp.MustCompile(`reason="([^"]+)"`)
		match := re.FindStringSubmatch(text)
		if len(match) > 1 {
			reason := match[1]
			if reason == "post-compaction" {
				return "🔄 **[Auto-Continuation]** Post-compaction resume"
			}
			return fmt.Sprintf("🔄 **[Auto-Continuation]** %s", reason)
		}
		return "🔄 **[Auto-Continuation]** Resume"
	}
	if strings.HasPrefix(text, "<IMAGE") {
		return "🔄 **[Auto-Continuation]** Image processing"
	}
	if strings.HasPrefix(text, "<COMPACTION_NOTICE") {
		return "🔄 **[Auto-Continuation]** Compaction notice"
	}
	return "🔄 **[Auto-Continuation]**"
}

// FormatUserBackfillMessage formats an unsynced background user turn with clear blockquotes and attribution.
func FormatUserBackfillMessage(turn *genai.Content) string {
	if turn == nil {
		return ""
	}

	text := strings.TrimSpace(contentText(turn))
	if text == "" {
		return ""
	}

	lines := strings.Split(text, "\n")
	var sb strings.Builder
	sb.WriteString("> 👤 **[User Turn]**\n")
	for _, l := range lines {
		sb.WriteString("> ")
		sb.WriteString(l)
		sb.WriteString("\n")
	}

	return strings.TrimSpace(sb.String())
}

// FormatAssistantBackfillMessage extracts clean assistant text from a model turn (excluding thought reasoning).
func FormatAssistantBackfillMessage(turn *genai.Content) string {
	if turn == nil {
		return ""
	}

	var sb strings.Builder
	for _, p := range turn.Parts {
		if p != nil && !p.Thought && p.Text != "" {
			sb.WriteString(p.Text)
		}
	}

	return strings.TrimSpace(sb.String())
}

var (
	scratchpadExpandRegex = regexp.MustCompile(`(?i)<SCRATCHPAD_EXPAND\s+([^>]+)\s*/?>`)
	expandIDRegex         = regexp.MustCompile(`(?i)id=\\?["']([^"'\\\s]+)["']\\?`)
)

// ExpandScratchpadSentinels scans message text for <SCRATCHPAD_EXPAND id="X" /> sentinels (D61)
// and substitutes the referenced scratchpad text inline via sdk.GetScratchpad.
// If resolution fails (e.g. entry missing or evicted), the tag is left untouched (graceful fallback).
func ExpandScratchpadSentinels(client AgentClient, agentID string, wsDir string, text string) string {
	if client == nil || agentID == "" || (!strings.Contains(text, "<SCRATCHPAD_EXPAND") && !strings.Contains(text, "<scratchpad_expand")) {
		return text
	}

	return scratchpadExpandRegex.ReplaceAllStringFunc(text, func(match string) string {
		idMatch := expandIDRegex.FindStringSubmatch(match)
		if len(idMatch) < 2 {
			return match
		}
		id := idMatch[1]

		resp, err := client.GetScratchpad(context.Background(), &agentv1.GetScratchpadRequest{
			AgentId:      agentID,
			EntryId:      id,
			WorkspaceDir: wsDir,
		})
		if err != nil || resp == nil || resp.GetText() == "" {
			return match
		}

		return resp.GetText()
	})
}
