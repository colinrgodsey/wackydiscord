package bot

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"github.com/colinrgodsey/wackypub/pkg/agent"
	"google.golang.org/genai"
)

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
		h := sha256.Sum256([]byte(fmt.Sprintf("%s:%s", turn.Role, agent.ContentText(turn))))
		return hex.EncodeToString(h[:])
	}

	h := sha256.Sum256(payload)
	return hex.EncodeToString(h[:])
}

// DiffUnsyncedTurns calculates which turns in session history have not yet been posted to Discord.
// It leverages both fast index lookup and content hash matching to survive compaction events.
func DiffUnsyncedTurns(turns []*genai.Content, lastHash string, lastIndex int) ([]*genai.Content, int, string) {
	if len(turns) == 0 {
		return nil, 0, ""
	}

	newLastIdx := len(turns) - 1
	newLastHash := ComputeTurnHash(turns[newLastIdx])

	// Case 1: Brand new binding with no sync history
	if lastHash == "" {
		// Return empty diff if nothing was recorded, or caller can choose backfill limit
		return nil, newLastIdx, newLastHash
	}

	// Case 2: Fast path - index is valid and hash matches exactly
	if lastIndex >= 0 && lastIndex < len(turns) && ComputeTurnHash(turns[lastIndex]) == lastHash {
		if lastIndex+1 < len(turns) {
			return turns[lastIndex+1:], newLastIdx, newLastHash
		}
		return nil, newLastIdx, newLastHash
	}

	// Case 3: Hash scan - compaction or turn additions shifted indices
	foundIdx := -1
	for i := len(turns) - 1; i >= 0; i-- {
		if ComputeTurnHash(turns[i]) == lastHash {
			foundIdx = i
			break
		}
	}

	if foundIdx >= 0 {
		if foundIdx+1 < len(turns) {
			return turns[foundIdx+1:], newLastIdx, newLastHash
		}
		return nil, newLastIdx, newLastHash
	}

	// Case 4: Hash not found (session was truncated/pruned from the end, or completely reset).
	// Do NOT replay historical prefix turns into the channel; simply update the sync markers.
	return nil, newLastIdx, newLastHash
}

// FormatUserBackfillMessage formats an unsynced background user turn with clear blockquotes and attribution.
func FormatUserBackfillMessage(turn *genai.Content) string {
	if turn == nil {
		return ""
	}

	text := strings.TrimSpace(agent.ContentText(turn))
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

// FormatToolTurnSummary formats tool call and response parts for verbose mode.
func FormatToolTurnSummary(turn *genai.Content) string {
	if turn == nil {
		return ""
	}

	var sb strings.Builder
	for _, p := range turn.Parts {
		if p == nil {
			continue
		}
		if p.FunctionCall != nil {
			argsBytes, err := json.MarshalIndent(p.FunctionCall.Args, "", "  ")
			if err != nil {
				argsBytes, _ = json.Marshal(p.FunctionCall.Args)
			}
			argsStr := string(argsBytes)
			if len(argsStr) > 1200 {
				argsStr = argsStr[:1200] + "\n... (truncated)"
			}
			sb.WriteString(fmt.Sprintf("🔧 **Tool Call:** `%s`\n```json\n%s\n```\n", p.FunctionCall.Name, argsStr))
		}
		if p.FunctionResponse != nil {
			if p.FunctionResponse.Response != nil {
				if outStr, ok := p.FunctionResponse.Response["output"].(string); ok && outStr != "" {
					summary := strings.TrimSpace(outStr)
					if len(summary) > 1200 {
						summary = summary[:1200] + "\n... (truncated)"
					}
					sb.WriteString(fmt.Sprintf("⚡ **Tool Output:** `%s`\n```\n%s\n```\n", p.FunctionResponse.Name, summary))
				} else {
					respBytes, err := json.MarshalIndent(p.FunctionResponse.Response, "", "  ")
					if err != nil {
						respBytes, _ = json.Marshal(p.FunctionResponse.Response)
					}
					summary := string(respBytes)
					if len(summary) > 1200 {
						summary = summary[:1200] + "\n... (truncated)"
					}
					sb.WriteString(fmt.Sprintf("⚡ **Tool Response:** `%s`\n```json\n%s\n```\n", p.FunctionResponse.Name, summary))
				}
			} else {
				sb.WriteString(fmt.Sprintf("⚡ **Tool Response:** `%s`\n", p.FunctionResponse.Name))
			}
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
func ExpandScratchpadSentinels(sdk *agent.AgentSDK, agentID string, text string) string {
	if sdk == nil || agentID == "" || (!strings.Contains(text, "<SCRATCHPAD_EXPAND") && !strings.Contains(text, "<scratchpad_expand")) {
		return text
	}

	return scratchpadExpandRegex.ReplaceAllStringFunc(text, func(match string) string {
		idMatch := expandIDRegex.FindStringSubmatch(match)
		if len(idMatch) < 2 {
			return match
		}
		id := idMatch[1]

		content, err := sdk.GetScratchpad(agentID, id, nil, nil)
		if err != nil || content == "" {
			return match
		}

		return content
	})
}
