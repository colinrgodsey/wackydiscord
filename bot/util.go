package bot

import "strings"

const (
	// MaxDiscordMessageLength is the maximum character limit for a single Discord message.
	MaxDiscordMessageLength = 2000
)

// SplitDiscordMessage splits long text into chunks that fit within Discord's 2000-character limit.
// It attempts to split at paragraph breaks (\n\n), then line breaks (\n), then spaces,
// and falls back to hard truncation only when a single continuous word exceeds the limit.
func SplitDiscordMessage(content string, maxLen ...int) []string {
	limit := MaxDiscordMessageLength
	if len(maxLen) > 0 && maxLen[0] > 0 {
		limit = maxLen[0]
	}

	if len(content) <= limit {
		if strings.TrimSpace(content) == "" {
			return nil
		}
		return []string{content}
	}

	var chunks []string
	remaining := content

	for len(remaining) > 0 {
		remaining = strings.TrimLeft(remaining, "\r\n")
		if len(remaining) <= limit {
			if trimmed := strings.TrimSpace(remaining); trimmed != "" {
				chunks = append(chunks, trimmed)
			}
			break
		}

		sub := remaining[:limit]
		splitIdx := -1
		advance := -1

		// 1. Try splitting at paragraph break
		if idx := strings.LastIndex(sub, "\n\n"); idx > 0 {
			splitIdx = idx
			advance = idx + 2
		} else if idx := strings.LastIndex(sub, "\n"); idx > 0 {
			// 2. Try splitting at single newline
			splitIdx = idx
			advance = idx + 1
		} else if idx := strings.LastIndex(sub, " "); idx > 0 {
			// 3. Try splitting at space
			splitIdx = idx
			advance = idx + 1
		} else {
			// 4. Hard split at limit
			splitIdx = limit
			advance = limit
		}

		chunk := strings.TrimSpace(remaining[:splitIdx])
		if chunk != "" {
			chunks = append(chunks, chunk)
		}
		remaining = remaining[advance:]
	}

	return chunks
}
