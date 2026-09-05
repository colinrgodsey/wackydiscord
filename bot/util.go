package bot

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

const (
	// MaxDiscordMessageLength is the maximum character limit for a single Discord message.
	MaxDiscordMessageLength = 2000

	// MaxAttachmentBytes is the hard cap for attachment downloads (20MB).
	MaxAttachmentBytes = 20 * 1024 * 1024

	// MaxInlineTextBytes is the maximum byte size for inlining a text attachment into prompt text (100KB).
	MaxInlineTextBytes = 100 * 1024

	// MaxAttachmentsPerMessage is the maximum number of attachments processed per message turn.
	MaxAttachmentsPerMessage = 5

	// AttachmentDownloadTimeout is the per-file timeout for downloading an attachment.
	AttachmentDownloadTimeout = 10 * time.Second

	// TotalAttachmentBudget is the maximum cumulative time allowed for downloading all attachments in a message.
	TotalAttachmentBudget = 30 * time.Second
)

var (
	// ErrAttachmentTooLarge is returned when an attachment exceeds the 20MB limit.
	ErrAttachmentTooLarge = errors.New("attachment exceeds maximum allowed size of 20MB")
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

// downloadAttachment downloads an attachment from url using an io.LimitReader capped at maxBytes+1.
// If maxBytes <= 0, defaults to MaxAttachmentBytes (20MB).
// Returns ErrAttachmentTooLarge if the downloaded content exceeds maxBytes.
// Enforces a 10-second per-file timeout, bounded by any deadline on ctx.
func downloadAttachment(ctx context.Context, url string, maxBytes ...int64) ([]byte, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	limit := int64(MaxAttachmentBytes)
	if len(maxBytes) > 0 && maxBytes[0] > 0 {
		limit = maxBytes[0]
	}

	fileCtx, cancel := context.WithTimeout(ctx, AttachmentDownloadTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(fileCtx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("bad status downloading attachment: %s", resp.Status)
	}

	if resp.ContentLength > limit {
		return nil, ErrAttachmentTooLarge
	}

	lr := io.LimitReader(resp.Body, limit+1)
	data, err := io.ReadAll(lr)
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > limit {
		return nil, ErrAttachmentTooLarge
	}
	return data, nil
}
