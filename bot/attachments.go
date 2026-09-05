package bot

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"path/filepath"
	"regexp"
	"strings"
	"unicode"

	"github.com/bwmarrin/discordgo"
	"github.com/colinrgodsey/wackypub/pkg/agent"
)

// inlineTextExtensions defines the allowlist of file extensions permitted for inline text display.
var inlineTextExtensions = map[string]bool{
	".txt":   true,
	".md":    true,
	".py":    true,
	".go":    true,
	".json":  true,
	".yaml":  true,
	".yml":   true,
	".toml":  true,
	".csv":   true,
	".log":   true,
	".diff":  true,
	".patch": true,
	".ini":   true,
	".sh":    true,
	".sql":   true,
	".html":  true,
	".css":   true,
	".xml":   true,
}

var createdByRegex = regexp.MustCompile(`^[a-z0-9_-]{1,32}$`)

// SanitizeFilename strips control characters, path components, and limits the filename to 80 characters.
func SanitizeFilename(name string) string {
	name = filepath.Base(name)
	var b strings.Builder
	for _, r := range name {
		if !unicode.IsControl(r) {
			b.WriteRune(r)
		}
	}
	s := strings.TrimSpace(b.String())
	if s == "" || s == "." {
		s = "attachment"
	}
	if len(s) > 80 {
		s = s[:80]
	}
	return s
}

// SanitizeCreatedBy validates and sanitizes a createdBy string to conform to ^[a-z0-9_-]{1,32}$.
func SanitizeCreatedBy(createdBy string) string {
	createdBy = strings.ToLower(strings.TrimSpace(createdBy))
	var b strings.Builder
	for _, r := range createdBy {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_' || r == '-' {
			b.WriteRune(r)
		}
	}
	s := b.String()
	if len(s) > 32 {
		s = s[:32]
	}
	if s == "" || !createdByRegex.MatchString(s) {
		return "discord_att"
	}
	return s
}

// IsInlineText applies the multi-layer classifier to determine whether a file can be inlined as text:
// 1. Extension allowlist
// 2. Size <= MaxInlineTextBytes (100KB)
// 3. No NUL bytes (bytes.ContainsRune(data, 0))
// 4. Magic-byte veto: first 512 bytes must detect as text/* MIME
func IsInlineText(filename string, data []byte) bool {
	ext := strings.ToLower(filepath.Ext(filename))
	if !inlineTextExtensions[ext] {
		return false
	}
	if len(data) > MaxInlineTextBytes {
		return false
	}
	if bytes.ContainsRune(data, 0) {
		return false
	}
	prefix := data
	if len(prefix) > 512 {
		prefix = prefix[:512]
	}
	mime := http.DetectContentType(prefix)
	if !strings.HasPrefix(mime, "text/") {
		return false
	}
	return true
}

// fenceLang maps a file extension to a markdown code fence language identifier.
func fenceLang(ext string) string {
	ext = strings.ToLower(strings.TrimPrefix(ext, "."))
	switch ext {
	case "yml":
		return "yaml"
	default:
		return ext
	}
}

// dynamicFence returns a sequence of backticks strictly longer than any sequence of backticks in content.
// Minimum fence length is 3 backticks.
func dynamicFence(content string) string {
	maxConsecutive := 0
	curr := 0
	for i := 0; i < len(content); i++ {
		if content[i] == '`' {
			curr++
			if curr > maxConsecutive {
				maxConsecutive = curr
			}
		} else {
			curr = 0
		}
	}
	fenceLen := 3
	if maxConsecutive >= fenceLen {
		fenceLen = maxConsecutive + 1
	}
	return strings.Repeat("`", fenceLen)
}

// FormatInlinedText formats an inlined text attachment with dynamic markdown fences.
func FormatInlinedText(filename string, content string) string {
	sanitizedName := SanitizeFilename(filename)
	ext := filepath.Ext(sanitizedName)
	lang := fenceLang(ext)
	fence := dynamicFence(content)
	if !strings.HasSuffix(content, "\n") {
		content += "\n"
	}
	return fmt.Sprintf("[Attached file: %s]\n%s%s\n%s%s", sanitizedName, fence, lang, content, fence)
}

// isImageAttachment checks if an attachment is an image based on ContentType or extension.
func isImageAttachment(att *discordgo.MessageAttachment) bool {
	if att == nil {
		return false
	}
	if strings.HasPrefix(att.ContentType, "image/") {
		return true
	}
	ext := strings.ToLower(filepath.Ext(att.Filename))
	switch ext {
	case ".png", ".jpg", ".jpeg", ".webp", ".gif":
		return true
	}
	return false
}

// ProcessAttachmentsResult contains the outputs of processing message attachments.
type ProcessAttachmentsResult struct {
	PromptText       string
	Notices          []string
	ImagesDownloaded int
}

// ProcessAttachments handles up to MaxAttachmentsPerMessage attachments with timeout and budget bounds.
// Text files (<= 100KB) are inlined with dynamic fences.
// Binary or large files (<= 20MB) are stored in agent scratchpad and referenced in turn text.
// Files exceeding 20MB are rejected with a notice.
// Remainder attachments past the cap of 5 are reported in a user-visible notice.
func (b *Bot) ProcessAttachments(ctx context.Context, agentID string, attachments []*discordgo.MessageAttachment) (*ProcessAttachmentsResult, error) {
	result := &ProcessAttachmentsResult{}
	if len(attachments) == 0 {
		return result, nil
	}

	totalAtts := len(attachments)
	processCount := totalAtts
	if processCount > MaxAttachmentsPerMessage {
		processCount = MaxAttachmentsPerMessage
	}

	var sections []string

	budgetCtx := ctx
	if budgetCtx == nil {
		budgetCtx = context.Background()
	}
	if _, hasDeadline := budgetCtx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		budgetCtx, cancel = context.WithTimeout(budgetCtx, TotalAttachmentBudget)
		defer cancel()
	}

	for i := 0; i < processCount; i++ {
		att := attachments[i]
		if att == nil || att.URL == "" {
			continue
		}

		sanitizedName := SanitizeFilename(att.Filename)
		data, err := downloadAttachment(budgetCtx, att.URL, MaxAttachmentBytes)
		if err != nil {
			if errors.Is(err, ErrAttachmentTooLarge) {
				notice := fmt.Sprintf("⚠️ Attachment %q exceeds 20MB limit and was rejected.", sanitizedName)
				result.Notices = append(result.Notices, notice)
				sections = append(sections, fmt.Sprintf("[Attached file: %s skipped: exceeds maximum allowed size of 20MB]", sanitizedName))
			} else {
				notice := fmt.Sprintf("⚠️ Failed to download attachment %q: %v", sanitizedName, err)
				result.Notices = append(result.Notices, notice)
				sections = append(sections, fmt.Sprintf("[Attached file: %s skipped: download error: %v]", sanitizedName, err))
			}
			continue
		}

		if isImageAttachment(att) {
			if len(data) > 0 {
				if _, addErr := b.SDK.AddMedia(agentID, bytes.NewReader(data)); addErr == nil {
					result.ImagesDownloaded++
				} else {
					log.Printf("⚠️ AddMedia failed for attachment %s: %v", sanitizedName, addErr)
				}
			}
			continue
		}

		if IsInlineText(att.Filename, data) {
			inlined := FormatInlinedText(att.Filename, string(data))
			sections = append(sections, inlined)
		} else {
			// Binary or large file (>100KB) -> Scratchpad
			agentDir := b.SDK.AgentDir(agentID)
			createdBy := SanitizeCreatedBy("discord_att")
			mimeType := att.ContentType
			if mimeType == "" {
				prefix := data
				if len(prefix) > 512 {
					prefix = prefix[:512]
				}
				mimeType = http.DetectContentType(prefix)
			}
			entry, spErr := agent.CreateBinaryScratchpad(agentDir, data, createdBy, mimeType)
			if spErr != nil {
				log.Printf("⚠️ CreateBinaryScratchpad failed for %s: %v", sanitizedName, spErr)
				sections = append(sections, fmt.Sprintf("[Attached file: %s skipped: failed to save to scratchpad: %v]", sanitizedName, spErr))
			} else {
				ref := fmt.Sprintf("[Attached file: %s (saved to scratchpad entry '%s', %d bytes). Use scratchpad tools to inspect.]",
					sanitizedName, entry.ID, len(data))
				sections = append(sections, ref)
			}
		}
	}

	if totalAtts > MaxAttachmentsPerMessage {
		var skippedNames []string
		for _, att := range attachments[MaxAttachmentsPerMessage:] {
			if att != nil {
				skippedNames = append(skippedNames, SanitizeFilename(att.Filename))
			}
		}
		remCount := len(skippedNames)
		remMsg := fmt.Sprintf("Attachment cap (%d) reached; %d attachment(s) skipped: %s",
			MaxAttachmentsPerMessage, remCount, strings.Join(skippedNames, ", "))
		result.Notices = append(result.Notices, "⚠️ "+remMsg)
		sections = append(sections, fmt.Sprintf("[Notice: %s]", remMsg))
	}

	if len(sections) > 0 {
		result.PromptText = strings.Join(sections, "\n\n")
	}

	return result, nil
}
