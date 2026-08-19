package bot

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/colinrgodsey/wackypub/pkg/agent"
	"google.golang.org/genai"
)

// HandleMessageCreate processes regular messages sent to bound Discord channels.
func (b *Bot) HandleMessageCreate(s *discordgo.Session, m *discordgo.MessageCreate) {
	// Ignore bot messages to prevent infinite reply loops
	if m.Author == nil || m.Author.Bot {
		return
	}

	binding := b.State.GetBinding(m.ChannelID)
	if binding == nil {
		return
	}

	// Verify bound agent exists and has valid configuration
	insp, err := b.SDK.InspectAgent(binding.AgentID)
	if err != nil || insp == nil || !insp.AgentDirExists {
		_ = SendAgentMessage(s, m.ChannelID, "System", fmt.Sprintf("❌ **Binding Error:** Bound agent %q does not exist in workspace `%s`. Use `/bind <agent_id>` to connect a valid agent.", binding.AgentID, b.WsDir), nil)
		return
	}
	if insp.RuntimeJSONExists && !insp.RuntimeJSONValid {
		_ = SendAgentMessage(s, m.ChannelID, "System", fmt.Sprintf("⚠️ **Agent Configuration Error:** Agent %q has an invalid `runtime.json`: %s", binding.AgentID, insp.RuntimeJSONError), nil)
		return
	}

	// 1. Auto-fill check: replay any background session turns that occurred since last sync
	b.autoFillUnsyncedTurns(s, binding, m.ChannelID)

	// 2. Prepare user message text & attachments
	userText := strings.TrimSpace(m.Content)
	if userText == "" && len(m.Attachments) == 0 {
		return
	}

	// If image attachments exist, process them
	if len(m.Attachments) > 0 {
		for _, att := range m.Attachments {
			if att == nil || att.URL == "" {
				continue
			}
			// If image, download and append as media turn
			if strings.HasPrefix(att.ContentType, "image/") || strings.HasSuffix(strings.ToLower(att.Filename), ".png") || strings.HasSuffix(strings.ToLower(att.Filename), ".jpg") || strings.HasSuffix(strings.ToLower(att.Filename), ".jpeg") {
				if imgBytes, err := downloadAttachment(att.URL); err == nil && len(imgBytes) > 0 {
					_, _ = b.SDK.AddMedia(binding.AgentID, bytes.NewReader(imgBytes))
				}
			}
		}
	}

	if userText == "" {
		userText = "[User attached image]"
	}

	// 3. Mark channel as actively generating to prevent file watcher race & echoes
	binding.PendingUserText = userText
	userTurnContent := genai.NewContentFromText(userText, "user")
	binding.PendingUserHash = ComputeTurnHash(userTurnContent)
	binding.IsGenerating = true
	_ = b.State.SetBinding(binding)

	defer func() {
		bnd := b.State.GetBinding(m.ChannelID)
		if bnd != nil && bnd.IsGenerating {
			bnd.IsGenerating = false
			_ = b.State.SetBinding(bnd)
		}
	}()

	stopTyping := make(chan struct{})
	go func() {
		// Send initial typing indicator
		_ = s.ChannelTyping(m.ChannelID)
		ticker := time.NewTicker(6 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				_ = s.ChannelTyping(m.ChannelID)
			case <-stopTyping:
				return
			}
		}
	}()

	// 4. Generate agent turn under session lock
	ctx := context.Background()
	initialTurns, _ := b.SDK.ReadSession(binding.AgentID)
	startIdx := len(initialTurns)

	respText, err := b.SDK.AddAndGenerateTurn(ctx, binding.AgentID, userText)
	close(stopTyping)

	// 5. Read newly generated turns, emit verbose tool logs if enabled, and update sync marker
	allTurns, _ := b.SDK.ReadSession(binding.AgentID)
	if len(allTurns) > 0 {
		if binding.Verbose && len(allTurns) > startIdx+1 {
			// allTurns[startIdx] is the newly appended user message.
			// allTurns[startIdx+1 : len(allTurns)-1] are intermediate tool calls and responses.
			for _, turn := range allTurns[startIdx+1:] {
				toolText := FormatToolTurnSummary(turn)
				if toolText != "" {
					_ = SendAgentMessage(s, m.ChannelID, "Tools", toolText, nil)
				}
			}
		}

		binding.LastTurnIndex = len(allTurns) - 1
		binding.LastTurnHash = ComputeTurnHash(allTurns[binding.LastTurnIndex])
		binding.PendingUserHash = ""
		binding.PendingUserText = ""
		binding.IsGenerating = false
		_ = b.State.SetBinding(binding)
	}

	// 6. Send assistant response
	var wh *discordgo.Webhook
	if binding.WebhookID != "" && binding.WebhookToken != "" {
		wh = &discordgo.Webhook{ID: binding.WebhookID, Token: binding.WebhookToken}
	} else if newWH, err := EnsureWebhook(s, m.ChannelID); err == nil && newWH != nil {
		wh = newWH
		binding.WebhookID = newWH.ID
		binding.WebhookToken = newWH.Token
		_ = b.State.SetBinding(binding)
	}

	if err != nil {
		_ = SendAgentMessage(s, m.ChannelID, "System", fmt.Sprintf("❌ **Agent error:** %v", err), nil)
		return
	}

	if strings.TrimSpace(respText) != "" {
		_ = SendAgentMessage(s, m.ChannelID, binding.AgentID, respText, wh)
	}
}

func (b *Bot) autoFillUnsyncedTurns(s *discordgo.Session, binding *ChannelBinding, channelID string) {
	if binding == nil || binding.IsGenerating {
		return
	}

	turns, err := b.SDK.ReadSession(binding.AgentID)
	if err != nil || len(turns) == 0 {
		return
	}

	unsynced, newIdx, newHash := DiffUnsyncedTurns(turns, binding.LastTurnHash, binding.LastTurnIndex)
	if len(unsynced) == 0 {
		return
	}

	var wh *discordgo.Webhook
	if binding.WebhookID != "" && binding.WebhookToken != "" {
		wh = &discordgo.Webhook{ID: binding.WebhookID, Token: binding.WebhookToken}
	}

	for _, turn := range unsynced {
		if turn.Role == "user" {
			turnHash := ComputeTurnHash(turn)
			turnText := strings.TrimSpace(agent.ContentText(turn))
			if (binding.PendingUserHash != "" && turnHash == binding.PendingUserHash) || (binding.PendingUserText != "" && turnText == strings.TrimSpace(binding.PendingUserText)) {
				// This user turn was originated directly in this Discord channel! Do not echo it back.
				binding.PendingUserHash = ""
				binding.PendingUserText = ""
				continue
			}

			if binding.Verbose {
				toolText := FormatToolTurnSummary(turn)
				if toolText != "" {
					_ = SendAgentMessage(s, channelID, "Tools", toolText, nil)
				}
			}

			text := FormatUserBackfillMessage(turn)
			if text != "" {
				_ = SendAgentMessage(s, channelID, "User", text, nil)
			}
		} else {
			if binding.Verbose {
				toolText := FormatToolTurnSummary(turn)
				if toolText != "" {
					_ = SendAgentMessage(s, channelID, "Tools", toolText, nil)
				}
			}

			text := FormatAssistantBackfillMessage(turn)
			if text != "" {
				_ = SendAgentMessage(s, channelID, binding.AgentID, text, wh)
			}
		}
	}

	binding.LastTurnIndex = newIdx
	binding.LastTurnHash = newHash
	_ = b.State.SetBinding(binding)
}

func downloadAttachment(url string) ([]byte, error) {
	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("bad status downloading attachment: %s", resp.Status)
	}

	return io.ReadAll(resp.Body)
}
