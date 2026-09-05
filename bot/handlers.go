package bot

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/colinrgodsey/wackypub/pkg/agent"
	"google.golang.org/genai"
)

// HandleMessageCreate processes regular messages sent to bound Discord channels.
func (b *Bot) HandleMessageCreate(s *discordgo.Session, m *discordgo.MessageCreate) {
	// Ignore bot messages and webhook messages to prevent infinite reply loops
	if m.Author == nil || m.Author.Bot || m.WebhookID != "" {
		return
	}

	// Pre-lock existence check: fast path to skip unbound channels without allocating a channel mutex
	if b.State.GetBinding(m.ChannelID) == nil {
		return
	}

	unlock := b.State.LockChannel(m.ChannelID)
	defer unlock()

	// Post-lock fresh read: guarantees up-to-date binding snapshot under lock and guards unbinding race
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
	if _, err := b.autoFillUnsyncedTurns(s, binding, m.ChannelID, 0); err != nil {
		log.Printf("⚠️ auto-fill unsynced turns error in channel %s: %v", m.ChannelID, err)
	}

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
	if err := b.State.SetBinding(binding); err != nil {
		log.Printf("⚠️ failed to persist IsGenerating state in channel %s: %v", m.ChannelID, err)
	}

	defer func() {
		bnd := b.State.GetBinding(m.ChannelID)
		if bnd != nil && bnd.IsGenerating {
			if bnd.AgentID != binding.AgentID {
				log.Printf("⚠️ channel %s was rebound to agent %q during generation for %q; skipping deferred IsGenerating reset", m.ChannelID, bnd.AgentID, binding.AgentID)
				return
			}
			bnd.IsGenerating = false
			if err := b.State.SetBinding(bnd); err != nil {
				log.Printf("⚠️ failed to reset IsGenerating in channel %s: %v", m.ChannelID, err)
			}
		}
	}()

	stopTyping := make(chan struct{})
	if s != nil {
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
	}

	// 4. Ensure Webhook is available for persona delivery
	var wh *discordgo.Webhook
	if binding.WebhookID != "" && binding.WebhookToken != "" {
		wh = &discordgo.Webhook{ID: binding.WebhookID, Token: binding.WebhookToken}
	} else if newWH, err := EnsureWebhook(s, m.ChannelID); err == nil && newWH != nil {
		wh = newWH
		binding.WebhookID = newWH.ID
		binding.WebhookToken = newWH.Token
		if err := b.State.SetBinding(binding); err != nil {
			log.Printf("⚠️ failed to persist webhook for channel %s: %v", m.ChannelID, err)
		}
	}

	ctx := context.Background()

	var streamErr error
	for chunk, err := range b.SDK.AddAndGenerateTurnStream(ctx, binding.AgentID, userText, func(w string) {
		log.Printf("⚠️ hook warning for agent %s: %s", binding.AgentID, w)
	}) {
		if err != nil {
			streamErr = err
			break
		}
		if strings.TrimSpace(chunk) != "" {
			expandedText := ExpandScratchpadSentinels(b.SDK, binding.AgentID, chunk)
			_ = SendAgentMessage(s, m.ChannelID, binding.AgentID, expandedText, wh)
		}
	}
	close(stopTyping)

	// 5. Read newly generated turns, emit verbose tool logs if enabled, and update sync marker
	allTurns, _ := b.SDK.ReadSession(binding.AgentID)
	if len(allTurns) > 0 {
		if binding.Verbose {
			unsynced, _, _ := DiffUnsyncedTurns(allTurns, binding.LastTurnHash, binding.LastTurnIndex)
			for _, turn := range unsynced {
				toolText := FormatToolTurnSummary(turn)
				if toolText != "" {
					_ = SendAgentMessage(s, m.ChannelID, "Tools", toolText, nil)
				}
			}
		}

		if bnd := b.State.GetBinding(m.ChannelID); bnd != nil {
			if bnd.AgentID == binding.AgentID {
				bnd.LastTurnIndex = len(allTurns) - 1
				bnd.LastTurnHash = ComputeTurnHash(allTurns[bnd.LastTurnIndex])
				bnd.PendingUserHash = ""
				bnd.PendingUserText = ""
			} else {
				log.Printf("⚠️ channel %s was rebound to agent %q during generation for %q; skipping sync-marker update", m.ChannelID, bnd.AgentID, binding.AgentID)
			}
			bnd.IsGenerating = false
			if err := b.State.SetBinding(bnd); err != nil {
				log.Printf("⚠️ failed to update binding after generation in channel %s: %v", m.ChannelID, err)
			}
		}
	}

	if streamErr != nil {
		if errors.Is(streamErr, context.Canceled) || strings.Contains(streamErr.Error(), "context canceled") {
			_ = SendAgentMessage(s, m.ChannelID, "System", "⏹️ Turn stopped.", nil)
			return
		}
		_ = SendAgentMessage(s, m.ChannelID, "System", fmt.Sprintf("❌ **Agent error:** %v", streamErr), nil)
		return
	}
}

// autoFillUnsyncedTurns replays background session turns not yet seen in Discord.
// Caller MUST already hold the per-channel lock (via State.LockChannel) to avoid concurrency races (D76).
// If limit > 0, backfills at most limit turns (or the last limit turns if fully synced).
// Returns the number of turns backfilled and any persistence error.
func (b *Bot) autoFillUnsyncedTurns(s *discordgo.Session, binding *ChannelBinding, channelID string, limit int) (int, error) {
	if binding == nil || binding.IsGenerating {
		return 0, nil
	}

	turns, err := b.SDK.ReadSession(binding.AgentID)
	if err != nil {
		return 0, fmt.Errorf("failed to read session turns: %w", err)
	}
	if len(turns) == 0 {
		return 0, nil
	}

	unsynced, newIdx, newHash := DiffUnsyncedTurns(turns, binding.LastTurnHash, binding.LastTurnIndex)
	if limit > 0 && len(unsynced) == 0 && len(turns) > 0 {
		if limit > len(turns) {
			limit = len(turns)
		}
		unsynced = turns[len(turns)-limit:]
	} else if limit > 0 && len(unsynced) > limit {
		unsynced = unsynced[len(unsynced)-limit:]
	}

	if len(unsynced) == 0 {
		return 0, nil
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
				text = ExpandScratchpadSentinels(b.SDK, binding.AgentID, text)
				_ = SendAgentMessage(s, channelID, binding.AgentID, text, wh)
			}
		}
	}

	binding.LastTurnIndex = newIdx
	binding.LastTurnHash = newHash
	if err := b.State.SetBinding(binding); err != nil {
		return len(unsynced), fmt.Errorf("failed to update binding sync markers: %w", err)
	}
	return len(unsynced), nil
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
