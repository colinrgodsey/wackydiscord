package bot

import (
	"context"
	"errors"
	"fmt"
	"log"
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

	turnUnlock := b.State.LockChannelTurn(m.ChannelID)
	defer turnUnlock()

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

	if len(m.Attachments) > 0 {
		attCtx, cancel := context.WithTimeout(context.Background(), TotalAttachmentBudget)
		defer cancel()

		attResult, _ := b.ProcessAttachments(attCtx, binding.AgentID, m.Attachments)
		if attResult != nil {
			for _, notice := range attResult.Notices {
				_ = SendAgentMessage(s, m.ChannelID, "System", notice, nil)
			}
			if attResult.PromptText != "" {
				if userText != "" {
					userText = userText + "\n\n" + attResult.PromptText
				} else {
					userText = attResult.PromptText
				}
			} else if userText == "" && attResult.ImagesDownloaded > 0 {
				userText = "[User attached image]"
			}
		}
	}

	if userText == "" {
		return
	}

	// 3. Atomic registration: Under LockChannelSync, perform [AddUserTurn + AddPendingUserHash]
	var res *agent.UserTurnResult
	var addErr error

	func() {
		syncUnlock := b.State.LockChannelSync(m.ChannelID)
		defer syncUnlock()

		bnd := b.State.GetBinding(m.ChannelID)
		if bnd == nil || bnd.AgentID != binding.AgentID {
			return
		}

		bnd.IsGenerating = true
		_ = b.State.SetBinding(bnd)

		res, addErr = b.SDK.AddUserTurn(binding.AgentID, userText)
		if addErr != nil {
			return
		}

		freshBnd := b.State.GetBinding(m.ChannelID)
		if freshBnd != nil && freshBnd.AgentID == binding.AgentID {
			actualHash := ComputeTurnHash(res.Content)
			freshBnd.AddPendingUserHash(actualHash)
			freshBnd.IsGenerating = true
			if freshBnd.LastTurnHash == "" {
				freshBnd.LastTurnIndex = 0
				freshBnd.LastTurnHash = actualHash
			}
			if err := b.State.SetBinding(freshBnd); err != nil {
				log.Printf("⚠️ failed to persist IsGenerating state in channel %s: %v", m.ChannelID, err)
			}
		}
	}()

	if addErr != nil {
		_ = SendAgentMessage(s, m.ChannelID, "System", fmt.Sprintf("❌ **Turn error:** %v", addErr), nil)
		return
	}

	if res == nil {
		return
	}

	for _, w := range res.Warnings {
		log.Printf("⚠️ hook warning for agent %s: %s", binding.AgentID, w)
	}

	defer func() {
		syncUnlock := b.State.LockChannelSync(m.ChannelID)
		defer syncUnlock()

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
	if binding.WebhookID == "" || binding.WebhookToken == "" {
		if newWH, err := EnsureWebhook(s, m.ChannelID); err == nil && newWH != nil {
			func() {
				syncUnlock := b.State.LockChannelSync(m.ChannelID)
				defer syncUnlock()
				if bnd := b.State.GetBinding(m.ChannelID); bnd != nil && bnd.AgentID == binding.AgentID {
					bnd.WebhookID = newWH.ID
					bnd.WebhookToken = newWH.Token
					_ = b.State.SetBinding(bnd)
				}
			}()
		}
	}

	ctx := context.Background()

	var streamErr error
	for _, err := range b.SDK.GenerateTurnStream(ctx, binding.AgentID) {
		if err != nil {
			streamErr = err
			break
		}
		// SessionWatcher is now the primary live renderer. Do not post duplicate live chunks from handler loop.
	}
	close(stopTyping)

	func() {
		syncUnlock := b.State.LockChannelSync(m.ChannelID)
		defer syncUnlock()
		if bnd := b.State.GetBinding(m.ChannelID); bnd != nil && bnd.AgentID == binding.AgentID {
			bnd.IsGenerating = false
			if err := b.State.SetBinding(bnd); err != nil {
				log.Printf("⚠️ failed to reset IsGenerating in channel %s: %v", m.ChannelID, err)
			}
		}
	}()

	// Synchronously call FlushNow to flush any remaining turns without waiting for 400ms trailing timer
	if b.Watcher != nil {
		b.Watcher.FlushNow(binding.AgentID)
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
// It acquires LockChannelSync(channelID) for atomic state reads and commits.
// If limit > 0, backfills at most limit turns (or the last limit turns if fully synced).
// Returns the number of turns backfilled and any persistence error.
func (b *Bot) autoFillUnsyncedTurns(s *discordgo.Session, binding *ChannelBinding, channelID string, limit int) (int, error) {
	syncUnlock := b.State.LockChannelSync(channelID)
	bnd := b.State.GetBinding(channelID)
	if bnd == nil {
		syncUnlock()
		return 0, nil
	}

	turns, err := b.SDK.ReadSession(bnd.AgentID)
	if err != nil {
		syncUnlock()
		return 0, fmt.Errorf("failed to read session turns: %w", err)
	}
	if len(turns) == 0 {
		syncUnlock()
		return 0, nil
	}

	unsynced, newIdx, newHash := DiffUnsyncedTurns(turns, bnd.LastTurnHash, bnd.LastTurnIndex)
	if limit > 0 && len(unsynced) == 0 && len(turns) > 0 {
		if limit > len(turns) {
			limit = len(turns)
		}
		unsynced = turns[len(turns)-limit:]
	} else if limit > 0 && len(unsynced) > limit {
		unsynced = unsynced[len(unsynced)-limit:]
	}

	if len(unsynced) == 0 {
		if bnd.LastTurnHash == "" && newHash != "" {
			bnd.ConsumePendingUserHash(newHash)
			bnd.LastTurnIndex = newIdx
			bnd.LastTurnHash = newHash
			if err := b.State.SetBinding(bnd); err != nil {
				log.Printf("⚠️ failed to persist initial sync markers: %v", err)
			}
		}
		syncUnlock()
		return 0, nil
	}

	type turnItem struct {
		turn   *genai.Content
		isEcho bool
	}
	var toProcess []turnItem
	hasConsumedHash := false

	for _, turn := range unsynced {
		if turn == nil {
			continue
		}
		if turn.Role == "user" {
			turnHash := ComputeTurnHash(turn)
			if bnd.ConsumePendingUserHash(turnHash) {
				hasConsumedHash = true
				toProcess = append(toProcess, turnItem{turn: turn, isEcho: true})
				continue
			}
		}
		toProcess = append(toProcess, turnItem{turn: turn, isEcho: false})
	}

	if hasConsumedHash {
		if err := b.State.SetBinding(bnd); err != nil {
			log.Printf("⚠️ failed to persist consumed pending user hash: %v", err)
		}
	}

	agentID := bnd.AgentID
	verbose := bnd.Verbose
	whID := bnd.WebhookID
	whToken := bnd.WebhookToken

	syncUnlock()

	var wh *discordgo.Webhook
	if whID != "" && whToken != "" {
		wh = &discordgo.Webhook{ID: whID, Token: whToken}
	}

	var toolSummaries []string
	flushToolBatch := func() {
		if len(toolSummaries) == 0 {
			return
		}
		combined := strings.Join(toolSummaries, "\n\n")
		toolSummaries = nil
		chunks := SplitDiscordMessage(combined, MaxDiscordMessageLength)
		for _, chunk := range chunks {
			_ = SendAgentMessage(s, channelID, "Tools", chunk, nil)
		}
	}

	for _, item := range toProcess {
		turn := item.turn
		if item.isEcho {
			flushToolBatch()
			continue
		}

		if IsSyntheticHarnessTurn(turn) {
			flushToolBatch()
			if verbose {
				badge := FormatSyntheticHarnessTurn(turn)
				if badge != "" {
					_ = SendAgentMessage(s, channelID, "System", badge, nil)
				}
			}
			continue
		}

		toolText := FormatToolTurnSummary(turn)
		if toolText != "" && verbose {
			toolSummaries = append(toolSummaries, toolText)
		}

		if turn.Role == "user" {
			text := FormatUserBackfillMessage(turn)
			if text != "" {
				flushToolBatch()
				_ = SendAgentMessage(s, channelID, "User", text, nil)
			}
		} else {
			text := FormatAssistantBackfillMessage(turn)
			if text != "" {
				flushToolBatch()
				text = ExpandScratchpadSentinels(b.SDK, agentID, text)
				_ = SendAgentMessage(s, channelID, agentID, text, wh)
			}
		}
	}
	flushToolBatch()

	syncUnlock = b.State.LockChannelSync(channelID)
	if finalBnd := b.State.GetBinding(channelID); finalBnd != nil && finalBnd.AgentID == agentID {
		finalBnd.LastTurnIndex = newIdx
		finalBnd.LastTurnHash = newHash
		if err := b.State.SetBinding(finalBnd); err != nil {
			syncUnlock()
			return len(unsynced), fmt.Errorf("failed to update binding sync markers: %w", err)
		}
	}
	syncUnlock()

	return len(unsynced), nil
}
