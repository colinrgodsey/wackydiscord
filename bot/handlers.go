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
	agentv1 "github.com/colinrgodsey/wackypub/pkg/agent/v1"
	"google.golang.org/genai"
)

// HandleMessageCreate processes regular messages sent to bound Discord channels.
func (b *Bot) HandleMessageCreate(s *discordgo.Session, m *discordgo.MessageCreate) {
	// Ignore bot messages and webhook messages to prevent infinite reply loops
	if m.Author == nil || m.Author.Bot || m.WebhookID != "" {
		return
	}

	if !b.State.IsAllowedUser(m.Author.ID) {
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
	insp, err := b.SDK.InspectAgent(context.Background(), &agentv1.InspectAgentRequest{AgentId: binding.AgentID})
	if err != nil || insp == nil || !insp.GetAgentDirExists() {
		_ = SendAgentMessage(s, m.ChannelID, "System", fmt.Sprintf("❌ **Binding Error:** Bound agent %q does not exist in workspace `%s`. Use `/bind <agent_id>` to connect a valid agent.", binding.AgentID, b.WsDir), nil)
		return
	}
	if insp.GetRuntimeJsonExists() && !insp.GetRuntimeJsonValid() {
		_ = SendAgentMessage(s, m.ChannelID, "System", fmt.Sprintf("⚠️ **Agent Configuration Error:** Agent %q has an invalid `runtime.json`: %s", binding.AgentID, insp.GetRuntimeJsonError()), nil)
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

	// 3. Mark channel as actively generating under LockChannelSync
	expectedHash := ComputeTurnHash(genai.NewContentFromText(userText, "user"))
	var shouldProceed bool
	func() {
		syncUnlock := b.State.LockChannelSync(m.ChannelID)
		defer syncUnlock()

		bnd := b.State.GetBinding(m.ChannelID)
		if bnd == nil || bnd.AgentID != binding.AgentID {
			return
		}

		bnd.IsGenerating = true
		bnd.AddPendingUserHash(expectedHash)
		if err := b.State.SetBinding(bnd); err != nil {
			log.Printf("⚠️ failed to persist IsGenerating state in channel %s: %v", m.ChannelID, err)
		}
		shouldProceed = true
	}()

	if !shouldProceed {
		return
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

	// Perform AddUserTurn WITHOUT holding LockChannelSync. Holding LockChannelSync across
	// AddUserTurn deadlocks with callers that hold agent session locks and rebind channels.
	res, addErr := b.SDK.AddUserTurn(context.Background(), &agentv1.AddUserTurnRequest{
		AgentId: binding.AgentID,
		Message: userText,
	})
	if addErr != nil {
		_ = SendAgentMessage(s, m.ChannelID, "System", fmt.Sprintf("❌ **Turn error:** %v", addErr), nil)
		return
	}

	if res == nil {
		return
	}

	func() {
		syncUnlock := b.State.LockChannelSync(m.ChannelID)
		defer syncUnlock()

		freshBnd := b.State.GetBinding(m.ChannelID)
		if freshBnd != nil && freshBnd.AgentID == binding.AgentID {
			actualHash := ComputeTurnHash(SessionTurnToContent(res.GetTurn()))
			if actualHash != expectedHash {
				freshBnd.AddPendingUserHash(actualHash)
			}
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

	for _, w := range res.GetWarnings() {
		log.Printf("⚠️ hook warning for agent %s: %s", binding.AgentID, w)
	}

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

	stream := agent.NewInProcessStream[agentv1.GenerateTurnStreamResponse](ctx, 16)
	errCh := make(chan error, 1)
	go func() {
		defer stream.Close()
		errCh <- b.SDK.GenerateTurnStream(&agentv1.GenerateTurnStreamRequest{AgentId: binding.AgentID}, stream)
	}()

	for range stream.Chunks() {
		// SessionWatcher is now the primary live renderer. Do not post duplicate live chunks from handler loop.
	}
	streamErr := <-errCh
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

	if _, err := b.SDK.ReadSession(context.Background(), &agentv1.ReadSessionRequest{AgentId: bnd.AgentID}); err != nil {
		syncUnlock()
		return 0, fmt.Errorf("failed to read session turns: %w", err)
	}

	turns, err := agent.ReadSessionTurns(b.SDK.AgentDir(bnd.AgentID))
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

	// Echo-bug guard (claude diagnosis, 2026-09-17): a leading-edge sync can observe a
	// user-only tail before the assistant response is written - the fsnotify event for the
	// user turn fires while the handler is still generating. Rendering that tail as a
	// standalone [User Turn] backfill echoes the user's own message back into the channel
	// (the reported screenshot: the user text verbatim under a date-hook preamble). Defer
	// the tail while IsGenerating is set; once a paired assistant turn exists (or generation
	// clears), the next sync re-diffs it. When IsGenerating has cleared and no assistant
	// arrived, the tail is a genuine orphan and renders per existing backfill semantics.
	deferredTail := false
	if bnd.IsGenerating && len(unsynced) > 0 {
		if last := unsynced[len(unsynced)-1]; last.Role == "user" && !IsSyntheticHarnessTurn(last) && strings.TrimSpace(agent.ContentText(last)) != "" {
			// Only a text-bearing user turn can echo back as a [User Turn] message. Tool-response
			// turns share the user role but carry FunctionResponse parts with empty ContentText;
			// deferring those would stall the tool-cycle watermark mid-generation.
			deferredTail = true
			unsynced = unsynced[:len(unsynced)-1]
			// Do not advance the watermark past the deferred user turn; the next sync must
			// re-see it (paired with its assistant, or orphaned after generation clears).
			if newIdx-1 >= 0 {
				newIdx = newIdx - 1
				newHash = ComputeTurnHash(turns[newIdx])
			} else {
				newIdx = -1
				newHash = ""
			}
		}
	}
	if deferredTail && len(unsynced) == 0 {
		// Only the deferred tail was unsynced; nothing to render this pass and the
		// watermark stays put so the next sync can pair or orphan it.
		syncUnlock()
		return 0, nil
	}

	type turnItem struct {
		turn   *genai.Content
		isEcho bool
	}
	var toProcess []turnItem

	for _, turn := range unsynced {
		if turn == nil {
			continue
		}
		if turn.Role == "user" {
			turnHash := ComputeTurnHash(turn)
			if bnd.ConsumePendingUserHash(turnHash) {
				toProcess = append(toProcess, turnItem{turn: turn, isEcho: true})
				continue
			}
		}
		toProcess = append(toProcess, turnItem{turn: turn, isEcho: false})
	}

	bnd.LastTurnIndex = newIdx
	bnd.LastTurnHash = newHash
	if err := b.State.SetBinding(bnd); err != nil {
		syncUnlock()
		return 0, fmt.Errorf("failed to update binding sync markers: %w", err)
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

	return len(unsynced), nil
}
