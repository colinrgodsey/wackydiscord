package bot

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/bwmarrin/discordgo"
	"github.com/colinrgodsey/wackypub/pkg/agent"
	agentv1 "github.com/colinrgodsey/wackypub/pkg/agent/v1"
	"google.golang.org/genai"
)

// SlashCommands defines the Discord application commands registered by wackydiscord.
// errGeneratingBusy is the common rejection shown when a command that would race a
// live generation turn arrives while the agent is already generating.
const errGeneratingBusy = "⚠️ Agent is currently generating; please wait for generation to complete before running `%s`."

var SlashCommands = []*discordgo.ApplicationCommand{
	{
		Name:        "bind",
		Description: "Bind this Discord channel to a WackyPub agent in the workspace",
		Options: []*discordgo.ApplicationCommandOption{
			{
				Type:        discordgo.ApplicationCommandOptionString,
				Name:        "agent",
				Description: "The agent ID to bind to this channel",
				Required:    true,
			},
		},
	},
	{
		Name:        "unbind",
		Description: "Unbind this Discord channel from its current WackyPub agent",
	},
	{
		Name:        "status",
		Description: "Show the bound agent's status, turn count, and configuration",
	},
	{
		Name:        "fill",
		Description: "Backfill unposted background session turns for the bound agent",
		Options: []*discordgo.ApplicationCommandOption{
			{
				Type:        discordgo.ApplicationCommandOptionInteger,
				Name:        "limit",
				Description: "Maximum number of past turns to backfill (default: all unsynced)",
				Required:    false,
			},
		},
	},
	{
		Name:        "verbose",
		Description: "Toggle verbose tool execution output for this channel",
		Options: []*discordgo.ApplicationCommandOption{
			{
				Type:        discordgo.ApplicationCommandOptionBoolean,
				Name:        "enabled",
				Description: "Enable or disable verbose tool display",
				Required:    false,
			},
		},
	},
	{
		Name:        "agents",
		Description: "List all agents discovered in the WackyPub workspace",
	},
	{
		Name:        "stop",
		Description: "Cancel the in-flight turn of the bound agent",
	},
	{
		Name:        "compact",
		Description: "Archive the bound agent's oldest session turns into MEMORY.md",
		Options: []*discordgo.ApplicationCommandOption{
			{
				Type:        discordgo.ApplicationCommandOptionBoolean,
				Name:        "force",
				Description: "Archive turns even when the session is still under the compaction threshold",
				Required:    false,
			},
		},
	},
	{
		Name:        "claim",
		Description: "Claim ownership of this bot instance",
	},
	{
		Name:        "unclaim",
		Description: "Release ownership of this bot instance",
	},
}

// interactionUserID extracts the caller's Discord user ID from an interaction, handling both guild Member and DM User.
func interactionUserID(i *discordgo.InteractionCreate) string {
	if i == nil {
		return ""
	}
	if i.Member != nil && i.Member.User != nil {
		return i.Member.User.ID
	}
	if i.User != nil {
		return i.User.ID
	}
	return ""
}

// RegisterSlashCommands registers all application commands globally or for a specific guild.
func (b *Bot) RegisterSlashCommands(guildID string) error {
	for _, cmd := range SlashCommands {
		_, err := b.Session.ApplicationCommandCreate(b.AppID, guildID, cmd)
		if err != nil {
			return fmt.Errorf("failed to register slash command %q: %w", cmd.Name, err)
		}
	}
	return nil
}

// HandleInteraction processes slash command interactions.
func (b *Bot) HandleInteraction(s *discordgo.Session, i *discordgo.InteractionCreate) {
	if i.Type != discordgo.InteractionApplicationCommand {
		return
	}

	data := i.ApplicationCommandData()
	uid := interactionUserID(i)
	if data.Name != "claim" && !b.State.IsAllowedUser(uid) {
		msg := "❌ This bot is claimed by another user."
		if b.State.ClaimedUser() == "" {
			msg = "❌ This bot is unclaimed — run `/claim` to take ownership."
		}
		b.respondInteraction(s, i, msg, true)
		return
	}

	switch data.Name {
	case "claim":
		b.handleClaimCommand(s, i)
	case "unclaim":
		b.handleUnclaimCommand(s, i)
	case "bind":
		b.handleBindCommand(s, i)
	case "unbind":
		b.handleUnbindCommand(s, i)
	case "status":
		b.handleStatusCommand(s, i)
	case "fill":
		b.handleFillCommand(s, i)
	case "verbose":
		b.handleVerboseCommand(s, i)
	case "agents":
		b.handleAgentsCommand(s, i)
	case "stop":
		b.handleStopCommand(s, i)
	case "compact":
		b.handleCompactCommand(s, i)
	}
}

func (b *Bot) handleClaimCommand(s *discordgo.Session, i *discordgo.InteractionCreate) {
	uid := interactionUserID(i)
	if uid == "" {
		b.respondInteraction(s, i, "❌ Unable to identify Discord user.", true)
		return
	}

	ok, owner, err := b.State.TryClaim(uid)
	if err != nil {
		b.respondInteraction(s, i, "❌ Failed to persist state, try again.", true)
		return
	}
	if ok {
		b.respondInteraction(s, i, fmt.Sprintf("✅ Bot claimed successfully by <@%s>.", uid), true)
		return
	}

	b.respondInteraction(s, i, fmt.Sprintf("❌ This bot is already claimed by <@%s>.", owner), true)
}

func (b *Bot) handleUnclaimCommand(s *discordgo.Session, i *discordgo.InteractionCreate) {
	uid := interactionUserID(i)
	if uid == "" {
		b.respondInteraction(s, i, "❌ Unable to identify Discord user.", true)
		return
	}

	ok, err := b.State.Unclaim(uid)
	if err != nil {
		b.respondInteraction(s, i, "❌ Failed to persist state, try again.", true)
		return
	}
	if !ok {
		b.respondInteraction(s, i, "❌ Failed to unclaim: you are not the current owner.", true)
		return
	}

	b.respondInteraction(s, i, "✅ Bot ownership released. Anyone can now claim the bot using `/claim`.", true)
}

func (b *Bot) handleBindCommand(s *discordgo.Session, i *discordgo.InteractionCreate) {
	turnUnlock := b.State.LockChannelTurn(i.ChannelID)
	defer turnUnlock()

	agentID := ""
	for _, opt := range i.ApplicationCommandData().Options {
		if opt.Name == "agent" {
			agentID = strings.TrimSpace(opt.StringValue())
		}
	}

	if agentID == "" || strings.Contains(agentID, "/") || strings.Contains(agentID, "\\") || agentID == "." || agentID == ".." {
		b.respondInteraction(s, i, "❌ A valid agent ID (alphanumeric directory name) is required.", true)
		return
	}

	// Verify agent exists
	insp, err := b.SDK.InspectAgent(context.Background(), &agentv1.InspectAgentRequest{AgentId: agentID})
	if err != nil || insp == nil || !insp.GetAgentDirExists() {
		listResp, listErr := b.SDK.ListAgents(context.Background(), &agentv1.ListAgentsRequest{})
		availStr := "none"
		if listErr != nil {
			availStr = fmt.Sprintf("could not read the workspace, listing failed: %v", listErr)
		} else if listResp != nil && len(listResp.GetAgentIds()) > 0 {
			availStr = strings.Join(listResp.GetAgentIds(), ", ")
		}
		b.respondInteraction(s, i, fmt.Sprintf("❌ Agent %q was not found in workspace %s.\nAvailable agents: %s", agentID, b.WsDir, availStr), true)
		return
	}

	if insp.GetRuntimeJsonExists() && !insp.GetRuntimeJsonValid() {
		b.respondInteraction(s, i, fmt.Sprintf("❌ Agent %q has an invalid `runtime.json`: %s", agentID, insp.GetRuntimeJsonError()), true)
		return
	}

	modelName := "default"
	if insp.GetRuntimeJsonValid() {
		if cfg, err := agent.LoadRuntimeConfig(insp.GetAgentDir()); err == nil && cfg != nil && cfg.Model != "" {
			modelName = cfg.Model
		}
	}

	// Look up or create channel webhook
	var whID, whToken string
	if wh, err := EnsureWebhook(s, i.ChannelID); err == nil && wh != nil {
		whID = wh.ID
		whToken = wh.Token
	}

	// Read existing turns to initialize sync state. A failed read must refuse the bind
	// rather than seed an empty baseline: lastIdx -1 looks identical to "nothing has been
	// synced yet", and the next sync pass would then replay the agent's entire history into
	// the channel as unsynced turns.
	readResp, err := b.SDK.ReadSession(context.Background(), &agentv1.ReadSessionRequest{AgentId: agentID})
	if err != nil {
		b.respondInteraction(s, i, fmt.Sprintf("❌ Failed to read session history for agent %q, refusing to bind with an unknown baseline: %v", agentID, err), true)
		return
	}
	var turns []*genai.Content
	for _, t := range readResp.GetTurns() {
		turns = append(turns, SessionTurnToContent(t))
	}
	lastIdx := len(turns) - 1
	lastHash := ""
	if lastIdx >= 0 {
		lastHash = ComputeTurnHash(turns[lastIdx])
	}

	binding := &ChannelBinding{
		ChannelID:     i.ChannelID,
		GuildID:       i.GuildID,
		AgentID:       agentID,
		LastTurnIndex: lastIdx,
		LastTurnHash:  lastHash,
		WebhookID:     whID,
		WebhookToken:  whToken,
	}

	syncUnlock := b.State.LockChannelSync(i.ChannelID)
	err = b.State.SetBinding(binding)
	syncUnlock()
	if err != nil {
		b.respondInteraction(s, i, fmt.Sprintf("❌ Failed to save binding: %v", err), true)
		return
	}

	if b.Watcher != nil {
		if err := b.Watcher.WatchAgent(agentID); err != nil {
			log.Printf("⚠️ failed to watch agent %q for live session updates: %v", agentID, err)
		}
	}

	b.respondInteraction(s, i, fmt.Sprintf("✅ Channel bound to agent **%s** (model: `%s`)!\nMessages sent in this channel will drive this agent.", agentID, modelName), false)
}

func (b *Bot) handleUnbindCommand(s *discordgo.Session, i *discordgo.InteractionCreate) {
	// Pre-lock existence check
	if b.State.GetBinding(i.ChannelID) == nil {
		b.respondInteraction(s, i, "ℹ️ This channel is not currently bound to any agent.", true)
		return
	}

	turnUnlock := b.State.LockChannelTurn(i.ChannelID)
	defer turnUnlock()

	syncUnlock := b.State.LockChannelSync(i.ChannelID)
	defer syncUnlock()

	binding := b.State.GetBinding(i.ChannelID)
	if binding == nil {
		b.respondInteraction(s, i, "ℹ️ This channel is not currently bound to any agent.", true)
		return
	}

	agentID := binding.AgentID
	if err := b.State.RemoveBinding(i.ChannelID); err != nil {
		b.respondInteraction(s, i, fmt.Sprintf("❌ Failed to unbind channel: %v", err), true)
		return
	}

	if b.Watcher != nil {
		b.Watcher.UnwatchAgent(agentID)
	}

	b.respondInteraction(s, i, fmt.Sprintf("🔓 Channel unbound from agent **%s**.", agentID), false)
}

func (b *Bot) handleStatusCommand(s *discordgo.Session, i *discordgo.InteractionCreate) {
	binding := b.State.GetBinding(i.ChannelID)
	if binding == nil {
		b.respondInteraction(s, i, "ℹ️ This channel is not currently bound to any agent. Use `/bind <agent_id>` to connect one.", true)
		return
	}

	insp, err := b.SDK.InspectAgent(context.Background(), &agentv1.InspectAgentRequest{AgentId: binding.AgentID})
	if err != nil || insp == nil || !insp.GetAgentDirExists() {
		b.respondInteraction(s, i, fmt.Sprintf("❌ **Binding Error:** Bound agent %q does not exist in workspace `%s`.", binding.AgentID, b.WsDir), true)
		return
	}

	memResp, memErr := b.SDK.ReadMemory(context.Background(), &agentv1.ReadMemoryRequest{AgentId: binding.AgentID})
	mem := ""
	if memErr != nil {
		mem = fmt.Sprintf("memory unavailable: %v", memErr)
	} else if memResp != nil {
		mem = memResp.GetMemoryMd()
	}
	memSnippet := strings.TrimSpace(mem)
	if len(memSnippet) > 300 {
		memSnippet = memSnippet[:300] + "..."
	}
	if memSnippet == "" {
		memSnippet = "*(empty)*"
	}

	modelName := "default"
	endpoint := "default"
	if insp.GetRuntimeJsonValid() {
		if cfg, err := agent.LoadRuntimeConfig(insp.GetAgentDir()); err == nil && cfg != nil {
			if cfg.Model != "" {
				modelName = cfg.Model
			}
			if cfg.Endpoint != "" {
				endpoint = cfg.Endpoint
			}
		}
	} else if insp.GetRuntimeJsonExists() && !insp.GetRuntimeJsonValid() {
		modelName = fmt.Sprintf("error: %s", insp.GetRuntimeJsonError())
	}

	status := fmt.Sprintf(
		"🤖 **Agent:** `%s`\n"+
			"📡 **Model:** `%s` (%s)\n"+
			"💬 **Session Turns:** %d\n"+
			"🔧 **Tools Available:** %d (%s)\n"+
			"🔊 **Verbose Mode:** `%v`\n\n"+
			"🧠 **Memory Preview:**\n```markdown\n%s\n```",
		insp.GetAgentId(),
		modelName,
		endpoint,
		insp.GetSessionTurnCount(),
		len(insp.GetDiscoveredTools()),
		strings.Join(insp.GetDiscoveredTools(), ", "),
		binding.Verbose,
		memSnippet,
	)

	b.respondInteraction(s, i, status, false)
}

func (b *Bot) handleFillCommand(s *discordgo.Session, i *discordgo.InteractionCreate) {
	// Pre-lock existence check
	if b.State.GetBinding(i.ChannelID) == nil {
		b.respondInteraction(s, i, "❌ This channel is not bound to any agent. Use `/bind <agent_id>` first.", true)
		return
	}

	// Defer response to allow backfill processing. If the ack itself fails there is nothing
	// to edit later - Discord never learns we're working on it - so skip straight to
	// aborting instead of doing the backfill work and then also failing to report it.
	if s != nil && i != nil && i.Interaction != nil {
		if err := s.InteractionRespond(i.Interaction, &discordgo.InteractionResponse{
			Type: discordgo.InteractionResponseDeferredChannelMessageWithSource,
		}); err != nil {
			log.Printf("⚠️ failed to defer interaction response in channel %s: %v", i.ChannelID, err)
			return
		}
	}

	syncUnlock := b.State.LockChannelSync(i.ChannelID)
	binding := b.State.GetBinding(i.ChannelID)
	if binding == nil {
		syncUnlock()
		b.editInteractionResponse(s, i, "❌ This channel is not bound to any agent. Use `/bind <agent_id>` first.")
		return
	}

	if binding.IsGenerating {
		syncUnlock()
		b.editInteractionResponse(s, i, fmt.Sprintf(errGeneratingBusy, "/fill"))
		return
	}
	syncUnlock()

	insp, err := b.SDK.InspectAgent(context.Background(), &agentv1.InspectAgentRequest{AgentId: binding.AgentID})
	if err != nil || insp == nil || !insp.GetAgentDirExists() {
		b.editInteractionResponse(s, i, fmt.Sprintf("❌ **Binding Error:** Bound agent %q does not exist in workspace `%s`.", binding.AgentID, b.WsDir))
		return
	}

	limit := -1
	if i != nil && i.Data != nil {
		for _, opt := range i.ApplicationCommandData().Options {
			if opt.Name == "limit" {
				limit = int(opt.IntValue())
			}
		}
	}

	count, err := b.autoFillUnsyncedTurns(s, binding, i.ChannelID, limit)
	if err != nil {
		b.editInteractionResponse(s, i, fmt.Sprintf("❌ Failed to backfill session turns: %v", err))
		return
	}

	if count == 0 {
		b.editInteractionResponse(s, i, "✅ Channel is already up to date with session history.")
		return
	}

	b.editInteractionResponse(s, i, fmt.Sprintf("✅ Backfilled %d session turns for **%s**.", count, binding.AgentID))
}

func (b *Bot) handleVerboseCommand(s *discordgo.Session, i *discordgo.InteractionCreate) {
	syncUnlock := b.State.LockChannelSync(i.ChannelID)
	defer syncUnlock()

	binding := b.State.GetBinding(i.ChannelID)
	if binding == nil {
		b.respondInteraction(s, i, "❌ This channel is not bound to any agent. Use `/bind <agent_id>` first.", true)
		return
	}

	hasExplicit := false
	newVal := !binding.Verbose
	for _, opt := range i.ApplicationCommandData().Options {
		if opt.Name == "enabled" {
			newVal = opt.BoolValue()
			hasExplicit = true
		}
	}
	_ = hasExplicit

	binding.Verbose = newVal
	if err := b.State.SetBinding(binding); err != nil {
		b.respondInteraction(s, i, fmt.Sprintf("❌ Failed to update verbose setting: %v", err), true)
		return
	}

	b.respondInteraction(s, i, fmt.Sprintf("🔊 Verbose mode set to **`%v`** for agent **%s**.", newVal, binding.AgentID), false)
}

func (b *Bot) handleAgentsCommand(s *discordgo.Session, i *discordgo.InteractionCreate) {
	resp, err := b.SDK.ListAgents(context.Background(), &agentv1.ListAgentsRequest{})
	if err != nil {
		b.respondInteraction(s, i, fmt.Sprintf("❌ Failed to list agents: %v", err), true)
		return
	}
	ids := resp.GetAgentIds()

	if len(ids) == 0 {
		b.respondInteraction(s, i, fmt.Sprintf("📂 No agents found in workspace `%s`.", b.WsDir), true)
		return
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("📂 **Workspace Agents (%d found):**\n\n", len(ids)))
	for _, id := range ids {
		insp, err := b.SDK.InspectAgent(context.Background(), &agentv1.InspectAgentRequest{AgentId: id})
		if err != nil || insp == nil {
			sb.WriteString(fmt.Sprintf("• `%s` *(inspection error)*\n", id))
			continue
		}
		statusIcon := "⚪"
		modelName := "default"
		if insp.GetRuntimeJsonValid() {
			statusIcon = "🟢"
			if cfg, err := agent.LoadRuntimeConfig(insp.GetAgentDir()); err == nil && cfg != nil && cfg.Model != "" {
				modelName = cfg.Model
			}
		} else if insp.GetRuntimeJsonExists() {
			statusIcon = "🟡"
		}
		sb.WriteString(fmt.Sprintf("%s **`%s`** — Model: `%s`, Turns: %d, Tools: %d\n", statusIcon, id, modelName, insp.GetSessionTurnCount(), len(insp.GetDiscoveredTools())))
	}

	b.respondInteraction(s, i, sb.String(), false)
}

// handleStopCommand cancels the in-flight generation for the agent bound to the
// channel where /stop was invoked. Authorization is enforced in HandleInteraction:
// /stop is not the /claim command, so the D102 allowlist gate (IsAllowedUser) runs
// first and only the claimed owner reaches this handler - nobody else can cancel
// a turn they did not start.
//
// Cancel semantics (chosen and documented): PARTIAL COMMIT. HandleMessageCreate
// persists the user's message to session.jsonl via AddUserTurn BEFORE generation
// starts, so a cancelled turn keeps the user turn and cleanly aborts the model
// turn - no torn/partial model output is ever written. The orphaned user turn is
// reconciled with the next user message by CleanSessionTurns. The SDK unregisters
// the in-flight turn when its stream unwinds, so a subsequent /stop reports
// 'no in-flight turn' instead of cancelling a stale registration.
func (b *Bot) handleStopCommand(s *discordgo.Session, i *discordgo.InteractionCreate) {
	binding := b.State.GetBinding(i.ChannelID)
	if binding == nil {
		b.respondInteraction(s, i, "ℹ️ This channel is not currently bound to any agent. Use `/bind <agent_id>` to connect one.", true)
		return
	}

	if _, err := b.SDK.CancelTurn(context.Background(), &agentv1.CancelTurnRequest{AgentId: binding.AgentID}); err != nil {
		b.respondInteraction(s, i, fmt.Sprintf("ℹ️ No in-flight turn for agent **%s**.", binding.AgentID), false)
		return
	}

	b.respondInteraction(s, i, fmt.Sprintf("⏹️ Turn cancelled for agent **%s**.", binding.AgentID), false)
}

func (b *Bot) respondInteraction(s *discordgo.Session, i *discordgo.InteractionCreate, message string, ephemeral bool) {
	if s == nil || i == nil || i.Interaction == nil {
		return
	}
	flags := discordgo.MessageFlags(0)
	if ephemeral {
		flags = discordgo.MessageFlagsEphemeral
	}

	if err := s.InteractionRespond(i.Interaction, &discordgo.InteractionResponse{
		Type: discordgo.InteractionResponseChannelMessageWithSource,
		Data: &discordgo.InteractionResponseData{
			Content: message,
			Flags:   flags,
		},
	}); err != nil {
		log.Printf("⚠️ failed to respond to interaction in channel %s: %v", i.ChannelID, err)
	}
}

func (b *Bot) editInteractionResponse(s *discordgo.Session, i *discordgo.InteractionCreate, message string) {
	if s == nil || i == nil || i.Interaction == nil {
		return
	}
	if _, err := s.InteractionResponseEdit(i.Interaction, &discordgo.WebhookEdit{
		Content: &message,
	}); err != nil {
		log.Printf("⚠️ failed to edit interaction response in channel %s: %v", i.ChannelID, err)
	}
}

// handleCompactCommand archives the bound agent's oldest turns into MEMORY.md. The D44
// gates stay in charge unless the caller passes force, and either way the reply states what
// happened: a skipped compaction is otherwise indistinguishable from a command that did
// nothing at all.
func (b *Bot) handleCompactCommand(s *discordgo.Session, i *discordgo.InteractionCreate) {
	binding := b.State.GetBinding(i.ChannelID)
	if binding == nil {
		b.respondInteraction(s, i, "❌ This channel is not bound to any agent. Use `/bind <agent_id>` first.", true)
		return
	}

	// Compaction rewrites session.jsonl, which the runner is appending to. The session lock
	// would serialize it anyway, but a long generation would leave the caller staring at a
	// deferred Discord message, so this rejects cleanly instead of waiting.
	if binding.IsGenerating {
		b.respondInteraction(s, i, fmt.Sprintf(errGeneratingBusy, "/compact"), true)
		return
	}

	force := false
	for _, opt := range i.ApplicationCommandData().Options {
		// BoolValue panics on an option of another type, and the payload is Discord-supplied.
		if opt.Name == "force" && opt.Type == discordgo.ApplicationCommandOptionBoolean {
			force = opt.BoolValue()
		}
	}

	// Summarizing turns is a model call, so it does not fit inside Discord's interaction
	// window. If the ack itself fails there is nothing to edit later, so abort here instead
	// of running compaction and then also failing to report the result.
	if s != nil && i != nil && i.Interaction != nil {
		if err := s.InteractionRespond(i.Interaction, &discordgo.InteractionResponse{
			Type: discordgo.InteractionResponseDeferredChannelMessageWithSource,
		}); err != nil {
			log.Printf("⚠️ failed to defer interaction response in channel %s: %v", i.ChannelID, err)
			return
		}
	}

	// Resolved rather than called on the SDK directly, so a channel bound to a routed agent
	// sends compaction to the bridge that owns the session instead of hunting for a local one.
	client, cleanup, err := agent.ResolveAgentClient(context.Background(), b.SDK, binding.AgentID)
	if err != nil {
		b.editInteractionResponse(s, i, fmt.Sprintf("❌ Could not reach agent **%s**: %v", binding.AgentID, err))
		return
	}
	defer cleanup()

	before := b.sessionContextSnapshot(binding.AgentID)

	resp, err := client.CompactSession(context.Background(), &agentv1.CompactSessionRequest{
		AgentId:      binding.AgentID,
		Force:        force,
		WorkspaceDir: b.WsDir,
	})
	if err != nil {
		b.editInteractionResponse(s, i, fmt.Sprintf("❌ Compaction failed for **%s**: %v", binding.AgentID, err))
		return
	}
	if resp == nil {
		b.editInteractionResponse(s, i, fmt.Sprintf("❌ Compaction returned no result for **%s**.", binding.AgentID))
		return
	}

	if !resp.GetCompacted() {
		b.editInteractionResponse(s, i, compactSkippedMessage(binding.AgentID, before, force))
		return
	}
	b.editInteractionResponse(s, i, compactedMessage(binding.AgentID, before, b.sessionContextSnapshot(binding.AgentID)))
}

// sessionContextSnapshot decorates a compaction reply with turn and token counts. Errors
// become nil: the verdict comes from the RPC, these numbers only explain it.
func (b *Bot) sessionContextSnapshot(agentID string) *agentv1.InspectSessionContextResponse {
	resp, err := b.SDK.InspectSessionContext(context.Background(), &agentv1.InspectSessionContextRequest{
		AgentId:      agentID,
		WorkspaceDir: b.WsDir,
	})
	if err != nil {
		return nil
	}
	return resp
}

func compactedMessage(agentID string, before, after *agentv1.InspectSessionContextResponse) string {
	archived := before.GetTurnCount() - after.GetTurnCount()
	if archived > 0 {
		unit := "turns"
		if archived == 1 {
			unit = "turn"
		}
		return fmt.Sprintf(
			"🗜️ **Compacted %s**: archived %d %s (%d to %d). The next turn starts with the compacted context.",
			agentID, archived, unit, before.GetTurnCount(), after.GetTurnCount(),
		)
	}
	// A routed agent keeps its session behind its bridge, so there are no local counts to quote.
	return fmt.Sprintf("🗜️ **Compacted %s**. The next turn starts with the compacted context.", agentID)
}

// compactSkippedMessage names the gate that refused, because a bare no is what sends an
// operator to the logs.
func compactSkippedMessage(agentID string, before *agentv1.InspectSessionContextResponse, force bool) string {
	if before == nil || before.GetTurnCount() == 0 {
		return fmt.Sprintf("ℹ️ Nothing to compact for **%s**: no session turns to archive.", agentID)
	}
	if force {
		return fmt.Sprintf("ℹ️ Compaction was forced for **%s** and archived nothing.", agentID)
	}
	return fmt.Sprintf(
		"ℹ️ Nothing to compact for **%s** yet: %d of %d tokens (%.0f%% of the compaction threshold) across %d turns. Use `/compact force:true` to archive anyway.",
		agentID,
		before.GetEstimatedTotalTokens(),
		before.GetCompactionThreshold(),
		before.GetPercentToThreshold(),
		before.GetTurnCount(),
	)
}
