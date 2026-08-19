package bot

import (
	"fmt"
	"strings"

	"github.com/bwmarrin/discordgo"
)

// SlashCommands defines the Discord application commands registered by wackydiscord.
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
	switch data.Name {
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
	}
}

func (b *Bot) handleBindCommand(s *discordgo.Session, i *discordgo.InteractionCreate) {
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
	insp, err := b.SDK.InspectAgent(agentID)
	if err != nil || insp == nil || !insp.AgentDirExists {
		availAgents, _ := b.SDK.ListAgents()
		availStr := "none"
		if len(availAgents) > 0 {
			availStr = strings.Join(availAgents, ", ")
		}
		b.respondInteraction(s, i, fmt.Sprintf("❌ Agent %q was not found in workspace %s.\nAvailable agents: %s", agentID, b.WsDir, availStr), true)
		return
	}

	if insp.RuntimeJSONExists && !insp.RuntimeJSONValid {
		b.respondInteraction(s, i, fmt.Sprintf("❌ Agent %q has an invalid `runtime.json`: %s", agentID, insp.RuntimeJSONError), true)
		return
	}

	modelName := "default"
	if insp.RuntimeConfig != nil && insp.RuntimeConfig.Model != "" {
		modelName = insp.RuntimeConfig.Model
	}

	// Look up or create channel webhook
	var whID, whToken string
	if wh, err := EnsureWebhook(s, i.ChannelID); err == nil && wh != nil {
		whID = wh.ID
		whToken = wh.Token
	}

	// Read existing turns to initialize sync state
	turns, _ := b.SDK.ReadSession(agentID)
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

	if err := b.State.SetBinding(binding); err != nil {
		b.respondInteraction(s, i, fmt.Sprintf("❌ Failed to save binding: %v", err), true)
		return
	}

	if b.Watcher != nil {
		_ = b.Watcher.WatchAgent(agentID)
	}

	b.respondInteraction(s, i, fmt.Sprintf("✅ Channel bound to agent **%s** (model: `%s`)!\nMessages sent in this channel will drive this agent.", agentID, modelName), false)
}

func (b *Bot) handleUnbindCommand(s *discordgo.Session, i *discordgo.InteractionCreate) {
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

	insp, err := b.SDK.InspectAgent(binding.AgentID)
	if err != nil || insp == nil || !insp.AgentDirExists {
		b.respondInteraction(s, i, fmt.Sprintf("❌ **Binding Error:** Bound agent %q does not exist in workspace `%s`.", binding.AgentID, b.WsDir), true)
		return
	}

	mem, _ := b.SDK.ReadMemory(binding.AgentID)
	memSnippet := strings.TrimSpace(mem)
	if len(memSnippet) > 300 {
		memSnippet = memSnippet[:300] + "..."
	}
	if memSnippet == "" {
		memSnippet = "*(empty)*"
	}

	modelName := "default"
	endpoint := "default"
	if insp.RuntimeConfig != nil {
		if insp.RuntimeConfig.Model != "" {
			modelName = insp.RuntimeConfig.Model
		}
		if insp.RuntimeConfig.Endpoint != "" {
			endpoint = insp.RuntimeConfig.Endpoint
		}
	} else if insp.RuntimeJSONExists && !insp.RuntimeJSONValid {
		modelName = fmt.Sprintf("error: %s", insp.RuntimeJSONError)
	}

	status := fmt.Sprintf(
		"🤖 **Agent:** `%s`\n"+
			"📡 **Model:** `%s` (%s)\n"+
			"💬 **Session Turns:** %d\n"+
			"🔧 **Tools Available:** %d (%s)\n"+
			"🔊 **Verbose Mode:** `%v`\n\n"+
			"🧠 **Memory Preview:**\n```markdown\n%s\n```",
		insp.AgentID,
		modelName,
		endpoint,
		insp.SessionTurnCount,
		len(insp.DiscoveredTools),
		strings.Join(insp.DiscoveredTools, ", "),
		binding.Verbose,
		memSnippet,
	)

	b.respondInteraction(s, i, status, false)
}

func (b *Bot) handleFillCommand(s *discordgo.Session, i *discordgo.InteractionCreate) {
	binding := b.State.GetBinding(i.ChannelID)
	if binding == nil {
		b.respondInteraction(s, i, "❌ This channel is not bound to any agent. Use `/bind <agent_id>` first.", true)
		return
	}

	// Defer response to allow backfill processing
	_ = s.InteractionRespond(i.Interaction, &discordgo.InteractionResponse{
		Type: discordgo.InteractionResponseDeferredChannelMessageWithSource,
	})

	insp, err := b.SDK.InspectAgent(binding.AgentID)
	if err != nil || insp == nil || !insp.AgentDirExists {
		b.editInteractionResponse(s, i, fmt.Sprintf("❌ **Binding Error:** Bound agent %q does not exist in workspace `%s`.", binding.AgentID, b.WsDir))
		return
	}

	turns, err := b.SDK.ReadSession(binding.AgentID)
	if err != nil {
		b.editInteractionResponse(s, i, fmt.Sprintf("❌ Failed to read session turns: %v", err))
		return
	}

	limit := -1
	for _, opt := range i.ApplicationCommandData().Options {
		if opt.Name == "limit" {
			limit = int(opt.IntValue())
		}
	}

	unsynced, newIdx, newHash := DiffUnsyncedTurns(turns, binding.LastTurnHash, binding.LastTurnIndex)
	if limit > 0 && len(unsynced) > limit {
		unsynced = unsynced[len(unsynced)-limit:]
	}

	if len(unsynced) == 0 {
		b.editInteractionResponse(s, i, "✅ Channel is already up to date with session history.")
		return
	}

	b.editInteractionResponse(s, i, fmt.Sprintf("🔄 Backfilling %d session turns for **%s**...", len(unsynced), binding.AgentID))

	var wh *discordgo.Webhook
	if binding.WebhookID != "" && binding.WebhookToken != "" {
		wh = &discordgo.Webhook{ID: binding.WebhookID, Token: binding.WebhookToken}
	}

	for _, turn := range unsynced {
		if turn.Role == "user" {
			if binding.Verbose {
				toolText := FormatToolTurnSummary(turn)
				if toolText != "" {
					_ = SendAgentMessage(s, i.ChannelID, "Tools", toolText, nil)
				}
			}
			text := FormatUserBackfillMessage(turn)
			if text != "" {
				_ = SendAgentMessage(s, i.ChannelID, "User", text, nil)
			}
		} else {
			if binding.Verbose {
				toolText := FormatToolTurnSummary(turn)
				if toolText != "" {
					_ = SendAgentMessage(s, i.ChannelID, "Tools", toolText, nil)
				}
			}
			text := FormatAssistantBackfillMessage(turn)
			if text != "" {
				_ = SendAgentMessage(s, i.ChannelID, binding.AgentID, text, wh)
			}
		}
	}

	binding.LastTurnIndex = newIdx
	binding.LastTurnHash = newHash
	_ = b.State.SetBinding(binding)
}

func (b *Bot) handleVerboseCommand(s *discordgo.Session, i *discordgo.InteractionCreate) {
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
	ids, err := b.SDK.ListAgents()
	if err != nil {
		b.respondInteraction(s, i, fmt.Sprintf("❌ Failed to list agents: %v", err), true)
		return
	}

	if len(ids) == 0 {
		b.respondInteraction(s, i, fmt.Sprintf("📂 No agents found in workspace `%s`.", b.WsDir), true)
		return
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("📂 **Workspace Agents (%d found):**\n\n", len(ids)))
	for _, id := range ids {
		insp, err := b.SDK.InspectAgent(id)
		if err != nil || insp == nil {
			sb.WriteString(fmt.Sprintf("• `%s` *(inspection error)*\n", id))
			continue
		}
		statusIcon := "⚪"
		modelName := "default"
		if insp.RuntimeJSONValid && insp.RuntimeConfig != nil {
			statusIcon = "🟢"
			if insp.RuntimeConfig.Model != "" {
				modelName = insp.RuntimeConfig.Model
			}
		} else if insp.RuntimeJSONExists {
			statusIcon = "🟡"
		}
		sb.WriteString(fmt.Sprintf("%s **`%s`** — Model: `%s`, Turns: %d, Tools: %d\n", statusIcon, id, modelName, insp.SessionTurnCount, len(insp.DiscoveredTools)))
	}

	b.respondInteraction(s, i, sb.String(), false)
}

func (b *Bot) respondInteraction(s *discordgo.Session, i *discordgo.InteractionCreate, message string, ephemeral bool) {
	flags := discordgo.MessageFlags(0)
	if ephemeral {
		flags = discordgo.MessageFlagsEphemeral
	}

	_ = s.InteractionRespond(i.Interaction, &discordgo.InteractionResponse{
		Type: discordgo.InteractionResponseChannelMessageWithSource,
		Data: &discordgo.InteractionResponseData{
			Content: message,
			Flags:   flags,
		},
	})
}

func (b *Bot) editInteractionResponse(s *discordgo.Session, i *discordgo.InteractionCreate, message string) {
	_, _ = s.InteractionResponseEdit(i.Interaction, &discordgo.WebhookEdit{
		Content: &message,
	})
}
