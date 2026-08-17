package bot

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/bwmarrin/discordgo"
	"github.com/colinrgodsey/wackypub/pkg/agent"
)

// Config defines the runtime configuration options for the WackyDiscord bot.
type Config struct {
	Token         string
	WorkspaceDir  string
	StateFilePath string
	GuildID       string // Optional: registers slash commands to a specific guild for instant availability
}

// Bot manages the Discord gateway session, channel bindings, and WackyPub agent integration.
type Bot struct {
	Session *discordgo.Session
	SDK     *agent.AgentSDK
	State   *State
	Watcher *SessionWatcher
	WsDir   string
	GuildID string
	AppID   string
}

// NewBot initializes a new Bot instance with the provided configuration.
func NewBot(cfg Config) (*Bot, error) {
	if cfg.Token == "" {
		return nil, fmt.Errorf("discord bot token is required")
	}

	wsDir := cfg.WorkspaceDir
	if wsDir == "" {
		wsDir = "."
	}
	absWsDir, err := filepath.Abs(wsDir)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve workspace directory %s: %w", wsDir, err)
	}

	// Verify workspace directory contains WACKYPUB_ROOT marker file
	markerPath := filepath.Join(absWsDir, agent.RootMarkerFile)
	if _, err := os.Stat(markerPath); err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("invalid workspace directory %q: missing %s marker file", absWsDir, agent.RootMarkerFile)
		}
		return nil, fmt.Errorf("failed to check workspace marker file in %q: %w", absWsDir, err)
	}

	statePath := cfg.StateFilePath
	if statePath == "" {
		statePath = filepath.Join(absWsDir, DefaultStateFileName)
	}

	st, err := NewState(statePath)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize state: %w", err)
	}

	dg, err := discordgo.New("Bot " + cfg.Token)
	if err != nil {
		return nil, fmt.Errorf("failed to create discord session: %w", err)
	}

	// Request message content and guild intents
	dg.Identify.Intents = discordgo.IntentsGuildMessages |
		discordgo.IntentsDirectMessages |
		discordgo.IntentsMessageContent |
		discordgo.IntentsGuilds

	b := &Bot{
		Session: dg,
		SDK:     agent.NewSDK(absWsDir),
		State:   st,
		WsDir:   absWsDir,
		GuildID: cfg.GuildID,
	}

	sw, err := NewSessionWatcher(b)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize session watcher: %w", err)
	}
	b.Watcher = sw

	// Register event handlers
	dg.AddHandler(b.handleReady)
	dg.AddHandler(b.HandleInteraction)
	dg.AddHandler(b.HandleMessageCreate)

	return b, nil
}

func (b *Bot) handleReady(s *discordgo.Session, r *discordgo.Ready) {
	if s.State != nil && s.State.User != nil {
		b.AppID = s.State.User.ID
		log.Printf("🤖 Logged in as %s#%s (ID: %s)", s.State.User.Username, s.State.User.Discriminator, b.AppID)
	}

	log.Printf("📂 Workspace directory: %s", b.WsDir)
	if err := b.RegisterSlashCommands(b.GuildID); err != nil {
		log.Printf("⚠️ Warning: failed to register slash commands: %v", err)
	} else {
		log.Printf("⚡ Slash commands registered successfully.")
	}

	// Validate loaded bindings
	warnings := b.ValidateBindings()
	for _, w := range warnings {
		log.Printf("⚠️ Binding warning: %s", w)
	}
}

// ValidateBindings inspects all channel bindings and returns any configuration or existence warnings.
func (b *Bot) ValidateBindings() []string {
	var warnings []string
	bindings := b.State.GetAllBindings()
	for channelID, binding := range bindings {
		insp, err := b.SDK.InspectAgent(binding.AgentID)
		if err != nil || insp == nil || !insp.AgentDirExists {
			warnings = append(warnings, fmt.Sprintf("channel %s is bound to missing agent %q in workspace %s", channelID, binding.AgentID, b.WsDir))
		} else if insp.RuntimeJSONExists && !insp.RuntimeJSONValid {
			warnings = append(warnings, fmt.Sprintf("channel %s is bound to agent %q with invalid runtime.json: %s", channelID, binding.AgentID, insp.RuntimeJSONError))
		}
	}
	return warnings
}

// Start opens the Discord WebSocket connection and blocks until context cancellation.
func (b *Bot) Start(ctx context.Context) error {
	if err := b.Session.Open(); err != nil {
		return fmt.Errorf("failed to open discord gateway connection: %w", err)
	}
	defer b.Session.Close()

	if b.Watcher != nil {
		b.Watcher.Start(ctx)
		defer b.Watcher.Close()
	}

	log.Printf("🚀 WackyDiscord bot is running. Press Ctrl+C to exit.")
	<-ctx.Done()
	log.Printf("Shutting down WackyDiscord bot...")
	return nil
}

// SyncAgentToChannels iterates through all channel bindings for an agent and backfills unseen turns.
func (b *Bot) SyncAgentToChannels(agentID string) {
	bindings := b.State.GetAllBindings()
	for _, binding := range bindings {
		if binding.AgentID == agentID {
			b.autoFillUnsyncedTurns(b.Session, &binding, binding.ChannelID)
		}
	}
}
