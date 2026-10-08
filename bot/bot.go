package bot

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/bwmarrin/discordgo"
)

// Config defines the runtime configuration options for the WackyDiscord bot.
type Config struct {
	Token         string
	WorkspaceDir  string
	StateFilePath string
	GuildID       string // Optional: registers slash commands to a specific guild for instant availability
	DefaultOpen   *bool  // Optional: override allowlist default policy when unclaimed
	WackypubBin   string // Optional: path to the wackypub binary; defaults to "wackypub" on PATH
}

// Bot manages the Discord gateway session, channel bindings, and WackyPub agent integration.
type Bot struct {
	Session *discordgo.Session
	// Client is the protocol surface: every agent operation crosses the stdio boundary.
	Client AgentClient
	// server is the stdio backend backing Client. nil when Client was injected, which
	// is how the tests drive the bot without a binary.
	server  *SpawnedServer
	feed    *sessionFeed
	State   *State
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
	markerPath := filepath.Join(absWsDir, RootMarkerFile)
	if _, err := os.Stat(markerPath); err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("invalid workspace directory %q: missing %s marker file", absWsDir, RootMarkerFile)
		}
		return nil, fmt.Errorf("failed to check workspace marker file in %q: %w", absWsDir, err)
	}

	statePath := cfg.StateFilePath
	if statePath == "" {
		statePath = filepath.Join(absWsDir, DefaultStateFileName)
	}

	// Ensure clean call chain state on startup (D59). This is a process-global mutation,
	// which would be a real hazard in a process shared with other goroutines that read these
	// vars concurrently - it is safe here only because this runs once at bot construction,
	// before any A2A call-chain machinery starts, and the bot is the only thing in this
	// process. os.Unsetenv only errors on a malformed key, which these fixed constant names
	// can never be, so the error is discarded rather than checked.
	_ = os.Unsetenv(EnvAgent2Agent)
	_ = os.Unsetenv(EnvCallChain)

	st, err := NewState(statePath)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize state: %w", err)
	}
	if cfg.DefaultOpen != nil {
		st.SetDefaultOpen(*cfg.DefaultOpen)
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

	server, err := SpawnServer(context.Background(), cfg.WackypubBin, absWsDir)
	if err != nil {
		return nil, fmt.Errorf("failed to start agent protocol server: %w", err)
	}

	b := &Bot{
		Session: dg,
		Client:  server,
		server:  server,
		State:   st,
		WsDir:   absWsDir,
		GuildID: cfg.GuildID,
	}
	b.feed = newSessionFeed(b)

	if err := awaitAgentService(context.Background(), server, absWsDir, agentReadinessTimeout, agentReadinessPollEvery); err != nil {
		warnIfServiceUnreachable(err, absWsDir)
	}

	// Register event handlers
	dg.AddHandler(b.handleReady)
	dg.AddHandler(b.HandleInteraction)
	dg.AddHandler(b.HandleMessageCreate)

	return b, nil
}

// activeClient returns the protocol client for operations.
func (b *Bot) activeClient() AgentClient {
	return b.Client
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
		presence, insp, err := b.classifyAgent(context.Background(), binding.AgentID)
		switch presence {
		case agentServiceUnreachable:
			if b.isBridged(binding.AgentID) {
				warnings = append(warnings, fmt.Sprintf("channel %s is bound to inaccessible bridged agent %q: %v", channelID, binding.AgentID, err))
			} else {
				warnings = append(warnings, fmt.Sprintf("channel %s is bound to agent %q but agent service did not answer (workspace %s): %v", channelID, binding.AgentID, b.WsDir, err))
			}
		case agentAbsent:
			warnings = append(warnings, fmt.Sprintf("channel %s is bound to missing agent %q in workspace %s", channelID, binding.AgentID, b.WsDir))
		case agentAvailable:
			if !b.isBridged(binding.AgentID) && insp.GetRuntimeJsonExists() && !insp.GetRuntimeJsonValid() {
				warnings = append(warnings, fmt.Sprintf("channel %s is bound to agent %q with invalid runtime.json: %s", channelID, binding.AgentID, insp.GetRuntimeJsonError()))
			}
		}
	}
	return warnings
}

// Start opens the Discord WebSocket connection and blocks until context cancellation or a
// fatal downstream failure. A stdio child dying is not fatal by itself: gRPC reconnects
// through the ProcessDialer with a fresh child. Only a transport that stays broken past
// the watchdog threshold (or an unrecoverable spawn failure) makes Start return an error,
// which the CLI turns into a non-zero exit so systemd restarts the service.
func (b *Bot) Start(ctx context.Context) error {
	if b.Session != nil {
		if err := b.Session.Open(); err != nil {
			return fmt.Errorf("failed to open discord gateway connection: %w", err)
		}
		defer b.Session.Close()
	}

	b.startFeed(ctx)
	defer func() {
		if b.server != nil {
			if err := b.server.Close(); err != nil {
				log.Printf("⚠️ agent protocol server shutdown error: %v", err)
			}
		}
	}()

	fatal := b.server.watchConnectivity(ctx)

	log.Printf("🚀 WackyDiscord bot is running. Press Ctrl+C to exit.")
	select {
	case <-ctx.Done():
		log.Printf("Shutting down WackyDiscord bot...")
		return nil
	case err := <-fatal:
		if err != nil {
			log.Printf("🛑 %v", err)
		}
		return err
	}
}

// startFeed launches the session feed worker and re-arms subscriptions for every
// persisted binding. feed.start() alone starts the worker/supervisors but watches
// nothing; the re-arm is what keeps cross-client turns surfacing after a restart
// (see watchExistingBindings). Kept as its own seam so tests can drive the real
// startup wiring without a discord gateway or a spawned server.
func (b *Bot) startFeed(ctx context.Context) {
	if b.feed == nil {
		return
	}
	b.feed.start(ctx)
	b.watchExistingBindings()
}

// watchExistingBindings subscribes the session feed to every persisted binding at
// startup, mirroring the retired fsnotify watcher which watched all bound agents
// when it started (watcher.go Start()). feed.start() only launches the worker and
// supervisors; without this pass a binding loaded from state gets NO subscription
// until it is bound again, so cross-client turns never surface in Discord (the D118
// async-watch regression: tools changes fired the old file watch regardless of
// writer; the protocol subscription must be re-armed for every binding).
func (b *Bot) watchExistingBindings() {
	bindings := b.State.GetAllBindings()
	for _, binding := range bindings {
		if binding.AgentID != "" {
			b.feed.watch(binding.AgentID)
		}
	}
}

// SyncAgentToChannels iterates through all channel bindings for an agent and backfills unseen turns.
func (b *Bot) SyncAgentToChannels(agentID string) {
	if b.isBridged(agentID) {
		return
	}
	bindings := b.State.GetAllBindings()
	for _, binding := range bindings {
		if binding.AgentID == agentID {
			func() {
				drainUnlock := b.State.LockChannelDrain(binding.ChannelID)
				defer drainUnlock()
				if _, err := b.autoFillUnsyncedTurns(b.Session, nil, binding.ChannelID, 0); err != nil {
					log.Printf("⚠️ background auto-sync error for agent %s in channel %s: %v", agentID, binding.ChannelID, err)
				}
			}()
		}
	}
}
