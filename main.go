package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/colinrgodsey/wackydiscord/bot"
	"github.com/colinrgodsey/wackypub/pkg/agent"
	"github.com/spf13/cobra"
)

const (
	// EnvDiscordBotToken is the standard environment variable name for the Discord bot token.
	EnvDiscordBotToken = "DISCORD_BOT_TOKEN"
)

var (
	flagWs            string
	flagStateFile     string
	flagGuildID       string
	flagDefaultOpen   bool
	flagDefaultPolicy string
)

var rootCmd = &cobra.Command{
	Use:   "wackydiscord",
	Short: "Workspace-driven Discord REPL & multi-agent channel bridge",
	Long: `WackyDiscord bridges Discord channels to folder-based WackyPub AI agents.
It supports channel-to-agent bindings (/bind), session backfilling (/fill),
webhook persona impersonation, and real-time interactive conversations.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		isExplicit := cmd.Flags().Changed("ws")
		wsDir, err := agent.ResolveWorkspaceDir(flagWs, isExplicit)
		if err != nil {
			return fmt.Errorf("invalid workspace: %w", err)
		}

		token, err := resolveToken(cmd, os.Getenv)
		if err != nil {
			return err
		}

		var defaultOpenPtr *bool
		if cmd.Flags().Changed("default-policy") {
			v := strings.ToLower(strings.TrimSpace(flagDefaultPolicy)) != "closed"
			defaultOpenPtr = &v
		} else if cmd.Flags().Changed("default-open") {
			defaultOpenPtr = &flagDefaultOpen
		}

		b, err := bot.NewBot(bot.Config{
			Token:         token,
			WorkspaceDir:  wsDir,
			StateFilePath: flagStateFile,
			GuildID:       flagGuildID,
			DefaultOpen:   defaultOpenPtr,
		})
		if err != nil {
			return fmt.Errorf("failed to initialize wackydiscord bot: %w", err)
		}

		ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer cancel()

		return b.Start(ctx)
	},
}

func init() {
	rootCmd.Flags().StringVar(&flagWs, "ws", ".", "WackyPub workspace directory path")
	// --token stays registered after removal so that a stale launch config fails with the
	// migration instruction instead of cobra's bare "unknown flag". It is hidden, and nothing
	// reads its value.
	rootCmd.Flags().String("token", "", "removed: set "+EnvDiscordBotToken+" instead")
	_ = rootCmd.Flags().MarkHidden("token")
	rootCmd.Flags().StringVar(&flagStateFile, "state-file", "", "Custom state file path (default: <ws_dir>/.wackydiscord.json)")
	rootCmd.Flags().StringVar(&flagGuildID, "guild-id", "", "Optional: register slash commands immediately to a specific guild ID")
	rootCmd.Flags().StringVar(&flagDefaultPolicy, "default-policy", "open", "Allowlist policy when unclaimed: 'open' or 'closed'")
	rootCmd.Flags().BoolVar(&flagDefaultOpen, "default-open", true, "Allow interaction before bot is claimed (starting default: true)")
}

// resolveToken reads the Discord bot token from the environment only.
//
// A token in argv is world-readable: /proc/<pid>/cmdline has no permission filter, so any local
// user (or any process listing captured into a log or an agent transcript) sees it.
// /proc/<pid>/environ, where this reads from, is owner-only.
func resolveToken(cmd *cobra.Command, getenv func(string) string) (string, error) {
	if cmd.Flags().Changed("token") {
		return "", fmt.Errorf("--token was removed because argv is world-readable in ps: set the %s environment variable instead (for systemd, EnvironmentFile= pointing at a mode-0600 file, not Environment=, which is readable via systemctl show)", EnvDiscordBotToken)
	}
	token := strings.TrimSpace(getenv(EnvDiscordBotToken))
	if token == "" {
		return "", fmt.Errorf("discord bot token is required: set the %s environment variable", EnvDiscordBotToken)
	}
	return token, nil
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
