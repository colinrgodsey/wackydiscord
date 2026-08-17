package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
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
	flagWs        string
	flagToken     string
	flagStateFile string
	flagGuildID   string
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

		token := flagToken
		if token == "" {
			token = os.Getenv(EnvDiscordBotToken)
		}
		if token == "" {
			return fmt.Errorf("discord bot token is required: provide --token or set %s environment variable", EnvDiscordBotToken)
		}

		b, err := bot.NewBot(bot.Config{
			Token:         token,
			WorkspaceDir:  wsDir,
			StateFilePath: flagStateFile,
			GuildID:       flagGuildID,
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
	rootCmd.Flags().StringVar(&flagToken, "token", "", "Discord bot token (or DISCORD_BOT_TOKEN env var)")
	rootCmd.Flags().StringVar(&flagStateFile, "state-file", "", "Custom state file path (default: <ws_dir>/.wackydiscord.json)")
	rootCmd.Flags().StringVar(&flagGuildID, "guild-id", "", "Optional: register slash commands immediately to a specific guild ID")
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
