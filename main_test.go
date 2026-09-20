package main

import (
	"errors"
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

func tokenFlagCommand() *cobra.Command {
	cmd := &cobra.Command{Use: "wackydiscord", RunE: func(*cobra.Command, []string) error { return nil }}
	cmd.Flags().String("token", "", "removed")
	return cmd
}

func TestResolveTokenReadsEnvironment(t *testing.T) {
	got, err := resolveToken(tokenFlagCommand(), func(string) string { return "  env-token  " })
	if err != nil {
		t.Fatalf("resolveToken: %v", err)
	}
	if got != "env-token" {
		t.Errorf("token = %q, want the trimmed environment value", got)
	}
}

// The whole point of the change: a token supplied on the command line is refused rather than
// accepted-then-ignored, because argv is world-readable for the process lifetime.
func TestResolveTokenRefusesArgv(t *testing.T) {
	cmd := tokenFlagCommand()
	if err := cmd.Flags().Parse([]string{"--token", "argv-token"}); err != nil {
		t.Fatalf("Parse: %v", err)
	}

	got, err := resolveToken(cmd, func(string) string { return "env-token" })
	if got != "" {
		t.Errorf("a token read from argv was accepted: %q", got)
	}
	if err == nil {
		t.Fatal("expected --token to be refused")
	}
	if !strings.Contains(err.Error(), EnvDiscordBotToken) {
		t.Errorf("the refusal must name the replacement, got: %v", err)
	}
	if !strings.Contains(err.Error(), "ps") {
		t.Errorf("the refusal should say why, got: %v", err)
	}
}

func TestResolveTokenRequiresEnvironment(t *testing.T) {
	_, err := resolveToken(tokenFlagCommand(), func(string) string { return "   " })
	if err == nil {
		t.Fatal("expected an error when no token is configured")
	}
	if !strings.Contains(err.Error(), EnvDiscordBotToken) {
		t.Errorf("the error must name the variable to set, got: %v", err)
	}
	// Never echo a token-like value in an error: these strings reach logs and stderr.
	if strings.Contains(err.Error(), "   ") && strings.Contains(err.Error(), "set the") {
		t.Errorf("error echoed the raw value: %v", err)
	}
}

// The launch surface itself: --token must stay registered so stale units fail with the
// migration instruction, and hidden so it is not offered as a supported option.
func TestTokenFlagIsHiddenTombstone(t *testing.T) {
	flag := rootCmd.Flags().Lookup("token")
	if flag == nil {
		t.Fatal("--token should remain registered as a tombstone so stale launch configs get the migration error")
	}
	if !flag.Hidden {
		t.Error("--token must not be advertised as a supported option")
	}
	if strings.Contains(flag.Usage, "Discord bot token (") {
		t.Errorf("the help text should not offer --token as the way to pass a token: %q", flag.Usage)
	}
}

func TestEnvVariableNameIsStable(t *testing.T) {
	if EnvDiscordBotToken != "DISCORD_BOT_TOKEN" {
		t.Errorf("renaming the environment variable breaks every existing launcher; %q", EnvDiscordBotToken)
	}
}

func TestUnsetEnvironmentIsNotAValidToken(t *testing.T) {
	sentinel := errors.New("unused")
	_ = sentinel
	if _, err := resolveToken(tokenFlagCommand(), func(string) string { return "" }); err == nil {
		t.Error("an empty token must not start the bot")
	}
}
