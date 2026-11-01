package bot

import (
	"reflect"
	"testing"
)

func TestEnvWithout(t *testing.T) {
	t.Run("present entry removed, others keep order", func(t *testing.T) {
		env := []string{"PATH=/bin", "DISCORD_BOT_TOKEN=supersecret", "HOME=/home/x"}
		got := envWithout(env, EnvDiscordBotToken)
		want := []string{"PATH=/bin", "HOME=/home/x"}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})
	t.Run("absent: values unchanged", func(t *testing.T) {
		env := []string{"PATH=/bin", "HOME=/home/x"}
		if got := envWithout(env, EnvDiscordBotToken); !reflect.DeepEqual(got, env) {
			t.Errorf("got %v, want %v", got, env)
		}
	})
	t.Run("malformed entries without = are kept", func(t *testing.T) {
		env := []string{"MALFORMED", "DISCORD_BOT_TOKEN=supersecret", "=weird", "OK=1"}
		got := envWithout(env, EnvDiscordBotToken)
		want := []string{"MALFORMED", "=weird", "OK=1"}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})
	t.Run("empty input", func(t *testing.T) {
		if got := envWithout(nil, EnvDiscordBotToken); len(got) != 0 {
			t.Errorf("got %v, want empty", got)
		}
	})
	t.Run("exact name match only", func(t *testing.T) {
		env := []string{"DISCORD_BOT_TOKEN2=x", "XDISCORD_BOT_TOKEN=y", "DISCORD_BOT_TOKEN=z", "PATH=/bin"}
		got := envWithout(env, EnvDiscordBotToken)
		want := []string{"DISCORD_BOT_TOKEN2=x", "XDISCORD_BOT_TOKEN=y", "PATH=/bin"}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})
}
