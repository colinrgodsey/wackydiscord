package bot

// EnvDiscordBotToken is the standard environment variable name for the Discord
// bot token. It lives in bot (where spawned children get their env) rather
// than in main so the spawn sites and main share one definition.
const EnvDiscordBotToken = "DISCORD_BOT_TOKEN"

// envWithout returns env with every entry named exactly name removed. The
// match is on a full NAME= prefix, so FOO_BAR= never matches name FOO, and
// a malformed entry without '=' cannot match and is kept verbatim.
func envWithout(env []string, name string) []string {
	prefix := name + "="
	out := make([]string, 0, len(env))
	for _, e := range env {
		if len(e) >= len(prefix) && e[:len(prefix)] == prefix {
			continue
		}
		out = append(out, e)
	}
	return out
}
