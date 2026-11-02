# wackydiscord

A Discord bridge for [wackypub](https://github.com/colinrgodsey/wackypub): bind a
Discord channel to a wackypub agent, and the two become one surface. The agent's
session streams live into the channel (assistant turns, tool progress, results),
and user chat in the channel is relayed to the agent as turns. It is the
human-facing window into a wackypub workspace.

## Setup

1. Create a Discord application and bot, and get its token. Follow the standard
   Discord bot setup: <https://docs.openclaw.ai/channels/discord/setup>.
2. Run the bot with the token in the environment:

   ```bash
   DISCORD_BOT_TOKEN=your-token wackydiscord
   ```

   The bot exits with an explicit error if `DISCORD_BOT_TOKEN` is unset, so a
   missing token is never silent.

3. Invite the bot to your server, then run `/claim` (first-time ownership) and
   `/bind <agent_id>` in the channel you want to connect.

For the big picture — workspaces, agents, models, and the rest of the CLI — see
the [wackypub README](https://github.com/colinrgodsey/wackypub).

## Slash commands

All commands are per-channel: they act on the agent bound to the channel they
run in.

| Command | Arguments | What it does |
| --- | --- | --- |
| `/claim` | — | Claim ownership of this bot instance. The first user to run it becomes the owner; other users are blocked until it is released. |
| `/unclaim` | — | Release ownership of this bot instance, so someone else can claim it. |
| `/bind` | `agent` (required) | Bind this channel to a wackypub agent in the workspace. After binding, the agent's session streams here and channel chat drives it. |
| `/unbind` | — | Unbind this channel from its current agent. The agent keeps its session; only the channel link is dropped. |
| `/status` | — | Show the bound agent's status, turn count, and configuration. |
| `/agents` | — | List all agents discovered in the wackypub workspace. |
| `/fill` | `limit` (optional) | Backfill unposted background session turns into the channel — e.g. turns that happened while the bot was offline. `limit` caps how many past turns are backfilled; the default is all unsynced turns. |
| `/add` | `message` (required) | Queue a message into the bound agent's session **without** starting a turn. The message is appended to the session and the agent's next turn sees it — use it to drop notes or context for the agent to absorb later. Rejected while the agent is mid-turn. |
| `/stop` | — | Cancel the bound agent's in-flight turn. Reports cleanly when there is no turn running. |
| `/compact` | `force` (optional) | Archive the bound agent's oldest session turns into `MEMORY.md`. By default this happens automatically at the compaction threshold; `force=true` archives even when the session is still under the threshold. Rejected while the agent is mid-turn. |
| `/aside` | `question` (required) | Ask the bound agent a one-shot question against its current context, with no turn and nothing persisted. The agent answers from what it knows; the session is untouched. Works even while a turn is running. |
| `/verbose` | `enabled` (optional) | Toggle verbose tool-execution output for this channel. Bare `/verbose` flips the current setting; pass `true`/`false` to set it explicitly. |

`/bind`, `/unbind`, `/status`, `/fill`, `/add`, `/stop`, `/compact`, `/aside`,
and `/verbose` require a bound channel and answer with a clear error otherwise.
`/claim` and `/unclaim` gate the whole bot: while claimed, every other command
is refused for non-owners.

## Live session streaming

Beyond the slash commands, a bound channel is a live view of the agent: new
assistant turns, tool calls, and results are posted to the channel as they
happen. There is no refresh and no polling — the bot subscribes to the agent's
session feed and streams each turn into the channel. `/fill` exists for the
gaps (turns that happened while the bot wasn't watching).