package bot

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/bwmarrin/discordgo"
)

// modelCLITimeout bounds the bind-time model lookup. The model CLI spawns the
// bridge harness and establishes a session, so its cost class is the
// bind-time InspectAgent bridge spawn the same command already performs;
// the timeout exists so a wedged harness cannot hang the bind indefinitely.
const modelCLITimeout = 30 * time.Second

// bridgedModelGet resolves the harness-confirmed model of a bridged agent
// through the model CLI (tasks/wackyacp/model-cli consumer path, replaces
// the wackypub#81 RPC): it execs the REMOTE_MANIFEST route's bridge binary
// in model-CLI form, with CWD = the agent folder, and parses the JSON
// {"model": ..., "configOptions": ...} on stdout. The bot remains a pure
// stdio-protocol client for chat; this exec is the only out-of-band touch.
// The route's first token is the wackyacp bridge binary, which owns the
// model verb and re-resolves the harness from the same manifest.
// Any failure returns an error and the caller degrades to the placeholder.
func (b *Bot) bridgedModelGet(agentID string) (string, error) {
	route, routed := routedAgentRoute(b.WsDir, agentID)
	if !routed {
		return "", fmt.Errorf("agent %s has no route in %s", agentID, filepath.Join(b.WsDir, RemoteManifestFile))
	}
	_, cmdPart, _ := strings.Cut(route, ":")
	fields := strings.Fields(cmdPart)
	if len(fields) == 0 {
		return "", fmt.Errorf("route for %s has no command", agentID)
	}
	agentFolder := filepath.Join(b.WsDir, agentID)
	ctx, cancel := context.WithTimeout(context.Background(), modelCLITimeout)
	defer cancel()
	var stderr bytes.Buffer
	cmd := exec.CommandContext(ctx, fields[0], "model", "get")
	cmd.Dir = agentFolder
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("model CLI for %s: %v: %s", agentID, err, bytes.TrimSpace(stderr.Bytes()))
	}
	var resp struct {
		Model string `json:"model"`
	}
	if err := json.Unmarshal(out, &resp); err != nil {
		return "", fmt.Errorf("model CLI for %s: %v: %s", agentID, err, bytes.TrimSpace(out))
	}
	if resp.Model == "" {
		return "", fmt.Errorf("model CLI for %s returned an empty model: %s", agentID, bytes.TrimSpace(out))
	}
	return resp.Model, nil
}

// confirmBridgedModel stores the model-CLI-confirmed model on the channel
// binding and edits the bind ack in place with it. The binding is re-read
// under the sync lock so an unbind or rebind during the CLI exec cannot
// resurrect a stale binding or edit a superseded ack.
func (b *Bot) confirmBridgedModel(s *discordgo.Session, i *discordgo.InteractionCreate, agentID, model string) {
	syncUnlock := b.State.LockChannelSync(i.ChannelID)
	defer syncUnlock()
	bnd := b.State.GetBinding(i.ChannelID)
	if bnd == nil || bnd.AgentID != agentID {
		return
	}
	bnd.Model = model
	if err := b.State.SetBinding(bnd); err != nil {
		log.Printf("⚠️ failed to save confirmed model for channel %s: %v", i.ChannelID, err)
		return
	}
	b.editInteractionResponse(s, i, bindAckMessage(agentID, model))
}
