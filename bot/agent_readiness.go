package bot

import (
	"context"
	"fmt"
	"log"
	"time"

	agentv1 "github.com/colinrgodsey/wackypub/pkg/agent/v1"
)

const (
	// agentReadinessTimeout bounds how long startup waits for the spawned child to answer its
	// first call. grpc.NewClient is lazy, so a successful SpawnServer proves the pipe exists and
	// nothing more: the service may still be booting when the first Discord event lands.
	agentReadinessTimeout = 5 * time.Second

	agentReadinessPollEvery = 100 * time.Millisecond

	// validationRetryAttempts bounds the per-message retry of a failed inspection. The window
	// it exists to cover is short, so the retry is short too: a genuinely dead child should
	// produce an honest failure rather than a stalled turn handler.
	validationRetryAttempts = 3

	validationRetryBackoff = 150 * time.Millisecond
)

// agentPresence is the outcome of asking the agent service about one agent. The three cases
// used to be one: an unreachable service and an agent that is genuinely gone produced the same
// "does not exist in workspace" notice, which sent operators hunting for a binding that was
// fine when the real cause was a child that had not finished starting or had just died.
type agentPresence int

const (
	// agentAvailable means the service answered and the agent folder exists.
	agentAvailable agentPresence = iota

	// agentAbsent means the service answered normally and reported the agent folder missing.
	// Only this case may be reported to a user as "does not exist".
	agentAbsent

	// agentServiceUnreachable means the call never produced an answer at all, whether the
	// child was not listening yet, has died, or rejected the request. None of those entitle
	// the caller to claim anything about the agent's existence.
	agentServiceUnreachable
)

// classifyAgent asks the service about agentID and sorts the outcome into the three cases.
// A nil response with no error is treated as unreachable: there is no answer to reason about,
// and guessing "absent" from a missing answer is the bug this file exists to fix.
func (b *Bot) classifyAgent(ctx context.Context, agentID string) (agentPresence, *agentv1.InspectAgentResponse, error) {
	if b.Client == nil {
		return agentServiceUnreachable, nil, fmt.Errorf("agent protocol client is not initialised")
	}
	insp, err := b.Client.InspectAgent(ctx, &agentv1.InspectAgentRequest{AgentId: agentID, WorkspaceDir: b.WsDir})
	if err != nil {
		return agentServiceUnreachable, nil, err
	}
	if insp == nil {
		return agentServiceUnreachable, nil, fmt.Errorf("agent service returned no inspection for %q", agentID)
	}
	if !insp.GetAgentDirExists() {
		return agentAbsent, insp, nil
	}
	return agentAvailable, insp, nil
}

// classifyAgentWithRetry classifies once and, if the service is unreachable, tries a few more
// times before giving up. Only transport failures are retried: an answer of "that agent folder
// does not exist" is final and re-asking wastes the user's turn.
func (b *Bot) classifyAgentWithRetry(ctx context.Context, agentID string) (agentPresence, *agentv1.InspectAgentResponse, error) {
	presence, insp, err := b.classifyAgent(ctx, agentID)
	for attempt := 1; presence == agentServiceUnreachable && attempt < validationRetryAttempts; attempt++ {
		select {
		case <-ctx.Done():
			return presence, insp, ctx.Err()
		case <-time.After(validationRetryBackoff):
		}
		presence, insp, err = b.classifyAgent(ctx, agentID)
	}
	return presence, insp, err
}

// awaitAgentService blocks until the spawned child answers a cheap call, or the timeout goes
// off. It is a startup convenience, not a gate on correctness: the per-message path retries and
// reports accurately regardless, so a caller that decides to start anyway is not unsafe.
func awaitAgentService(ctx context.Context, client AgentClient, wsDir string, timeout, interval time.Duration) error {
	if client == nil {
		return fmt.Errorf("agent protocol client is not initialised")
	}
	deadline := time.Now().Add(timeout)
	var lastErr error
	for {
		_, err := client.ListAgents(ctx, &agentv1.ListAgentsRequest{WorkspaceDir: wsDir})
		if err == nil {
			return nil
		}
		lastErr = err
		if time.Now().After(deadline) {
			return fmt.Errorf("agent service did not become ready within %s: %w", timeout, lastErr)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("waiting for agent service: %w", ctx.Err())
		case <-time.After(interval):
		}
	}
}

// warnIfServiceUnreachable records that startup checks could not reach the service. The bot
// still starts: bindings are left intact and the first message either succeeds once the child
// comes up or fails with a message that names the service rather than blaming the binding.
func warnIfServiceUnreachable(err error, wsDir string) {
	log.Printf("\u26a0\ufe0f agent service is not answering yet (workspace %s): %v - starting anyway, messages will report readiness rather than blame bindings", wsDir, err)
}
