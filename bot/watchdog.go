package bot

import (
	"context"
	"fmt"
	"log"
	"time"

	"google.golang.org/grpc/connectivity"
)

// childDeathWatchdog bounds how long the stdio transport may stay broken before the bot
// gives up and exits non-zero for a systemd restart. A single child death never trips it:
// gRPC reconnects through the ProcessDialer with a fresh child, and the channel returns
// to Ready while the clock is still running.
var childDeathWatchdog = 15 * time.Second

// watchConnectivity starts the transport watchdog and returns a channel that receives one
// fatal error when the stdio backend is broken beyond recovery, in either of the two ways
// the dialer can be:
//
//   - a spawn failure that cannot recover on its own (e.g. the wackypub binary is
//     missing); that trips immediately, no threshold wait, or
//   - the channel staying out of Ready past childDeathWatchdog, whichever dial failures
//     are keeping it there.
//
// The clock counts any time the channel is not Ready - including the Idle and Connecting
// states a broken channel cycles through on each re-dial - and only a return to Ready
// clears it. A channel that is Idle before its first dial (startup, no RPCs yet) is not
// an outage. A nil receiver (test-injected client, no real server) returns a channel that
// never fires. The channel is always closed when the watchdog goroutine exits.
func (s *SpawnedServer) watchConnectivity(ctx context.Context) <-chan error {
	return s.watchConnectivityFor(ctx, childDeathWatchdog)
}

// watchConnectivityFor is watchConnectivity with an explicit threshold, for tests.
func (s *SpawnedServer) watchConnectivityFor(ctx context.Context, threshold time.Duration) <-chan error {
	fatal := make(chan error, 1)
	if s == nil || s.conn == nil || s.dialer == nil {
		return fatal
	}
	go func() {
		defer close(fatal)
		fire := func(err error) {
			select {
			case fatal <- err:
			case <-ctx.Done():
			}
		}
		// The channel-level state is the balancer's aggregate: while a transport is
		// broken and re-dialing it sits in a steady TRANSIENT_FAILURE with no state
		// transitions, so the watchdog polls the state rather than waiting on change
		// notifications. One locked state read per tick is the cost.
		tick := time.NewTicker(100 * time.Millisecond)
		defer tick.Stop()
		state := s.conn.GetState()
		everActive := state != connectivity.Idle
		var downSince time.Time
		var lastNudge time.Time
		for {
			if state == connectivity.Ready {
				downSince = time.Time{}
			} else if state != connectivity.Idle || everActive {
				// A permanent spawn failure cannot be fixed by any amount of re-dialing.
				if err := s.dialer.lastDialError(); isPermanentSpawnError(err) {

					log.Printf("🛑 stdio spawn failure will not recover (%v); exiting for systemd restart", err)
					fire(fmt.Errorf("downstream wackypub spawn failure is unrecoverable: %w", err))
					return
				}
				if downSince.IsZero() {
					downSince = time.Now()
				} else if time.Since(downSince) >= threshold {
					broken := time.Since(downSince).Round(time.Second)
					log.Printf("🛑 stdio transport broken for %s; exiting for systemd restart", broken)
					fire(fmt.Errorf("stdio transport stayed broken for %s", broken))
					return
				}
			}
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				if ns := s.conn.GetState(); ns != state {
					if ns != connectivity.Idle {
						everActive = true
					}
					state = ns
				}
				// A dead child with an idle channel is a transport disconnect gRPC does not
				// re-dial on its own: the subchannel sits idle waiting for a request that
				// never comes. Nudge it so the dialer gets its next spawn.
				if state == connectivity.Idle && everActive && !s.dialer.childAlive() && time.Since(lastNudge) >= 250*time.Millisecond {
					s.conn.Connect()
					lastNudge = time.Now()
				}
			}
		}
	}()
	return fatal
}
