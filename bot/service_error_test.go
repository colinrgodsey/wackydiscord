package bot

import (
	"context"
	"errors"
	"strings"
	"syscall"
	"testing"
	"time"

	agentv1 "github.com/colinrgodsey/wackypub/pkg/agent/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestIsChildDeathError(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"colin live signature", status.Error(codes.Unavailable, "error reading from server: read |0: file already closed"), true},
		{"kill caught between reads", status.Error(codes.Unavailable, "error reading from server: EOF"), true},
		{"closed pipe alone", status.Error(codes.Unavailable, "read |0: file already closed"), true},
		{"other unavailable stays raw", status.Error(codes.Unavailable, "connection refused"), false},
		{"right text wrong code", status.Error(codes.Internal, "error reading from server: EOF"), false},
		{"bare text without a status", errors.New("error reading from server: EOF"), false},
		{"canceled is not death", status.Error(codes.Canceled, "context canceled"), false},
		{"nil", nil, false},
	}
	for _, tc := range cases {
		if got := isChildDeathError(tc.err); got != tc.want {
			t.Errorf("%s: isChildDeathError(%v) = %v, want %v", tc.name, tc.err, got, tc.want)
		}
	}
}

func TestDescribeServiceErrorDistinguishesOurKillFromCrash(t *testing.T) {
	srv := spawnTestServer(t, testChildBin)
	b := &Bot{server: srv}
	dead := status.Error(codes.Unavailable, "error reading from server: read |0: file already closed")

	if text, ok := b.describeServiceError(dead); !ok || text != serviceCrashNotice {
		t.Errorf("death with no kill by us must read as a crash, got %q (ok=%v)", text, ok)
	}

	if err := srv.Close(); err != nil {
		t.Fatalf("closing the server: %v", err)
	}
	if text, ok := b.describeServiceError(dead); !ok || text != serviceRestartNotice {
		t.Errorf("death after our own teardown must read as a restart, got %q (ok=%v)", text, ok)
	}

	if text, ok := b.describeServiceError(status.Error(codes.Unavailable, "connection refused")); ok {
		t.Errorf("a spawn failure must stay on the raw path, got %q", text)
	}
}

func TestDescribeServiceErrorWithoutAServer(t *testing.T) {
	b := &Bot{}
	dead := status.Error(codes.Unavailable, "error reading from server: EOF")
	if text, ok := b.describeServiceError(dead); !ok || text != serviceCrashNotice {
		t.Errorf("an injected-client bot must not panic and must report a crash, got %q (ok=%v)", text, ok)
	}
}

// Acceptance: the failure a channel actually sees when the serving child dies under a live
// call. The child is really spawned and really SIGKILLed, so the error is the transport's own
// rather than a hand-written status, and the notice must not be raw rpc text.
func TestKilledChildMidCallMapsToFriendlyNotice(t *testing.T) {
	for attempt := 1; attempt <= 5; attempt++ {
		srv := spawnTestServer(t, testChildBin)
		pid := listChildPID(t, srv)
		if err := syscall.Kill(pid, syscall.SIGKILL); err != nil {
			t.Fatalf("SIGKILL child %d: %v", pid, err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
		_, err := srv.ListAgents(ctx, &agentv1.ListAgentsRequest{})
		cancel()
		if err == nil {
			// The re-dial landed before this call, so the death window was missed.
			srv.Close()
			continue
		}

		b := &Bot{server: srv}
		text, ok := b.describeServiceError(err)
		if !ok {
			t.Fatalf("attempt %d: a real child death did not map, err=%v", attempt, err)
		}
		if strings.Contains(text, "rpc error") || strings.Contains(text, "Unavailable") {
			t.Fatalf("raw rpc text leaked into the notice: %q", text)
		}
		if text != serviceCrashNotice {
			t.Fatalf("a child we did not kill must read as a crash, got %q", text)
		}
		return
	}
	t.Fatal("no post-kill failure observed across 5 attempts, so the death window was never exercised")
}
