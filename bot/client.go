package bot

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"time"

	agentv1 "github.com/colinrgodsey/wackypub/pkg/agent/v1"
	"github.com/colinrgodsey/wackypub/pkg/stdio"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// DefaultWackypubBin is the binary spawned to serve the protocol when no override is given.
const DefaultWackypubBin = "wackypub"

// reapGrace bounds how long a spawned server is given to exit before it is force-killed,
// so a wedged child can never hold the bot's shutdown open indefinitely.
const reapGrace = 5 * time.Second

// AgentClient is the protocol surface the bot actually uses. It is a narrowed view of
// agentv1.AgentServiceClient rather than that interface itself: the bot depends on fifteen
// calls out of sixty, so the test doubles only implement those fifteen, and adding a call
// to the bot becomes a deliberate change here instead of silent scope creep.
type AgentClient interface {
	InspectAgent(ctx context.Context, in *agentv1.InspectAgentRequest, opts ...grpc.CallOption) (*agentv1.InspectAgentResponse, error)
	ListAgents(ctx context.Context, in *agentv1.ListAgentsRequest, opts ...grpc.CallOption) (*agentv1.ListAgentsResponse, error)
	ReadSession(ctx context.Context, in *agentv1.ReadSessionRequest, opts ...grpc.CallOption) (*agentv1.ReadSessionResponse, error)
	ReadMemory(ctx context.Context, in *agentv1.ReadMemoryRequest, opts ...grpc.CallOption) (*agentv1.ReadMemoryResponse, error)
	InspectSessionContext(ctx context.Context, in *agentv1.InspectSessionContextRequest, opts ...grpc.CallOption) (*agentv1.InspectSessionContextResponse, error)
	AddUserTurn(ctx context.Context, in *agentv1.AddUserTurnRequest, opts ...grpc.CallOption) (*agentv1.AddUserTurnResponse, error)
	GenerateTurnStream(ctx context.Context, in *agentv1.GenerateTurnStreamRequest, opts ...grpc.CallOption) (agentv1.AgentService_GenerateTurnStreamClient, error)
	CancelTurn(ctx context.Context, in *agentv1.CancelTurnRequest, opts ...grpc.CallOption) (*agentv1.CancelTurnResponse, error)
	CompactSession(ctx context.Context, in *agentv1.CompactSessionRequest, opts ...grpc.CallOption) (*agentv1.CompactSessionResponse, error)
	AsideQuestion(ctx context.Context, in *agentv1.AsideQuestionRequest, opts ...grpc.CallOption) (*agentv1.AsideQuestionResponse, error)
	AddMedia(ctx context.Context, in *agentv1.AddMediaRequest, opts ...grpc.CallOption) (*agentv1.AddMediaResponse, error)
	GetScratchpad(ctx context.Context, in *agentv1.GetScratchpadRequest, opts ...grpc.CallOption) (*agentv1.GetScratchpadResponse, error)
	CreateScratchpad(ctx context.Context, in *agentv1.CreateScratchpadRequest, opts ...grpc.CallOption) (*agentv1.CreateScratchpadResponse, error)
	ReadSessionEvents(ctx context.Context, in *agentv1.ReadSessionEventsRequest, opts ...grpc.CallOption) (*agentv1.ReadSessionEventsResponse, error)
	SubscribeSession(ctx context.Context, in *agentv1.SubscribeSessionRequest, opts ...grpc.CallOption) (agentv1.AgentService_SubscribeSessionClient, error)
}

// SpawnedServer is a wackypub child serving AgentService over stdin/stdout, plus the
// connection dialed through it. Close reaps the child; the bot holds exactly one of these
// for its lifetime, because the watch subscription needs a long-lived stream and paying a
// Go process start on every chat message would add startup latency to each one.
type SpawnedServer struct {
	// The full protocol client is embedded, so SpawnedServer satisfies AgentClient by
	// construction and Close below is the only extra behavior the bot needs.
	agentv1.AgentServiceClient
	conn      *grpc.ClientConn
	stdioConn *stdio.Conn
	cmd       *exec.Cmd
	died      chan error
	waitDone  chan error
	connMu    sync.Mutex
	closed    bool
}

// Cmd returns the underlying exec.Cmd for the spawned child.
func (s *SpawnedServer) Cmd() *exec.Cmd {
	if s == nil {
		return nil
	}
	return s.cmd
}

// Pid returns the process ID of the spawned child process, or 0 if not running.
func (s *SpawnedServer) Pid() int {
	if s == nil || s.cmd == nil || s.cmd.Process == nil {
		return 0
	}
	return s.cmd.Process.Pid
}

// Died returns a receive-only channel that is sent an error when the downstream
// server process exits unexpectedly while running.
func (s *SpawnedServer) Died() <-chan error {
	if s == nil {
		return nil
	}
	return s.died
}

// SpawnServer launches bin in stdio-serve mode with its working directory set to the
// resolved workspace root, so the child resolves agents exactly as the CLI would.
func SpawnServer(ctx context.Context, bin, workspaceDir string) (*SpawnedServer, error) {
	if bin == "" {
		bin = DefaultWackypubBin
	}
	cmd := exec.CommandContext(ctx, bin, "stdio-serve")
	cmd.Dir = workspaceDir

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("opening stdin pipe: %w", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		_ = stdin.Close()
		return nil, fmt.Errorf("opening stdout pipe: %w", err)
	}

	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	cmd.WaitDelay = reapGrace

	if err := cmd.Start(); err != nil {
		_ = stdin.Close()
		_ = stdout.Close()
		return nil, fmt.Errorf("spawning %s stdio-serve in %s: %w", bin, workspaceDir, err)
	}

	// Create client Conn without embedding cmd, so that SpawnedServer exclusively owns cmd.Wait.
	conn := stdio.NewClientConn(nil, stdin, stdout, reapGrace)

	gc, err := grpc.NewClient("passthrough:///stdio",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return conn, nil }),
	)
	if err != nil {
		_ = conn.Close()
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		return nil, fmt.Errorf("constructing stdio grpc client: %w", err)
	}

	server := &SpawnedServer{
		AgentServiceClient: agentv1.NewAgentServiceClient(gc),
		conn:               gc,
		stdioConn:          conn,
		cmd:                cmd,
		died:               make(chan error, 1),
		waitDone:           make(chan error, 1),
	}

	go func() {
		waitErr := cmd.Wait()
		server.connMu.Lock()
		closed := server.closed
		server.connMu.Unlock()
		server.waitDone <- waitErr
		if !closed {
			if waitErr == nil {
				waitErr = errors.New("downstream wackypub protocol server exited unexpectedly")
			}
			server.died <- waitErr
		}
	}()

	return server, nil
}

// dispatch picks the protocol client that serves agentID. Bridge routes are resolved by the
// caller of wackypub, never by its stdio service, which serves native agents only. Refusing
// a routed agent is deliberate: falling through to the native client would let a same-named
// local folder answer for an agent that lives behind a bridge.
func (b *Bot) dispatch(agentID string) (AgentClient, func() error, error) {
	if route, routed := routedAgentRoute(b.WsDir, agentID); routed {
		return nil, nil, fmt.Errorf("agent %q is routed to a bridge (%s); wackydiscord drives native agents over the stdio protocol only", agentID, route)
	}
	return b.activeClient(), func() error { return nil }, nil
}

// Close tears down the connection and reaps the spawned child. Idempotent: the bot calls it
// on shutdown and a failed start may call it too.
func (s *SpawnedServer) Close() error {
	if s == nil {
		return nil
	}
	s.connMu.Lock()
	if s.closed {
		s.connMu.Unlock()
		return nil
	}
	s.closed = true
	s.connMu.Unlock()

	var errs []error
	if s.conn != nil {
		if err := s.conn.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if s.stdioConn != nil {
		if err := s.stdioConn.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	// Wait for child to exit on stdin EOF, escalating to SIGKILL on process group if reapGrace expires
	select {
	case err := <-s.waitDone:
		if err != nil && !errors.Is(err, os.ErrProcessDone) && !strings.Contains(err.Error(), "signal: killed") {
			// clean exit or normal signal
		}
	case <-time.After(reapGrace):
		if s.cmd != nil && s.cmd.Process != nil {
			pgid, err := syscall.Getpgid(s.cmd.Process.Pid)
			if err == nil {
				_ = syscall.Kill(-pgid, syscall.SIGKILL)
			} else {
				_ = s.cmd.Process.Signal(syscall.SIGKILL)
			}
		}
		<-s.waitDone
	}

	return errors.Join(errs...)
}

// Compile-time proof the narrowed view matches the real protocol client.
var _ AgentClient = agentv1.AgentServiceClient(nil)
