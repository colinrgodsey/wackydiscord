package bot

import (
	"context"
	"fmt"
	"net"
	"os/exec"
	"sync"
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
	conn   *grpc.ClientConn
	connMu sync.Mutex
	closed bool
}

// SpawnServer launches bin in stdio-serve mode with its working directory set to the
// resolved workspace root, so the child resolves agents exactly as the CLI would.
func SpawnServer(ctx context.Context, bin, workspaceDir string) (*SpawnedServer, error) {
	if bin == "" {
		bin = DefaultWackypubBin
	}
	cmd := exec.CommandContext(ctx, bin, "stdio-serve")
	cmd.Dir = workspaceDir

	conn, err := stdio.DialCommand(ctx, cmd, reapGrace)
	if err != nil {
		return nil, fmt.Errorf("spawning %s stdio-serve in %s: %w", bin, workspaceDir, err)
	}

	gc, err := grpc.NewClient("passthrough:///stdio",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return conn, nil }),
	)
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("constructing stdio grpc client: %w", err)
	}
	return &SpawnedServer{AgentServiceClient: agentv1.NewAgentServiceClient(gc), conn: gc}, nil
}

// dispatch picks the protocol client that serves agentID. Bridge routes are resolved by the
// caller of wackypub, never by its stdio service, which serves native agents only. Refusing
// a routed agent is deliberate: falling through to the native client would let a same-named
// local folder answer for an agent that lives behind a bridge.
func (b *Bot) dispatch(agentID string) (AgentClient, func() error, error) {
	if route, routed := routedAgentRoute(b.WsDir, agentID); routed {
		return nil, nil, fmt.Errorf("agent %q is routed to a bridge (%s); wackydiscord drives native agents over the stdio protocol only", agentID, route)
	}
	return b.Client, func() error { return nil }, nil
}

// Close tears down the connection and reaps the spawned child. Idempotent: the bot calls it
// on shutdown and a failed start may call it too.
func (s *SpawnedServer) Close() error {
	if s == nil {
		return nil
	}
	s.connMu.Lock()
	defer s.connMu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	return s.conn.Close()
}

// Compile-time proof the narrowed view matches the real protocol client.
var _ AgentClient = agentv1.AgentServiceClient(nil)
