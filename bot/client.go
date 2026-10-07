package bot

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	agentv1 "github.com/colinrgodsey/wackypub/pkg/agent/v1"
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
	AddAndGenerateTurnStream(ctx context.Context, in *agentv1.AddAndGenerateTurnStreamRequest, opts ...grpc.CallOption) (agentv1.AgentService_AddAndGenerateTurnStreamClient, error)
	SubscribeSession(ctx context.Context, in *agentv1.SubscribeSessionRequest, opts ...grpc.CallOption) (agentv1.AgentService_SubscribeSessionClient, error)
}

// SpawnedServer is the protocol surface plus lifecycle for the bot's stdio backend. The
// child process itself is owned by a ProcessDialer: every gRPC dial spawns a fresh
// wackypub stdio-serve, so a child death is a transport disconnect that gRPC heals with a
// re-dial, not a dead conn the bot must restart over. The watch subscription still needs
// a long-lived stream, which the per-dial child provides for as long as it lives.
type SpawnedServer struct {
	// The full protocol client is embedded, so SpawnedServer satisfies AgentClient by
	// construction and Close below is the only extra behavior the bot needs.
	agentv1.AgentServiceClient
	conn   *grpc.ClientConn
	dialer *ProcessDialer
	connMu sync.Mutex
	closed bool
}

// SpawnServer constructs the stdio protocol client with reconnection: the dialer spawns a
// fresh child per dial, and the gRPC idle reaper is disabled (idle timeout 0), because its
// 30-minute default closed the transport on idle, EOF'd the child's stdin, and killed it -
// the observed restart spiral.
func SpawnServer(ctx context.Context, bin, workspaceDir string) (*SpawnedServer, error) {
	return spawnServerWithIdleTimeout(ctx, bin, workspaceDir, 0)
}

// spawnServerWithIdleTimeout is SpawnServer with an explicit gRPC idle timeout, for tests
// that exercise the idle mechanism on a short clock.
func spawnServerWithIdleTimeout(ctx context.Context, bin, workspaceDir string, idleTimeout time.Duration) (*SpawnedServer, error) {
	dialer := NewProcessDialer(ctx, bin, workspaceDir)
	gc, err := grpc.NewClient("passthrough:///stdio",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(dialer.Dial),
		grpc.WithIdleTimeout(idleTimeout),
	)
	if err != nil {
		_ = dialer.Close()
		return nil, fmt.Errorf("constructing stdio grpc client: %w", err)
	}
	return &SpawnedServer{AgentServiceClient: agentv1.NewAgentServiceClient(gc), conn: gc, dialer: dialer}, nil
}

// Close tears down the client: refuses new spawns, closes the transport, reaps the live
// child. Idempotent: the bot calls it on shutdown and a failed start may call it too.
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
	var errs []error
	if s.dialer != nil {
		if err := s.dialer.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if err := s.conn.Close(); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

// Compile-time proof the narrowed view matches the real protocol client.
var _ AgentClient = agentv1.AgentServiceClient(nil)
