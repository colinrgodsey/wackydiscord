// Command stdiochild is the minimal AgentService-over-stdio child the bot's
// ProcessDialer tests spawn in place of wackypub. It speaks the same envelope
// as wackypub stdio-serve (gRPC over stdin/stdout, GracefulStop on stdin EOF)
// but serves no workspace, so the tests exercise process lifecycle, not agent
// behavior.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/grpc"

	agentv1 "github.com/colinrgodsey/wackypub/pkg/agent/v1"
	"github.com/colinrgodsey/wackypub/pkg/stdio"
)

type service struct {
	agentv1.UnimplementedAgentServiceServer
}

// ListAgents reports the child's own pid as its single agent id so a test can
// tell a replacement child from the original one.
func (s *service) ListAgents(ctx context.Context, req *agentv1.ListAgentsRequest) (*agentv1.ListAgentsResponse, error) {
	return &agentv1.ListAgentsResponse{AgentIds: []string{fmt.Sprintf("child%d", os.Getpid())}}, nil
}

// SubscribeSession holds the stream open until the client or the process goes
// away, like a live wackypub watch would.
func (s *service) SubscribeSession(req *agentv1.SubscribeSessionRequest, stream agentv1.AgentService_SubscribeSessionServer) error {
	<-stream.Context().Done()
	return stream.Context().Err()
}

func main() {
	grpcServer := grpc.NewServer()
	agentv1.RegisterAgentServiceServer(grpcServer, &service{})

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	conn := stdio.NewConn(os.Stdin, os.Stdout)
	if err := stdio.ServeContext(ctx, grpcServer, conn); err != nil {
		fmt.Fprintf(os.Stderr, "stdiochild: %v\n", err)
		os.Exit(1)
	}
}
