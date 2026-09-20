package node

import (
	"context"
	"net"
	"testing"
	"time"

	raftpb "github.com/bubskee/stranded/proto/raftpb/raft/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func TestGRPCRequestVoteEndToEnd(t *testing.T) {
	const bufSize = 1024 * 1024

	listener := bufconn.Listen(bufSize)
	server := grpc.NewServer()

	n, err := New(Config{
		ID:                 "node-a",
		DataDir:            t.TempDir(),
		ElectionTimeoutMin: 10 * time.Second,
		ElectionTimeoutMax: 10 * time.Second,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	raftpb.RegisterRaftServiceServer(
		server,
		NewGRPCServer(n),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = n.Run(ctx)
	}()

	go func() {
		_ = server.Serve(listener)
	}()
	defer server.Stop()

	dialer := func(context.Context, string) (net.Conn, error) {
		return listener.Dial()
	}

	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	defer conn.Close()

	client := raftpb.NewRaftServiceClient(conn)

	callCtx, callCancel := context.WithTimeout(
		context.Background(),
		time.Second,
	)
	defer callCancel()

	reply, err := client.RequestVote(
		callCtx,
		&raftpb.RequestVoteRequest{
			Term:        3,
			CandidateId: "node-b",
		},
	)
	if err != nil {
		t.Fatalf("RequestVote: %v", err)
	}

	if reply.Term != 3 {
		t.Errorf("reply term: got %d, want 3", reply.Term)
	}

	if !reply.VoteGranted {
		t.Fatal("eligible RequestVote was not granted")
	}

	n.mu.Lock()
	term := n.persistent.CurrentTerm
	votedFor := n.persistent.VotedFor
	n.mu.Unlock()

	if term != 3 {
		t.Errorf("node term: got %d, want 3", term)
	}

	if votedFor != "node-b" {
		t.Errorf(
			"node vote: got %q, want %q",
			votedFor,
			"node-b",
		)
	}
}
