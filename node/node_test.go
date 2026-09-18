package node

import (
	"context"
	"testing"

	raftpb "github.com/bubskee/stranded/proto/raftpb/raft/v1"
	"google.golang.org/grpc"
)

type fakeRaftClient struct {
	requestVoteFn   func(context.Context, *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error)
	appendEntriesFn func(context.Context, *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error)
	submitCommandFn func(context.Context, *raftpb.SubmitCommandRequest) (*raftpb.SubmitCommandResponse, error)
}

func (f fakeRaftClient) RequestVote(
	ctx context.Context,
	req *raftpb.RequestVoteRequest,
	opts ...grpc.CallOption,
) (*raftpb.RequestVoteResponse, error) {
	return f.requestVoteFn(ctx, req)
}

func (f fakeRaftClient) AppendEntries(
	ctx context.Context,
	req *raftpb.AppendEntriesRequest,
	opts ...grpc.CallOption,
) (*raftpb.AppendEntriesResponse, error) {
	return f.appendEntriesFn(ctx, req)
}

func (f fakeRaftClient) SubmitCommand(
	ctx context.Context,
	req *raftpb.SubmitCommandRequest,
	opts ...grpc.CallOption,
) (*raftpb.SubmitCommandResponse, error) {
	return f.submitCommandFn(ctx, req)
}

func TestGRPCTransportRequestVote(t *testing.T) {
	var received *raftpb.RequestVoteRequest

	client := fakeRaftClient{
		requestVoteFn: func(
			ctx context.Context,
			req *raftpb.RequestVoteRequest,
		) (*raftpb.RequestVoteResponse, error) {
			received = req

			return &raftpb.RequestVoteResponse{
				Term:        8,
				VoteGranted: true,
			}, nil
		},
	}

	transport := newGRPCTransport(map[PeerID]raftpb.RaftServiceClient{
		"node-b": client,
	})

	got, err := transport.RequestVote(
		context.Background(),
		"node-b",
		&RequestVoteArgs{
			Term:         7,
			CandidateID:  "node-a",
			LastLogIndex: 42,
			LastLogTerm:  6,
		},
	)
	if err != nil {
		t.Fatalf("RequestVote: %v", err)
	}

	if received == nil {
		t.Fatal("client did not receive RequestVote")
	}

	if received.Term != 7 {
		t.Errorf("term: got %d, want 7", received.Term)
	}

	if received.CandidateId != "node-a" {
		t.Errorf("candidate id: got %q, want %q", received.CandidateId, "node-a")
	}

	if received.LastLogIndex != 42 {
		t.Errorf("last log index: got %d, want 42", received.LastLogIndex)
	}

	if received.LastLogTerm != 6 {
		t.Errorf("last log term: got %d, want 6", received.LastLogTerm)
	}

	if got.Term != 8 {
		t.Errorf("reply term: got %d, want 8", got.Term)
	}

	if !got.VoteGranted {
		t.Error("vote granted: got false, want true")
	}
}
