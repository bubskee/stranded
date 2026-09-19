package node

import (
	"context"

	raftpb "github.com/bubskee/stranded/proto/raftpb/raft/v1"
)

// grpcServer is the inbound half of the gRPC boundary
type grpcServer struct {
	raftpb.UnimplementedRaftServiceServer
	node *Node
}

func (s *grpcServer) RequestVote(
	ctx context.Context,
	req *raftpb.RequestVoteRequest,
) (*raftpb.RequestVoteResponse, error) {
	reply, err := s.node.submitRequestVote(ctx, RequestVoteArgs{
		Term:         req.Term,
		CandidateID:  PeerID(req.CandidateId),
		LastLogIndex: req.LastLogIndex,
		LastLogTerm:  req.LastLogTerm,
	})
	if err != nil {
		return nil, err
	}

	return &raftpb.RequestVoteResponse{
		Term:        reply.Term,
		VoteGranted: reply.VoteGranted,
	}, nil
}

func (s *grpcServer) AppendEntries(ctx context.Context, args *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error) {
	s.node.mu.Lock()
	defer s.node.mu.Unlock()
	return &raftpb.AppendEntriesResponse{}, nil
}

func (s *grpcServer) ClientRequest(ctx context.Context, args *raftpb.SubmitCommandRequest) (*raftpb.SubmitCommandResponse, error) {
	// TODO: if not leader, reply success=false with leader_hint set
	// TODO: if leader, append to log, replicate, wait for commit, reply
	return nil, nil
}
