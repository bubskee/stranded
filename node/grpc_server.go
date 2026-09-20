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

func (s *grpcServer) AppendEntries(
	ctx context.Context,
	req *raftpb.AppendEntriesRequest,
) (*raftpb.AppendEntriesResponse, error) {
	entries := make([]LogEntry, len(req.Entries))
	for i, entry := range req.Entries {
		entries[i] = LogEntry{
			Term:    entry.Term,
			Index:   entry.Index,
			Command: entry.Command,
		}
	}

	reply, err := s.node.submitAppendEntries(ctx, AppendEntriesArgs{
		Term:         req.Term,
		LeaderID:     PeerID(req.LeaderId),
		PrevLogIndex: req.PrevLogIndex,
		PrevLogTerm:  req.PrevLogTerm,
		Entries:      entries,
		LeaderCommit: req.LeaderCommit,
	})
	if err != nil {
		return nil, err
	}

	return &raftpb.AppendEntriesResponse{
		Term:          reply.Term,
		Success:       reply.Success,
		ConflictIndex: reply.ConflictIndex,
		ConflictTerm:  reply.ConflictTerm,
	}, nil
}

func (s *grpcServer) SubmitCommand(
	ctx context.Context,
	req *raftpb.SubmitCommandRequest,
) (*raftpb.SubmitCommandResponse, error) {
	result, err := s.node.submitClientRequest(ctx, req.Command)
	if err != nil {
		return nil, err
	}

	return &raftpb.SubmitCommandResponse{
		Success:    result.success,
		LeaderHint: string(result.leaderHint),
	}, nil
}

func NewGRPCServer(n *Node) raftpb.RaftServiceServer {
	return &grpcServer{node: n}
}
