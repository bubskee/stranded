package node

import (
	"context"

	"github.com/bubskee/stranded/proto/raftpb"
)

// grpcServer adapts *Node to the raftpb.RaftServer interface, keeping wire
// concerns out of node.go.
type grpcServer struct {
	raftpb.UnimplementedRaftServer
	node *Node
}

func (s *grpcServer) RequestVote(ctx context.Context, args *raftpb.RequestVoteArgs) (*raftpb.RequestVoteReply, error) {
	s.node.mu.Lock()
	defer s.node.mu.Unlock()
	return s.node.RequestVote(ctx, args)
}

func (s *grpcServer) AppendEntries(ctx context.Context, args *raftpb.AppendEntriesArgs) (*raftpb.AppendEntriesReply, error) {
	s.node.mu.Lock()
	defer s.node.mu.Unlock()
	return s.node.AppendEntries(ctx, args)
}

func (s *grpcServer) ClientRequest(ctx context.Context, args *raftpb.ClientRequestArgs) (*raftpb.ClientRequestReply, error) {
	// TODO: if not leader, reply success=false with leader_hint set
	// TODO: if leader, append to log, replicate, wait for commit, reply
	return nil, nil
}
