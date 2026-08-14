package node

import (
	"context"
	"fmt"

	raftpb "github.com/bubskee/stranded/proto/raftpb/raft/v1"
)

// grpcTransport is the outbound half of the gRPC boundary: it's how Node
// calls peers, and satisfies Transport
type grpcTransport struct {
	clients map[PeerID]raftpb.RaftServiceClient
}

// newGRPCTransport takes already-dialed clients rather than dialing itself,
// so construction/connection-lifecycle concerns stay in cmd/node/main.go
// and this type stays purely about the request/response translation.
func newGRPCTransport(clients map[PeerID]raftpb.RaftServiceClient) *grpcTransport {
	return &grpcTransport{clients: clients}
}

func (t *grpcTransport) RequestVote(ctx context.Context, peer PeerID, args *RequestVoteArgs) (*RequestVoteReply, error) {
	client, ok := t.clients[peer]
	if !ok {
		return nil, fmt.Errorf("transport: no client for peer %q", peer)
	}

	resp, err := client.RequestVote(ctx, &raftpb.RequestVoteRequest{
		Term:         args.Term,
		CandidateId:  string(args.CandidateID),
		LastLogIndex: args.LastLogIndex,
		LastLogTerm:  args.LastLogTerm,
	})
	if err != nil {
		return nil, err
	}

	return &RequestVoteReply{
		Term:        resp.GetTerm(),
		VoteGranted: resp.GetVoteGranted(),
	}, nil
}

func (t *grpcTransport) AppendEntries(ctx context.Context, peer PeerID, args *AppendEntriesArgs) (*AppendEntriesReply, error) {
	client, ok := t.clients[peer]
	if !ok {
		return nil, fmt.Errorf("transport: no client for peer %q", peer)
	}

	entries := make([]*raftpb.LogEntry, len(args.Entries))
	for i, e := range args.Entries {
		entries[i] = &raftpb.LogEntry{
			Term:    e.Term,
			Index:   e.Index,
			Command: e.Command,
		}
	}

	resp, err := client.AppendEntries(ctx, &raftpb.AppendEntriesRequest{
		Term:         args.Term,
		LeaderId:     string(args.LeaderID),
		PrevLogIndex: args.PrevLogIndex,
		PrevLogTerm:  args.PrevLogTerm,
		Entries:      entries,
		LeaderCommit: args.LeaderCommit,
	})
	if err != nil {
		return nil, err
	}

	return &AppendEntriesReply{
		Term:          resp.GetTerm(),
		Success:       resp.GetSuccess(),
		ConflictIndex: resp.GetConflictIndex(),
		ConflictTerm:  resp.GetConflictTerm(),
	}, nil
}
