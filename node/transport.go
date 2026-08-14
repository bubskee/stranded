package node

import "context"

// These mirror the RPC fields from the Raft paper (§5.1–§5.3) and the
// wire types in raftpb, but are owned by the node package so that Node's
// core logic never has a compile-time dependency on gRPC or any other
// transport. grpc_transport.go converts to/from raftpb at the boundary

type RequestVoteArgs struct {
	Term         uint64
	CandidateID  PeerID
	LastLogIndex uint64
	LastLogTerm  uint64
}

type RequestVoteReply struct {
	Term        uint64
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         uint64
	LeaderID     PeerID
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []LogEntry
	LeaderCommit uint64
}

type AppendEntriesReply struct {
	Term          uint64
	Success       bool
	ConflictIndex uint64
	ConflictTerm  uint64
}

// Transport is everything Node needs in order to call a peer. Node depends
// only on this interface, never on a concrete transport
type Transport interface {
	RequestVote(ctx context.Context, peer PeerID, args *RequestVoteArgs) (*RequestVoteReply, error)
	AppendEntries(ctx context.Context, peer PeerID, args *AppendEntriesArgs) (*AppendEntriesReply, error)
}
