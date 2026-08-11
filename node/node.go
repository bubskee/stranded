package node

import (
	"context"
	"sync"
	"time"

	"github.com/bubskee/stranded/proto/raftpb"
)

type Node struct {
	mu   sync.Mutex
	cfg  Config
	role Role

	persistent  PersistentState
	volatile    VolatileState
	leaderState *LeaderState // nil unless role == Leader

	peers map[PeerID]raftpb.RaftClient

	electionTimer *time.Timer

	applyCh chan LogEntry // committed entries, ready for the state machine
}

func New(cfg Config) (*Node, error) {
	// TODO: dial peers, build raftpb.RaftClient per peer
	// TODO: loadPersistentState(cfg.DataDir) — fresh PersistentState{} if none exists
	return nil, nil
}

// Run drives the node's main loop until ctx is cancelled: election timeout
// handling and role transitions live here.
func (n *Node) Run(ctx context.Context) error {
	// TODO
	return nil
}

func (n *Node) resetElectionTimer() {
	// TODO: randomized duration in [ElectionTimeoutMin, ElectionTimeoutMax)
}
