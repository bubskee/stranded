package node

import (
	"context"
	"math/rand/v2"
	"sync"
	"time"
)

type Node struct {
	mu   sync.Mutex
	cfg  Config
	role Role

	persistent  PersistentState
	volatile    VolatileState
	leaderState *LeaderState // nil unless role == Leader

	transport Transport

	electionTimer *time.Timer

	applyCh chan LogEntry // committed entries, ready for the state machine
}

func New(cfg Config) (*Node, error) {
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
	min := n.cfg.ElectionTimeoutMin
	max := n.cfg.ElectionTimeoutMax

	timeout := min
	if max > min {
		timeout += rand.N(max - min)
	}

	if n.electionTimer == nil {
		n.electionTimer = time.NewTimer(timeout)
		return
	}

	n.electionTimer.Reset(timeout)
}

func (n *Node) RequestVote(ctx context.Context) {
	// TODO: election impl
}

func (n *Node) AppendEntries(ctx context.Context) {
	// TODO: persist impl
}
