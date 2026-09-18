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

type election struct {
	term uint64
	args RequestVoteArgs
}

func New(cfg Config) (*Node, error) {
	// TODO: loadPersistentState(cfg.DataDir) — fresh PersistentState{} if none exists
	return nil, nil
}

// Run drives the node's main loop until ctx is cancelled: election timeout
// handling and role transitions live here.
func (n *Node) Run(ctx context.Context) error {
	n.resetElectionTimer()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case <-n.electionTimer.C:
			e := n.startElection()
			n.resetElectionTimer()
			n.sendRequestVotes(ctx, e)
		}
	}
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

func (n *Node) AppendEntries(ctx context.Context) {
	// TODO: persist impl
}

func (n *Node) startElection() election {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.role = Candidate
	n.persistent.CurrentTerm++
	n.persistent.VotedFor = n.cfg.ID

	args := RequestVoteArgs{
		Term:        n.persistent.CurrentTerm,
		CandidateID: n.cfg.ID,
	}

	if len(n.persistent.Log) > 0 {
		last := n.persistent.Log[len(n.persistent.Log)-1]
		args.LastLogIndex = last.Index
		args.LastLogTerm = last.Term
	}

	return election{
		term: n.persistent.CurrentTerm,
		args: args,
	}
}

func (n *Node) sendRequestVotes(ctx context.Context, e election) {
	for peer := range n.cfg.Peers {
		go n.requestVote(ctx, peer, e)
	}
}

func (n *Node) requestVote(ctx context.Context, peer PeerID, e election) {
	_, _ = n.transport.RequestVote(ctx, peer, &e.args)
}
