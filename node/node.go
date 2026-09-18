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

	persistent     PersistentState
	volatile       VolatileState
	candidateState *CandidateState // nil unless role == Candidate
	leaderState    *LeaderState    // nil unless role == Leader

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
	n.leaderState = nil

	n.persistent.CurrentTerm++
	n.persistent.VotedFor = n.cfg.ID

	n.candidateState = &CandidateState{
		Votes: map[PeerID]bool{
			n.cfg.ID: true,
		},
	}

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
	reply, err := n.transport.RequestVote(ctx, peer, &e.args)
	if err != nil {
		return
	}

	n.handleVoteReply(peer, e.term, reply)
}

func (n *Node) handleVoteReply(
	peer PeerID,
	electionTerm uint64,
	reply *RequestVoteReply,
) {
	n.mu.Lock()
	defer n.mu.Unlock()

	// A higher term supersedes whatever election/state we're currently in.
	if reply.Term > n.persistent.CurrentTerm {
		n.becomeFollowerLocked(reply.Term)
		return
	}

	// This RPC belongs to an election that is no longer current.
	if n.role != Candidate || n.persistent.CurrentTerm != electionTerm {
		return
	}

	// Stale reply from an earlier term.
	if reply.Term < electionTerm {
		return
	}

	if _, seen := n.candidateState.Votes[peer]; seen {
		return
	}

	n.candidateState.Votes[peer] = reply.VoteGranted

	granted := 0
	for _, vote := range n.candidateState.Votes {
		if vote {
			granted++
		}
	}

	clusterSize := len(n.cfg.Peers) + 1
	quorum := clusterSize/2 + 1

	if granted >= quorum {
		n.becomeLeaderLocked()
	}
}

func (n *Node) becomeLeaderLocked() {
	n.role = Leader
	n.candidateState = nil

	lastIndex := uint64(0)
	if len(n.persistent.Log) > 0 {
		lastIndex = n.persistent.Log[len(n.persistent.Log)-1].Index
	}

	n.leaderState = &LeaderState{
		NextIndex:  make(map[PeerID]uint64, len(n.cfg.Peers)),
		MatchIndex: make(map[PeerID]uint64, len(n.cfg.Peers)),
	}

	for peer := range n.cfg.Peers {
		n.leaderState.NextIndex[peer] = lastIndex + 1
		n.leaderState.MatchIndex[peer] = 0
	}
}

func (n *Node) becomeFollowerLocked(term uint64) {
	n.role = Follower
	n.persistent.CurrentTerm = term
	n.persistent.VotedFor = ""
	n.candidateState = nil
	n.leaderState = nil
}
