package node

import "context"

type election struct {
	term uint64
	args RequestVoteArgs
}

func (n *Node) startElection() (election, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	next := n.persistent
	next.CurrentTerm++
	next.VotedFor = n.cfg.ID

	if err := n.storage.Save(next); err != nil {
		return election{}, err
	}

	n.persistent = next
	n.role = Candidate
	n.leaderState = nil

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

	e := election{
		term: n.persistent.CurrentTerm,
		args: args,
	}

	if n.hasElectionQuorumLocked() {
		n.becomeLeaderLocked()
	}

	return e, nil
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

	if n.hasElectionQuorumLocked() {
		n.becomeLeaderLocked()
	}
}

func (n *Node) hasElectionQuorumLocked() bool {
	granted := 0
	for _, vote := range n.candidateState.Votes {
		if vote {
			granted++
		}
	}

	clusterSize := len(n.cfg.Peers) + 1
	quorum := clusterSize/2 + 1

	return granted >= quorum
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
