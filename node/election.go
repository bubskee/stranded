package node

import "context"

type election struct {
	term uint64
	args RequestVoteArgs
}

type requestVoteCall struct {
	args  RequestVoteArgs
	reply chan requestVoteResult
}

type requestVoteResult struct {
	reply RequestVoteReply
	err   error
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

func (n *Node) sendRequestVotes(
	ctx context.Context,
	e election,
	errCh chan<- error,
) {
	for peer := range n.cfg.Peers {
		go func(peer PeerID) {
			if err := n.requestVote(ctx, peer, e); err != nil {
				select {
				case errCh <- err:
				case <-ctx.Done():
				}
			}
		}(peer)
	}
}

func (n *Node) requestVote(
	ctx context.Context,
	peer PeerID,
	e election,
) error {
	reply, err := n.transport.RequestVote(ctx, peer, &e.args)
	if err != nil {
		// An unavailable peer is ordinary Raft behavior, not a node failure.
		return nil
	}

	return n.handleVoteReply(peer, e.term, reply)
}

func (n *Node) handleVoteReply(
	peer PeerID,
	electionTerm uint64,
	reply *RequestVoteReply,
) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if reply.Term > n.persistent.CurrentTerm {
		return n.becomeFollowerLocked(reply.Term)
	}

	if n.role != Candidate || n.persistent.CurrentTerm != electionTerm {
		return nil
	}

	if reply.Term < electionTerm {
		return nil
	}

	if _, seen := n.candidateState.Votes[peer]; seen {
		return nil
	}

	n.candidateState.Votes[peer] = reply.VoteGranted

	if n.hasElectionQuorumLocked() {
		n.becomeLeaderLocked()
	}

	return nil
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

func (n *Node) becomeFollowerLocked(term uint64) error {
	next := n.persistent
	next.CurrentTerm = term
	next.VotedFor = ""

	if err := n.storage.Save(next); err != nil {
		return err
	}

	n.persistent = next
	n.role = Follower
	n.candidateState = nil
	n.leaderState = nil

	return nil
}

func (n *Node) submitRequestVote(
	ctx context.Context,
	args RequestVoteArgs,
) (RequestVoteReply, error) {
	replyCh := make(chan requestVoteResult, 1)

	call := requestVoteCall{
		args:  args,
		reply: replyCh,
	}

	select {
	case n.requestVoteCh <- call:
	case <-ctx.Done():
		return RequestVoteReply{}, ctx.Err()
	}

	select {
	case result := <-replyCh:
		return result.reply, result.err
	case <-ctx.Done():
		return RequestVoteReply{}, ctx.Err()
	}
}

func (n *Node) processRequestVote(
	args RequestVoteArgs,
) (RequestVoteReply, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	reply := RequestVoteReply{
		Term: n.persistent.CurrentTerm,
	}

	if args.Term < n.persistent.CurrentTerm {
		return reply, nil
	}

	if args.Term > n.persistent.CurrentTerm {
		if err := n.becomeFollowerLocked(args.Term); err != nil {
			return RequestVoteReply{}, err
		}

		reply.Term = n.persistent.CurrentTerm
	}

	if n.persistent.VotedFor != "" &&
		n.persistent.VotedFor != args.CandidateID {
		return reply, nil
	}

	if !n.candidateLogUpToDateLocked(args) {
		return reply, nil
	}

	if n.persistent.VotedFor == "" {
		next := n.persistent
		next.VotedFor = args.CandidateID

		if err := n.storage.Save(next); err != nil {
			return RequestVoteReply{}, err
		}

		n.persistent = next
	}

	reply.Term = n.persistent.CurrentTerm
	reply.VoteGranted = true

	// More RequestVote semantics next.
	return reply, nil
}

func (n *Node) candidateLogUpToDateLocked(args RequestVoteArgs) bool {
	lastIndex := uint64(0)
	lastTerm := uint64(0)

	if len(n.persistent.Log) > 0 {
		last := n.persistent.Log[len(n.persistent.Log)-1]
		lastIndex = last.Index
		lastTerm = last.Term
	}

	if args.LastLogTerm != lastTerm {
		return args.LastLogTerm > lastTerm
	}

	return args.LastLogIndex >= lastIndex
}
