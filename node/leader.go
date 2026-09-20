package node

import "context"

type appendReplyEvent struct {
	peer       PeerID
	sentTerm   uint64
	matchIndex uint64
	reply      AppendEntriesReply
}

func (n *Node) sendInitialHeartbeats(
	ctx context.Context,
	replies chan<- appendReplyEvent,
) {
	n.mu.Lock()

	term := n.persistent.CurrentTerm
	leaderCommit := n.volatile.CommitIndex

	prevLogIndex := uint64(0)
	prevLogTerm := uint64(0)

	if len(n.persistent.Log) > 0 {
		last := n.persistent.Log[len(n.persistent.Log)-1]
		prevLogIndex = last.Index
		prevLogTerm = last.Term
	}

	peers := make([]PeerID, 0, len(n.cfg.Peers))
	for peer := range n.cfg.Peers {
		peers = append(peers, peer)
	}

	n.mu.Unlock()

	for _, peer := range peers {
		go n.sendHeartbeat(
			ctx,
			peer,
			AppendEntriesArgs{
				Term:         term,
				LeaderID:     n.cfg.ID,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				LeaderCommit: leaderCommit,
			},
			replies,
		)
	}
}

func (n *Node) sendHeartbeat(
	ctx context.Context,
	peer PeerID,
	args AppendEntriesArgs,
	replies chan<- appendReplyEvent,
) {
	reply, err := n.transport.AppendEntries(ctx, peer, &args)
	if err != nil {
		// An unavailable follower is ordinary Raft behavior.
		return
	}

	matchIndex := args.PrevLogIndex
	if len(args.Entries) > 0 {
		matchIndex = args.Entries[len(args.Entries)-1].Index
	}

	select {
	case replies <- appendReplyEvent{
		peer:       peer,
		sentTerm:   args.Term,
		matchIndex: matchIndex,
		reply:      *reply,
	}:
	case <-ctx.Done():
	}
}

func (n *Node) handleAppendEntriesReply(
	event appendReplyEvent,
) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if event.reply.Term > n.persistent.CurrentTerm {
		return n.becomeFollowerLocked(event.reply.Term)
	}

	// Ignore replies to an obsolete leadership term.
	if n.role != Leader ||
		event.sentTerm != n.persistent.CurrentTerm ||
		event.reply.Term < n.persistent.CurrentTerm {
		return nil
	}

	if !event.reply.Success {
		// Retry/backtracking comes next.
		return nil
	}

	if event.matchIndex > n.leaderState.MatchIndex[event.peer] {
		n.leaderState.MatchIndex[event.peer] = event.matchIndex
		n.leaderState.NextIndex[event.peer] = event.matchIndex + 1
	}

	return nil
}
