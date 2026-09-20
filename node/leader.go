package node

import "context"

type appendReplyEvent struct {
	peer     PeerID
	sentTerm uint64
	reply    AppendEntriesReply
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

	select {
	case replies <- appendReplyEvent{
		peer:     peer,
		sentTerm: args.Term,
		reply:    *reply,
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

	// The rest of leader replication semantics come next.
	return nil
}
