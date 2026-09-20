package node

import "context"

type appendReplyEvent struct {
	peer       PeerID
	sentTerm   uint64
	nextIndex  uint64
	matchIndex uint64
	reply      AppendEntriesReply
}

func (n *Node) sendInitialHeartbeats(
	ctx context.Context,
	replies chan<- appendReplyEvent,
) {
	for peer := range n.cfg.Peers {
		n.sendAppendEntries(ctx, peer, replies)
	}
}

func (n *Node) sendAppendEntries(
	ctx context.Context,
	peer PeerID,
	replies chan<- appendReplyEvent,
) {
	n.mu.Lock()

	if n.role != Leader {
		n.mu.Unlock()
		return
	}

	term := n.persistent.CurrentTerm
	nextIndex := n.leaderState.NextIndex[peer]
	leaderCommit := n.volatile.CommitIndex

	prevLogIndex := uint64(0)
	if nextIndex > 1 {
		prevLogIndex = nextIndex - 1
	}

	prevLogTerm := uint64(0)
	if prevLogIndex > 0 {
		for _, entry := range n.persistent.Log {
			if entry.Index == prevLogIndex {
				prevLogTerm = entry.Term
				break
			}
		}
	}

	var entries []LogEntry
	for _, entry := range n.persistent.Log {
		if entry.Index < nextIndex {
			continue
		}

		entry.Command = append([]byte(nil), entry.Command...)
		entries = append(entries, entry)
	}

	args := AppendEntriesArgs{
		Term:         term,
		LeaderID:     n.cfg.ID,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: leaderCommit,
	}

	n.mu.Unlock()

	go n.callAppendEntries(ctx, peer, nextIndex, args, replies)
}

func (n *Node) callAppendEntries(
	ctx context.Context,
	peer PeerID,
	nextIndex uint64,
	args AppendEntriesArgs,
	replies chan<- appendReplyEvent,
) {
	reply, err := n.transport.AppendEntries(ctx, peer, &args)
	if err != nil {
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
		nextIndex:  nextIndex,
		matchIndex: matchIndex,
		reply:      *reply,
	}:
	case <-ctx.Done():
	}
}

func (n *Node) handleAppendEntriesReply(
	event appendReplyEvent,
) (retry bool, commitAdvanced bool, err error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if event.reply.Term > n.persistent.CurrentTerm {
		return false, false, n.becomeFollowerLocked(event.reply.Term)
	}

	if n.role != Leader ||
		event.sentTerm != n.persistent.CurrentTerm ||
		event.reply.Term < n.persistent.CurrentTerm {
		return false, false, nil
	}

	if !event.reply.Success {
		currentNext := n.leaderState.NextIndex[event.peer]

		if event.nextIndex != currentNext {
			return false, false, nil
		}

		if currentNext <= 1 {
			return false, false, nil
		}

		n.leaderState.NextIndex[event.peer] = currentNext - 1
		return true, false, nil
	}

	if event.matchIndex > n.leaderState.MatchIndex[event.peer] {
		n.leaderState.MatchIndex[event.peer] = event.matchIndex
		n.leaderState.NextIndex[event.peer] = event.matchIndex + 1

		commitAdvanced = n.advanceLeaderCommitLocked()
	}

	return false, commitAdvanced, nil
}

func (n *Node) advanceLeaderCommitLocked() bool {
	current := n.volatile.CommitIndex
	currentTerm := n.persistent.CurrentTerm

	clusterSize := len(n.cfg.Peers) + 1
	quorum := clusterSize/2 + 1

	next := current

	for _, entry := range n.persistent.Log {
		if entry.Index <= current {
			continue
		}

		if entry.Term != currentTerm {
			continue
		}

		replicated := 1 // leader itself

		for peer := range n.cfg.Peers {
			if n.leaderState.MatchIndex[peer] >= entry.Index {
				replicated++
			}
		}

		if replicated >= quorum && entry.Index > next {
			next = entry.Index
		}
	}

	n.volatile.CommitIndex = next
	return next > current
}
