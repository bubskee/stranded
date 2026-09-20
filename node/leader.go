package node

import "context"

func (n *Node) sendInitialHeartbeats(ctx context.Context) {
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
		go func(peer PeerID) {
			args := AppendEntriesArgs{
				Term:         term,
				LeaderID:     n.cfg.ID,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				LeaderCommit: leaderCommit,
			}

			_, _ = n.transport.AppendEntries(ctx, peer, &args)
		}(peer)
	}
}
