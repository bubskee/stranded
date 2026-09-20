package node

import "context"

type clientRequestCall struct {
	command []byte
	reply   chan clientRequestResult
}

type clientRequestResult struct {
	success    bool
	leaderHint PeerID
	err        error
}

func (n *Node) submitClientRequest(
	ctx context.Context,
	command []byte,
) (clientRequestResult, error) {
	replyCh := make(chan clientRequestResult, 1)

	call := clientRequestCall{
		command: append([]byte(nil), command...),
		reply:   replyCh,
	}

	select {
	case n.clientRequestCh <- call:
	case <-ctx.Done():
		return clientRequestResult{}, ctx.Err()
	}

	select {
	case result := <-replyCh:
		return result, result.err
	case <-ctx.Done():
		return clientRequestResult{}, ctx.Err()
	}
}

func (n *Node) appendClientCommand(
	command []byte,
	reply chan clientRequestResult,
) (commitAdvanced bool, err error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	lastIndex := uint64(0)
	if len(n.persistent.Log) > 0 {
		lastIndex = n.persistent.Log[len(n.persistent.Log)-1].Index
	}

	entry := LogEntry{
		Term:    n.persistent.CurrentTerm,
		Index:   lastIndex + 1,
		Command: append([]byte(nil), command...),
	}

	next := n.persistent
	next.Log = append([]LogEntry(nil), n.persistent.Log...)
	next.Log = append(next.Log, entry)

	// Durable before publication or replication.
	if err := n.storage.Save(next); err != nil {
		return false, err
	}

	n.persistent = next

	if n.pendingClientRequests == nil {
		n.pendingClientRequests =
			make(map[uint64]chan clientRequestResult)
	}

	n.pendingClientRequests[entry.Index] = reply

	// Important for a single-node cluster.
	return n.advanceLeaderCommitLocked(), nil
}

func (n *Node) completeAppliedClientRequests() {
	n.mu.Lock()

	lastApplied := n.volatile.LastApplied

	var replies []chan clientRequestResult

	for index, reply := range n.pendingClientRequests {
		if index > lastApplied {
			continue
		}

		replies = append(replies, reply)
		delete(n.pendingClientRequests, index)
	}

	n.mu.Unlock()

	for _, reply := range replies {
		reply <- clientRequestResult{
			success: true,
		}
	}
}

// Caller must hold n.mu.
func (n *Node) takePendingClientRequestsLocked() []chan clientRequestResult {
	var replies []chan clientRequestResult

	for index, reply := range n.pendingClientRequests {
		replies = append(replies, reply)
		delete(n.pendingClientRequests, index)
	}

	return replies
}

func failClientRequests(replies []chan clientRequestResult) {
	for _, reply := range replies {
		reply <- clientRequestResult{
			success: false,
		}
	}
}
