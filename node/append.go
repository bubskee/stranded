package node

type appendEntriesCall struct {
	args  AppendEntriesArgs
	reply chan appendEntriesResult
}

type appendEntriesResult struct {
	reply AppendEntriesReply
	err   error
}

func (n *Node) processAppendEntries(
	args AppendEntriesArgs,
) (AppendEntriesReply, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	reply := AppendEntriesReply{
		Term: n.persistent.CurrentTerm,
	}

	if args.Term < n.persistent.CurrentTerm {
		return reply, nil
	}

	// later semantics
	return reply, nil
}
