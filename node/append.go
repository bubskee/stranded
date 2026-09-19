package node

import "context"

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

	if args.Term > n.persistent.CurrentTerm {
		if err := n.becomeFollowerLocked(args.Term); err != nil {
			return AppendEntriesReply{}, err
		}

		reply.Term = n.persistent.CurrentTerm
	}

	if args.Term == n.persistent.CurrentTerm && n.role == Candidate {
		n.role = Follower
		n.candidateState = nil
		n.leaderState = nil
	}

	if !n.logMatchesPrevLocked(args) {
		return reply, nil
	}

	reply.Success = true
	return reply, nil
}

func (n *Node) submitAppendEntries(
	ctx context.Context,
	args AppendEntriesArgs,
) (AppendEntriesReply, error) {
	replyCh := make(chan appendEntriesResult, 1)

	call := appendEntriesCall{
		args:  args,
		reply: replyCh,
	}

	select {
	case n.appendEntriesCh <- call:
	case <-ctx.Done():
		return AppendEntriesReply{}, ctx.Err()
	}

	select {
	case result := <-replyCh:
		return result.reply, result.err
	case <-ctx.Done():
		return AppendEntriesReply{}, ctx.Err()
	}
}

func (n *Node) logMatchesPrevLocked(args AppendEntriesArgs) bool {
	if args.PrevLogIndex == 0 {
		return args.PrevLogTerm == 0
	}

	// later
	return false
}
