package node

import (
	"context"
)

type appendEntriesCall struct {
	args  AppendEntriesArgs
	reply chan appendEntriesResult
}

type appendEntriesResult struct {
	reply AppendEntriesReply
	err   error
}

type appendEntriesProcessResult struct {
	reply         AppendEntriesReply
	failedClients []chan clientRequestResult
}

func (n *Node) processAppendEntries(
	args AppendEntriesArgs,
) (appendEntriesProcessResult, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	result := appendEntriesProcessResult{
		reply: AppendEntriesReply{
			Term: n.persistent.CurrentTerm,
		},
	}

	if args.Term < n.persistent.CurrentTerm {
		return result, nil
	}

	if args.Term > n.persistent.CurrentTerm {
		pending, err := n.becomeFollowerLocked(args.Term)
		if err != nil {
			return result, err
		}

		result.failedClients = pending
		result.reply.Term = n.persistent.CurrentTerm
	}

	if args.Term == n.persistent.CurrentTerm && n.role == Candidate {
		n.role = Follower
		n.candidateState = nil
		n.leaderState = nil
	}

	if !n.logMatchesPrevLocked(args) {
		return result, nil
	}

	if len(args.Entries) > 0 {
		next := n.persistent
		next.Log = append([]LogEntry(nil), n.persistent.Log...)

		changed := false

		for i, incoming := range args.Entries {
			found := false

			for j, existing := range next.Log {
				if existing.Index != incoming.Index {
					continue
				}

				found = true

				if existing.Term != incoming.Term {
					// Conflict: discard this entry and everything after it,
					// then append the leader's remaining suffix.
					next.Log = append(
						append([]LogEntry(nil), next.Log[:j]...),
						args.Entries[i:]...,
					)
					changed = true
				}

				break
			}

			if changed {
				break
			}

			if !found {
				// Follower's log ends before the leader's suffix.
				next.Log = append(next.Log, args.Entries[i:]...)
				changed = true
				break
			}
		}

		if changed {
			if err := n.storage.Save(next); err != nil {
				return result, err
			}

			n.persistent = next
		}
	}

	matchedIndex := args.PrevLogIndex
	if len(args.Entries) > 0 {
		matchedIndex = args.Entries[len(args.Entries)-1].Index
	}

	commitIndex := min(args.LeaderCommit, matchedIndex)
	if commitIndex > n.volatile.CommitIndex {
		n.volatile.CommitIndex = commitIndex
	}

	result.reply.Success = true
	return result, nil
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

	for _, entry := range n.persistent.Log {
		if entry.Index == args.PrevLogIndex {
			return entry.Term == args.PrevLogTerm
		}
	}

	return false
}
