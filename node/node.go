package node

import (
	"context"
	"errors"
	"math/rand/v2"
	"os"
	"sync"
	"time"
)

type Node struct {
	mu   sync.Mutex
	cfg  Config
	role Role

	persistent     PersistentState
	volatile       VolatileState
	candidateState *CandidateState
	leaderState    *LeaderState

	storage   Storage
	transport Transport

	electionTimer *time.Timer

	applyCh         chan LogEntry
	requestVoteCh   chan requestVoteCall
	appendEntriesCh chan appendEntriesCall
}

func New(cfg Config) (*Node, error) {
	storage := newFileStorage(cfg.DataDir)

	persistent, err := storage.Load()
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}

		persistent = PersistentState{}
	}

	return &Node{
		cfg:             cfg,
		role:            Follower,
		persistent:      persistent,
		storage:         storage,
		applyCh:         make(chan LogEntry),
		requestVoteCh:   make(chan requestVoteCall),
		appendEntriesCh: make(chan appendEntriesCall),
	}, nil
}

func (n *Node) Run(ctx context.Context) error {
	n.resetElectionTimer()

	voteReplies := make(chan voteReplyEvent)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case event := <-voteReplies:
			becameLeader, err := n.handleVoteReply(
				event.peer,
				event.electionTerm,
				&event.reply,
			)
			if err != nil {
				return err
			}

			if becameLeader {
				n.sendInitialHeartbeats(ctx)
			}

		// RequestVote case
		case call := <-n.requestVoteCh:
			reply, err := n.processRequestVote(call.args)

			if err == nil && reply.VoteGranted {
				n.resetElectionTimer()
			}

			call.reply <- requestVoteResult{
				reply: reply,
				err:   err,
			}

			if err != nil {
				return err
			}

		// AppendEntries case
		case call := <-n.appendEntriesCh:
			reply, err := n.processAppendEntries(call.args)

			if err == nil && call.args.Term >= reply.Term {
				n.resetElectionTimer()
			}

			if err == nil && reply.Success {
				err = n.applyCommitted(ctx)
			}

			call.reply <- appendEntriesResult{
				reply: reply,
				err:   err,
			}

			if err != nil {
				return err
			}

		case <-n.electionTimer.C:
			e, err := n.startElection()
			if err != nil {
				return err
			}

			n.resetElectionTimer()
			n.sendRequestVotes(ctx, e, voteReplies)
		}
	}
}

func (n *Node) resetElectionTimer() {
	min := n.cfg.ElectionTimeoutMin
	max := n.cfg.ElectionTimeoutMax

	timeout := min
	if max > min {
		timeout += rand.N(max - min)
	}

	if n.electionTimer == nil {
		n.electionTimer = time.NewTimer(timeout)
		return
	}

	n.electionTimer.Reset(timeout)
}
