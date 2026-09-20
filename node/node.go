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
	clientRequestCh chan clientRequestCall
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
		clientRequestCh: make(chan clientRequestCall),
	}, nil
}

func (n *Node) Run(ctx context.Context) error {
	n.resetElectionTimer()

	voteReplies := make(chan voteReplyEvent)
	appendReplies := make(chan appendReplyEvent)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case call := <-n.clientRequestCh:
			n.mu.Lock()
			role := n.role
			n.mu.Unlock()

			if role != Leader {
				call.reply <- clientRequestResult{
					success: false,
				}
				continue
			}

			// Leader handling comes in the next red.
			call.reply <- clientRequestResult{
				success: false,
			}

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
				n.sendInitialHeartbeats(ctx, appendReplies)
			}

		case event := <-appendReplies:
			retry, commitAdvanced, err := n.handleAppendEntriesReply(event)
			if err != nil {
				return err
			}

			if commitAdvanced {
				if err := n.applyCommitted(ctx); err != nil {
					return err
				}
			}

			if retry {
				n.sendAppendEntries(ctx, event.peer, appendReplies)
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
