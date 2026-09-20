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

	electionTimer         *time.Timer
	pendingClientRequests map[uint64]chan clientRequestResult

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

			commitAdvanced, err := n.appendClientCommand(
				call.command,
				call.reply,
			)
			if err != nil {
				call.reply <- clientRequestResult{
					err: err,
				}
				return err
			}

			if commitAdvanced {
				if err := n.applyCommitted(ctx); err != nil {
					return err
				}
				n.completeAppliedClientRequests()
			}

			for peer := range n.cfg.Peers {
				n.sendAppendEntries(ctx, peer, appendReplies)
			}

		case event := <-voteReplies:
			result, err := n.handleVoteReply(
				event.peer,
				event.electionTerm,
				&event.reply,
			)
			if err != nil {
				return err
			}

			failClientRequests(result.failedClients)

			if result.becameLeader {
				n.sendAppendEntriesToAll(ctx, appendReplies)
			}

		// appendReplies case
		case event := <-appendReplies:
			result, err := n.handleAppendEntriesReply(event)
			if err != nil {
				return err
			}

			failClientRequests(result.failedClients)

			if result.commitAdvanced {
				if err := n.applyCommitted(ctx); err != nil {
					return err
				}

				n.completeAppliedClientRequests()
			}

			if result.retry {
				n.sendAppendEntries(ctx, event.peer, appendReplies)
			}

		// RequestVote case
		case call := <-n.requestVoteCh:
			result, err := n.processRequestVote(call.args)

			failClientRequests(result.failedClients)

			if err == nil && result.reply.VoteGranted {
				n.resetElectionTimer()
			}

			call.reply <- requestVoteResult{
				reply: result.reply,
				err:   err,
			}

			if err != nil {
				return err
			}

		// AppendEntries case
		case call := <-n.appendEntriesCh:
			result, err := n.processAppendEntries(call.args)

			failClientRequests(result.failedClients)

			if err == nil && call.args.Term >= result.reply.Term {
				n.resetElectionTimer()
			}

			if err == nil && result.reply.Success {
				err = n.applyCommitted(ctx)
			}

			call.reply <- appendEntriesResult{
				reply: result.reply,
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
