package node

import (
	"context"
	"errors"
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

	electionElapsed         int
	heartbeatElapsed        int
	randomizedElectionTicks int

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
	ticker := time.NewTicker(n.tickInterval())
	defer ticker.Stop()

	return n.run(ctx, ticker.C)
}

func (n *Node) run(ctx context.Context, ticks <-chan time.Time) error {
	defer func() {
		n.mu.Lock()
		pending := n.takePendingClientRequestsLocked()
		n.mu.Unlock()

		failClientRequests(pending)
	}()

	n.resetElectionTimeout()

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
				n.resetElectionTimeout()
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
				n.resetElectionTimeout()
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

		case <-ticks:
			result := n.tick()

			if result.sendHeartbeat {
				n.sendAppendEntriesToAll(ctx, appendReplies)
			}

			if result.startElection {
				e, commitAdvanced, err := n.startElection()
				if err != nil {
					return err
				}

				if commitAdvanced {
					if err := n.applyCommitted(ctx); err != nil {
						return err
					}

					n.completeAppliedClientRequests()
				}

				n.sendRequestVotes(ctx, e, voteReplies)
			}
		}
	}
}

func NewWithTransport(cfg Config, transport Transport) (*Node, error) {
	n, err := New(cfg)
	if err != nil {
		return nil, err
	}

	n.transport = transport
	return n, nil
}

func (n *Node) ApplyCh() <-chan LogEntry {
	return n.applyCh
}
