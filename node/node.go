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

	applyCh       chan LogEntry
	requestVoteCh chan requestVoteCall
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
		cfg:           cfg,
		role:          Follower,
		persistent:    persistent,
		storage:       storage,
		requestVoteCh: make(chan requestVoteCall),
	}, nil
}

func (n *Node) Run(ctx context.Context) error {
	n.resetElectionTimer()

	errCh := make(chan error, 1)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case err := <-errCh:
			return err

		case call := <-n.requestVoteCh:
			reply, err := n.processRequestVote(call.args)

			call.reply <- requestVoteResult{
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
			n.sendRequestVotes(ctx, e, errCh)
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
