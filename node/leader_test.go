package node

import (
	"context"
	"testing"
	"testing/synctest"
	"time"
)

type higherTermAppendReplyTransport struct{}

func (higherTermAppendReplyTransport) RequestVote(
	ctx context.Context,
	peer PeerID,
	args *RequestVoteArgs,
) (*RequestVoteReply, error) {
	return &RequestVoteReply{
		Term:        args.Term,
		VoteGranted: true,
	}, nil
}

func (higherTermAppendReplyTransport) AppendEntries(
	ctx context.Context,
	peer PeerID,
	args *AppendEntriesArgs,
) (*AppendEntriesReply, error) {
	return &AppendEntriesReply{
		Term:    args.Term + 1,
		Success: false,
	}, nil
}

func TestHigherTermAppendEntriesReplyStepsDownLeader(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		storage := &memoryStorage{}

		n := &Node{
			cfg: Config{
				ID: "node-a",
				Peers: map[PeerID]string{
					"node-b": "",
				},
				ElectionTimeoutMin: 100 * time.Millisecond,
				ElectionTimeoutMax: 100 * time.Millisecond,
			},
			transport: higherTermAppendReplyTransport{},
			storage:   storage,
		}

		go func() {
			_ = n.Run(ctx)
		}()

		time.Sleep(100 * time.Millisecond)
		synctest.Wait()

		n.mu.Lock()
		role := n.role
		term := n.persistent.CurrentTerm
		votedFor := n.persistent.VotedFor
		leaderState := n.leaderState
		n.mu.Unlock()

		if role != Follower {
			t.Errorf("role after higher-term AppendEntries reply: got %s, want follower", role)
		}

		if term != 2 {
			t.Errorf("term after higher-term AppendEntries reply: got %d, want 2", term)
		}

		if votedFor != "" {
			t.Errorf("vote after stepping down: got %q, want no vote", votedFor)
		}

		if leaderState != nil {
			t.Errorf("leader state after stepping down: got %+v, want nil", leaderState)
		}

		storage.mu.Lock()
		persistedTerm := storage.state.CurrentTerm
		storage.mu.Unlock()

		if persistedTerm != 2 {
			t.Errorf("persisted term: got %d, want 2", persistedTerm)
		}
	})
}
