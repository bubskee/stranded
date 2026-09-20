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

type successfulAppendReplyTransport struct{}

func (successfulAppendReplyTransport) RequestVote(
	ctx context.Context,
	peer PeerID,
	args *RequestVoteArgs,
) (*RequestVoteReply, error) {
	return &RequestVoteReply{
		Term:        args.Term,
		VoteGranted: true,
	}, nil
}

func (successfulAppendReplyTransport) AppendEntries(
	ctx context.Context,
	peer PeerID,
	args *AppendEntriesArgs,
) (*AppendEntriesReply, error) {
	return &AppendEntriesReply{
		Term:    args.Term,
		Success: true,
	}, nil
}

func TestSuccessfulAppendEntriesReplyAdvancesFollowerProgress(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		n := &Node{
			cfg: Config{
				ID: "node-a",
				Peers: map[PeerID]string{
					"node-b": "",
				},
				ElectionTimeoutMin: 100 * time.Millisecond,
				ElectionTimeoutMax: 100 * time.Millisecond,
			},
			persistent: PersistentState{
				CurrentTerm: 1,
				Log: []LogEntry{
					{Term: 1, Index: 1},
					{Term: 1, Index: 2},
				},
			},
			transport: successfulAppendReplyTransport{},
			storage:   &memoryStorage{},
		}

		go func() {
			_ = n.Run(ctx)
		}()

		time.Sleep(100 * time.Millisecond)
		synctest.Wait()

		n.mu.Lock()
		role := n.role
		matchIndex := n.leaderState.MatchIndex["node-b"]
		nextIndex := n.leaderState.NextIndex["node-b"]
		n.mu.Unlock()

		if role != Leader {
			t.Fatalf("role after election: got %s, want leader", role)
		}

		if matchIndex != 2 {
			t.Errorf("match index after successful AppendEntries: got %d, want 2", matchIndex)
		}

		if nextIndex != 3 {
			t.Errorf("next index after successful AppendEntries: got %d, want 3", nextIndex)
		}
	})
}
