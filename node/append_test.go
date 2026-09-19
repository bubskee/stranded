package node

import (
	"context"
	"testing"
	"testing/synctest"
	"time"
)

func TestRunHigherTermAppendEntriesStepsDown(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		storage := &memoryStorage{
			state: PersistentState{
				CurrentTerm: 1,
				VotedFor:    "node-a",
			},
		}

		n := &Node{
			cfg: Config{
				ID:                 "node-a",
				ElectionTimeoutMin: time.Second,
				ElectionTimeoutMax: time.Second,
			},
			role: Candidate,
			persistent: PersistentState{
				CurrentTerm: 1,
				VotedFor:    "node-a",
			},
			candidateState: &CandidateState{
				Votes: map[PeerID]bool{
					"node-a": true,
				},
			},
			storage:         storage,
			appendEntriesCh: make(chan appendEntriesCall),
		}

		go func() {
			_ = n.Run(ctx)
		}()

		reply, err := n.submitAppendEntries(ctx, AppendEntriesArgs{
			Term:     2,
			LeaderID: "node-b",
		})
		if err != nil {
			t.Fatalf("submit AppendEntries: %v", err)
		}

		if reply.Term != 2 {
			t.Errorf("reply term: got %d, want 2", reply.Term)
		}

		n.mu.Lock()
		role := n.role
		term := n.persistent.CurrentTerm
		votedFor := n.persistent.VotedFor
		candidateState := n.candidateState
		n.mu.Unlock()

		if role != Follower {
			t.Errorf("role after higher-term AppendEntries: got %s, want follower", role)
		}

		if term != 2 {
			t.Errorf("term after higher-term AppendEntries: got %d, want 2", term)
		}

		if votedFor != "" {
			t.Errorf("vote after higher-term AppendEntries: got %q, want no vote", votedFor)
		}

		if candidateState != nil {
			t.Errorf(
				"candidate state after higher-term AppendEntries: got %+v, want nil",
				candidateState,
			)
		}

		storage.mu.Lock()
		persisted := storage.state
		storage.mu.Unlock()

		if persisted.CurrentTerm != 2 {
			t.Errorf("persisted term: got %d, want 2", persisted.CurrentTerm)
		}

		if persisted.VotedFor != "" {
			t.Errorf("persisted vote: got %q, want no vote", persisted.VotedFor)
		}
	})
}
