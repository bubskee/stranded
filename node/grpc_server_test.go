package node

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	raftpb "github.com/bubskee/stranded/proto/raftpb/raft/v1"
)

func TestGRPCRequestVoteRoutesThroughNode(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		storage := &memoryStorage{
			state: PersistentState{
				CurrentTerm: 3,
			},
		}

		n := &Node{
			cfg: Config{
				ID:                 "node-a",
				ElectionTimeoutMin: time.Second,
				ElectionTimeoutMax: time.Second,
			},
			role: Follower,
			persistent: PersistentState{
				CurrentTerm: 3,
			},
			storage:       storage,
			requestVoteCh: make(chan requestVoteCall),
		}

		go func() {
			_ = n.Run(ctx)
		}()

		server := &grpcServer{node: n}

		reply, err := server.RequestVote(ctx, &raftpb.RequestVoteRequest{
			Term:        3,
			CandidateId: "node-b",
		})
		if err != nil {
			t.Fatalf("RequestVote: %v", err)
		}

		if reply.Term != 3 {
			t.Errorf("reply term: got %d, want 3", reply.Term)
		}

		if !reply.VoteGranted {
			t.Error("eligible RequestVote was not granted")
		}

		n.mu.Lock()
		votedFor := n.persistent.VotedFor
		n.mu.Unlock()

		if votedFor != "node-b" {
			t.Errorf("vote after RequestVote: got %q, want %q", votedFor, "node-b")
		}
	})
}
