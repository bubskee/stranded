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

func TestGRPCAppendEntriesRoutesThroughNode(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		n := &Node{
			cfg: Config{
				ID:                 "node-a",
				ElectionTimeoutMin: 100 * time.Millisecond,
				ElectionTimeoutMax: 100 * time.Millisecond,
			},
			role: Follower,
			persistent: PersistentState{
				CurrentTerm: 3,
			},
			storage:         &memoryStorage{},
			appendEntriesCh: make(chan appendEntriesCall),
		}

		go func() {
			_ = n.Run(ctx)
		}()

		server := &grpcServer{node: n}

		time.Sleep(75 * time.Millisecond)

		reply, err := server.AppendEntries(ctx, &raftpb.AppendEntriesRequest{
			Term:     3,
			LeaderId: "node-b",
		})
		if err != nil {
			t.Fatalf("AppendEntries: %v", err)
		}

		if reply.Term != 3 {
			t.Errorf("reply term: got %d, want 3", reply.Term)
		}

		// Reach the original election deadline. Routing through Run should
		// have reset it.
		time.Sleep(25 * time.Millisecond)
		synctest.Wait()

		n.mu.Lock()
		role := n.role
		term := n.persistent.CurrentTerm
		n.mu.Unlock()

		if role != Follower {
			t.Errorf("role at old election deadline: got %s, want follower", role)
		}

		if term != 3 {
			t.Errorf("term at old election deadline: got %d, want 3", term)
		}
	})
}

func TestGRPCSubmitCommandRejectsOnFollower(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		n, err := New(Config{
			ID:                 "node-a",
			DataDir:            t.TempDir(),
			ElectionTimeoutMin: time.Second,
			ElectionTimeoutMax: time.Second,
		})
		if err != nil {
			t.Fatalf("New: %v", err)
		}

		go func() {
			_ = n.Run(ctx)
		}()

		server := &grpcServer{node: n}

		reply, err := server.SubmitCommand(
			ctx,
			&raftpb.SubmitCommandRequest{
				ClientId:  "client-1",
				RequestId: 1,
				Command:   []byte("set x=1"),
			},
		)
		if err != nil {
			t.Fatalf("SubmitCommand: %v", err)
		}

		if reply.Success {
			t.Fatal("follower accepted client command")
		}

		n.mu.Lock()
		log := append([]LogEntry(nil), n.persistent.Log...)
		n.mu.Unlock()

		if len(log) != 0 {
			t.Errorf(
				"follower log changed after client command: got %+v, want empty",
				log,
			)
		}
	})
}
