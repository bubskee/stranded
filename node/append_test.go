package node

import (
	"context"
	"errors"
	"reflect"
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

func TestRunHigherTermAppendEntriesPersistenceFailureDoesNotPublishFollowerState(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		persistErr := errors.New("disk exploded")
		recorder := &eventRecorder{}

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
			storage: &recordingStorage{
				recorder: recorder,
				err:      persistErr,
			},
			appendEntriesCh: make(chan appendEntriesCall),
		}

		ctx, cancel := context.WithCancel(context.Background())

		runErr := make(chan error, 1)
		go func() {
			runErr <- n.Run(ctx)
		}()

		reply, err := n.submitAppendEntries(ctx, AppendEntriesArgs{
			Term:     2,
			LeaderID: "node-b",
		})

		if !errors.Is(err, persistErr) {
			t.Errorf("submit AppendEntries error: got %v, want %v", err, persistErr)
		}

		if reply.Success {
			t.Error("AppendEntries succeeded after persistence failure")
		}

		n.mu.Lock()
		role := n.role
		term := n.persistent.CurrentTerm
		votedFor := n.persistent.VotedFor
		candidateState := n.candidateState
		n.mu.Unlock()

		if role != Candidate {
			t.Errorf("role after persistence failure: got %s, want candidate", role)
		}

		if term != 1 {
			t.Errorf("term after persistence failure: got %d, want 1", term)
		}

		if votedFor != "node-a" {
			t.Errorf(
				"vote after persistence failure: got %q, want %q",
				votedFor,
				"node-a",
			)
		}

		if candidateState == nil {
			t.Error("candidate state cleared after persistence failure")
		}

		gotEvents := recorder.snapshot()
		wantEvents := []string{"save"}

		if !reflect.DeepEqual(gotEvents, wantEvents) {
			t.Errorf(
				"effects after persistence failure: got %v, want %v",
				gotEvents,
				wantEvents,
			)
		}

		select {
		case err := <-runErr:
			if !errors.Is(err, persistErr) {
				t.Errorf("Run error: got %v, want %v", err, persistErr)
			}

		default:
			cancel()
			synctest.Wait()
			<-runErr
			t.Error("Run did not stop after persistence failure")
		}
	})
}

func TestRunCurrentTermAppendEntriesStepsCandidateDown(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		recorder := &eventRecorder{}

		n := &Node{
			cfg: Config{
				ID:                 "node-a",
				ElectionTimeoutMin: time.Second,
				ElectionTimeoutMax: time.Second,
			},
			role: Candidate,
			persistent: PersistentState{
				CurrentTerm: 2,
				VotedFor:    "node-a",
			},
			candidateState: &CandidateState{
				Votes: map[PeerID]bool{
					"node-a": true,
				},
			},
			storage: &recordingStorage{
				recorder: recorder,
			},
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
			t.Errorf(
				"role after current-term AppendEntries: got %s, want follower",
				role,
			)
		}

		if term != 2 {
			t.Errorf("term changed: got %d, want 2", term)
		}

		if votedFor != "node-a" {
			t.Errorf(
				"vote changed: got %q, want %q",
				votedFor,
				"node-a",
			)
		}

		if candidateState != nil {
			t.Errorf(
				"candidate state after stepdown: got %+v, want nil",
				candidateState,
			)
		}

		if got := recorder.snapshot(); len(got) != 0 {
			t.Errorf("unexpected persistence effects: got %v, want none", got)
		}
	})
}

func TestRunAcceptsAppendEntriesAtStartOfLog(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

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
			storage:         &memoryStorage{},
			appendEntriesCh: make(chan appendEntriesCall),
		}

		go func() {
			_ = n.Run(ctx)
		}()

		reply, err := n.submitAppendEntries(ctx, AppendEntriesArgs{
			Term:         3,
			LeaderID:     "node-b",
			PrevLogIndex: 0,
			PrevLogTerm:  0,
		})
		if err != nil {
			t.Fatalf("submit AppendEntries: %v", err)
		}

		if reply.Term != 3 {
			t.Errorf("reply term: got %d, want 3", reply.Term)
		}

		if !reply.Success {
			t.Error("AppendEntries at start of log was rejected")
		}
	})
}
