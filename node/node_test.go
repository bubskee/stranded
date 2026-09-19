package node

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"testing/synctest"
	"time"
)

func TestResetElectionTimer(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		n := &Node{
			cfg: Config{
				ElectionTimeoutMin: 100 * time.Millisecond,
				ElectionTimeoutMax: 100 * time.Millisecond,
			},
			storage: &memoryStorage{},
		}

		n.resetElectionTimer()

		time.Sleep(99 * time.Millisecond)

		select {
		case <-n.electionTimer.C:
			t.Fatal("election timer fired too early")
		default:
		}

		time.Sleep(time.Millisecond)

		select {
		case <-n.electionTimer.C:
			// expected
		default:
			t.Fatal("election timer did not fire")
		}
	})
}

func TestResetElectionTimerRestartsCountdown(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		n := &Node{
			cfg: Config{
				ElectionTimeoutMin: 100 * time.Millisecond,
				ElectionTimeoutMax: 100 * time.Millisecond,
			},
			storage: &memoryStorage{},
		}

		n.resetElectionTimer()

		// Almost reach the original deadline.
		time.Sleep(75 * time.Millisecond)

		// Restart the countdown from here.
		n.resetElectionTimer()

		// We've now reached the original deadline: 100ms since creation,
		// but only 25ms since reset.
		time.Sleep(25 * time.Millisecond)

		select {
		case <-n.electionTimer.C:
			t.Fatal("election timer fired according to old deadline")
		default:
		}

		// Still just before the new deadline.
		time.Sleep(74 * time.Millisecond)

		select {
		case <-n.electionTimer.C:
			t.Fatal("election timer fired too early after reset")
		default:
		}

		// Exactly 100ms since reset.
		time.Sleep(time.Millisecond)

		select {
		case <-n.electionTimer.C:
			// expected
		default:
			t.Fatal("election timer did not fire after reset deadline")
		}
	})
}

func TestNewLoadsPersistentState(t *testing.T) {
	dir := t.TempDir()

	want := PersistentState{
		CurrentTerm: 7,
		VotedFor:    "node-b",
		Log: []LogEntry{
			{Term: 3, Index: 1, Command: []byte("first")},
			{Term: 7, Index: 2, Command: []byte("second")},
		},
	}

	storage := newFileStorage(dir)
	if err := storage.Save(want); err != nil {
		t.Fatalf("save persistent state: %v", err)
	}

	n, err := New(Config{
		ID:      "node-a",
		DataDir: dir,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if !reflect.DeepEqual(n.persistent, want) {
		t.Fatalf(
			"persistent state after restart:\n got: %+v\nwant: %+v",
			n.persistent,
			want,
		)
	}

	if n.role != Follower {
		t.Errorf("role after restart: got %s, want follower", n.role)
	}
}

func TestNewWithMissingStateStartsFresh(t *testing.T) {
	n, err := New(Config{
		ID:      "node-a",
		DataDir: t.TempDir(),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if !reflect.DeepEqual(n.persistent, PersistentState{}) {
		t.Fatalf(
			"fresh persistent state: got %+v, want zero value",
			n.persistent,
		)
	}

	if n.role != Follower {
		t.Errorf("fresh role: got %s, want follower", n.role)
	}
}

func TestNewFailsOnCorruptPersistentState(t *testing.T) {
	dir := t.TempDir()

	path := filepath.Join(dir, persistentStateFile)
	if err := os.WriteFile(path, []byte("not json"), 0o644); err != nil {
		t.Fatalf("write corrupt state: %v", err)
	}

	_, err := New(Config{
		ID:      "node-a",
		DataDir: dir,
	})
	if err == nil {
		t.Fatal("New succeeded with corrupt persistent state")
	}
}

func TestNewInitializesRequestVoteChannel(t *testing.T) {
	n, err := New(Config{
		ID:      "node-a",
		DataDir: t.TempDir(),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if n.requestVoteCh == nil {
		t.Fatal("requestVoteCh is nil")
	}
}

func TestRunHigherTermRequestVotePersistenceFailureStopsNode(t *testing.T) {
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
			requestVoteCh: make(chan requestVoteCall),
		}

		ctx, cancel := context.WithCancel(context.Background())

		runErr := make(chan error, 1)
		go func() {
			runErr <- n.Run(ctx)
		}()

		_, err := n.submitRequestVote(ctx, RequestVoteArgs{
			Term:        2,
			CandidateID: "node-b",
		})

		if !errors.Is(err, persistErr) {
			t.Errorf("submit RequestVote error: got %v, want %v", err, persistErr)
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

		select {
		case err := <-runErr:
			if !errors.Is(err, persistErr) {
				t.Errorf("Run error: got %v, want %v", err, persistErr)
			}

		default:
			// Cleanup for the intentionally-red implementation, where Run
			// reports the error to the caller but keeps running.
			cancel()
			synctest.Wait()
			<-runErr
			t.Error("Run did not stop after persistence failure")
		}
	})
}

func TestGrantedRequestVoteResetsElectionTimer(t *testing.T) {
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
			storage:       &memoryStorage{},
			requestVoteCh: make(chan requestVoteCall),
		}

		go func() {
			_ = n.Run(ctx)
		}()

		// Approach the original election deadline.
		time.Sleep(75 * time.Millisecond)

		reply, err := n.submitRequestVote(ctx, RequestVoteArgs{
			Term:        3,
			CandidateID: "node-b",
		})
		if err != nil {
			t.Fatalf("submit RequestVote: %v", err)
		}

		if !reply.VoteGranted {
			t.Fatal("eligible RequestVote was not granted")
		}

		// Reach the original deadline: only 25ms have elapsed since granting.
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

		// Still immediately before the new deadline.
		time.Sleep(74 * time.Millisecond)
		synctest.Wait()

		n.mu.Lock()
		role = n.role
		n.mu.Unlock()

		if role != Follower {
			t.Errorf("role before reset election deadline: got %s, want follower", role)
		}

		// 100ms since the granted vote: election may now begin.
		time.Sleep(time.Millisecond)
		synctest.Wait()

		n.mu.Lock()
		term = n.persistent.CurrentTerm
		n.mu.Unlock()

		if term != 4 {
			t.Errorf("term after reset election deadline: got %d, want 4", term)
		}
	})
}

func TestDeniedRequestVoteDoesNotResetElectionTimer(t *testing.T) {
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
				Log: []LogEntry{
					{Term: 3, Index: 5},
				},
			},
			storage:       &memoryStorage{},
			requestVoteCh: make(chan requestVoteCall),
		}

		go func() {
			_ = n.Run(ctx)
		}()

		// Approach the original election deadline.
		time.Sleep(75 * time.Millisecond)

		reply, err := n.submitRequestVote(ctx, RequestVoteArgs{
			Term:         3,
			CandidateID:  "node-b",
			LastLogTerm:  2,
			LastLogIndex: 100,
		})
		if err != nil {
			t.Fatalf("submit RequestVote: %v", err)
		}

		if reply.VoteGranted {
			t.Fatal("RequestVote with stale log was granted")
		}

		// The denied request must not move the deadline.
		time.Sleep(25 * time.Millisecond)
		synctest.Wait()

		n.mu.Lock()
		role := n.role
		term := n.persistent.CurrentTerm
		n.mu.Unlock()

		if term != 4 {
			t.Errorf("term at original election deadline: got %d, want 4", term)
		}

		if role != Leader {
			t.Errorf("role at original election deadline: got %s, want leader", role)
		}
	})
}
