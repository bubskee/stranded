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
				TickInterval:       10 * time.Millisecond,
			},
			role: Follower,
			persistent: PersistentState{
				CurrentTerm: 3,
			},
			storage:       &memoryStorage{},
			requestVoteCh: make(chan requestVoteCall),
		}

		ticks := make(chan time.Time)

		go func() {
			_ = n.run(ctx, ticks)
		}()

		advance := func(count int) {
			t.Helper()

			for range count {
				ticks <- time.Time{}
				synctest.Wait()
			}
		}

		// Process seven ticks before resetting the election timeout.
		advance(7)
		synctest.Wait()

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

		// Original deadline: three ticks since the reset.
		advance(3)
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

		// Nine ticks since reset; the tenth has not arrived.
		advance(6)
		synctest.Wait()

		n.mu.Lock()
		role = n.role
		n.mu.Unlock()

		if role != Follower {
			t.Errorf("role before reset election deadline: got %s, want follower", role)
		}

		// 100ms since the granted vote: election may now begin.
		advance(1)
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

func TestRunRejectsStaleAppendEntries(t *testing.T) {
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
			Term:     2,
			LeaderID: "node-b",
		})
		if err != nil {
			t.Fatalf("submit AppendEntries: %v", err)
		}

		if reply.Term != 3 {
			t.Errorf("reply term: got %d, want 3", reply.Term)
		}

		if reply.Success {
			t.Error("stale AppendEntries was accepted")
		}
	})
}

func TestNewInitializesAppendEntriesChannel(t *testing.T) {
	n, err := New(Config{
		ID:      "node-a",
		DataDir: t.TempDir(),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if n.appendEntriesCh == nil {
		t.Fatal("appendEntriesCh is nil")
	}
}

func TestNewInitializesApplyChannel(t *testing.T) {
	n, err := New(Config{
		ID:      "node-a",
		DataDir: t.TempDir(),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if n.applyCh == nil {
		t.Fatal("applyCh is nil")
	}
}

func TestCurrentTermAppendEntriesResetsElectionTimer(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		n := &Node{
			cfg: Config{
				ID:                 "node-a",
				ElectionTimeoutMin: 100 * time.Millisecond,
				ElectionTimeoutMax: 100 * time.Millisecond,
				TickInterval:       10 * time.Millisecond,
			},
			role: Follower,
			persistent: PersistentState{
				CurrentTerm: 3,
			},
			storage:         &memoryStorage{},
			appendEntriesCh: make(chan appendEntriesCall),
		}

		ticks := make(chan time.Time)

		go func() {
			_ = n.run(ctx, ticks)
		}()

		advance := func(count int) {
			t.Helper()

			for range count {
				ticks <- time.Time{}
				synctest.Wait()
			}
		}

		// Process seven ticks before resetting the election timeout.
		advance(7)
		synctest.Wait()

		_, err := n.submitAppendEntries(ctx, AppendEntriesArgs{
			Term:     3,
			LeaderID: "node-b",
		})
		if err != nil {
			t.Fatalf("submit AppendEntries: %v", err)
		}

		// Original deadline: three ticks since the reset.
		advance(3)
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

		// Nine ticks since reset; the tenth has not arrived.
		advance(6)
		synctest.Wait()

		n.mu.Lock()
		role = n.role
		n.mu.Unlock()

		if role != Follower {
			t.Errorf("role before reset election deadline: got %s, want follower", role)
		}

		// 100ms since AppendEntries: election may now begin.
		advance(1)
		synctest.Wait()

		n.mu.Lock()
		term = n.persistent.CurrentTerm
		n.mu.Unlock()

		if term != 4 {
			t.Errorf("term after reset election deadline: got %d, want 4", term)
		}
	})
}

func TestStaleAppendEntriesDoesNotResetElectionTimer(t *testing.T) {
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

		time.Sleep(75 * time.Millisecond)

		reply, err := n.submitAppendEntries(ctx, AppendEntriesArgs{
			Term:     2,
			LeaderID: "node-b",
		})
		if err != nil {
			t.Fatalf("submit AppendEntries: %v", err)
		}

		if reply.Term != 3 {
			t.Errorf("reply term: got %d, want 3", reply.Term)
		}

		if reply.Success {
			t.Fatal("stale AppendEntries succeeded")
		}

		// Reach the original deadline. Stale leader traffic must not
		// postpone the election.
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

func TestRunAppliesNewlyCommittedEntriesInOrder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		entry1 := LogEntry{
			Term:    1,
			Index:   1,
			Command: []byte("one"),
		}
		entry2 := LogEntry{
			Term:    2,
			Index:   2,
			Command: []byte("two"),
		}
		entry3 := LogEntry{
			Term:    3,
			Index:   3,
			Command: []byte("three"),
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
				Log: []LogEntry{
					entry1,
					entry2,
					entry3,
				},
			},
			volatile: VolatileState{
				CommitIndex: 1,
				LastApplied: 1,
			},
			storage:         &memoryStorage{},
			applyCh:         make(chan LogEntry, 2),
			appendEntriesCh: make(chan appendEntriesCall),
		}

		go func() {
			_ = n.Run(ctx)
		}()

		reply, err := n.submitAppendEntries(ctx, AppendEntriesArgs{
			Term:         3,
			LeaderID:     "node-b",
			PrevLogIndex: 3,
			PrevLogTerm:  3,
			LeaderCommit: 3,
		})
		if err != nil {
			t.Fatalf("submit AppendEntries: %v", err)
		}

		if !reply.Success {
			t.Fatal("AppendEntries was rejected")
		}

		got2 := <-n.applyCh
		got3 := <-n.applyCh

		if !reflect.DeepEqual(got2, entry2) {
			t.Errorf("first applied entry: got %+v, want %+v", got2, entry2)
		}

		if !reflect.DeepEqual(got3, entry3) {
			t.Errorf("second applied entry: got %+v, want %+v", got3, entry3)
		}

		n.mu.Lock()
		lastApplied := n.volatile.LastApplied
		n.mu.Unlock()

		if lastApplied != 3 {
			t.Errorf("last applied: got %d, want 3", lastApplied)
		}
	})
}

func TestRunDoesNotApplyCommittedEntryTwice(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		entry1 := LogEntry{
			Term:    1,
			Index:   1,
			Command: []byte("one"),
		}
		entry2 := LogEntry{
			Term:    2,
			Index:   2,
			Command: []byte("two"),
		}

		n := &Node{
			cfg: Config{
				ID:                 "node-a",
				ElectionTimeoutMin: time.Second,
				ElectionTimeoutMax: time.Second,
			},
			role: Follower,
			persistent: PersistentState{
				CurrentTerm: 2,
				Log: []LogEntry{
					entry1,
					entry2,
				},
			},
			volatile: VolatileState{
				CommitIndex: 1,
				LastApplied: 1,
			},
			storage:         &memoryStorage{},
			applyCh:         make(chan LogEntry, 2),
			appendEntriesCh: make(chan appendEntriesCall),
		}

		go func() {
			_ = n.Run(ctx)
		}()

		firstReply, err := n.submitAppendEntries(ctx, AppendEntriesArgs{
			Term:         2,
			LeaderID:     "node-b",
			PrevLogIndex: 2,
			PrevLogTerm:  2,
			LeaderCommit: 2,
		})
		if err != nil {
			t.Fatalf("first AppendEntries: %v", err)
		}

		if !firstReply.Success {
			t.Fatal("first AppendEntries was rejected")
		}

		select {
		case got := <-n.applyCh:
			if !reflect.DeepEqual(got, entry2) {
				t.Errorf("first applied entry: got %+v, want %+v", got, entry2)
			}
		default:
			t.Fatal("entry 2 was not applied")
		}

		secondReply, err := n.submitAppendEntries(ctx, AppendEntriesArgs{
			Term:         2,
			LeaderID:     "node-b",
			PrevLogIndex: 2,
			PrevLogTerm:  2,
			LeaderCommit: 2,
		})
		if err != nil {
			t.Fatalf("second AppendEntries: %v", err)
		}

		if !secondReply.Success {
			t.Fatal("second AppendEntries was rejected")
		}

		select {
		case got := <-n.applyCh:
			t.Fatalf("entry applied twice: got %+v", got)
		default:
		}

		n.mu.Lock()
		commitIndex := n.volatile.CommitIndex
		lastApplied := n.volatile.LastApplied
		n.mu.Unlock()

		if commitIndex != 2 {
			t.Errorf("commit index: got %d, want 2", commitIndex)
		}

		if lastApplied != 2 {
			t.Errorf("last applied: got %d, want 2", lastApplied)
		}
	})
}
