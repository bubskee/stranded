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

func TestRunAcceptsAppendEntriesWithMatchingPreviousEntry(t *testing.T) {
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
				Log: []LogEntry{
					{Term: 1, Index: 1},
					{Term: 2, Index: 2},
					{Term: 2, Index: 3},
				},
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
			PrevLogIndex: 2,
			PrevLogTerm:  2,
		})
		if err != nil {
			t.Fatalf("submit AppendEntries: %v", err)
		}

		if !reply.Success {
			t.Error("AppendEntries with matching previous entry was rejected")
		}
	})
}

func TestRunAppendsEntryAfterMatchingPrefix(t *testing.T) {
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
			storage:         storage,
			appendEntriesCh: make(chan appendEntriesCall),
		}

		go func() {
			_ = n.Run(ctx)
		}()

		entry := LogEntry{
			Term:    3,
			Index:   1,
			Command: []byte("set x=1"),
		}

		reply, err := n.submitAppendEntries(ctx, AppendEntriesArgs{
			Term:         3,
			LeaderID:     "node-b",
			PrevLogIndex: 0,
			PrevLogTerm:  0,
			Entries:      []LogEntry{entry},
		})
		if err != nil {
			t.Fatalf("submit AppendEntries: %v", err)
		}

		if !reply.Success {
			t.Fatal("AppendEntries with matching prefix was rejected")
		}

		n.mu.Lock()
		gotLog := append([]LogEntry(nil), n.persistent.Log...)
		n.mu.Unlock()

		if !reflect.DeepEqual(gotLog, []LogEntry{entry}) {
			t.Errorf("log after AppendEntries: got %+v, want %+v", gotLog, []LogEntry{entry})
		}

		storage.mu.Lock()
		persistedLog := append([]LogEntry(nil), storage.state.Log...)
		storage.mu.Unlock()

		if !reflect.DeepEqual(persistedLog, []LogEntry{entry}) {
			t.Errorf(
				"persisted log after AppendEntries: got %+v, want %+v",
				persistedLog,
				[]LogEntry{entry},
			)
		}
	})
}

func TestRunAppendEntriesPersistenceFailureDoesNotPublishLog(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		persistErr := errors.New("disk exploded")
		recorder := &eventRecorder{}

		old := LogEntry{Term: 1, Index: 1, Command: []byte("old")}
		sentinel := LogEntry{Term: 99, Index: 99, Command: []byte("sentinel")}

		backing := []LogEntry{old, sentinel}
		log := backing[:1]

		n := &Node{
			cfg: Config{
				ID:                 "node-a",
				ElectionTimeoutMin: time.Second,
				ElectionTimeoutMax: time.Second,
			},
			role: Follower,
			persistent: PersistentState{
				CurrentTerm: 3,
				Log:         log,
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

		entry := LogEntry{
			Term:    3,
			Index:   2,
			Command: []byte("new"),
		}

		reply, err := n.submitAppendEntries(ctx, AppendEntriesArgs{
			Term:         3,
			LeaderID:     "node-b",
			PrevLogIndex: 1,
			PrevLogTerm:  1,
			Entries:      []LogEntry{entry},
		})

		if !errors.Is(err, persistErr) {
			t.Errorf("submit AppendEntries error: got %v, want %v", err, persistErr)
		}

		if reply.Success {
			t.Error("AppendEntries succeeded after persistence failure")
		}

		n.mu.Lock()
		gotLog := append([]LogEntry(nil), n.persistent.Log...)
		n.mu.Unlock()

		if !reflect.DeepEqual(gotLog, []LogEntry{old}) {
			t.Errorf("log published after persistence failure: got %+v", gotLog)
		}

		// Catch mutation through spare capacity in the old slice.
		if !reflect.DeepEqual(backing[1], sentinel) {
			t.Errorf("old log backing array mutated: got %+v, want %+v", backing[1], sentinel)
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

func TestRunReplacesConflictingLogSuffix(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		oldLog := []LogEntry{
			{Term: 1, Index: 1, Command: []byte("one")},
			{Term: 2, Index: 2, Command: []byte("old-two")},
			{Term: 2, Index: 3, Command: []byte("old-three")},
		}

		storage := &memoryStorage{
			state: PersistentState{
				CurrentTerm: 4,
				Log:         oldLog,
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
				CurrentTerm: 4,
				Log:         oldLog,
			},
			storage:         storage,
			appendEntriesCh: make(chan appendEntriesCall),
		}

		go func() {
			_ = n.Run(ctx)
		}()

		replacement := LogEntry{
			Term:    3,
			Index:   2,
			Command: []byte("new-two"),
		}

		reply, err := n.submitAppendEntries(ctx, AppendEntriesArgs{
			Term:         4,
			LeaderID:     "node-b",
			PrevLogIndex: 1,
			PrevLogTerm:  1,
			Entries:      []LogEntry{replacement},
		})
		if err != nil {
			t.Fatalf("submit AppendEntries: %v", err)
		}

		if !reply.Success {
			t.Fatal("AppendEntries with matching prefix was rejected")
		}

		wantLog := []LogEntry{
			oldLog[0],
			replacement,
		}

		n.mu.Lock()
		gotLog := append([]LogEntry(nil), n.persistent.Log...)
		n.mu.Unlock()

		if !reflect.DeepEqual(gotLog, wantLog) {
			t.Errorf(
				"log after conflicting AppendEntries:\n got: %+v\nwant: %+v",
				gotLog,
				wantLog,
			)
		}

		storage.mu.Lock()
		persistedLog := append([]LogEntry(nil), storage.state.Log...)
		storage.mu.Unlock()

		if !reflect.DeepEqual(persistedLog, wantLog) {
			t.Errorf(
				"persisted log after conflicting AppendEntries:\n got: %+v\nwant: %+v",
				persistedLog,
				wantLog,
			)
		}
	})
}

func TestRunKeepsMatchingOverlapAndAppendsNewSuffix(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		oldLog := []LogEntry{
			{Term: 1, Index: 1},
			{Term: 2, Index: 2},
			{Term: 3, Index: 3},
		}

		storage := &memoryStorage{
			state: PersistentState{
				CurrentTerm: 4,
				Log:         oldLog,
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
				CurrentTerm: 4,
				Log:         oldLog,
			},
			storage:         storage,
			appendEntriesCh: make(chan appendEntriesCall),
		}

		go func() {
			_ = n.Run(ctx)
		}()

		entry4 := LogEntry{Term: 4, Index: 4}

		reply, err := n.submitAppendEntries(ctx, AppendEntriesArgs{
			Term:         4,
			LeaderID:     "node-b",
			PrevLogIndex: 1,
			PrevLogTerm:  1,
			Entries: []LogEntry{
				{Term: 2, Index: 2},
				{Term: 3, Index: 3},
				entry4,
			},
		})
		if err != nil {
			t.Fatalf("submit AppendEntries: %v", err)
		}

		if !reply.Success {
			t.Fatal("AppendEntries was rejected")
		}

		wantLog := []LogEntry{
			oldLog[0],
			oldLog[1],
			oldLog[2],
			entry4,
		}

		n.mu.Lock()
		gotLog := append([]LogEntry(nil), n.persistent.Log...)
		n.mu.Unlock()

		if !reflect.DeepEqual(gotLog, wantLog) {
			t.Errorf("log after overlapping AppendEntries: got %+v, want %+v", gotLog, wantLog)
		}
	})
}

func TestRunAdvancesCommitIndexFromLeaderCommit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		log := []LogEntry{
			{Term: 1, Index: 1},
			{Term: 2, Index: 2},
			{Term: 3, Index: 3},
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
				Log:         log,
			},
			volatile: VolatileState{
				CommitIndex: 1,
				LastApplied: 1,
			},
			applyCh:         make(chan LogEntry, 1),
			storage:         &memoryStorage{},
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
			LeaderCommit: 2,
		})
		if err != nil {
			t.Fatalf("submit AppendEntries: %v", err)
		}

		if !reply.Success {
			t.Fatal("AppendEntries was rejected")
		}

		n.mu.Lock()
		commitIndex := n.volatile.CommitIndex
		n.mu.Unlock()

		if commitIndex != 2 {
			t.Errorf("commit index: got %d, want 2", commitIndex)
		}
	})
}

func TestRunCapsCommitIndexAtLastLogIndex(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		log := []LogEntry{
			{Term: 1, Index: 1},
			{Term: 2, Index: 2},
			{Term: 3, Index: 3},
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
				Log:         log,
			},
			volatile: VolatileState{
				CommitIndex: 1,
				LastApplied: 1,
			},
			applyCh:         make(chan LogEntry, 2),
			storage:         &memoryStorage{},
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
			LeaderCommit: 99,
		})
		if err != nil {
			t.Fatalf("submit AppendEntries: %v", err)
		}

		if !reply.Success {
			t.Fatal("AppendEntries was rejected")
		}

		n.mu.Lock()
		commitIndex := n.volatile.CommitIndex
		n.mu.Unlock()

		if commitIndex != 3 {
			t.Errorf("commit index: got %d, want 3", commitIndex)
		}
	})
}

func TestRunDoesNotDecreaseCommitIndex(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		log := []LogEntry{
			{Term: 1, Index: 1},
			{Term: 2, Index: 2},
			{Term: 3, Index: 3},
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
				Log:         log,
			},
			volatile: VolatileState{
				CommitIndex: 3,
				LastApplied: 3,
			},
			storage:         &memoryStorage{},
			appendEntriesCh: make(chan appendEntriesCall),
		}

		go func() {
			_ = n.Run(ctx)
		}()

		_, err := n.submitAppendEntries(ctx, AppendEntriesArgs{
			Term:         3,
			LeaderID:     "node-b",
			PrevLogIndex: 3,
			PrevLogTerm:  3,
			LeaderCommit: 1,
		})
		if err != nil {
			t.Fatalf("submit AppendEntries: %v", err)
		}

		n.mu.Lock()
		commitIndex := n.volatile.CommitIndex
		n.mu.Unlock()

		if commitIndex != 3 {
			t.Errorf("commit index decreased: got %d, want 3", commitIndex)
		}
	})
}

func TestRunAppliesNewlyCommittedEntry(t *testing.T) {
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
			applyCh:         make(chan LogEntry, 1),
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
			LeaderCommit: 2,
		})
		if err != nil {
			t.Fatalf("submit AppendEntries: %v", err)
		}

		if !reply.Success {
			t.Fatal("AppendEntries was rejected")
		}

		select {
		case got := <-n.applyCh:
			if !reflect.DeepEqual(got, entry2) {
				t.Errorf("applied entry: got %+v, want %+v", got, entry2)
			}

		default:
			t.Fatal("newly committed entry was not applied")
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
