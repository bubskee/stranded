package node

import (
	"context"
	"errors"
	"reflect"
	"sync"
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

		if matchIndex != 3 {
			t.Errorf("match index after successful AppendEntries: got %d, want 3", matchIndex)
		}

		if nextIndex != 4 {
			t.Errorf("next index after successful AppendEntries: got %d, want 4", nextIndex)
		}
	})
}

func TestSuccessfulAppendEntriesReplyDoesNotRegressFollowerProgress(t *testing.T) {
	n := &Node{
		role: Leader,
		persistent: PersistentState{
			CurrentTerm: 3,
		},
		leaderState: &LeaderState{
			MatchIndex: map[PeerID]uint64{
				"node-b": 2,
			},
			NextIndex: map[PeerID]uint64{
				"node-b": 3,
			},
		},
		storage: &memoryStorage{},
	}

	// A newer RPC proves replication through index 5.
	_, _, err := n.handleAppendEntriesReply(appendReplyEvent{
		peer:       "node-b",
		sentTerm:   3,
		matchIndex: 5,
		reply: AppendEntriesReply{
			Term:    3,
			Success: true,
		},
	})
	if err != nil {
		t.Fatalf("newer AppendEntries reply: %v", err)
	}

	// Then an older in-flight RPC arrives late and only proves index 3.
	_, _, err = n.handleAppendEntriesReply(appendReplyEvent{
		peer:       "node-b",
		sentTerm:   3,
		matchIndex: 3,
		reply: AppendEntriesReply{
			Term:    3,
			Success: true,
		},
	})
	if err != nil {
		t.Fatalf("older AppendEntries reply: %v", err)
	}

	n.mu.Lock()
	matchIndex := n.leaderState.MatchIndex["node-b"]
	nextIndex := n.leaderState.NextIndex["node-b"]
	n.mu.Unlock()

	if matchIndex != 5 {
		t.Errorf("match index regressed: got %d, want 5", matchIndex)
	}

	if nextIndex != 6 {
		t.Errorf("next index regressed: got %d, want 6", nextIndex)
	}
}

func TestStaleTermAppendEntriesReplyDoesNotMutateFollowerProgress(t *testing.T) {
	n := &Node{
		role: Leader,
		persistent: PersistentState{
			CurrentTerm: 4,
		},
		leaderState: &LeaderState{
			MatchIndex: map[PeerID]uint64{
				"node-b": 5,
			},
			NextIndex: map[PeerID]uint64{
				"node-b": 6,
			},
		},
		storage: &memoryStorage{},
	}

	_, _, err := n.handleAppendEntriesReply(appendReplyEvent{
		peer:       "node-b",
		sentTerm:   3,
		matchIndex: 9,
		reply: AppendEntriesReply{
			Term:    3,
			Success: true,
		},
	})
	if err != nil {
		t.Fatalf("stale AppendEntries reply: %v", err)
	}

	n.mu.Lock()
	matchIndex := n.leaderState.MatchIndex["node-b"]
	nextIndex := n.leaderState.NextIndex["node-b"]
	n.mu.Unlock()

	if matchIndex != 5 {
		t.Errorf("match index changed after stale reply: got %d, want 5", matchIndex)
	}

	if nextIndex != 6 {
		t.Errorf("next index changed after stale reply: got %d, want 6", nextIndex)
	}
}

type retryAppendTransport struct {
	mu    sync.Mutex
	calls []AppendEntriesArgs
}

func (t *retryAppendTransport) RequestVote(
	ctx context.Context,
	peer PeerID,
	args *RequestVoteArgs,
) (*RequestVoteReply, error) {
	return &RequestVoteReply{
		Term:        args.Term,
		VoteGranted: true,
	}, nil
}

func (t *retryAppendTransport) AppendEntries(
	ctx context.Context,
	peer PeerID,
	args *AppendEntriesArgs,
) (*AppendEntriesReply, error) {
	t.mu.Lock()
	call := *args
	call.Entries = append([]LogEntry(nil), args.Entries...)
	t.calls = append(t.calls, call)
	callNumber := len(t.calls)
	t.mu.Unlock()

	if callNumber == 1 {
		return &AppendEntriesReply{
			Term:    args.Term,
			Success: false,
		}, nil
	}

	return &AppendEntriesReply{
		Term:    args.Term,
		Success: true,
	}, nil
}

func TestFailedAppendEntriesBacksUpAndRetries(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		transport := &retryAppendTransport{}

		entry1 := LogEntry{
			Term:    1,
			Index:   1,
			Command: []byte("one"),
		}
		entry2 := LogEntry{
			Term:    1,
			Index:   2,
			Command: []byte("two"),
		}

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
				Log: []LogEntry{
					entry1,
					entry2,
				},
			},
			transport: transport,
			storage:   &memoryStorage{},
		}

		go func() {
			_ = n.Run(ctx)
		}()

		time.Sleep(100 * time.Millisecond)
		synctest.Wait()

		transport.mu.Lock()
		calls := append([]AppendEntriesArgs(nil), transport.calls...)
		transport.mu.Unlock()

		if len(calls) < 2 {
			t.Fatalf("AppendEntries calls: got %d, want at least 2", len(calls))
		}

		first := calls[0]
		if first.PrevLogIndex != 2 {
			t.Errorf(
				"first PrevLogIndex: got %d, want 2",
				first.PrevLogIndex,
			)
		}

		second := calls[1]

		if second.PrevLogIndex != 1 {
			t.Errorf(
				"retry PrevLogIndex: got %d, want 1",
				second.PrevLogIndex,
			)
		}

		if second.PrevLogTerm != 1 {
			t.Errorf(
				"retry PrevLogTerm: got %d, want 1",
				second.PrevLogTerm,
			)
		}

		wantEntries := []LogEntry{
			entry2,
			{
				Term:  1,
				Index: 3,
			},
		}

		if !reflect.DeepEqual(second.Entries, wantEntries) {
			t.Errorf(
				"retry entries: got %+v, want %+v",
				second.Entries,
				wantEntries,
			)
		}
	})
}

func TestStaleAppendEntriesRejectionDoesNotRegressFollowerProgress(t *testing.T) {
	n := &Node{
		role: Leader,
		persistent: PersistentState{
			CurrentTerm: 3,
		},
		leaderState: &LeaderState{
			MatchIndex: map[PeerID]uint64{
				"node-b": 5,
			},
			NextIndex: map[PeerID]uint64{
				"node-b": 6,
			},
		},
		storage: &memoryStorage{},
	}

	// This rejection belongs to an older RPC that was sent when
	// node-b's NextIndex was still 3. Since then, newer replication
	// has advanced it to 6.
	retry, _, err := n.handleAppendEntriesReply(appendReplyEvent{
		peer:      "node-b",
		sentTerm:  3,
		nextIndex: 3,
		reply: AppendEntriesReply{
			Term:    3,
			Success: false,
		},
	})
	if err != nil {
		t.Fatalf("stale AppendEntries rejection: %v", err)
	}

	if retry {
		t.Fatal("stale AppendEntries rejection requested a retry")
	}

	n.mu.Lock()
	matchIndex := n.leaderState.MatchIndex["node-b"]
	nextIndex := n.leaderState.NextIndex["node-b"]
	n.mu.Unlock()

	if matchIndex != 5 {
		t.Errorf("match index changed after stale rejection: got %d, want 5", matchIndex)
	}

	if nextIndex != 6 {
		t.Errorf("next index regressed after stale rejection: got %d, want 6", nextIndex)
	}
}

func TestSuccessfulAppendEntriesReplyAdvancesLeaderCommitIndex(t *testing.T) {
	n := &Node{
		cfg: Config{
			ID: "node-a",
			Peers: map[PeerID]string{
				"node-b": "",
				"node-c": "",
			},
		},
		role: Leader,
		persistent: PersistentState{
			CurrentTerm: 3,
			Log: []LogEntry{
				{Term: 2, Index: 1},
				{Term: 3, Index: 2},
			},
		},
		volatile: VolatileState{
			CommitIndex: 0,
		},
		leaderState: &LeaderState{
			MatchIndex: map[PeerID]uint64{
				"node-b": 0,
				"node-c": 0,
			},
			NextIndex: map[PeerID]uint64{
				"node-b": 1,
				"node-c": 1,
			},
		},
		storage: &memoryStorage{},
	}

	_, _, err := n.handleAppendEntriesReply(appendReplyEvent{
		peer:       "node-b",
		sentTerm:   3,
		nextIndex:  1,
		matchIndex: 2,
		reply: AppendEntriesReply{
			Term:    3,
			Success: true,
		},
	})
	if err != nil {
		t.Fatalf("successful AppendEntries reply: %v", err)
	}

	n.mu.Lock()
	matchIndex := n.leaderState.MatchIndex["node-b"]
	commitIndex := n.volatile.CommitIndex
	n.mu.Unlock()

	if matchIndex != 2 {
		t.Errorf("match index: got %d, want 2", matchIndex)
	}

	if commitIndex != 2 {
		t.Errorf("commit index: got %d, want 2", commitIndex)
	}
}

func TestLeaderDoesNotCommitOldTermEntryFromReplicaCountAlone(t *testing.T) {
	n := &Node{
		cfg: Config{
			ID: "node-a",
			Peers: map[PeerID]string{
				"node-b": "",
				"node-c": "",
			},
		},
		role: Leader,
		persistent: PersistentState{
			CurrentTerm: 3,
			Log: []LogEntry{
				{Term: 2, Index: 1},
			},
		},
		leaderState: &LeaderState{
			MatchIndex: map[PeerID]uint64{
				"node-b": 0,
				"node-c": 0,
			},
			NextIndex: map[PeerID]uint64{
				"node-b": 1,
				"node-c": 1,
			},
		},
		storage: &memoryStorage{},
	}

	_, _, err := n.handleAppendEntriesReply(appendReplyEvent{
		peer:       "node-b",
		sentTerm:   3,
		nextIndex:  1,
		matchIndex: 1,
		reply: AppendEntriesReply{
			Term:    3,
			Success: true,
		},
	})
	if err != nil {
		t.Fatalf("successful AppendEntries reply: %v", err)
	}

	n.mu.Lock()
	commitIndex := n.volatile.CommitIndex
	n.mu.Unlock()

	if commitIndex != 0 {
		t.Errorf(
			"old-term entry committed from replica count alone: got %d, want 0",
			commitIndex,
		)
	}
}

func TestLeaderAppendsCurrentTermNoOpOnElection(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		transport := newLeaderHeartbeatTransport()
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
			transport: transport,
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
		log := append([]LogEntry(nil), n.persistent.Log...)
		n.mu.Unlock()

		if role != Leader {
			t.Fatalf("role after election: got %s, want leader", role)
		}

		if term != 1 {
			t.Fatalf("term after election: got %d, want 1", term)
		}

		if len(log) != 1 {
			t.Fatalf("log entries after becoming leader: got %d, want 1", len(log))
		}

		got := log[0]

		if got.Term != 1 {
			t.Errorf("no-op term: got %d, want 1", got.Term)
		}

		if got.Index != 1 {
			t.Errorf("no-op index: got %d, want 1", got.Index)
		}

		if got.Command != nil {
			t.Errorf("no-op command: got %q, want nil", got.Command)
		}

		storage.mu.Lock()
		persisted := storage.state
		storage.mu.Unlock()

		if len(persisted.Log) != 1 {
			t.Fatalf(
				"persisted log entries after becoming leader: got %d, want 1",
				len(persisted.Log),
			)
		}

		if !reflect.DeepEqual(persisted.Log[0], got) {
			t.Errorf(
				"persisted no-op: got %+v, want %+v",
				persisted.Log[0],
				got,
			)
		}
	})
}

type failSecondSaveStorage struct {
	mu    sync.Mutex
	state PersistentState
	saves int
	err   error
}

func (s *failSecondSaveStorage) Load() (PersistentState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.state, nil
}

func (s *failSecondSaveStorage) Save(state PersistentState) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.saves++

	if s.saves == 2 {
		return s.err
	}

	s.state = state
	return nil
}

func TestLeaderNoOpPersistenceFailurePreventsBecomingLeader(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		persistErr := errors.New("disk exploded")

		storage := &failSecondSaveStorage{
			err: persistErr,
		}

		n := &Node{
			cfg: Config{
				ID:                 "node-a",
				Peers:              map[PeerID]string{},
				ElectionTimeoutMin: 100 * time.Millisecond,
				ElectionTimeoutMax: 100 * time.Millisecond,
			},
			storage: storage,
		}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		runErr := make(chan error, 1)
		go func() {
			runErr <- n.Run(ctx)
		}()

		time.Sleep(100 * time.Millisecond)
		synctest.Wait()

		select {
		case err := <-runErr:
			if !errors.Is(err, persistErr) {
				t.Fatalf("Run error: got %v, want %v", err, persistErr)
			}
		default:
			t.Fatal("Run did not return leader no-op persistence error")
		}

		n.mu.Lock()
		role := n.role
		term := n.persistent.CurrentTerm
		votedFor := n.persistent.VotedFor
		log := append([]LogEntry(nil), n.persistent.Log...)
		candidateState := n.candidateState
		leaderState := n.leaderState
		n.mu.Unlock()

		if role != Candidate {
			t.Errorf(
				"role after no-op persistence failure: got %s, want candidate",
				role,
			)
		}

		if term != 1 {
			t.Errorf("term after no-op persistence failure: got %d, want 1", term)
		}

		if votedFor != "node-a" {
			t.Errorf(
				"vote after no-op persistence failure: got %q, want %q",
				votedFor,
				"node-a",
			)
		}

		if len(log) != 0 {
			t.Errorf(
				"log published after no-op persistence failure: got %+v, want empty",
				log,
			)
		}

		if candidateState == nil {
			t.Error("candidate state cleared after no-op persistence failure")
		}

		if leaderState != nil {
			t.Errorf(
				"leader state published after no-op persistence failure: got %+v, want nil",
				leaderState,
			)
		}

		storage.mu.Lock()
		saves := storage.saves
		persisted := storage.state
		storage.mu.Unlock()

		if saves != 2 {
			t.Errorf("Save calls: got %d, want 2", saves)
		}

		if persisted.CurrentTerm != 1 {
			t.Errorf(
				"persisted term after failure: got %d, want 1",
				persisted.CurrentTerm,
			)
		}

		if persisted.VotedFor != "node-a" {
			t.Errorf(
				"persisted vote after failure: got %q, want %q",
				persisted.VotedFor,
				"node-a",
			)
		}

		if len(persisted.Log) != 0 {
			t.Errorf(
				"no-op persisted despite failed save: got %+v",
				persisted.Log,
			)
		}
	})
}

func TestLeaderAppliesEntryAfterQuorumCommit(t *testing.T) {
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
			transport: successfulAppendReplyTransport{},
			storage:   &memoryStorage{},
			applyCh:   make(chan LogEntry, 1),
		}

		go func() {
			_ = n.Run(ctx)
		}()

		time.Sleep(100 * time.Millisecond)
		synctest.Wait()

		n.mu.Lock()
		commitIndex := n.volatile.CommitIndex
		lastApplied := n.volatile.LastApplied
		n.mu.Unlock()

		if commitIndex != 1 {
			t.Fatalf("commit index: got %d, want 1", commitIndex)
		}

		select {
		case got := <-n.applyCh:
			want := LogEntry{
				Term:  1,
				Index: 1,
			}

			if !reflect.DeepEqual(got, want) {
				t.Errorf("applied entry: got %+v, want %+v", got, want)
			}
		default:
			t.Fatal("newly committed leader entry was not applied")
		}

		if lastApplied != 1 {
			t.Errorf("last applied: got %d, want 1", lastApplied)
		}
	})
}
