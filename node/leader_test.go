package node

import (
	"context"
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

		if matchIndex != 2 {
			t.Errorf("match index after successful AppendEntries: got %d, want 2", matchIndex)
		}

		if nextIndex != 3 {
			t.Errorf("next index after successful AppendEntries: got %d, want 3", nextIndex)
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
	_, err := n.handleAppendEntriesReply(appendReplyEvent{
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
	_, err = n.handleAppendEntriesReply(appendReplyEvent{
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

	_, err := n.handleAppendEntriesReply(appendReplyEvent{
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

		if !reflect.DeepEqual(second.Entries, []LogEntry{entry2}) {
			t.Errorf(
				"retry entries: got %+v, want %+v",
				second.Entries,
				[]LogEntry{entry2},
			)
		}
	})
}
