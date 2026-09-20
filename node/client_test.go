package node

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"testing/synctest"
	"time"
)

type gatedClientAppendTransport struct {
	calls   chan AppendEntriesArgs
	release chan struct{}
}

func newGatedClientAppendTransport() *gatedClientAppendTransport {
	return &gatedClientAppendTransport{
		calls:   make(chan AppendEntriesArgs, 1),
		release: make(chan struct{}),
	}
}

func (t *gatedClientAppendTransport) RequestVote(
	ctx context.Context,
	peer PeerID,
	args *RequestVoteArgs,
) (*RequestVoteReply, error) {
	panic("unexpected RequestVote")
}

func (t *gatedClientAppendTransport) AppendEntries(
	ctx context.Context,
	peer PeerID,
	args *AppendEntriesArgs,
) (*AppendEntriesReply, error) {
	call := *args
	call.Entries = append([]LogEntry(nil), args.Entries...)
	for i := range call.Entries {
		call.Entries[i].Command =
			append([]byte(nil), call.Entries[i].Command...)
	}

	select {
	case t.calls <- call:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	select {
	case <-t.release:
		return &AppendEntriesReply{
			Term:    args.Term,
			Success: true,
		}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func TestLeaderClientRequestPersistsReplicatesAndWaitsForCommit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		noop := LogEntry{
			Term:  3,
			Index: 1,
		}

		persistent := PersistentState{
			CurrentTerm: 3,
			Log:         []LogEntry{noop},
		}

		storage := &memoryStorage{
			state: persistent,
		}

		transport := newGatedClientAppendTransport()

		n := &Node{
			cfg: Config{
				ID: "node-a",
				Peers: map[PeerID]string{
					"node-b": "",
				},
				ElectionTimeoutMin: time.Hour,
				ElectionTimeoutMax: time.Hour,
			},
			role:       Leader,
			persistent: persistent,
			volatile: VolatileState{
				CommitIndex: 1,
				LastApplied: 1,
			},
			leaderState: &LeaderState{
				NextIndex: map[PeerID]uint64{
					"node-b": 2,
				},
				MatchIndex: map[PeerID]uint64{
					"node-b": 1,
				},
			},
			storage:         storage,
			transport:       transport,
			applyCh:         make(chan LogEntry, 1),
			requestVoteCh:   make(chan requestVoteCall),
			appendEntriesCh: make(chan appendEntriesCall),
			clientRequestCh: make(chan clientRequestCall),
		}

		go func() {
			_ = n.Run(ctx)
		}()

		resultCh := make(chan clientRequestResult, 1)
		errCh := make(chan error, 1)

		command := []byte("set x=1")

		go func() {
			result, err := n.submitClientRequest(ctx, command)
			if err != nil {
				errCh <- err
				return
			}

			resultCh <- result
		}()

		synctest.Wait()

		// The request must not complete merely because the leader
		// received it.
		select {
		case result := <-resultCh:
			t.Fatalf(
				"client request returned before commit: %+v",
				result,
			)
		case err := <-errCh:
			t.Fatalf("client request failed before commit: %v", err)
		default:
		}

		// It should already be durably in the leader's log.
		n.mu.Lock()
		log := append([]LogEntry(nil), n.persistent.Log...)
		n.mu.Unlock()

		wantEntry := LogEntry{
			Term:    3,
			Index:   2,
			Command: []byte("set x=1"),
		}

		if len(log) != 2 {
			t.Fatalf("leader log entries: got %d, want 2", len(log))
		}

		if !reflect.DeepEqual(log[1], wantEntry) {
			t.Errorf(
				"leader client entry: got %+v, want %+v",
				log[1],
				wantEntry,
			)
		}

		storage.mu.Lock()
		persisted := append([]LogEntry(nil), storage.state.Log...)
		storage.mu.Unlock()

		if len(persisted) != 2 {
			t.Fatalf(
				"persisted log entries: got %d, want 2",
				len(persisted),
			)
		}

		if !reflect.DeepEqual(persisted[1], wantEntry) {
			t.Errorf(
				"persisted client entry: got %+v, want %+v",
				persisted[1],
				wantEntry,
			)
		}

		// Replication should have started immediately.
		var appendCall AppendEntriesArgs

		select {
		case appendCall = <-transport.calls:
		default:
			t.Fatal("client entry was not sent to follower")
		}

		if appendCall.PrevLogIndex != 1 {
			t.Errorf(
				"PrevLogIndex: got %d, want 1",
				appendCall.PrevLogIndex,
			)
		}

		if !reflect.DeepEqual(
			appendCall.Entries,
			[]LogEntry{wantEntry},
		) {
			t.Errorf(
				"replicated entries: got %+v, want %+v",
				appendCall.Entries,
				[]LogEntry{wantEntry},
			)
		}

		// Let the follower acknowledge index 2.
		close(transport.release)
		synctest.Wait()

		var result clientRequestResult
		select {
		case result = <-resultCh:
		case err := <-errCh:
			t.Fatalf("client request after commit: %v", err)
		default:
			t.Fatal("committed client request did not complete")
		}

		if !result.success {
			t.Fatal("committed client request returned success=false")
		}

		select {
		case applied := <-n.applyCh:
			if !reflect.DeepEqual(applied, wantEntry) {
				t.Errorf(
					"applied entry: got %+v, want %+v",
					applied,
					wantEntry,
				)
			}
		default:
			t.Fatal("committed client entry was not applied")
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

func TestStepdownDetachesPendingClientRequests(t *testing.T) {
	reply := make(chan clientRequestResult, 1)

	n := &Node{
		role: Leader,
		persistent: PersistentState{
			CurrentTerm: 3,
		},
		leaderState: &LeaderState{
			NextIndex:  map[PeerID]uint64{},
			MatchIndex: map[PeerID]uint64{},
		},
		pendingClientRequests: map[uint64]chan clientRequestResult{
			2: reply,
		},
		storage: &memoryStorage{},
	}

	n.mu.Lock()
	detached, err := n.becomeFollowerLocked(4)
	remaining := len(n.pendingClientRequests)
	n.mu.Unlock()

	if err != nil {
		t.Fatalf("become follower: %v", err)
	}

	if len(detached) != 1 {
		t.Fatalf("detached requests: got %d, want 1", len(detached))
	}

	if detached[0] != reply {
		t.Fatal("detached the wrong client waiter")
	}

	if remaining != 0 {
		t.Errorf("pending requests: got %d, want 0", remaining)
	}

	select {
	case result := <-reply:
		t.Fatalf("stepdown notified client before explicit completion: %+v", result)
	default:
	}

	failClientRequests(detached)

	select {
	case result := <-reply:
		if result.success {
			t.Fatal("pending request succeeded after leadership loss")
		}
	default:
		t.Fatal("detached client request was not failed")
	}
}

func TestRunFailsPendingClientsOnHigherTermRequest(t *testing.T) {
	tests := []struct {
		name    string
		request func(context.Context, *Node) (bool, error)
	}{
		{
			name: "RequestVote with stale log",
			request: func(ctx context.Context, n *Node) (bool, error) {
				reply, err := n.submitRequestVote(ctx, RequestVoteArgs{
					Term:         4,
					CandidateID:  "node-b",
					LastLogIndex: 0,
					LastLogTerm:  0,
				})
				return reply.VoteGranted, err
			},
		},
		{
			name: "AppendEntries with log mismatch",
			request: func(ctx context.Context, n *Node) (bool, error) {
				reply, err := n.submitAppendEntries(ctx, AppendEntriesArgs{
					Term:         4,
					LeaderID:     "node-b",
					PrevLogIndex: 2,
					PrevLogTerm:  2,
				})
				return reply.Success, err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				clientReply := make(chan clientRequestResult, 1)

				persistent := PersistentState{
					CurrentTerm: 3,
					Log: []LogEntry{
						{Term: 3, Index: 1},
						{Term: 3, Index: 2, Command: []byte("set x=1")},
					},
				}

				n := &Node{
					cfg: Config{
						ID:                 "node-a",
						ElectionTimeoutMin: time.Hour,
						ElectionTimeoutMax: time.Hour,
					},
					role:       Leader,
					persistent: persistent,
					leaderState: &LeaderState{
						NextIndex:  map[PeerID]uint64{},
						MatchIndex: map[PeerID]uint64{},
					},
					storage: &memoryStorage{state: persistent},
					pendingClientRequests: map[uint64]chan clientRequestResult{
						2: clientReply,
					},
					requestVoteCh:   make(chan requestVoteCall),
					appendEntriesCh: make(chan appendEntriesCall),
				}

				runDone := make(chan error, 1)
				go func() {
					runDone <- n.Run(ctx)
				}()

				accepted, err := tt.request(ctx, n)
				if err != nil {
					t.Fatalf("higher-term request: %v", err)
				}
				if accepted {
					t.Fatal("request unexpectedly accepted")
				}

				select {
				case result := <-clientReply:
					if result.success {
						t.Fatal("pending client succeeded after leadership loss")
					}
					if result.err != nil {
						t.Fatalf("unexpected client error: %v", result.err)
					}
				default:
					t.Fatal("Run did not resolve pending client")
				}

				n.mu.Lock()
				remaining := len(n.pendingClientRequests)
				n.mu.Unlock()

				if remaining != 0 {
					t.Errorf("pending requests: got %d, want 0", remaining)
				}

				cancel()
				if err := <-runDone; err != context.Canceled {
					t.Fatalf("Run returned %v, want context.Canceled", err)
				}
			})
		})
	}
}

func TestRunFailsDetachedClientsBeforeReturningStorageError(t *testing.T) {
	tests := []struct {
		name    string
		request func(context.Context, *Node) error
	}{
		{
			name: "persist granted vote",
			request: func(ctx context.Context, n *Node) error {
				_, err := n.submitRequestVote(ctx, RequestVoteArgs{
					Term:         4,
					CandidateID:  "node-b",
					LastLogIndex: 2,
					LastLogTerm:  3,
				})
				return err
			},
		},
		{
			name: "persist incoming entries",
			request: func(ctx context.Context, n *Node) error {
				_, err := n.submitAppendEntries(ctx, AppendEntriesArgs{
					Term:         4,
					LeaderID:     "node-b",
					PrevLogIndex: 2,
					PrevLogTerm:  3,
					Entries: []LogEntry{
						{Term: 4, Index: 3, Command: []byte("set y=2")},
					},
				})
				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				saveErr := errors.New("second save failed")
				clientReply := make(chan clientRequestResult, 1)

				persistent := PersistentState{
					CurrentTerm: 3,
					VotedFor:    "node-a",
					Log: []LogEntry{
						{Term: 3, Index: 1},
						{Term: 3, Index: 2, Command: []byte("set x=1")},
					},
				}

				storage := &failSecondSaveStorage{
					state: persistent,
					err:   saveErr,
				}

				n := &Node{
					cfg: Config{
						ID:                 "node-a",
						ElectionTimeoutMin: time.Hour,
						ElectionTimeoutMax: time.Hour,
					},
					role:       Leader,
					persistent: persistent,
					leaderState: &LeaderState{
						NextIndex:  map[PeerID]uint64{},
						MatchIndex: map[PeerID]uint64{},
					},
					storage: storage,
					pendingClientRequests: map[uint64]chan clientRequestResult{
						2: clientReply,
					},
					requestVoteCh:   make(chan requestVoteCall),
					appendEntriesCh: make(chan appendEntriesCall),
				}

				runDone := make(chan error, 1)
				go func() {
					runDone <- n.Run(ctx)
				}()

				if err := tt.request(ctx, n); !errors.Is(err, saveErr) {
					t.Fatalf("request error: got %v, want %v", err, saveErr)
				}

				if err := <-runDone; !errors.Is(err, saveErr) {
					t.Fatalf("Run error: got %v, want %v", err, saveErr)
				}

				// Run has exited: all its state changes are now observable.
				if storage.saves != 2 {
					t.Fatalf("save attempts: got %d, want 2", storage.saves)
				}

				if n.role != Follower || n.persistent.CurrentTerm != 4 {
					t.Fatalf(
						"stepdown not published: role=%v term=%d",
						n.role,
						n.persistent.CurrentTerm,
					)
				}

				if len(n.pendingClientRequests) != 0 {
					t.Fatal("pending clients remained attached after stepdown")
				}

				select {
				case result := <-clientReply:
					if result.success {
						t.Fatal("pending client succeeded after leadership loss")
					}
				default:
					t.Fatal("detached client was lost on storage error")
				}
			})
		})
	}
}

func TestRunCancellationFailsPendingClientRequests(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		reply := make(chan clientRequestResult, 1)

		n := &Node{
			cfg: Config{
				ElectionTimeoutMin: time.Hour,
				ElectionTimeoutMax: time.Hour,
			},
			role: Leader,
			persistent: PersistentState{
				CurrentTerm: 3,
			},
			leaderState: &LeaderState{
				NextIndex:  map[PeerID]uint64{},
				MatchIndex: map[PeerID]uint64{},
			},
			pendingClientRequests: map[uint64]chan clientRequestResult{
				2: reply,
			},
			storage: &memoryStorage{},
		}

		runDone := make(chan error, 1)
		go func() {
			runDone <- n.Run(ctx)
		}()

		synctest.Wait()
		cancel()

		if err := <-runDone; !errors.Is(err, context.Canceled) {
			t.Fatalf("Run error: got %v, want context.Canceled", err)
		}

		select {
		case result := <-reply:
			if result.success {
				t.Fatal("pending client succeeded on shutdown")
			}
		default:
			t.Fatal("Run exited without resolving pending client")
		}

		if remaining := len(n.pendingClientRequests); remaining != 0 {
			t.Errorf("pending requests: got %d, want 0", remaining)
		}
	})
}

func TestRunStorageFailureFailsPendingClientRequests(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		saveErr := errors.New("stepdown save failed")
		reply := make(chan clientRequestResult, 1)

		persistent := PersistentState{
			CurrentTerm: 3,
			VotedFor:    "node-a",
		}

		n := &Node{
			cfg: Config{
				ID:                 "node-a",
				ElectionTimeoutMin: time.Hour,
				ElectionTimeoutMax: time.Hour,
			},
			role:       Leader,
			persistent: persistent,
			leaderState: &LeaderState{
				NextIndex:  map[PeerID]uint64{},
				MatchIndex: map[PeerID]uint64{},
			},
			storage: &failSecondSaveStorage{
				state: persistent,
				saves: 1, // Arm failure on the next Save.
				err:   saveErr,
			},
			pendingClientRequests: map[uint64]chan clientRequestResult{
				2: reply,
			},
			requestVoteCh: make(chan requestVoteCall),
		}

		runDone := make(chan error, 1)
		go func() {
			runDone <- n.Run(ctx)
		}()

		_, err := n.submitRequestVote(ctx, RequestVoteArgs{
			Term:        4,
			CandidateID: "node-b",
		})
		if !errors.Is(err, saveErr) {
			t.Fatalf("request error: got %v, want %v", err, saveErr)
		}

		if err := <-runDone; !errors.Is(err, saveErr) {
			t.Fatalf("Run error: got %v, want %v", err, saveErr)
		}

		// Failed persistence must not publish the stepdown.
		if n.role != Leader || n.persistent.CurrentTerm != 3 {
			t.Fatalf(
				"failed stepdown changed state: role=%v term=%d",
				n.role,
				n.persistent.CurrentTerm,
			)
		}

		select {
		case result := <-reply:
			if result.success {
				t.Fatal("pending client succeeded after storage failure")
			}
		default:
			t.Fatal("Run exited without resolving pending client")
		}

		if remaining := len(n.pendingClientRequests); remaining != 0 {
			t.Errorf("pending requests: got %d, want 0", remaining)
		}
	})
}

func TestIdleLeaderSendsPeriodicHeartbeatsWithoutStartingElection(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		transport := newGatedClientAppendTransport()
		close(transport.release) // Acknowledge each heartbeat immediately.

		persistent := PersistentState{
			CurrentTerm: 3,
			VotedFor:    "node-a",
			Log: []LogEntry{
				{Term: 3, Index: 1},
			},
		}

		n := &Node{
			cfg: Config{
				ID: "node-a",
				Peers: map[PeerID]string{
					"node-b": "",
				},
				ElectionTimeoutMin: 100 * time.Millisecond,
				ElectionTimeoutMax: 100 * time.Millisecond,
				HeartbeatInterval:  20 * time.Millisecond,
			},
			role:       Leader,
			persistent: persistent,
			volatile: VolatileState{
				CommitIndex: 1,
				LastApplied: 1,
			},
			leaderState: &LeaderState{
				NextIndex: map[PeerID]uint64{
					"node-b": 2,
				},
				MatchIndex: map[PeerID]uint64{
					"node-b": 1,
				},
			},
			storage:   &memoryStorage{state: persistent},
			transport: transport,
		}

		runDone := make(chan error, 1)
		go func() {
			runDone <- n.Run(ctx)
		}()

		synctest.Wait()

		// Six heartbeat intervals carry us past the election timeout.
		for beat := 1; beat <= 6; beat++ {
			time.Sleep(20 * time.Millisecond)
			synctest.Wait()

			select {
			case call := <-transport.calls:
				if call.Term != 3 {
					t.Fatalf("heartbeat %d: term got %d, want 3", beat, call.Term)
				}
				if len(call.Entries) != 0 {
					t.Fatalf("heartbeat %d: unexpected entries", beat)
				}
				if call.PrevLogIndex != 1 || call.PrevLogTerm != 3 {
					t.Fatalf("heartbeat %d: wrong log anchor: %+v", beat, call)
				}
				if call.LeaderCommit != 1 {
					t.Fatalf(
						"heartbeat %d: LeaderCommit got %d, want 1",
						beat, call.LeaderCommit,
					)
				}
			default:
				t.Fatalf("heartbeat %d was not sent", beat)
			}

			n.mu.Lock()
			role := n.role
			term := n.persistent.CurrentTerm
			n.mu.Unlock()

			if role != Leader || term != 3 {
				t.Fatalf(
					"after heartbeat %d: role=%v term=%d",
					beat, role, term,
				)
			}
		}

		cancel()
		if err := <-runDone; !errors.Is(err, context.Canceled) {
			t.Fatalf("Run returned %v, want context.Canceled", err)
		}
	})
}
