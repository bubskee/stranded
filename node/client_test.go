package node

import (
	"context"
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
