package node

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"testing/synctest"
	"time"
)

type clusterTransport struct {
	nodes map[PeerID]*Node
}

func (tr *clusterTransport) RequestVote(
	ctx context.Context,
	peer PeerID,
	args *RequestVoteArgs,
) (*RequestVoteReply, error) {
	reply, err := tr.nodes[peer].submitRequestVote(ctx, *args)
	return &reply, err
}

func (tr *clusterTransport) AppendEntries(
	ctx context.Context,
	peer PeerID,
	args *AppendEntriesArgs,
) (*AppendEntriesReply, error) {
	reply, err := tr.nodes[peer].submitAppendEntries(ctx, *args)
	return &reply, err
}

type testCluster struct {
	ctx    context.Context
	cancel context.CancelFunc
	nodes  map[PeerID]*Node
	ticks  map[PeerID]chan time.Time
	done   map[PeerID]chan error
}

// Call inside synctest.Test.
func newTestCluster(ids ...PeerID) *testCluster {
	ctx, cancel := context.WithCancel(context.Background())

	c := &testCluster{
		ctx:    ctx,
		cancel: cancel,
		nodes:  make(map[PeerID]*Node),
		ticks:  make(map[PeerID]chan time.Time),
		done:   make(map[PeerID]chan error),
	}

	transport := &clusterTransport{nodes: c.nodes}

	// Finish building the shared routing map before starting any node.
	for _, id := range ids {
		peers := make(map[PeerID]string)
		for _, peer := range ids {
			if peer != id {
				peers[peer] = ""
			}
		}

		c.nodes[id] = &Node{
			cfg: Config{
				ID:                 id,
				Peers:              peers,
				TickInterval:       10 * time.Millisecond,
				ElectionTimeoutMin: 100 * time.Millisecond,
				ElectionTimeoutMax: 100 * time.Millisecond,
				HeartbeatInterval:  20 * time.Millisecond,
			},
			role:            Follower,
			storage:         &memoryStorage{},
			transport:       transport,
			applyCh:         make(chan LogEntry, 16),
			requestVoteCh:   make(chan requestVoteCall),
			appendEntriesCh: make(chan appendEntriesCall),
			clientRequestCh: make(chan clientRequestCall),
		}
		c.ticks[id] = make(chan time.Time)
		c.done[id] = make(chan error, 1)
	}

	for _, id := range ids {
		go func() {
			c.done[id] <- c.nodes[id].run(c.ctx, c.ticks[id])
		}()
	}

	synctest.Wait()
	return c
}

func (c *testCluster) advance(t *testing.T, id PeerID, count int) {
	t.Helper()

	for range count {
		select {
		case c.ticks[id] <- time.Time{}:
		case err := <-c.done[id]:
			// Preserve the result for shutdown.
			c.done[id] <- err
			t.Fatalf("%s exited before tick: %v", id, err)
		}
		synctest.Wait()
	}
}

func (c *testCluster) stop(t *testing.T) {
	t.Helper()

	c.cancel()
	for id, done := range c.done {
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Errorf("%s run error: got %v, want context.Canceled", id, err)
		}
	}
}

func TestClusterReplicatesAndAppliesClientCommand(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newTestCluster("node-a", "node-b", "node-c")
		defer c.stop(t)

		// Only A reaches its election deadline.
		c.advance(t, "node-a", 10)

		for id, n := range c.nodes {
			n.mu.Lock()
			role := n.role
			term := n.persistent.CurrentTerm
			n.mu.Unlock()

			wantRole := Follower
			if id == "node-a" {
				wantRole = Leader
			}

			if role != wantRole || term != 1 {
				t.Fatalf(
					"%s after election: role=%v term=%d, want role=%v term=1",
					id, role, term, wantRole,
				)
			}
		}

		clientDone := make(chan clientRequestResult, 1)
		go func() {
			result, err := c.nodes["node-a"].submitClientRequest(
				c.ctx,
				[]byte("set x=1"),
			)
			result.err = err
			clientDone <- result
		}()

		synctest.Wait()

		select {
		case result := <-clientDone:
			if result.err != nil || !result.success {
				t.Fatalf("client request failed: %+v", result)
			}
		default:
			t.Fatal("client request did not complete after replication")
		}

		// Propagate the newly advanced LeaderCommit to both followers.
		c.advance(t, "node-a", 2)

		want := []LogEntry{
			{Term: 1, Index: 1}, // Leader's no-op.
			{Term: 1, Index: 2, Command: []byte("set x=1")},
		}

		for id, n := range c.nodes {
			for _, expected := range want {
				select {
				case got := <-n.applyCh:
					if !reflect.DeepEqual(got, expected) {
						t.Fatalf(
							"%s applied %+v, want %+v",
							id, got, expected,
						)
					}
				default:
					t.Fatalf("%s did not apply index %d", id, expected.Index)
				}
			}

			select {
			case extra := <-n.applyCh:
				t.Fatalf("%s applied unexpected extra entry: %+v", id, extra)
			default:
			}

			n.mu.Lock()
			commitIndex := n.volatile.CommitIndex
			lastApplied := n.volatile.LastApplied
			n.mu.Unlock()

			if commitIndex != 2 || lastApplied != 2 {
				t.Errorf(
					"%s: commit=%d applied=%d, want both 2",
					id, commitIndex, lastApplied,
				)
			}

			persisted, err := n.storage.Load()
			if err != nil {
				t.Fatalf("%s load storage: %v", id, err)
			}
			if !reflect.DeepEqual(persisted.Log, want) {
				t.Errorf(
					"%s persisted log: got %+v, want %+v",
					id, persisted.Log, want,
				)
			}
		}
	})
}
