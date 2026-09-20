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

type clusterTransport struct {
	nodes    map[PeerID]*Node
	contexts map[PeerID]context.Context

	mu       sync.RWMutex
	isolated map[PeerID]bool
}

var errClusterPartition = errors.New("cluster link partitioned")

func (tr *clusterTransport) destination(
	from, to PeerID,
) (*Node, context.Context, error) {
	tr.mu.RLock()
	defer tr.mu.RUnlock()

	if tr.isolated[from] || tr.isolated[to] {
		return nil, nil, errClusterPartition
	}

	return tr.nodes[to], tr.contexts[to], nil
}

func (tr *clusterTransport) RequestVote(
	ctx context.Context,
	peer PeerID,
	args *RequestVoteArgs,
) (*RequestVoteReply, error) {
	n, peerCtx, err := tr.destination(args.CandidateID, peer)
	if err != nil {
		return nil, err
	}

	callCtx, cancel := tr.callContext(ctx, peerCtx)
	defer cancel()

	reply, err := n.submitRequestVote(callCtx, *args)
	return &reply, err
}

func (tr *clusterTransport) AppendEntries(
	ctx context.Context,
	peer PeerID,
	args *AppendEntriesArgs,
) (*AppendEntriesReply, error) {
	n, peerCtx, err := tr.destination(args.LeaderID, peer)
	if err != nil {
		return nil, err
	}

	callCtx, cancel := tr.callContext(ctx, peerCtx)
	defer cancel()

	reply, err := n.submitAppendEntries(callCtx, *args)
	return &reply, err
}

func (tr *clusterTransport) callContext(
	ctx context.Context,
	peerCtx context.Context,
) (context.Context, context.CancelFunc) {
	callCtx, cancel := context.WithCancel(ctx)

	stop := context.AfterFunc(peerCtx, cancel)
	if peerCtx.Err() != nil {
		cancel()
	}

	return callCtx, func() {
		stop()
		cancel()
	}
}

type testCluster struct {
	ctx          context.Context
	cancel       context.CancelFunc
	nodes        map[PeerID]*Node
	ticks        map[PeerID]chan time.Time
	done         map[PeerID]chan error
	nodeContexts map[PeerID]context.Context
	nodeCancels  map[PeerID]context.CancelFunc

	transport *clusterTransport
}

// Call inside synctest.Test.
func newTestCluster(t *testing.T, ids ...PeerID) *testCluster {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())

	c := &testCluster{
		ctx:          ctx,
		cancel:       cancel,
		nodes:        make(map[PeerID]*Node),
		ticks:        make(map[PeerID]chan time.Time),
		done:         make(map[PeerID]chan error),
		nodeContexts: make(map[PeerID]context.Context),
		nodeCancels:  make(map[PeerID]context.CancelFunc),
	}

	transport := &clusterTransport{
		nodes:    c.nodes,
		contexts: c.nodeContexts,
		isolated: make(map[PeerID]bool),
	}
	c.transport = transport

	// Finish building the shared routing map before starting any node.
	for _, id := range ids {
		peers := make(map[PeerID]string)
		for _, peer := range ids {
			if peer != id {
				peers[peer] = ""
			}
		}

		n, err := New(Config{
			ID:                 id,
			Peers:              peers,
			DataDir:            t.TempDir(),
			TickInterval:       10 * time.Millisecond,
			ElectionTimeoutMin: 100 * time.Millisecond,
			ElectionTimeoutMax: 100 * time.Millisecond,
			HeartbeatInterval:  20 * time.Millisecond,
		})
		if err != nil {
			c.cancel()
			t.Fatalf("create %s: %v", id, err)
		}

		n.transport = transport
		n.applyCh = make(chan LogEntry, 16)
		c.nodes[id] = n
		c.ticks[id] = make(chan time.Time)
		c.done[id] = make(chan error, 1)

		nodeCtx, nodeCancel := context.WithCancel(c.ctx)
		c.nodeContexts[id] = nodeCtx
		c.nodeCancels[id] = nodeCancel
	}

	for _, id := range ids {
		n := c.nodes[id]
		ctx := c.nodeContexts[id]
		ticks := c.ticks[id]
		done := c.done[id]

		go func() {
			done <- n.run(ctx, ticks)
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

func (c *testCluster) stopNode(t *testing.T, id PeerID) {
	t.Helper()

	done, ok := c.done[id]
	if !ok {
		t.Fatalf("%s is not running", id)
	}

	c.nodeCancels[id]()

	err := <-done
	delete(c.done, id) // Cluster cleanup only waits for remaining nodes.

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("%s run error: got %v, want context.Canceled", id, err)
	}

	synctest.Wait()
}

func (c *testCluster) isolate(id PeerID) {
	synctest.Wait()

	c.transport.mu.Lock()
	c.transport.isolated[id] = true
	c.transport.mu.Unlock()
}

func (c *testCluster) heal(id PeerID) {
	synctest.Wait()

	c.transport.mu.Lock()
	delete(c.transport.isolated, id)
	c.transport.mu.Unlock()
}

func (c *testCluster) restartNode(t *testing.T, id PeerID) {
	t.Helper()

	if _, running := c.done[id]; running {
		t.Fatalf("%s must be stopped before restart", id)
	}

	synctest.Wait()

	old := c.nodes[id]

	// New reopens the same data directory and loads persistent state.
	n, err := New(old.cfg)
	if err != nil {
		t.Fatalf("restart %s: %v", id, err)
	}

	n.transport = c.transport
	n.applyCh = make(chan LogEntry, 16)

	ctx, cancel := context.WithCancel(c.ctx)
	ticks := make(chan time.Time)
	done := make(chan error, 1)

	c.transport.mu.Lock()
	c.nodes[id] = n
	c.nodeContexts[id] = ctx
	c.transport.mu.Unlock()

	c.nodeCancels[id] = cancel
	c.ticks[id] = ticks
	c.done[id] = done

	go func() {
		done <- n.run(ctx, ticks)
	}()

	synctest.Wait()
}

// SCENARIOS

func TestClusterReplicatesAndAppliesClientCommand(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newTestCluster(t, "node-a", "node-b", "node-c")
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

func TestClusterContinuesAfterLeaderStops(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newTestCluster(t, "node-a", "node-b", "node-c")
		defer c.stop(t)

		submit := func(id PeerID, command string) {
			t.Helper()

			clientDone := make(chan clientRequestResult, 1)
			go func() {
				result, err := c.nodes[id].submitClientRequest(
					c.ctx,
					[]byte(command),
				)
				result.err = err
				clientDone <- result
			}()

			synctest.Wait()

			select {
			case result := <-clientDone:
				if result.err != nil || !result.success {
					t.Fatalf("%s client request failed: %+v", id, result)
				}
			default:
				t.Fatalf("%s client request did not complete", id)
			}
		}

		// A leads term 1 and commits the first command.
		c.advance(t, "node-a", 10)
		submit("node-a", "set x=1")
		c.advance(t, "node-a", 2)

		for _, id := range []PeerID{"node-b", "node-c"} {
			n := c.nodes[id]
			n.mu.Lock()
			commitIndex := n.volatile.CommitIndex
			n.mu.Unlock()

			if commitIndex != 2 {
				t.Fatalf(
					"%s before leader loss: commit=%d, want 2",
					id, commitIndex,
				)
			}
		}

		c.stopNode(t, "node-a")

		// B reaches its deadline. B + C still form a majority.
		c.advance(t, "node-b", 10)

		for _, id := range []PeerID{"node-b", "node-c"} {
			n := c.nodes[id]
			n.mu.Lock()
			role := n.role
			term := n.persistent.CurrentTerm
			n.mu.Unlock()

			wantRole := Follower
			if id == "node-b" {
				wantRole = Leader
			}

			if role != wantRole || term != 2 {
				t.Fatalf(
					"%s after replacement election: role=%v term=%d, "+
						"want role=%v term=2",
					id, role, term, wantRole,
				)
			}
		}

		submit("node-b", "set y=2")
		c.advance(t, "node-b", 2)

		want := []LogEntry{
			{Term: 1, Index: 1},
			{Term: 1, Index: 2, Command: []byte("set x=1")},
			{Term: 2, Index: 3},
			{Term: 2, Index: 4, Command: []byte("set y=2")},
		}

		for _, id := range []PeerID{"node-b", "node-c"} {
			n := c.nodes[id]

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

			if commitIndex != 4 || lastApplied != 4 {
				t.Errorf(
					"%s: commit=%d applied=%d, want both 4",
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

func TestClusterWithoutQuorumDoesNotCommitClientCommand(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newTestCluster(t, "node-a", "node-b", "node-c")
		defer c.stop(t)

		c.advance(t, "node-a", 10)
		c.advance(t, "node-a", 2)

		leader := c.nodes["node-a"]

		// Establish and consume the committed leader no-op.
		select {
		case got := <-leader.applyCh:
			want := LogEntry{Term: 1, Index: 1}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("initial applied entry: got %+v, want %+v", got, want)
			}
		default:
			t.Fatal("leader did not apply its initial no-op")
		}

		c.stopNode(t, "node-b")
		c.stopNode(t, "node-c")

		clientDone := make(chan clientRequestResult, 1)
		go func() {
			result, err := leader.submitClientRequest(
				c.ctx,
				[]byte("set x=1"),
			)
			result.err = err
			clientDone <- result
		}()

		synctest.Wait()

		// Cross an election timeout and several heartbeat intervals.
		c.advance(t, "node-a", 12)

		select {
		case result := <-clientDone:
			t.Fatalf("client returned without quorum: %+v", result)
		default:
		}

		wantLog := []LogEntry{
			{Term: 1, Index: 1},
			{Term: 1, Index: 2, Command: []byte("set x=1")},
		}

		persisted, err := leader.storage.Load()
		if err != nil {
			t.Fatalf("load leader storage: %v", err)
		}
		if !reflect.DeepEqual(persisted.Log, wantLog) {
			t.Fatalf(
				"persisted log: got %+v, want %+v",
				persisted.Log, wantLog,
			)
		}

		leader.mu.Lock()
		role := leader.role
		term := leader.persistent.CurrentTerm
		commitIndex := leader.volatile.CommitIndex
		lastApplied := leader.volatile.LastApplied
		pending := len(leader.pendingClientRequests)
		leader.mu.Unlock()

		if role != Leader || term != 1 {
			t.Errorf("isolated leader: role=%v term=%d, want leader term 1", role, term)
		}
		if commitIndex != 1 || lastApplied != 1 {
			t.Errorf(
				"without quorum: commit=%d applied=%d, want both 1",
				commitIndex, lastApplied,
			)
		}
		if pending != 1 {
			t.Errorf("pending requests: got %d, want 1", pending)
		}

		select {
		case got := <-leader.applyCh:
			t.Fatalf("applied entry without quorum: %+v", got)
		default:
		}

		// The client's context stays alive; node shutdown must resolve it.
		c.stopNode(t, "node-a")

		select {
		case result := <-clientDone:
			if result.success {
				t.Fatal("uncommitted client request succeeded on shutdown")
			}
		default:
			t.Fatal("shutdown did not resolve pending client")
		}

		if remaining := len(leader.pendingClientRequests); remaining != 0 {
			t.Errorf("pending requests after shutdown: got %d, want 0", remaining)
		}
	})
}

func TestClusterHealsPartitionAndReplacesUncommittedSuffix(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newTestCluster(t, "node-a", "node-b", "node-c")
		defer c.stop(t)

		startClient := func(id PeerID, command string) <-chan clientRequestResult {
			done := make(chan clientRequestResult, 1)
			go func() {
				result, err := c.nodes[id].submitClientRequest(
					c.ctx,
					[]byte(command),
				)
				result.err = err
				done <- result
			}()
			return done
		}

		// Establish the shared term-1 no-op.
		c.advance(t, "node-a", 10)
		c.advance(t, "node-a", 2)

		c.isolate("node-a")

		// A accepts locally, but cannot replicate to a majority.
		oldClient := startClient("node-a", "set x=old")
		synctest.Wait()

		select {
		case result := <-oldClient:
			t.Fatalf("isolated leader completed client request: %+v", result)
		default:
		}

		oldState, err := c.nodes["node-a"].storage.Load()
		if err != nil {
			t.Fatalf("load isolated leader storage: %v", err)
		}

		wantOldLog := []LogEntry{
			{Term: 1, Index: 1},
			{Term: 1, Index: 2, Command: []byte("set x=old")},
		}
		if !reflect.DeepEqual(oldState.Log, wantOldLog) {
			t.Fatalf(
				"isolated leader log: got %+v, want %+v",
				oldState.Log, wantOldLog,
			)
		}

		// B and C can elect a new leader without A.
		c.advance(t, "node-b", 10)

		b := c.nodes["node-b"]
		b.mu.Lock()
		role := b.role
		term := b.persistent.CurrentTerm
		b.mu.Unlock()

		if role != Leader || term != 2 {
			t.Fatalf("replacement leader: role=%v term=%d, want leader term 2", role, term)
		}

		newClient := startClient("node-b", "set x=new")
		synctest.Wait()

		select {
		case result := <-newClient:
			if result.err != nil || !result.success {
				t.Fatalf("majority-side client failed: %+v", result)
			}
		default:
			t.Fatal("majority-side client did not complete")
		}

		c.advance(t, "node-b", 2)

		// A still cannot know about B's election.
		select {
		case result := <-oldClient:
			t.Fatalf("isolated client completed before healing: %+v", result)
		default:
		}

		c.heal("node-a")
		c.advance(t, "node-b", 2)

		// Higher-term contact makes A relinquish its pending client.
		select {
		case result := <-oldClient:
			if result.success {
				t.Fatal("overwritten client command reported success")
			}
		default:
			t.Fatal("old leader did not resolve pending client after healing")
		}

		want := []LogEntry{
			{Term: 1, Index: 1},
			{Term: 2, Index: 2}, // B's no-op replaces A's command.
			{Term: 2, Index: 3, Command: []byte("set x=new")},
		}

		for id, n := range c.nodes {
			n.mu.Lock()
			role := n.role
			term := n.persistent.CurrentTerm
			commitIndex := n.volatile.CommitIndex
			lastApplied := n.volatile.LastApplied
			pending := len(n.pendingClientRequests)
			n.mu.Unlock()

			wantRole := Follower
			if id == "node-b" {
				wantRole = Leader
			}

			if role != wantRole || term != 2 {
				t.Errorf(
					"%s after healing: role=%v term=%d, want role=%v term=2",
					id, role, term, wantRole,
				)
			}
			if commitIndex != 3 || lastApplied != 3 {
				t.Errorf(
					"%s after healing: commit=%d applied=%d, want both 3",
					id, commitIndex, lastApplied,
				)
			}
			if pending != 0 {
				t.Errorf("%s pending requests: got %d, want 0", id, pending)
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
		}
	})
}

func TestClusterRestartedLeaderLoadsStateAndCatchesUp(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newTestCluster(t, "node-a", "node-b", "node-c")
		defer c.stop(t)

		submit := func(id PeerID, command string) {
			t.Helper()

			clientDone := make(chan clientRequestResult, 1)
			go func() {
				result, err := c.nodes[id].submitClientRequest(
					c.ctx,
					[]byte(command),
				)
				result.err = err
				clientDone <- result
			}()

			synctest.Wait()

			select {
			case result := <-clientDone:
				if result.err != nil || !result.success {
					t.Fatalf("%s client request failed: %+v", id, result)
				}
			default:
				t.Fatalf("%s client request did not complete", id)
			}
		}

		// A leads term 1 and commits the first command.
		c.advance(t, "node-a", 10)
		submit("node-a", "set x=1")
		c.advance(t, "node-a", 2)

		for _, id := range []PeerID{"node-b", "node-c"} {
			n := c.nodes[id]
			n.mu.Lock()
			commitIndex := n.volatile.CommitIndex
			n.mu.Unlock()

			if commitIndex != 2 {
				t.Fatalf(
					"%s before leader loss: commit=%d, want 2",
					id, commitIndex,
				)
			}
		}

		c.stopNode(t, "node-a")

		// B and C elect a replacement while A remains stopped.
		c.advance(t, "node-b", 10)

		for _, id := range []PeerID{"node-b", "node-c"} {
			n := c.nodes[id]
			n.mu.Lock()
			role := n.role
			term := n.persistent.CurrentTerm
			n.mu.Unlock()

			wantRole := Follower
			if id == "node-b" {
				wantRole = Leader
			}

			if role != wantRole || term != 2 {
				t.Fatalf(
					"%s after replacement election: role=%v term=%d, "+
						"want role=%v term=2",
					id, role, term, wantRole,
				)
			}
		}

		submit("node-b", "set y=2")
		c.advance(t, "node-b", 2)

		// Reconstruct A through New using its original data directory.
		c.restartNode(t, "node-a")

		a := c.nodes["node-a"]

		a.mu.Lock()
		loaded := a.persistent
		role := a.role
		commitIndex := a.volatile.CommitIndex
		lastApplied := a.volatile.LastApplied
		a.mu.Unlock()

		wantLoadedLog := []LogEntry{
			{Term: 1, Index: 1},
			{Term: 1, Index: 2, Command: []byte("set x=1")},
		}

		if loaded.CurrentTerm != 1 || loaded.VotedFor != "node-a" {
			t.Fatalf(
				"reloaded persistent state: term=%d vote=%q, "+
					"want term=1 vote=node-a",
				loaded.CurrentTerm, loaded.VotedFor,
			)
		}
		if !reflect.DeepEqual(loaded.Log, wantLoadedLog) {
			t.Fatalf(
				"reloaded log: got %+v, want %+v",
				loaded.Log, wantLoadedLog,
			)
		}
		if role != Follower || commitIndex != 0 || lastApplied != 0 {
			t.Fatalf(
				"restart volatile state: role=%v commit=%d applied=%d, "+
					"want follower and zero indices",
				role, commitIndex, lastApplied,
			)
		}

		// B supplies the missing suffix and its current commit index.
		c.advance(t, "node-b", 2)

		a.mu.Lock()
		role = a.role
		term := a.persistent.CurrentTerm
		a.mu.Unlock()

		if role != Follower || term != 2 {
			t.Fatalf(
				"recovered A: role=%v term=%d, want follower term 2",
				role, term,
			)
		}

		want := []LogEntry{
			{Term: 1, Index: 1},
			{Term: 1, Index: 2, Command: []byte("set x=1")},
			{Term: 2, Index: 3},
			{Term: 2, Index: 4, Command: []byte("set y=2")},
		}

		for _, id := range []PeerID{"node-a", "node-b", "node-c"} {
			n := c.nodes[id]

			// A rebuilds a fresh application from index 1.
			// B and C retain their original queued application history.
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

			if commitIndex != 4 || lastApplied != 4 {
				t.Errorf(
					"%s: commit=%d applied=%d, want both 4",
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
