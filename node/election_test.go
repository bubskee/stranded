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

type memoryStorage struct {
	mu    sync.Mutex
	state PersistentState
}

func (s *memoryStorage) Load() (PersistentState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.state, nil
}

func (s *memoryStorage) Save(state PersistentState) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.state = state
	return nil
}

type unavailableTransport struct{}

func (unavailableTransport) RequestVote(
	ctx context.Context,
	peer PeerID,
	args *RequestVoteArgs,
) (*RequestVoteReply, error) {
	return nil, errors.New("peer unavailable")
}

func (unavailableTransport) AppendEntries(
	ctx context.Context,
	peer PeerID,
	args *AppendEntriesArgs,
) (*AppendEntriesReply, error) {
	return nil, errors.New("peer unavailable")
}

func TestRunElectionTimeoutStartsElection(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		n := &Node{
			cfg: Config{
				ID: "node-a",
				Peers: map[PeerID]string{
					"node-b": "",
					"node-c": "",
				},
				ElectionTimeoutMin: 100 * time.Millisecond,
				ElectionTimeoutMax: 100 * time.Millisecond,
			},
			transport: unavailableTransport{},
			storage:   &memoryStorage{},
		}

		go func() {
			_ = n.Run(ctx)
		}()

		// Still a follower immediately before the deadline.
		time.Sleep(99 * time.Millisecond)
		synctest.Wait()

		n.mu.Lock()
		role := n.role
		n.mu.Unlock()

		if role != Follower {
			t.Fatalf("role before election timeout: got %s, want follower", role)
		}

		// Election timeout expires.
		time.Sleep(time.Millisecond)
		synctest.Wait()

		n.mu.Lock()
		role = n.role
		term := n.persistent.CurrentTerm
		votedFor := n.persistent.VotedFor
		n.mu.Unlock()

		if role != Candidate {
			t.Errorf("role after election timeout: got %s, want candidate", role)
		}

		if term != 1 {
			t.Errorf("term after election timeout: got %d, want 1", term)
		}

		if votedFor != "node-a" {
			t.Errorf("vote after election timeout: got %q, want %q", votedFor, "node-a")
		}
	})
}

type recordingTransport struct {
	mu       sync.Mutex
	voteArgs map[PeerID]RequestVoteArgs
}

func newRecordingTransport() *recordingTransport {
	return &recordingTransport{
		voteArgs: make(map[PeerID]RequestVoteArgs),
	}
}

func (t *recordingTransport) RequestVote(
	ctx context.Context,
	peer PeerID,
	args *RequestVoteArgs,
) (*RequestVoteReply, error) {
	t.mu.Lock()
	t.voteArgs[peer] = *args
	t.mu.Unlock()

	return nil, errors.New("peer unavailable")
}

func (t *recordingTransport) AppendEntries(
	ctx context.Context,
	peer PeerID,
	args *AppendEntriesArgs,
) (*AppendEntriesReply, error) {
	return nil, errors.New("peer unavailable")
}

func TestElectionSendsRequestVoteToPeers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		transport := newRecordingTransport()

		n := &Node{
			cfg: Config{
				ID: "node-a",
				Peers: map[PeerID]string{
					"node-b": "",
					"node-c": "",
				},
				ElectionTimeoutMin: 100 * time.Millisecond,
				ElectionTimeoutMax: 100 * time.Millisecond,
			},
			persistent: PersistentState{
				CurrentTerm: 4,
				Log: []LogEntry{
					{Term: 2, Index: 1},
					{Term: 4, Index: 2},
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

		want := RequestVoteArgs{
			Term:         5,
			CandidateID:  "node-a",
			LastLogIndex: 2,
			LastLogTerm:  4,
		}

		for _, peer := range []PeerID{"node-b", "node-c"} {
			transport.mu.Lock()
			got, ok := transport.voteArgs[peer]
			transport.mu.Unlock()

			if !ok {
				t.Errorf("no RequestVote sent to %q", peer)
				continue
			}

			if got != want {
				t.Errorf("RequestVote to %q: got %+v, want %+v", peer, got, want)
			}
		}
	})
}

type voteReplyTransport struct {
	replies map[PeerID]RequestVoteReply
}

func (t voteReplyTransport) RequestVote(
	ctx context.Context,
	peer PeerID,
	args *RequestVoteArgs,
) (*RequestVoteReply, error) {
	reply, ok := t.replies[peer]
	if !ok {
		return nil, errors.New("peer unavailable")
	}
	return &reply, nil
}

func (t voteReplyTransport) AppendEntries(
	ctx context.Context,
	peer PeerID,
	args *AppendEntriesArgs,
) (*AppendEntriesReply, error) {
	return nil, errors.New("peer unavailable")
}

func TestElectionMajorityBecomesLeader(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		n := &Node{
			cfg: Config{
				ID: "node-a",
				Peers: map[PeerID]string{
					"node-b": "",
					"node-c": "",
				},
				ElectionTimeoutMin: 100 * time.Millisecond,
				ElectionTimeoutMax: 100 * time.Millisecond,
			},
			transport: voteReplyTransport{
				replies: map[PeerID]RequestVoteReply{
					"node-b": {
						Term:        1,
						VoteGranted: true,
					},
					// node-c is unavailable
				},
			},
			storage: &memoryStorage{},
		}

		go func() {
			_ = n.Run(ctx)
		}()

		time.Sleep(100 * time.Millisecond)
		synctest.Wait()

		n.mu.Lock()
		role := n.role
		term := n.persistent.CurrentTerm
		n.mu.Unlock()

		if role != Leader {
			t.Errorf("role after majority vote: got %s, want leader", role)
		}

		if term != 1 {
			t.Errorf("term after election: got %d, want 1", term)
		}
	})
}

func TestHigherTermVoteReplyStepsDown(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		n := &Node{
			cfg: Config{
				ID: "node-a",
				Peers: map[PeerID]string{
					"node-b": "",
					"node-c": "",
				},
				ElectionTimeoutMin: 100 * time.Millisecond,
				ElectionTimeoutMax: 100 * time.Millisecond,
			},
			transport: voteReplyTransport{
				replies: map[PeerID]RequestVoteReply{
					"node-b": {
						Term:        2,
						VoteGranted: false,
					},
					// node-c is unavailable
				},
			},
			storage: &memoryStorage{},
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
		candidateState := n.candidateState
		n.mu.Unlock()

		if role != Follower {
			t.Errorf("role after higher-term reply: got %s, want follower", role)
		}

		if term != 2 {
			t.Errorf("term after higher-term reply: got %d, want 2", term)
		}

		if votedFor != "" {
			t.Errorf("vote after higher-term reply: got %q, want no vote", votedFor)
		}

		if candidateState != nil {
			t.Errorf("candidate state after stepping down: got %+v, want nil", candidateState)
		}
	})
}

func TestSingleNodeElectionBecomesLeader(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		n := &Node{
			cfg: Config{
				ID:                 "node-a",
				Peers:              map[PeerID]string{},
				ElectionTimeoutMin: 100 * time.Millisecond,
				ElectionTimeoutMax: 100 * time.Millisecond,
			},
			transport: unavailableTransport{},
			storage:   &memoryStorage{},
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
		n.mu.Unlock()

		if role != Leader {
			t.Errorf("role after single-node election: got %s, want leader", role)
		}

		if term != 1 {
			t.Errorf("term after single-node election: got %d, want 1", term)
		}

		if votedFor != "node-a" {
			t.Errorf("vote after single-node election: got %q, want %q", votedFor, "node-a")
		}
	})
}

type eventRecorder struct {
	mu     sync.Mutex
	events []string
}

func (r *eventRecorder) record(event string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.events = append(r.events, event)
}

func (r *eventRecorder) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]string(nil), r.events...)
}

type recordingStorage struct {
	recorder *eventRecorder
	err      error

	mu    sync.Mutex
	saved []PersistentState
}

func (s *recordingStorage) Load() (PersistentState, error) {
	return PersistentState{}, nil
}

func (s *recordingStorage) Save(state PersistentState) error {
	s.recorder.record("save")

	s.mu.Lock()
	s.saved = append(s.saved, state)
	s.mu.Unlock()

	return s.err
}

type orderingTransport struct {
	recorder *eventRecorder
}

func (t *orderingTransport) RequestVote(
	ctx context.Context,
	peer PeerID,
	args *RequestVoteArgs,
) (*RequestVoteReply, error) {
	t.recorder.record("request-vote")
	return nil, errors.New("peer unavailable")
}

func (t *orderingTransport) AppendEntries(
	ctx context.Context,
	peer PeerID,
	args *AppendEntriesArgs,
) (*AppendEntriesReply, error) {
	return nil, errors.New("peer unavailable")
}

func TestElectionPersistsBeforeRequestVote(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		recorder := &eventRecorder{}
		storage := &recordingStorage{recorder: recorder}

		n := &Node{
			cfg: Config{
				ID: "node-a",
				Peers: map[PeerID]string{
					"node-b": "",
				},
				ElectionTimeoutMin: 100 * time.Millisecond,
				ElectionTimeoutMax: 100 * time.Millisecond,
			},
			storage: storage,
			transport: &orderingTransport{
				recorder: recorder,
			},
		}

		go func() {
			_ = n.Run(ctx)
		}()

		time.Sleep(100 * time.Millisecond)
		synctest.Wait()

		gotEvents := recorder.snapshot()
		wantEvents := []string{"save", "request-vote"}

		if !reflect.DeepEqual(gotEvents, wantEvents) {
			t.Fatalf("election effects: got %v, want %v", gotEvents, wantEvents)
		}

		storage.mu.Lock()
		defer storage.mu.Unlock()

		if len(storage.saved) != 1 {
			t.Fatalf("saved states: got %d, want 1", len(storage.saved))
		}

		got := storage.saved[0]
		if got.CurrentTerm != 1 {
			t.Errorf("persisted term: got %d, want 1", got.CurrentTerm)
		}
		if got.VotedFor != "node-a" {
			t.Errorf("persisted vote: got %q, want %q", got.VotedFor, "node-a")
		}
	})
}

func TestElectionPersistenceFailurePreventsRequestVote(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		persistErr := errors.New("disk exploded")
		recorder := &eventRecorder{}

		n := &Node{
			cfg: Config{
				ID: "node-a",
				Peers: map[PeerID]string{
					"node-b": "",
				},
				ElectionTimeoutMin: 100 * time.Millisecond,
				ElectionTimeoutMax: 100 * time.Millisecond,
			},
			storage: &recordingStorage{
				recorder: recorder,
				err:      persistErr,
			},
			transport: &orderingTransport{
				recorder: recorder,
			},
		}

		ctx, cancel := context.WithCancel(context.Background())

		runErr := make(chan error, 1)
		go func() {
			runErr <- n.Run(ctx)
		}()

		time.Sleep(100 * time.Millisecond)
		synctest.Wait()

		gotEvents := recorder.snapshot()
		wantEvents := []string{"save"}

		if !reflect.DeepEqual(gotEvents, wantEvents) {
			t.Errorf("effects after persistence failure: got %v, want %v", gotEvents, wantEvents)
		}

		select {
		case err := <-runErr:
			if !errors.Is(err, persistErr) {
				t.Errorf("Run error: got %v, want %v", err, persistErr)
			}

		default:
			t.Error("Run did not return persistence error")

			// Cleanup for the intentionally-red implementation: Run is still
			// blocked in its event loop.
			cancel()
			synctest.Wait()
			<-runErr
		}
	})
}

func TestHigherTermVoteReplyPersistsFollowerState(t *testing.T) {
	recorder := &eventRecorder{}
	storage := &recordingStorage{recorder: recorder}

	n := &Node{
		cfg: Config{
			ID: "node-a",
			Peers: map[PeerID]string{
				"node-b": "",
				"node-c": "",
			},
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
		storage: storage,
	}

	n.handleVoteReply(
		"node-b",
		1,
		&RequestVoteReply{
			Term:        2,
			VoteGranted: false,
		},
	)

	storage.mu.Lock()
	defer storage.mu.Unlock()

	if len(storage.saved) != 1 {
		t.Fatalf("saved states: got %d, want 1", len(storage.saved))
	}

	got := storage.saved[0]

	if got.CurrentTerm != 2 {
		t.Errorf("persisted term: got %d, want 2", got.CurrentTerm)
	}

	if got.VotedFor != "" {
		t.Errorf("persisted vote: got %q, want no vote", got.VotedFor)
	}
}

func TestHigherTermVoteReplyPersistenceFailureDoesNotPublishFollowerState(t *testing.T) {
	persistErr := errors.New("disk exploded")
	recorder := &eventRecorder{}

	n := &Node{
		cfg: Config{
			ID: "node-a",
			Peers: map[PeerID]string{
				"node-b": "",
				"node-c": "",
			},
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
	}

	err := n.handleVoteReply(
		"node-b",
		1,
		&RequestVoteReply{
			Term:        2,
			VoteGranted: false,
		},
	)

	if !errors.Is(err, persistErr) {
		t.Fatalf("handleVoteReply error: got %v, want %v", err, persistErr)
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if n.role != Candidate {
		t.Errorf("role after persistence failure: got %s, want candidate", n.role)
	}

	if n.persistent.CurrentTerm != 1 {
		t.Errorf("term after persistence failure: got %d, want 1", n.persistent.CurrentTerm)
	}

	if n.persistent.VotedFor != "node-a" {
		t.Errorf(
			"vote after persistence failure: got %q, want %q",
			n.persistent.VotedFor,
			"node-a",
		)
	}

	if n.candidateState == nil {
		t.Error("candidate state cleared after persistence failure")
	}
}

func TestRunRejectsStaleRequestVote(t *testing.T) {
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
			storage:       &memoryStorage{},
			requestVoteCh: make(chan requestVoteCall),
		}

		go func() {
			_ = n.Run(ctx)
		}()

		reply, err := n.submitRequestVote(ctx, RequestVoteArgs{
			Term:        2,
			CandidateID: "node-b",
		})
		if err != nil {
			t.Fatalf("submit RequestVote: %v", err)
		}

		if reply.Term != 3 {
			t.Errorf("reply term: got %d, want 3", reply.Term)
		}

		if reply.VoteGranted {
			t.Error("stale RequestVote was granted")
		}
	})
}

func TestRunHigherTermRequestVoteStepsDown(t *testing.T) {
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
			storage:       storage,
			requestVoteCh: make(chan requestVoteCall),
		}

		go func() {
			_ = n.Run(ctx)
		}()

		reply, err := n.submitRequestVote(ctx, RequestVoteArgs{
			Term:        2,
			CandidateID: "node-b",
		})
		if err != nil {
			t.Fatalf("submit RequestVote: %v", err)
		}

		if reply.Term != 2 {
			t.Errorf("reply term: got %d, want 2", reply.Term)
		}

		n.mu.Lock()
		role := n.role
		term := n.persistent.CurrentTerm
		candidateState := n.candidateState
		n.mu.Unlock()

		if role != Follower {
			t.Errorf("role after higher-term RequestVote: got %s, want follower", role)
		}

		if term != 2 {
			t.Errorf("term after higher-term RequestVote: got %d, want 2", term)
		}

		if candidateState != nil {
			t.Errorf(
				"candidate state after higher-term RequestVote: got %+v, want nil",
				candidateState,
			)
		}

		storage.mu.Lock()
		persistedTerm := storage.state.CurrentTerm
		storage.mu.Unlock()

		if persistedTerm != 2 {
			t.Errorf("persisted term: got %d, want 2", persistedTerm)
		}
	})
}

func TestRunRejectsRequestVoteAfterVotingForAnotherCandidate(t *testing.T) {
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
				VotedFor:    "node-c",
			},
			storage:       &memoryStorage{},
			requestVoteCh: make(chan requestVoteCall),
		}

		go func() {
			_ = n.Run(ctx)
		}()

		reply, err := n.submitRequestVote(ctx, RequestVoteArgs{
			Term:        3,
			CandidateID: "node-b",
		})
		if err != nil {
			t.Fatalf("submit RequestVote: %v", err)
		}

		if reply.Term != 3 {
			t.Errorf("reply term: got %d, want 3", reply.Term)
		}

		if reply.VoteGranted {
			t.Error("RequestVote granted after voting for another candidate")
		}

		n.mu.Lock()
		votedFor := n.persistent.VotedFor
		n.mu.Unlock()

		if votedFor != "node-c" {
			t.Errorf("vote changed: got %q, want %q", votedFor, "node-c")
		}
	})
}

func TestRunGrantsRequestVoteWhenEligible(t *testing.T) {
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
			storage:       storage,
			requestVoteCh: make(chan requestVoteCall),
		}

		go func() {
			_ = n.Run(ctx)
		}()

		reply, err := n.submitRequestVote(ctx, RequestVoteArgs{
			Term:        3,
			CandidateID: "node-b",
		})
		if err != nil {
			t.Fatalf("submit RequestVote: %v", err)
		}

		if reply.Term != 3 {
			t.Errorf("reply term: got %d, want 3", reply.Term)
		}

		if !reply.VoteGranted {
			t.Error("eligible RequestVote was not granted")
		}

		n.mu.Lock()
		votedFor := n.persistent.VotedFor
		n.mu.Unlock()

		if votedFor != "node-b" {
			t.Errorf("vote after RequestVote: got %q, want %q", votedFor, "node-b")
		}

		storage.mu.Lock()
		persistedVote := storage.state.VotedFor
		storage.mu.Unlock()

		if persistedVote != "node-b" {
			t.Errorf("persisted vote: got %q, want %q", persistedVote, "node-b")
		}
	})
}

func TestRunRejectsRequestVoteWithStaleLog(t *testing.T) {
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
					{Term: 3, Index: 5},
				},
			},
			storage:       &memoryStorage{},
			requestVoteCh: make(chan requestVoteCall),
		}

		go func() {
			_ = n.Run(ctx)
		}()

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
			t.Error("RequestVote granted to candidate with stale log")
		}
	})
}

func TestRunRequestVotePersistenceFailureDoesNotGrantVote(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		persistErr := errors.New("disk exploded")
		recorder := &eventRecorder{}

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

		reply, err := n.submitRequestVote(ctx, RequestVoteArgs{
			Term:        3,
			CandidateID: "node-b",
		})

		if !errors.Is(err, persistErr) {
			t.Errorf("submit RequestVote error: got %v, want %v", err, persistErr)
		}

		if reply.VoteGranted {
			t.Error("RequestVote granted after persistence failure")
		}

		n.mu.Lock()
		votedFor := n.persistent.VotedFor
		n.mu.Unlock()

		if votedFor != "" {
			t.Errorf("vote published after persistence failure: got %q, want no vote", votedFor)
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
