package node

import (
	"context"
	"errors"
	"sync"
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
