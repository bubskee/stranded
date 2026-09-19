package node

import (
	"os"
	"path/filepath"
	"reflect"
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
			storage: &memoryStorage{},
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
			storage: &memoryStorage{},
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
