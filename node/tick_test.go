package node

import (
	"testing"
	"time"
)

func TestElectionTicksTriggerAtDeadline(t *testing.T) {
	n := &Node{
		role: Follower,
		cfg: Config{
			TickInterval:       10 * time.Millisecond,
			ElectionTimeoutMin: 100 * time.Millisecond,
			ElectionTimeoutMax: 100 * time.Millisecond,
		},
	}

	n.resetElectionTimeout()

	for tick := 1; tick < 10; tick++ {
		if result := n.tick(); result.startElection {
			t.Fatalf("election triggered early at tick %d", tick)
		}
	}

	if result := n.tick(); !result.startElection {
		t.Fatal("election did not trigger at tick 10")
	}
}

func TestResetElectionTimeoutRestartsTickCountdown(t *testing.T) {
	n := &Node{
		role: Follower,
		cfg: Config{
			TickInterval:       10 * time.Millisecond,
			ElectionTimeoutMin: 100 * time.Millisecond,
			ElectionTimeoutMax: 100 * time.Millisecond,
		},
	}

	n.resetElectionTimeout()

	for tick := 1; tick <= 7; tick++ {
		if result := n.tick(); result.startElection {
			t.Fatalf("election triggered before reset at tick %d", tick)
		}
	}

	n.resetElectionTimeout()

	// Cross the original deadline, but stay before the new one.
	for tick := 1; tick < 10; tick++ {
		if result := n.tick(); result.startElection {
			t.Fatalf("election triggered early at tick %d after reset", tick)
		}
	}

	if result := n.tick(); !result.startElection {
		t.Fatal("election did not trigger at tick 10 after reset")
	}
}
