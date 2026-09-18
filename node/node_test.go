package node

import (
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
