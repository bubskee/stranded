package node

import (
	"math/rand/v2"
	"time"
)

type tickResult struct {
	startElection bool
	sendHeartbeat bool
}

func (n *Node) tickInterval() time.Duration {
	if n.cfg.TickInterval > 0 {
		return n.cfg.TickInterval
	}
	return 10 * time.Millisecond
}

func ticksFor(duration, interval time.Duration) int {
	ticks := duration / interval
	if duration%interval != 0 {
		ticks++
	}
	if ticks < 1 {
		ticks = 1
	}
	return int(ticks)
}

// Caller must hold n.mu.
func (n *Node) resetElectionTimeoutLocked() {
	timeout := n.cfg.ElectionTimeoutMin
	if max := n.cfg.ElectionTimeoutMax; max > timeout {
		timeout += rand.N(max - timeout)
	}

	n.electionElapsed = 0
	n.randomizedElectionTicks = ticksFor(timeout, n.tickInterval())
}

func (n *Node) resetElectionTimeout() {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.resetElectionTimeoutLocked()
}

// Called by Run once for each delivered tick.
func (n *Node) tick() tickResult {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.role == Leader {
		n.heartbeatElapsed++

		interval := n.cfg.HeartbeatInterval
		if interval <= 0 {
			interval = 50 * time.Millisecond
		}

		if n.heartbeatElapsed >= ticksFor(interval, n.tickInterval()) {
			n.heartbeatElapsed = 0
			return tickResult{sendHeartbeat: true}
		}

		return tickResult{}
	}

	n.electionElapsed++

	if n.electionElapsed >= n.randomizedElectionTicks {
		n.resetElectionTimeoutLocked()
		return tickResult{startElection: true}
	}

	return tickResult{}
}
