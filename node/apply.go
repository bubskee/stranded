package node

import (
	"context"
	"fmt"
)

func (n *Node) applyCommitted(ctx context.Context) error {
	for {
		n.mu.Lock()

		nextIndex := n.volatile.LastApplied + 1
		if nextIndex > n.volatile.CommitIndex {
			n.mu.Unlock()
			return nil
		}

		var entry LogEntry
		found := false

		for _, candidate := range n.persistent.Log {
			if candidate.Index == nextIndex {
				entry = candidate

				// Don't expose the persistent log's []byte backing storage
				// across the application boundary.
				entry.Command = append([]byte(nil), candidate.Command...)

				found = true
				break
			}
		}

		n.mu.Unlock()

		if !found {
			return fmt.Errorf("committed log entry %d missing", nextIndex)
		}

		// Intentionally outside n.mu. For now this may backpressure Run;
		// see the apply-path design note.
		select {
		case n.applyCh <- entry:
		case <-ctx.Done():
			return ctx.Err()
		}

		n.mu.Lock()
		n.volatile.LastApplied = nextIndex
		n.mu.Unlock()
	}
}
