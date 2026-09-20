package node

import "time"

type PeerID string

type Config struct {
	ID      PeerID
	Peers   map[PeerID]string
	DataDir string

	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
	HeartbeatInterval  time.Duration

	TickInterval time.Duration
}
