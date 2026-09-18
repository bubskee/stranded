package node

type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	default:
		return "unknown"
	}
}

type LogEntry struct {
	Term    uint64
	Index   uint64
	Command []byte
}

// PersistentState must hit disk (see persist.go) before the node responds
// to any RPC whose correctness depends on it — RequestVote and AppendEntries
// both fall in this category per §5.6.
type PersistentState struct {
	CurrentTerm uint64
	VotedFor    PeerID // "" means no vote cast this term
	Log         []LogEntry
}

// VolatileState is reinitialized on every restart.
type VolatileState struct {
	CommitIndex uint64
	LastApplied uint64
}

// LeaderState exists only while Role == Leader; reinitialized after every
// election per §5.3.
type LeaderState struct {
	NextIndex  map[PeerID]uint64
	MatchIndex map[PeerID]uint64
}

type CandidateState struct {
	Votes map[PeerID]bool
}
