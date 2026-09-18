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

// PersistentState is the Raft state that must survive crashes. Changes must be
// made durable before any externally visible action whose correctness depends
// on them, such as granting a vote, acknowledging AppendEntries, or beginning
// an election in a new term.
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
