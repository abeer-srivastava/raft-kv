package node

import "encoding/json"

// RaftState represents the three possible states of a Raft node
type RaftState int

const (
	Follower RaftState = iota
	Candidate
	Leader
)

func (s RaftState) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

// LogEntry represents a single entry in the Raft log
type LogEntry struct {
	Term  uint64 `json:"term"`
	Index uint64 `json:"index"`
	Data  []byte `json:"data"`
}

// RequestVoteRequest is sent by candidates to gather votes
type RequestVoteRequest struct {
	Term         uint64 `json:"term"`
	CandidateID  int    `json:"candidate_id"`
	LastLogIndex uint64 `json:"last_log_index"`
	LastLogTerm  uint64 `json:"last_log_term"`
}

// RequestVoteResponse is the reply to RequestVoteRequest
type RequestVoteResponse struct {
	Term        uint64 `json:"term"`
	VoteGranted bool   `json:"vote_granted"`
}

// AppendEntriesRequest is sent by leader to replicate log entries
type AppendEntriesRequest struct {
	Term         uint64     `json:"term"`
	LeaderID     int        `json:"leader_id"`
	PrevLogIndex uint64     `json:"prev_log_index"`
	PrevLogTerm  uint64     `json:"prev_log_term"`
	Entries      []LogEntry `json:"entries"`
	LeaderCommit uint64     `json:"leader_commit"`
}

// AppendEntriesResponse is the reply to AppendEntriesRequest
type AppendEntriesResponse struct {
	Term          uint64 `json:"term"`
	Success       bool   `json:"success"`
	ConflictIndex uint64 `json:"conflict_index"`
	ConflictTerm  uint64 `json:"conflict_term"`
}

// InstallSnapshotRequest is sent by leader to stream snapshots to slow followers
type InstallSnapshotRequest struct {
	Term              uint64 `json:"term"`
	LeaderID          int    `json:"leader_id"`
	LastIncludedIndex uint64 `json:"last_included_index"`
	LastIncludedTerm  uint64 `json:"last_included_term"`
	Data              []byte `json:"data"`
	Done              bool   `json:"done"`
}

// InstallSnapshotResponse is the reply to InstallSnapshotRequest
type InstallSnapshotResponse struct {
	Term uint64 `json:"term"`
}

// RPCMessage is the envelope for RPC messages
type RPCMessage struct {
	Type string          `json:"type"`
	Data json.RawMessage `json:"data"`
}

// ClientRequest represents a client command sent to the cluster
type ClientRequest struct {
	Op    string `json:"op"` // "SET", "GET", "DELETE"
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}

// ClientResponse is sent back to the client after applying the command
type ClientResponse struct {
	Success    bool   `json:"success"`
	Value      string `json:"value,omitempty"`
	Error      string `json:"error,omitempty"`
	LeaderAddr string `json:"leader_addr,omitempty"`
}

// SnapshotData is sent through the snapshot channel to notify the state machine
// that it should replace its state with the provided snapshot data.
type SnapshotData struct {
	Index uint64 // Last included index in the snapshot
	Term  uint64 // Last included term in the snapshot
	Data  []byte // Serialized state machine state
}

// SnapshotProvider is a function that returns a snapshot of the current state machine.
// It is called by the Raft node when automatic compaction is triggered.
type SnapshotProvider func() ([]byte, error)
