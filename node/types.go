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
	Term    uint64 `json:"term"`
	Success bool   `json:"success"`
	// For optimization: follower's last log index to speed up backtracking
	ConflictIndex uint64 `json:"conflict_index"`
	ConflictTerm  uint64 `json:"conflict_term"`
}

// RPCMessage is the envelope for all RPC messages over the wire
type RPCMessage struct {
	Type string          `json:"type"`
	Data json.RawMessage `json:"data"`
}

// ClientRequest represents a client command sent to the cluster
type ClientRequest struct {
	Op    string `json:"op"`    // "SET", "GET", "DELETE"
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}

// ClientResponse is sent back to the client after applying the command
type ClientResponse struct {
	Success    bool   `json:"success"`
	Value      string `json:"value,omitempty"`
	Error      string `json:"error,omitempty"`
	LeaderAddr string `json:"leader_addr,omitempty"` // Set on redirect so clients can find the leader
}
