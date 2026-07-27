package node

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"sync"
	"time"

	raftlog "log"
)

const (
	electionTimeoutMin = 150 * time.Millisecond
	electionTimeoutMax = 300 * time.Millisecond
	heartbeatInterval  = 50 * time.Millisecond
)

// SendRequestVoteFunc sends a RequestVote RPC to a peer
type SendRequestVoteFunc func(peer int, req RequestVoteRequest) (*RequestVoteResponse, error)

// SendAppendEntriesFunc sends an AppendEntries RPC to a peer
type SendAppendEntriesFunc func(peer int, req AppendEntriesRequest) (*AppendEntriesResponse, error)

// RaftNode is the core Raft consensus node
type RaftNode struct {
	mu sync.Mutex

	id      int
	peers   []int
	commitCh chan<- LogEntry

	// Persistent state (must be persisted before responding to RPCs)
	currentTerm uint64
	votedFor    int
	raftLog     []LogEntry

	// Volatile state
	commitIndex uint64
	lastApplied uint64
	state       RaftState

	// Leader volatile state
	nextIndex  map[int]uint64
	matchIndex map[int]uint64

	// Channels for internal communication
	requestVoteCh   chan rpcMessage[RequestVoteRequest, RequestVoteResponse]
	appendEntriesCh chan rpcMessage[AppendEntriesRequest, AppendEntriesResponse]
	clientRequestCh chan clientMsg
	quit            chan struct{}
	quitOnce        sync.Once

	// RPC senders
	sendRequestVote   SendRequestVoteFunc
	sendAppendEntries SendAppendEntriesFunc

	// Persistence
	persistence *WAL

	// For testing
	onStateChange func(old, new RaftState)
}

// rpcMessage wraps an RPC request with a response channel
type rpcMessage[Req, Resp any] struct {
	req    Req
	respCh chan Resp
	peerID int
}

// clientMsg wraps a client request with a response channel
type clientMsg struct {
	req    ClientRequest
	respCh chan ClientResponse
}

// NewRaftNode creates a new Raft node
func NewRaftNode(
	id int,
	peers []int,
	commitCh chan<- LogEntry,
	sendReqVote SendRequestVoteFunc,
	sendAE SendAppendEntriesFunc,
	persistence *WAL,
) *RaftNode {
	raftLog := []LogEntry{{Term: 0, Index: 0}}

	node := &RaftNode{
		id:               id,
		peers:            peers,
		commitCh:         commitCh,
		currentTerm:      0,
		votedFor:         -1,
		raftLog:          raftLog,
		commitIndex:      0,
		lastApplied:      0,
		state:            Follower,
		nextIndex:        make(map[int]uint64),
		matchIndex:       make(map[int]uint64),
		requestVoteCh:    make(chan rpcMessage[RequestVoteRequest, RequestVoteResponse], 100),
		appendEntriesCh:  make(chan rpcMessage[AppendEntriesRequest, AppendEntriesResponse], 100),
		clientRequestCh:  make(chan clientMsg, 100),
		quit:             make(chan struct{}),
		sendRequestVote:  sendReqVote,
		sendAppendEntries: sendAE,
		persistence:      persistence,
	}

	if persistence != nil {
		if err := persistence.Recover(node); err != nil {
			raftlog.Printf("Node %d: failed to recover from WAL: %v", id, err)
		}
	}

	return node
}

// Start begins the Raft node's main event loop
func (n *RaftNode) Start() {
	go n.run()
}

// Stop gracefully shuts down the node
func (n *RaftNode) Stop() {
	n.quitOnce.Do(func() {
		close(n.quit)
	})
}

// GetState returns the current state of the node (for testing)
func (n *RaftNode) GetState() (RaftState, uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.state, n.currentTerm
}

// GetID returns the node's ID
func (n *RaftNode) GetID() int {
	return n.id
}

// GetLastLogIndex returns the index of the last log entry
func (n *RaftNode) GetLastLogIndex() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.raftLog[len(n.raftLog)-1].Index
}

// GetLastLogTerm returns the term of the last log entry
func (n *RaftNode) GetLastLogTerm() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.raftLog[len(n.raftLog)-1].Term
}

// GetCommitIndex returns the current commit index
func (n *RaftNode) GetCommitIndex() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.commitIndex
}

// SubmitRequestVote feeds a RequestVote RPC into the node
func (n *RaftNode) SubmitRequestVote(peerID int, req RequestVoteRequest) RequestVoteResponse {
	respCh := make(chan RequestVoteResponse, 1)
	n.requestVoteCh <- rpcMessage[RequestVoteRequest, RequestVoteResponse]{
		req:    req,
		respCh: respCh,
		peerID: peerID,
	}
	return <-respCh
}

// SubmitAppendEntries feeds an AppendEntries RPC into the node
func (n *RaftNode) SubmitAppendEntries(peerID int, req AppendEntriesRequest) AppendEntriesResponse {
	respCh := make(chan AppendEntriesResponse, 1)
	n.appendEntriesCh <- rpcMessage[AppendEntriesRequest, AppendEntriesResponse]{
		req:    req,
		respCh: respCh,
		peerID: peerID,
	}
	return <-respCh
}

// SubmitClientRequest feeds a client request into the node
func (n *RaftNode) SubmitClientRequest(req ClientRequest) ClientResponse {
	respCh := make(chan ClientResponse, 1)
	n.clientRequestCh <- clientMsg{
		req:    req,
		respCh: respCh,
	}
	return <-respCh
}

// replicateToPeer sends AppendEntries to a specific peer
func (n *RaftNode) replicateToPeer(peer int) {
	n.mu.Lock()
	if n.state != Leader {
		n.mu.Unlock()
		return
	}

	nextIdx := n.nextIndex[peer]
	prevLogIdx := nextIdx - 1
	prevLogTerm := n.getEntryAt(prevLogIdx).Term

	var entries []LogEntry
	if nextIdx <= n.raftLog[len(n.raftLog)-1].Index {
		entries = make([]LogEntry, 0)
		for i := nextIdx; i <= n.raftLog[len(n.raftLog)-1].Index; i++ {
			entries = append(entries, n.getEntryAt(i))
		}
	}

	req := AppendEntriesRequest{
		Term:         n.currentTerm,
		LeaderID:     n.id,
		PrevLogIndex: prevLogIdx,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: n.commitIndex,
	}
	n.mu.Unlock()

	resp, err := n.sendAppendEntries(peer, req)
	if err != nil {
		return
	}

	n.handleAppendEntriesResponse(peer, req, *resp)
}

// handleAppendEntriesResponse processes the response to an AppendEntries RPC
func (n *RaftNode) handleAppendEntriesResponse(peer int, req AppendEntriesRequest, resp AppendEntriesResponse) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if resp.Term > n.currentTerm {
		n.becomeFollower(resp.Term)
		return
	}

	if n.state != Leader || n.currentTerm != req.Term {
		return
	}

	if resp.Success {
		newNext := req.PrevLogIndex + uint64(len(req.Entries)) + 1
		newMatch := newNext - 1
		n.nextIndex[peer] = newNext
		n.matchIndex[peer] = newMatch
		n.advanceCommitIndex()
	} else {
		if resp.ConflictTerm > 0 {
			n.nextIndex[peer] = resp.ConflictIndex
		} else {
			if n.nextIndex[peer] > 1 {
				n.nextIndex[peer]--
			}
		}
		go n.replicateToPeer(peer)
	}
}

// advanceCommitIndex checks if we can advance the commit index
func (n *RaftNode) advanceCommitIndex() {
	for idx := n.raftLog[len(n.raftLog)-1].Index; idx > n.commitIndex; idx-- {
		entry := n.getEntryAt(idx)
		if entry.Term != n.currentTerm {
			continue
		}

		count := 1
		for _, peer := range n.peers {
			if n.matchIndex[peer] >= idx {
				count++
			}
		}

		majority := (len(n.peers)+1)/2 + 1
		if count >= majority {
			n.commitIndex = idx
			n.applyCommitted()
			break
		}
	}
}

// applyCommitted applies committed entries to the state machine
func (n *RaftNode) applyCommitted() {
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		entry := n.getEntryAt(n.lastApplied)
		n.commitCh <- entry
	}
}

// getEntryAt returns the log entry at the given index
func (n *RaftNode) getEntryAt(index uint64) LogEntry {
	if index >= uint64(len(n.raftLog)) {
		return LogEntry{Term: 0, Index: 0}
	}
	return n.raftLog[index]
}

// --- Main Event Loop ---

func (n *RaftNode) run() {
	var electionTimer *time.Timer
	var heartbeatTimer *time.Timer

	var resetElectionTimer func()
	var resetHeartbeatTimer func()

	resetElectionTimer = func() {
		if electionTimer != nil {
			electionTimer.Stop()
		}
		electionTimer = time.AfterFunc(n.randomElectionTimeout(), func() {
			select {
			case n.requestVoteCh <- rpcMessage[RequestVoteRequest, RequestVoteResponse]{
				req: RequestVoteRequest{Term: n.currentTerm + 1, CandidateID: n.id},
			}:
			default:
			}
		})
	}

	resetHeartbeatTimer = func() {
		if heartbeatTimer != nil {
			heartbeatTimer.Stop()
		}
		heartbeatTimer = time.AfterFunc(heartbeatInterval, func() {
			n.mu.Lock()
			if n.state == Leader {
				for _, peer := range n.peers {
					go n.replicateToPeer(peer)
				}
			}
			n.mu.Unlock()
			resetHeartbeatTimer()
		})
	}

	// Start election timer
	resetElectionTimer()

	defer func() {
		if electionTimer != nil {
			electionTimer.Stop()
		}
		if heartbeatTimer != nil {
			heartbeatTimer.Stop()
		}
	}()

	for {
		select {
		case <-n.quit:
			return

		case msg := <-n.requestVoteCh:
			n.handleRequestVoteMsg(msg, resetElectionTimer)

		case msg := <-n.appendEntriesCh:
			n.handleAppendEntriesMsg(msg, resetElectionTimer, resetHeartbeatTimer)

		case msg := <-n.clientRequestCh:
			n.handleClientMsg(msg)
		}
	}
}

// handleRequestVoteMsg handles an incoming RequestVote RPC
func (n *RaftNode) handleRequestVoteMsg(msg rpcMessage[RequestVoteRequest, RequestVoteResponse], resetTimer func()) {
	n.mu.Lock()
	defer n.mu.Unlock()

	req := msg.req

	// If this is an election timeout signal (self-directed)
	if req.CandidateID == n.id && req.Term == n.currentTerm+1 {
		n.startElection()
		return
	}

	resp := RequestVoteResponse{
		Term:        n.currentTerm,
		VoteGranted: false,
	}

	if req.Term < n.currentTerm {
		msg.respCh <- resp
		return
	}

	if req.Term > n.currentTerm {
		n.becomeFollower(req.Term)
	}

	canVote := n.votedFor == -1 || n.votedFor == req.CandidateID
	lastEntry := n.raftLog[len(n.raftLog)-1]
	logUpToDate := req.LastLogTerm > lastEntry.Term ||
		(req.LastLogTerm == lastEntry.Term && req.LastLogIndex >= lastEntry.Index)

	if canVote && logUpToDate {
		resp.VoteGranted = true
		resp.Term = n.currentTerm
		n.votedFor = req.CandidateID

		if n.persistence != nil {
			n.persistence.SaveTerm(n.currentTerm, n.votedFor)
		}
		resetTimer()
	}

	msg.respCh <- resp
}

// handleAppendEntriesMsg handles an incoming AppendEntries RPC
func (n *RaftNode) handleAppendEntriesMsg(msg rpcMessage[AppendEntriesRequest, AppendEntriesResponse], resetTimer func(), resetHeartbeat func()) {
	n.mu.Lock()
	defer n.mu.Unlock()

	req := msg.req

	if req.Term < n.currentTerm {
		msg.respCh <- AppendEntriesResponse{
			Term:    n.currentTerm,
			Success: false,
		}
		return
	}

	if req.Term >= n.currentTerm {
		n.becomeFollower(req.Term)
		n.votedFor = req.LeaderID
	}

	// Reset election timer on valid AppendEntries
	resetTimer()

	resp := AppendEntriesResponse{
		Term:    n.currentTerm,
		Success: false,
	}

	if req.PrevLogIndex > 0 {
		if req.PrevLogIndex >= uint64(len(n.raftLog)) {
			resp.ConflictIndex = uint64(len(n.raftLog))
			resp.ConflictTerm = 0
			msg.respCh <- resp
			return
		}

		if n.raftLog[req.PrevLogIndex].Term != req.PrevLogTerm {
			conflictTerm := n.raftLog[req.PrevLogIndex].Term
			resp.ConflictTerm = conflictTerm
			for i := req.PrevLogIndex; i > 0; i-- {
				if n.raftLog[i-1].Term != conflictTerm {
					resp.ConflictIndex = i
					break
				}
				if i == 1 {
					resp.ConflictIndex = 1
				}
			}
			msg.respCh <- resp
			return
		}
	}

	for _, entry := range req.Entries {
		if entry.Index < uint64(len(n.raftLog)) {
			if n.raftLog[entry.Index].Term != entry.Term {
				n.raftLog = n.raftLog[:entry.Index]
				n.raftLog = append(n.raftLog, entry)
			}
		} else {
			n.raftLog = append(n.raftLog, entry)
		}
	}

	if n.persistence != nil && len(req.Entries) > 0 {
		n.persistence.AppendEntries(req.Entries)
	}

	if req.LeaderCommit > n.commitIndex {
		n.commitIndex = raftMin(req.LeaderCommit, n.raftLog[len(n.raftLog)-1].Index)
		n.applyCommitted()
	}

	resp.Success = true
	resp.Term = n.currentTerm
	msg.respCh <- resp
}

// handleClientMsg handles a client request
func (n *RaftNode) handleClientMsg(msg clientMsg) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.state != Leader {
		msg.respCh <- ClientResponse{
			Success: false,
			Error:   fmt.Sprintf("not leader, leader is node %d", n.votedFor),
		}
		return
	}

	data, err := json.Marshal(msg.req)
	if err != nil {
		msg.respCh <- ClientResponse{
			Success: false,
			Error:   fmt.Sprintf("failed to marshal request: %v", err),
		}
		return
	}

	entry := LogEntry{
		Term:  n.currentTerm,
		Index: n.raftLog[len(n.raftLog)-1].Index + 1,
		Data:  data,
	}
	n.raftLog = append(n.raftLog, entry)

	if n.persistence != nil {
		n.persistence.AppendEntry(entry)
	}

	for _, peer := range n.peers {
		go n.replicateToPeer(peer)
	}

	go n.waitForCommit(entry.Index, msg.respCh)
}

// waitForCommit waits for a specific entry to be committed and sends the response
func (n *RaftNode) waitForCommit(index uint64, respCh chan ClientResponse) {
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()

	timeout := time.After(5 * time.Second)
	for {
		select {
		case <-ticker.C:
			n.mu.Lock()
			if n.commitIndex >= index {
				n.mu.Unlock()
				respCh <- ClientResponse{Success: true}
				return
			}
			n.mu.Unlock()
		case <-timeout:
			respCh <- ClientResponse{
				Success: false,
				Error:   "timeout waiting for commit",
			}
			return
		case <-n.quit:
			return
		}
	}
}

// --- State Transitions ---

// becomeFollower transitions the node to follower state
func (n *RaftNode) becomeFollower(term uint64) {
	oldState := n.state
	n.state = Follower
	n.currentTerm = term
	n.votedFor = -1

	if n.persistence != nil {
		n.persistence.SaveTerm(n.currentTerm, n.votedFor)
	}

	if n.onStateChange != nil {
		n.onStateChange(oldState, Follower)
	}
}

// startElection begins a new election
func (n *RaftNode) startElection() {
	if n.state == Leader {
		return
	}

	n.state = Candidate
	n.currentTerm++
	n.votedFor = n.id

	lastEntry := n.raftLog[len(n.raftLog)-1]
	term := n.currentTerm
	lastLogIndex := lastEntry.Index
	lastLogTerm := lastEntry.Term

	if n.persistence != nil {
		n.persistence.SaveTerm(n.currentTerm, n.votedFor)
	}

	raftlog.Printf("Node %d: starting election for term %d", n.id, term)

	votes := 1
	votesNeeded := (len(n.peers)+1)/2 + 1

	for _, peer := range n.peers {
		go func(p int) {
			req := RequestVoteRequest{
				Term:         term,
				CandidateID:  n.id,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}

			resp, err := n.sendRequestVote(p, req)
			if err != nil {
				return
			}

			n.mu.Lock()
			defer n.mu.Unlock()

			if resp.Term > n.currentTerm {
				n.becomeFollower(resp.Term)
				return
			}

			if n.state != Candidate || n.currentTerm != term {
				return
			}

			if resp.VoteGranted {
				votes++
				if votes >= votesNeeded {
					n.startLeader()
				}
			}
		}(peer)
	}
}

// startLeader transitions the node to leader state
func (n *RaftNode) startLeader() {
	if n.state != Candidate {
		return
	}

	n.state = Leader

	for _, peer := range n.peers {
		n.nextIndex[peer] = n.raftLog[len(n.raftLog)-1].Index + 1
		n.matchIndex[peer] = 0
	}

	raftlog.Printf("Node %d: became leader for term %d", n.id, n.currentTerm)

	if n.persistence != nil {
		n.persistence.SaveTerm(n.currentTerm, n.votedFor)
	}

	for _, peer := range n.peers {
		go n.replicateToPeer(peer)
	}
}

// randomElectionTimeout returns a random election timeout
func (n *RaftNode) randomElectionTimeout() time.Duration {
	return electionTimeoutMin + time.Duration(rand.Int63n(int64(electionTimeoutMax-electionTimeoutMin)))
}

func raftMin(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}
