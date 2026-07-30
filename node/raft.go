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
	electionTimeoutMin = 300 * time.Millisecond
	electionTimeoutMax = 500 * time.Millisecond
	heartbeatInterval  = 100 * time.Millisecond
)

// SendRequestVoteFunc sends a RequestVote RPC to a peer
type SendRequestVoteFunc func(peer int, req RequestVoteRequest) (*RequestVoteResponse, error)

// SendAppendEntriesFunc sends an AppendEntries RPC to a peer
type SendAppendEntriesFunc func(peer int, req AppendEntriesRequest) (*AppendEntriesResponse, error)

// RaftNode is the core Raft consensus node
type RaftNode struct {
	mu sync.Mutex

	id    int
	peers []int

	currentTerm uint64
	votedFor    int
	raftLog     []LogEntry

	leaderID int
	commitIndex uint64
	lastApplied uint64
	state       RaftState

	nextIndex  map[int]uint64
	matchIndex map[int]uint64

	commitCh        chan<- LogEntry
	requestVoteCh   chan rpcMessage[RequestVoteRequest, RequestVoteResponse]
	appendEntriesCh chan rpcMessage[AppendEntriesRequest, AppendEntriesResponse]
	clientRequestCh chan clientMsg
	quit            chan struct{}
	quitOnce        sync.Once

	sendRequestVote   SendRequestVoteFunc
	sendAppendEntries SendAppendEntriesFunc

	persistence *WAL

	heartbeatStopped chan struct{}

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
		id:                id,
		peers:             peers,
		commitCh:          commitCh,
		currentTerm:       0,
		votedFor:          -1,
		leaderID:          -1,
		raftLog:           raftLog,
		commitIndex:       0,
		lastApplied:       0,
		state:             Follower,
		nextIndex:         make(map[int]uint64),
		matchIndex:        make(map[int]uint64),
		requestVoteCh:     make(chan rpcMessage[RequestVoteRequest, RequestVoteResponse], 100),
		appendEntriesCh:   make(chan rpcMessage[AppendEntriesRequest, AppendEntriesResponse], 100),
		clientRequestCh:   make(chan clientMsg, 100),
		quit:              make(chan struct{}),
		sendRequestVote:   sendReqVote,
		sendAppendEntries: sendAE,
		persistence:       persistence,
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

// GetLeaderID returns the known leader ID (-1 if unknown)
func (n *RaftNode) GetLeaderID() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.leaderID
}

// GetLastLogIndex returns the index of the last log entry
func (n *RaftNode) GetLastLogIndex() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.lastLogIndex()
}

// GetLastLogTerm returns the term of the last log entry
func (n *RaftNode) GetLastLogTerm() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.lastLogEntry().Term
}

// GetCommitIndex returns the current commit index
func (n *RaftNode) GetCommitIndex() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.commitIndex
}

func (n *RaftNode) lastLogEntry() LogEntry {
	return n.raftLog[len(n.raftLog)-1]
}

func (n *RaftNode) lastLogIndex() uint64 {
	return n.raftLog[len(n.raftLog)-1].Index
}

func (n *RaftNode) getEntryAt(index uint64) LogEntry {
	if index >= uint64(len(n.raftLog)) {
		return LogEntry{Term: 0, Index: 0}
	}
	return n.raftLog[index]
}

// SubmitRequestVote feeds a RequestVote RPC into the node
func (n *RaftNode) SubmitRequestVote(peerID int, req RequestVoteRequest) RequestVoteResponse {
	respCh := make(chan RequestVoteResponse, 1)
	select {
	case n.requestVoteCh <- rpcMessage[RequestVoteRequest, RequestVoteResponse]{
		req:    req,
		respCh: respCh,
		peerID: peerID,
	}:
	case <-n.quit:
		return RequestVoteResponse{Term: 0, VoteGranted: false}
	}
	select {
	case resp := <-respCh:
		return resp
	case <-n.quit:
		return RequestVoteResponse{Term: 0, VoteGranted: false}
	}
}

// SubmitAppendEntries feeds an AppendEntries RPC into the node
func (n *RaftNode) SubmitAppendEntries(peerID int, req AppendEntriesRequest) AppendEntriesResponse {
	respCh := make(chan AppendEntriesResponse, 1)
	select {
	case n.appendEntriesCh <- rpcMessage[AppendEntriesRequest, AppendEntriesResponse]{
		req:    req,
		respCh: respCh,
		peerID: peerID,
	}:
	case <-n.quit:
		return AppendEntriesResponse{Term: 0, Success: false}
	}
	select {
	case resp := <-respCh:
		return resp
	case <-n.quit:
		return AppendEntriesResponse{Term: 0, Success: false}
	}
}

// SubmitClientRequest feeds a client request into the node
func (n *RaftNode) SubmitClientRequest(req ClientRequest) ClientResponse {
	respCh := make(chan ClientResponse, 1)
	select {
	case n.clientRequestCh <- clientMsg{
		req:    req,
		respCh: respCh,
	}:
	case <-n.quit:
		return ClientResponse{Success: false, Error: "node shutting down"}
	}
	select {
	case resp := <-respCh:
		return resp
	case <-n.quit:
		return ClientResponse{Success: false, Error: "node shutting down"}
	}
}

func (n *RaftNode) run() {
	n.mu.Lock()
	if n.lastApplied < n.commitIndex {
		raftlog.Printf("Node %d: replaying log entries %d..%d to state machine",
			n.id, n.lastApplied+1, n.commitIndex)
		n.applyCommitted()
	}
	n.mu.Unlock()

	electionTimer := time.NewTimer(n.randomElectionTimeout())
	defer electionTimer.Stop()

	for {
		select {
		case <-n.quit:
			n.mu.Lock()
			n.stopHeartbeatLocked()
			n.mu.Unlock()
			return

		case <-electionTimer.C:
			n.mu.Lock()
			if n.state != Leader {
				n.startElection()
			}
			n.mu.Unlock()
			electionTimer.Reset(n.randomElectionTimeout())

		case msg := <-n.requestVoteCh:
			resetTimer := n.handleRequestVoteMsg(msg)
			if resetTimer {
				if !electionTimer.Stop() {
					select {
					case <-electionTimer.C:
					default:
					}
				}
				electionTimer.Reset(n.randomElectionTimeout())
			}

		case msg := <-n.appendEntriesCh:
			resetTimer := n.handleAppendEntriesMsg(msg)
			if resetTimer {
				if !electionTimer.Stop() {
					select {
					case <-electionTimer.C:
					default:
					}
				}
				electionTimer.Reset(n.randomElectionTimeout())
			}

		case msg := <-n.clientRequestCh:
			n.handleClientMsg(msg)
		}
	}
}

func (n *RaftNode) handleRequestVoteMsg(msg rpcMessage[RequestVoteRequest, RequestVoteResponse]) bool {
	n.mu.Lock()
	defer n.mu.Unlock()

	req := msg.req
	resp := RequestVoteResponse{
		Term:        n.currentTerm,
		VoteGranted: false,
	}

	if req.Term < n.currentTerm {
		msg.respCh <- resp
		return false
	}

	if req.Term > n.currentTerm {
		n.becomeFollower(req.Term)
	}

	canVote := n.votedFor == -1 || n.votedFor == req.CandidateID

	lastEntry := n.lastLogEntry()
	logUpToDate := req.LastLogTerm > lastEntry.Term ||
		(req.LastLogTerm == lastEntry.Term && req.LastLogIndex >= lastEntry.Index)

	if canVote && logUpToDate {
		resp.VoteGranted = true
		resp.Term = n.currentTerm
		n.votedFor = req.CandidateID

		if n.persistence != nil {
			n.persistence.SaveTerm(n.currentTerm, n.votedFor)
		}

		msg.respCh <- resp
		return true
	}

	resp.Term = n.currentTerm
	msg.respCh <- resp
	return false
}

func (n *RaftNode) handleAppendEntriesMsg(msg rpcMessage[AppendEntriesRequest, AppendEntriesResponse]) bool {
	n.mu.Lock()
	defer n.mu.Unlock()

	req := msg.req

	if req.Term < n.currentTerm {
		msg.respCh <- AppendEntriesResponse{
			Term:    n.currentTerm,
			Success: false,
		}
		return false
	}

	if req.Term > n.currentTerm || n.state != Follower {
		n.becomeFollower(req.Term)
	}
	n.leaderID = req.LeaderID

	resp := AppendEntriesResponse{
		Term:    n.currentTerm,
		Success: false,
	}

	if req.PrevLogIndex > 0 {
		if req.PrevLogIndex >= uint64(len(n.raftLog)) {
			resp.ConflictIndex = uint64(len(n.raftLog))
			resp.ConflictTerm = 0
			msg.respCh <- resp
			return true // Still reset timer — leader is alive
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
			return true
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
		n.commitIndex = raftMin(req.LeaderCommit, n.lastLogIndex())
		n.applyCommitted()
	}

	resp.Success = true
	resp.Term = n.currentTerm
	msg.respCh <- resp
	return true
}

func (n *RaftNode) handleClientMsg(msg clientMsg) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.state != Leader {
		leader := n.leaderID
		errMsg := "not leader"
		if leader >= 0 {
			errMsg = fmt.Sprintf("not leader, leader is node %d", leader)
		}
		msg.respCh <- ClientResponse{
			Success: false,
			Error:   errMsg,
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
		Index: n.lastLogIndex() + 1,
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
	lastIdx := n.lastLogIndex()
	if nextIdx <= lastIdx {
		entries = make([]LogEntry, 0, lastIdx-nextIdx+1)
		for i := nextIdx; i <= lastIdx; i++ {
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
	currentTerm := n.currentTerm
	n.mu.Unlock()

	resp, err := n.sendAppendEntries(peer, req)
	if err != nil {
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if n.state != Leader || n.currentTerm != currentTerm {
		return
	}

	if resp.Term > n.currentTerm {
		n.becomeFollower(resp.Term)
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
		} else if n.nextIndex[peer] > 1 {
			n.nextIndex[peer]--
		}
		go n.replicateToPeer(peer)
	}
}

func (n *RaftNode) advanceCommitIndex() {
	lastIdx := n.lastLogIndex()
	for idx := lastIdx; idx > n.commitIndex; idx-- {
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

func (n *RaftNode) applyCommitted() {
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		entry := n.getEntryAt(n.lastApplied)
		n.commitCh <- entry
	}
}

func (n *RaftNode) waitForCommit(index uint64, respCh chan ClientResponse) {
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()

	timeout := time.After(5 * time.Second)
	for {
		select {
		case <-ticker.C:
			n.mu.Lock()
			committed := n.commitIndex >= index
			isLeader := n.state == Leader
			n.mu.Unlock()

			if committed {
				respCh <- ClientResponse{Success: true}
				return
			}
			if !isLeader {
				respCh <- ClientResponse{
					Success: false,
					Error:   "lost leadership while waiting for commit",
				}
				return
			}
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

func (n *RaftNode) becomeFollower(term uint64) {
	oldState := n.state
	n.state = Follower
	n.currentTerm = term
	n.votedFor = -1
	n.leaderID = -1

	n.stopHeartbeatLocked()

	if n.persistence != nil {
		n.persistence.SaveTerm(n.currentTerm, n.votedFor)
	}

	if n.onStateChange != nil && oldState != Follower {
		n.onStateChange(oldState, Follower)
	}
}

func (n *RaftNode) stopHeartbeatLocked() {
	if n.heartbeatStopped != nil {
		close(n.heartbeatStopped)
		n.heartbeatStopped = nil
	}
}

func (n *RaftNode) startElection() {
	if n.state == Leader {
		return
	}

	n.state = Candidate
	n.currentTerm++
	n.votedFor = n.id
	n.leaderID = -1

	lastEntry := n.lastLogEntry()
	term := n.currentTerm
	lastLogIndex := lastEntry.Index
	lastLogTerm := lastEntry.Term

	if n.persistence != nil {
		n.persistence.SaveTerm(n.currentTerm, n.votedFor)
	}

	raftlog.Printf("Node %d: starting election for term %d", n.id, term)

	votes := 1
	votesNeeded := (len(n.peers)+1)/2 + 1

	if votesNeeded <= 1 {
		n.startLeader()
		return
	}

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

			if n.state != Candidate || n.currentTerm != term {
				return
			}

			if resp.Term > n.currentTerm {
				n.becomeFollower(resp.Term)
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

func (n *RaftNode) startLeader() {
	if n.state != Candidate {
		return
	}

	n.state = Leader
	lastIdx := n.lastLogIndex()

	for _, peer := range n.peers {
		n.nextIndex[peer] = lastIdx + 1
		n.matchIndex[peer] = 0
	}

	raftlog.Printf("Node %d: became leader for term %d", n.id, n.currentTerm)

	if n.persistence != nil {
		n.persistence.SaveTerm(n.currentTerm, n.votedFor)
	}

	for _, peer := range n.peers {
		go n.replicateToPeer(peer)
	}

	n.heartbeatStopped = make(chan struct{})
	go n.heartbeatLoop(n.heartbeatStopped)
}

func (n *RaftNode) heartbeatLoop(stopped <-chan struct{}) {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			n.mu.Lock()
			if n.state == Leader {
				for _, peer := range n.peers {
					go n.replicateToPeer(peer)
				}
			}
			n.mu.Unlock()
		case <-stopped:
			return
		case <-n.quit:
			return
		}
	}
}

func (n *RaftNode) randomElectionTimeout() time.Duration {
	return electionTimeoutMin + time.Duration(rand.Int63n(int64(electionTimeoutMax-electionTimeoutMin)))
}

func raftMin(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}
