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

// SendInstallSnapshotFunc sends an InstallSnapshot RPC to a peer
type SendInstallSnapshotFunc func(peer int, req InstallSnapshotRequest) (*InstallSnapshotResponse, error)

// RaftNode is the core Raft consensus node
type RaftNode struct {
	mu sync.Mutex

	id    int
	peers []int

	currentTerm uint64
	votedFor    int
	raftLog     []LogEntry

	leaderID    int
	commitIndex uint64
	lastApplied uint64
	state       RaftState

	nextIndex   map[int]uint64
	matchIndex  map[int]uint64
	replicating map[int]bool // Tracks active replication loop per peer to prevent races

	commitCh          chan<- LogEntry
	snapshotNotifyCh  chan<- SnapshotData        // Notifies state machine to load snapshot
	commitWaiters     map[uint64][]chan struct{} // Instant notification channels for client waiters
	requestVoteCh     chan rpcMessage[RequestVoteRequest, RequestVoteResponse]
	appendEntriesCh   chan rpcMessage[AppendEntriesRequest, AppendEntriesResponse]
	installSnapshotCh chan rpcMessage[InstallSnapshotRequest, InstallSnapshotResponse]
	clientRequestCh   chan clientMsg
	quit              chan struct{}
	quitOnce          sync.Once

	sendRequestVote   SendRequestVoteFunc
	sendAppendEntries SendAppendEntriesFunc
	sendSnapshot      SendInstallSnapshotFunc

	persistence *WAL

	heartbeatStopped chan struct{}

	onStateChange func(old, new RaftState)

	snapshotIndex uint64
	snapshotTerm  uint64
	snapshotData  []byte

	// Automatic snapshot compaction
	appliedSinceSnapshot uint64
	snapshotThreshold    uint64           // After this many applied entries, trigger compaction (0 = disabled)
	snapshotProvider     SnapshotProvider // Callback to get serialized state machine state
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
		commitWaiters:     make(map[uint64][]chan struct{}),
		currentTerm:       0,
		votedFor:          -1,
		leaderID:          -1,
		raftLog:           raftLog,
		commitIndex:       0,
		lastApplied:       0,
		state:             Follower,
		nextIndex:         make(map[int]uint64),
		matchIndex:        make(map[int]uint64),
		replicating:       make(map[int]bool),
		requestVoteCh:     make(chan rpcMessage[RequestVoteRequest, RequestVoteResponse], 100),
		appendEntriesCh:   make(chan rpcMessage[AppendEntriesRequest, AppendEntriesResponse], 100),
		installSnapshotCh: make(chan rpcMessage[InstallSnapshotRequest, InstallSnapshotResponse], 100),
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

// SetSnapshotNotifyCh sets the channel for snapshot notifications to the state machine.
// When an InstallSnapshot RPC is received, the snapshot data is pushed to this channel
// so the state machine can replace its state entirely.
func (n *RaftNode) SetSnapshotNotifyCh(ch chan<- SnapshotData) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.snapshotNotifyCh = ch
}

// SetSnapshotConfig configures automatic log compaction.
// threshold: number of applied entries before triggering a snapshot (0 = disabled).
// provider: callback to get the current serialized state machine state.
func (n *RaftNode) SetSnapshotConfig(threshold uint64, provider SnapshotProvider) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.snapshotThreshold = threshold
	n.snapshotProvider = provider
}

// TakeSnapshot takes a snapshot of the state machine at the given index/term and compacts the log.
// This is called after the snapshot provider returns the serialized state.
func (n *RaftNode) TakeSnapshot(index, term uint64, data []byte) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if index <= n.snapshotIndex {
		return // Already have a newer snapshot
	}

	n.snapshotIndex = index
	n.snapshotTerm = term
	n.snapshotData = data

	// Compact the log: discard all entries up to and including `index`
	firstIdx := n.raftLog[0].Index
	if index >= firstIdx && index <= n.lastLogIndex() {
		offset := index - firstIdx
		newLog := make([]LogEntry, uint64(len(n.raftLog))-offset)
		copy(newLog, n.raftLog[offset:])
		n.raftLog = newLog
	} else if index > n.lastLogIndex() {
		// Snapshot is ahead of our log — replace entirely
		n.raftLog = []LogEntry{{Term: term, Index: index}}
	}

	n.appliedSinceSnapshot = 0

	raftlog.Printf("Node %d: snapshot taken at index=%d term=%d, log compacted to %d entries",
		n.id, index, term, len(n.raftLog))
}

// SetSendInstallSnapshot sets the optional InstallSnapshot RPC sender callback
func (n *RaftNode) SetSendInstallSnapshot(fn SendInstallSnapshotFunc) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.sendSnapshot = fn
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

// GetState returns the current state of the node
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
	if len(n.raftLog) == 0 {
		return LogEntry{Term: 0, Index: 0}
	}
	firstIdx := n.raftLog[0].Index
	if index < firstIdx || index >= firstIdx+uint64(len(n.raftLog)) {
		return LogEntry{Term: 0, Index: 0}
	}
	return n.raftLog[index-firstIdx]
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

// SubmitInstallSnapshot feeds an InstallSnapshot RPC into the node
func (n *RaftNode) SubmitInstallSnapshot(peerID int, req InstallSnapshotRequest) InstallSnapshotResponse {
	respCh := make(chan InstallSnapshotResponse, 1)
	select {
	case n.installSnapshotCh <- rpcMessage[InstallSnapshotRequest, InstallSnapshotResponse]{
		req:    req,
		respCh: respCh,
		peerID: peerID,
	}:
	case <-n.quit:
		return InstallSnapshotResponse{Term: 0}
	}
	select {
	case resp := <-respCh:
		return resp
	case <-n.quit:
		return InstallSnapshotResponse{Term: 0}
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

		case msg := <-n.installSnapshotCh:
			resetTimer := n.handleInstallSnapshotMsg(msg)
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

	firstIdx := n.raftLog[0].Index
	lastIdx := n.lastLogIndex()

	if req.PrevLogIndex > 0 {
		if req.PrevLogIndex > lastIdx {
			resp.ConflictIndex = lastIdx + 1
			resp.ConflictTerm = 0
			msg.respCh <- resp
			return true
		}

		if req.PrevLogIndex >= firstIdx {
			prevTerm := n.getEntryAt(req.PrevLogIndex).Term
			if prevTerm != req.PrevLogTerm {
				resp.ConflictTerm = prevTerm
				for i := req.PrevLogIndex; i >= firstIdx; i-- {
					if n.getEntryAt(i).Term != prevTerm {
						resp.ConflictIndex = i + 1
						break
					}
					if i == firstIdx {
						resp.ConflictIndex = firstIdx
					}
				}
				msg.respCh <- resp
				return true
			}
		}
	}

	for _, entry := range req.Entries {
		if entry.Index >= firstIdx && entry.Index <= n.lastLogIndex() {
			if n.getEntryAt(entry.Index).Term != entry.Term {
				// Truncate conflicting entries in memory and persist truncation event
				cutLocalIdx := entry.Index - firstIdx
				n.raftLog = n.raftLog[:cutLocalIdx]
				if n.persistence != nil {
					n.persistence.TruncateLog(entry.Index)
				}
				n.raftLog = append(n.raftLog, entry)
			}
		} else if entry.Index > n.lastLogIndex() {
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

func (n *RaftNode) handleInstallSnapshotMsg(msg rpcMessage[InstallSnapshotRequest, InstallSnapshotResponse]) bool {
	n.mu.Lock()
	defer n.mu.Unlock()

	req := msg.req
	resp := InstallSnapshotResponse{Term: n.currentTerm}

	if req.Term < n.currentTerm {
		msg.respCh <- resp
		return false
	}

	if req.Term > n.currentTerm || n.state != Follower {
		n.becomeFollower(req.Term)
	}
	n.leaderID = req.LeaderID

	if req.LastIncludedIndex <= n.commitIndex {
		msg.respCh <- resp
		return true
	}

	// Apply snapshot to state machine log prefix
	n.snapshotIndex = req.LastIncludedIndex
	n.snapshotTerm = req.LastIncludedTerm
	n.snapshotData = req.Data

	n.raftLog = []LogEntry{{Term: req.LastIncludedTerm, Index: req.LastIncludedIndex}}
	n.commitIndex = req.LastIncludedIndex
	n.lastApplied = req.LastIncludedIndex
	n.appliedSinceSnapshot = 0

	// Notify state machine to load the snapshot, replacing its current state.
	// This is a non-blocking send — if the channel is full, the snapshot is dropped
	// (the leader will retry via heartbeat).
	if n.snapshotNotifyCh != nil {
		select {
		case n.snapshotNotifyCh <- SnapshotData{
			Index: req.LastIncludedIndex,
			Term:  req.LastIncludedTerm,
			Data:  req.Data,
		}:
		default:
			raftlog.Printf("Node %d: snapshot notify channel full, skipping notification", n.id)
		}
	}

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

	doneCh := make(chan struct{})
	n.commitWaiters[entry.Index] = append(n.commitWaiters[entry.Index], doneCh)

	for _, peer := range n.peers {
		n.triggerReplicateToPeerLocked(peer)
	}

	go n.waitForCommitChannel(entry.Index, doneCh, msg.respCh)
}

func (n *RaftNode) triggerReplicateToPeerLocked(peer int) {
	if n.replicating[peer] {
		return
	}
	n.replicating[peer] = true
	go n.peerReplicationLoop(peer)
}

func (n *RaftNode) peerReplicationLoop(peer int) {
	defer func() {
		n.mu.Lock()
		n.replicating[peer] = false
		n.mu.Unlock()
	}()

	for {
		n.mu.Lock()
		if n.state != Leader {
			n.mu.Unlock()
			return
		}

		nextIdx := n.nextIndex[peer]
		firstIdx := n.raftLog[0].Index

		if nextIdx <= firstIdx && n.sendSnapshot != nil && n.snapshotIndex > 0 {
			// Peer is behind compaction window -> send InstallSnapshot RPC
			req := InstallSnapshotRequest{
				Term:              n.currentTerm,
				LeaderID:          n.id,
				LastIncludedIndex: n.snapshotIndex,
				LastIncludedTerm:  n.snapshotTerm,
				Data:              n.snapshotData,
				Done:              true,
			}
			currentTerm := n.currentTerm
			sendSnap := n.sendSnapshot
			n.mu.Unlock()

			resp, err := sendSnap(peer, req)
			if err != nil {
				return
			}

			n.mu.Lock()
			if n.state != Leader || n.currentTerm != currentTerm {
				n.mu.Unlock()
				return
			}
			if resp.Term > n.currentTerm {
				n.becomeFollower(resp.Term)
				n.mu.Unlock()
				return
			}
			n.nextIndex[peer] = n.snapshotIndex + 1
			n.matchIndex[peer] = n.snapshotIndex
			n.mu.Unlock()
			continue
		}

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
		sendAE := n.sendAppendEntries
		n.mu.Unlock()

		resp, err := sendAE(peer, req)
		if err != nil {
			return
		}

		n.mu.Lock()
		if n.state != Leader || n.currentTerm != currentTerm {
			n.mu.Unlock()
			return
		}

		if resp.Term > n.currentTerm {
			n.becomeFollower(resp.Term)
			n.mu.Unlock()
			return
		}

		if resp.Success {
			newNext := req.PrevLogIndex + uint64(len(req.Entries)) + 1
			newMatch := newNext - 1
			n.nextIndex[peer] = newNext
			n.matchIndex[peer] = newMatch
			n.advanceCommitIndex()

			// Check if peer is now fully caught up
			if n.nextIndex[peer] > n.lastLogIndex() {
				n.mu.Unlock()
				return
			}
			n.mu.Unlock()
		} else {
			if resp.ConflictTerm > 0 {
				n.nextIndex[peer] = resp.ConflictIndex
			} else if n.nextIndex[peer] > 1 {
				n.nextIndex[peer]--
			}
			n.mu.Unlock()
		}
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
			n.notifyCommitWaitersLocked()
			break
		}
	}
}

func (n *RaftNode) notifyCommitWaitersLocked() {
	for idx, channels := range n.commitWaiters {
		if idx <= n.commitIndex {
			for _, ch := range channels {
				close(ch)
			}
			delete(n.commitWaiters, idx)
		}
	}
}

func (n *RaftNode) applyCommitted() {
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		entry := n.getEntryAt(n.lastApplied)
		if entry.Index > 0 && len(entry.Data) > 0 {
			// Only send non-empty entries to the state machine.
			// No-op entries (empty Data) are used for leader commit confirmation
			// and should not be applied to the state machine.
			n.commitCh <- entry
		}
		n.appliedSinceSnapshot++
	}

	// Trigger automatic snapshot compaction if threshold is reached
	if n.snapshotThreshold > 0 && n.appliedSinceSnapshot >= n.snapshotThreshold && n.snapshotProvider != nil {
		go n.triggerAutoSnapshot()
	}
}

// waitForCommitChannel waits instantly on a notification channel without 5ms polling
func (n *RaftNode) waitForCommitChannel(index uint64, doneCh <-chan struct{}, respCh chan ClientResponse) {
	timeout := time.After(5 * time.Second)
	select {
	case <-doneCh:
		n.mu.Lock()
		isLeader := n.state == Leader
		n.mu.Unlock()

		if isLeader {
			respCh <- ClientResponse{Success: true}
		} else {
			respCh <- ClientResponse{
				Success: false,
				Error:   "lost leadership while waiting for commit",
			}
		}
	case <-timeout:
		respCh <- ClientResponse{
			Success: false,
			Error:   "timeout waiting for commit",
		}
	case <-n.quit:
		respCh <- ClientResponse{
			Success: false,
			Error:   "node shutting down",
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

	// Raft paper §8: Append a no-op entry in the current term immediately upon election.
	// This ensures entries from previous terms get committed as a side effect,
	// without waiting for the next client write.
	noopEntry := LogEntry{
		Term:  n.currentTerm,
		Index: n.lastLogIndex() + 1,
		Data:  nil, // No-op: empty data
	}
	n.raftLog = append(n.raftLog, noopEntry)
	if n.persistence != nil {
		n.persistence.AppendEntry(noopEntry)
	}
	raftlog.Printf("Node %d: appended no-op entry at index %d for term %d",
		n.id, noopEntry.Index, noopEntry.Term)

	for _, peer := range n.peers {
		n.triggerReplicateToPeerLocked(peer)
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
					n.triggerReplicateToPeerLocked(peer)
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

// triggerAutoSnapshot is called asynchronously when the applied entry count exceeds the threshold.
// It calls the snapshot provider to get the current state machine state,
// then calls TakeSnapshot to compact the log.
func (n *RaftNode) triggerAutoSnapshot() {
	n.mu.Lock()
	lastApplied := n.lastApplied
	entryAtApplied := n.getEntryAt(lastApplied)
	term := entryAtApplied.Term
	provider := n.snapshotProvider
	n.mu.Unlock()

	if provider == nil {
		return
	}

	data, err := provider()
	if err != nil {
		raftlog.Printf("Node %d: auto-snapshot provider failed: %v", n.id, err)
		return
	}

	n.TakeSnapshot(lastApplied, term, data)
}

func raftMin(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}
