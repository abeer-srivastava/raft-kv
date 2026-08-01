package node

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// testCluster is an in-memory cluster for testing Raft nodes without network
type testCluster struct {
	nodes   []*RaftNode
	clients []*testClient
}

// testClient provides in-memory RPC communication between nodes
type testClient struct {
	mu      sync.RWMutex
	addrMap map[int]*RaftNode
}

func newTestClient() *testClient {
	return &testClient{
		addrMap: make(map[int]*RaftNode),
	}
}

func (c *testClient) register(id int, node *RaftNode) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.addrMap[id] = node
}

func (c *testClient) sendRequestVote(peerID int, req RequestVoteRequest) (*RequestVoteResponse, error) {
	c.mu.RLock()
	node, ok := c.addrMap[peerID]
	c.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("peer %d not found", peerID)
	}

	resp := node.SubmitRequestVote(req.CandidateID, req)
	return &resp, nil
}

func (c *testClient) sendAppendEntries(peerID int, req AppendEntriesRequest) (*AppendEntriesResponse, error) {
	c.mu.RLock()
	node, ok := c.addrMap[peerID]
	c.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("peer %d not found", peerID)
	}

	resp := node.SubmitAppendEntries(req.LeaderID, req)
	return &resp, nil
}

func newTestCluster(nodeCount int) *testCluster {
	client := newTestClient()
	nodes := make([]*RaftNode, nodeCount)
	commitChs := make([]chan LogEntry, nodeCount)

	for i := 0; i < nodeCount; i++ {
		commitChs[i] = make(chan LogEntry, 100)
	}

	for i := 0; i < nodeCount; i++ {
		peers := make([]int, 0)
		for j := 0; j < nodeCount; j++ {
			if i != j {
				peers = append(peers, j)
			}
		}

		nodes[i] = NewRaftNode(
			i,
			peers,
			commitChs[i],
			client.sendRequestVote,
			client.sendAppendEntries,
			nil, // No persistence in tests
		)

		client.register(i, nodes[i])
	}

	return &testCluster{
		nodes:   nodes,
		clients: []*testClient{client},
	}
}

func (c *testCluster) start() {
	for _, node := range c.nodes {
		node.Start()
	}
}

func (c *testCluster) stop() {
	for _, node := range c.nodes {
		node.Stop()
	}
	// Give goroutines time to clean up
	time.Sleep(50 * time.Millisecond)
}

func (c *testCluster) waitForLeader(timeout time.Duration) *RaftNode {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, node := range c.nodes {
			state, _ := node.GetState()
			if state == Leader {
				return node
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	return nil
}

func TestElection(t *testing.T) {
	cluster := newTestCluster(3)
	defer cluster.stop()

	cluster.start()

	leader := cluster.waitForLeader(3 * time.Second)
	if leader == nil {
		t.Fatal("No leader elected within timeout")
	}

	state, term := leader.GetState()
	t.Logf("Leader elected: node %d, state=%s, term=%d", leader.GetID(), state, term)

	// Verify only one leader
	leaderCount := 0
	for _, node := range cluster.nodes {
		s, _ := node.GetState()
		if s == Leader {
			leaderCount++
		}
	}
	if leaderCount != 1 {
		t.Errorf("Expected 1 leader, got %d", leaderCount)
	}
}

func TestLogReplication(t *testing.T) {
	cluster := newTestCluster(3)
	defer cluster.stop()

	cluster.start()

	leader := cluster.waitForLeader(3 * time.Second)
	if leader == nil {
		t.Fatal("No leader elected within timeout")
	}

	// Create a client request
	req := ClientRequest{
		Op:    "SET",
		Key:   "foo",
		Value: "bar",
	}

	// Submit to leader
	resp := leader.SubmitClientRequest(req)
	if !resp.Success {
		t.Fatalf("Failed to submit client request: %s", resp.Error)
	}

	t.Log("Client request committed successfully")
}

func TestFollowerRedirect(t *testing.T) {
	cluster := newTestCluster(3)
	defer cluster.stop()

	cluster.start()

	leader := cluster.waitForLeader(3 * time.Second)
	if leader == nil {
		t.Fatal("No leader elected within timeout")
	}

	// Wait a bit for heartbeats to propagate leader ID to followers
	time.Sleep(200 * time.Millisecond)

	// Verify all followers know the correct leader ID
	leaderID := leader.GetID()
	for _, node := range cluster.nodes {
		state, _ := node.GetState()
		if state == Follower {
			if node.GetLeaderID() != leaderID {
				t.Errorf("Follower %d thinks leader is %d, actual leader is %d",
					node.GetID(), node.GetLeaderID(), leaderID)
			}
		}
	}

	// Find a follower
	var follower *RaftNode
	for _, node := range cluster.nodes {
		state, _ := node.GetState()
		if state == Follower {
			follower = node
			break
		}
	}
	if follower == nil {
		t.Fatal("No follower found")
	}

	// Submit to follower - should fail with correct leader redirect
	req := ClientRequest{
		Op:    "SET",
		Key:   "foo",
		Value: "bar",
	}

	resp := follower.SubmitClientRequest(req)
	if resp.Success {
		t.Fatal("Expected follower to reject client request")
	}

	expectedErr := fmt.Sprintf("not leader, leader is node %d", leaderID)
	if resp.Error != expectedErr {
		t.Errorf("Expected error %q, got %q", expectedErr, resp.Error)
	}

	t.Logf("Follower correctly rejected request: %s", resp.Error)
}

func TestConcurrentRequests(t *testing.T) {
	cluster := newTestCluster(3)
	defer cluster.stop()

	cluster.start()

	leader := cluster.waitForLeader(3 * time.Second)
	if leader == nil {
		t.Fatal("No leader elected within timeout")
	}

	var wg sync.WaitGroup

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			req := ClientRequest{
				Op:    "SET",
				Key:   fmt.Sprintf("key%d", i),
				Value: fmt.Sprintf("value%d", i),
			}

			resp := leader.SubmitClientRequest(req)
			if !resp.Success {
				t.Errorf("Failed to submit request %d: %s", i, resp.Error)
				return
			}
		}(i)
	}

	wg.Wait()
}

func TestTermIncrement(t *testing.T) {
	cluster := newTestCluster(3)
	defer cluster.stop()

	cluster.start()

	// Wait for initial leader
	leader := cluster.waitForLeader(3 * time.Second)
	if leader == nil {
		t.Fatal("No leader elected within timeout")
	}

	_, initialTerm := leader.GetState()
	t.Logf("Initial term: %d (node %d)", initialTerm, leader.GetID())

	// Stop the leader
	leaderID := leader.GetID()
	leader.Stop()

	// Wait for new leader (not the old one)
	deadline := time.Now().Add(5 * time.Second)
	var newLeader *RaftNode
	for time.Now().Before(deadline) {
		for _, node := range cluster.nodes {
			if node.GetID() == leaderID {
				continue // skip old leader
			}
			state, _ := node.GetState()
			if state == Leader {
				newLeader = node
				break
			}
		}
		if newLeader != nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if newLeader == nil {
		t.Fatal("No new leader elected after stopping old leader")
	}

	_, newTerm := newLeader.GetState()
	t.Logf("New term: %d (node %d)", newTerm, newLeader.GetID())

	if newTerm <= initialTerm {
		t.Errorf("Expected term to increase, got initial=%d, new=%d", initialTerm, newTerm)
	}
}

func TestWALVotedForZero(t *testing.T) {
	dir := t.TempDir()

	// Create WAL for node 0
	wal, err := NewWAL(dir, 0)
	if err != nil {
		t.Fatal(err)
	}

	// Save term with votedFor=0 (this was broken by omitempty)
	if err := wal.SaveTerm(1, 0); err != nil {
		t.Fatal(err)
	}
	wal.Close()

	// Open a fresh WAL and recover
	wal2, err := NewWAL(dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer wal2.Close()

	commitCh := make(chan LogEntry, 100)
	node := NewRaftNode(0, nil, commitCh, nil, nil, wal2)

	if node.votedFor != 0 {
		t.Errorf("Expected votedFor=0 after recovery, got %d", node.votedFor)
	}
	if node.currentTerm != 1 {
		t.Errorf("Expected currentTerm=1 after recovery, got %d", node.currentTerm)
	}
}

func TestFollowerLeaderIDAfterElection(t *testing.T) {
	cluster := newTestCluster(3)
	defer cluster.stop()

	cluster.start()

	leader := cluster.waitForLeader(3 * time.Second)
	if leader == nil {
		t.Fatal("No leader elected within timeout")
	}

	// Wait for heartbeats to propagate
	time.Sleep(300 * time.Millisecond)

	// Verify followers have correct leader ID
	for _, node := range cluster.nodes {
		state, _ := node.GetState()
		if state != Leader {
			if node.GetLeaderID() != leader.GetID() {
				t.Errorf("Follower %d: expected leaderID=%d, got %d",
					node.GetID(), leader.GetID(), node.GetLeaderID())
			}
		}
	}
}

func TestSplitVoteRecovery(t *testing.T) {
	cluster := newTestCluster(3)
	defer cluster.stop()

	cluster.start()

	// Must elect a leader within timeout despite potential split votes
	leader := cluster.waitForLeader(5 * time.Second)
	if leader == nil {
		t.Fatal("No leader elected after 5s (likely stuck in split vote)")
	}

	// Verify only one leader
	leaderCount := 0
	for _, node := range cluster.nodes {
		s, _ := node.GetState()
		if s == Leader {
			leaderCount++
		}
	}
	if leaderCount != 1 {
		t.Errorf("Expected 1 leader, got %d", leaderCount)
	}

	// Verify we can submit a request to the leader
	req := ClientRequest{Op: "SET", Key: "recovery", Value: "ok"}
	resp := leader.SubmitClientRequest(req)
	if !resp.Success {
		t.Fatalf("Failed to submit request after recovery: %s", resp.Error)
	}
}

func TestElectionWith2Nodes(t *testing.T) {
	// 2-node cluster is more prone to split votes
	cluster := newTestCluster(2)
	defer cluster.stop()

	cluster.start()

	leader := cluster.waitForLeader(5 * time.Second)
	if leader == nil {
		t.Fatal("No leader elected in 2-node cluster within timeout")
	}

	// Verify we can commit
	req := ClientRequest{Op: "SET", Key: "two", Value: "nodes"}
	resp := leader.SubmitClientRequest(req)
	if !resp.Success {
		t.Fatalf("Failed to commit in 2-node cluster: %s", resp.Error)
	}
}

func TestLeaderElectionAfterWALRecovery(t *testing.T) {
	dir := t.TempDir()

	// Create WAL for node 0 with node 0 voted for self in term 1
	wal0, err := NewWAL(dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	wal0.SaveTerm(1, 0)

	// Create WAL for node 1 with node 1 voted for self in term 1
	wal1, err := NewWAL(dir, 1)
	if err != nil {
		t.Fatal(err)
	}
	wal1.SaveTerm(1, 1)

	// Create WAL for node 2 with node 2 voted for self in term 1
	wal2, err := NewWAL(dir, 2)
	if err != nil {
		t.Fatal(err)
	}
	wal2.SaveTerm(1, 2)

	wal0.Close()
	wal1.Close()
	wal2.Close()

	// Recover and start cluster
	client := newTestClient()
	nodes := make([]*RaftNode, 3)
	commitChs := make([]chan LogEntry, 3)

	for i := 0; i < 3; i++ {
		commitChs[i] = make(chan LogEntry, 100)
	}

	for i := 0; i < 3; i++ {
		peers := []int{}
		for j := 0; j < 3; j++ {
			if i != j {
				peers = append(peers, j)
			}
		}

		var wal *WAL
		wal, _ = NewWAL(dir, i)

		nodes[i] = NewRaftNode(
			i, peers, commitChs[i],
			client.sendRequestVote,
			client.sendAppendEntries,
			wal,
		)
		client.register(i, nodes[i])
	}

	for _, n := range nodes {
		n.Start()
	}
	defer func() {
		for _, n := range nodes {
			n.Stop()
		}
		time.Sleep(50 * time.Millisecond)
	}()

	// Must elect a leader despite WAL state where all nodes voted for themselves
	leader := clusterWaitForLeader(nodes, 5*time.Second)
	if leader == nil {
		t.Fatal("No leader elected after WAL recovery (all voted for self in term 1)")
	}

	t.Logf("Leader elected: node %d", leader.GetID())
}

func TestSustainedLeadership(t *testing.T) {
	cluster := newTestCluster(3)
	defer cluster.stop()

	cluster.start()

	leader := cluster.waitForLeader(3 * time.Second)
	if leader == nil {
		t.Fatal("No leader elected within timeout")
	}

	leaderID := leader.GetID()

	// Submit multiple consecutive requests to verify the leader doesn't change
	for i := 0; i < 20; i++ {
		req := ClientRequest{
			Op:    "SET",
			Key:   fmt.Sprintf("key%d", i),
			Value: fmt.Sprintf("value%d", i),
		}
		resp := leader.SubmitClientRequest(req)
		if !resp.Success {
			t.Fatalf("Request %d failed: %s (leader changed?)", i, resp.Error)
		}

		// Verify leader hasn't changed
		state, _ := leader.GetState()
		if state != Leader {
			t.Fatalf("Leader changed after request %d (was %d, now state=%s)",
				i, leaderID, state)
		}
	}

	// Wait for heartbeats to propagate
	time.Sleep(200 * time.Millisecond)

	// Verify all followers still point to the same leader
	for _, node := range cluster.nodes {
		state, _ := node.GetState()
		if state == Follower {
			if node.GetLeaderID() != leaderID {
				t.Errorf("Follower %d lost track of leader: expected %d, got %d",
					node.GetID(), leaderID, node.GetLeaderID())
			}
		}
	}
}

// TestTermStability verifies that terms don't inflate rapidly (the original bug)
func TestTermStability(t *testing.T) {
	cluster := newTestCluster(3)
	defer cluster.stop()

	cluster.start()

	leader := cluster.waitForLeader(3 * time.Second)
	if leader == nil {
		t.Fatal("No leader elected within timeout")
	}

	_, term := leader.GetState()
	t.Logf("Leader elected at term %d", term)

	// In a healthy 3-node cluster, the term should be very low (1-3 typically)
	// The original bug would produce terms in the dozens or hundreds
	if term > 10 {
		t.Errorf("Term %d is suspiciously high — possible election storm", term)
	}

	// Wait 2 seconds and check term hasn't drifted
	time.Sleep(2 * time.Second)
	_, termAfter := leader.GetState()
	if termAfter != term {
		t.Errorf("Term changed from %d to %d during stable leadership", term, termAfter)
	}
}

func TestWALTruncationRecovery(t *testing.T) {
	dir := t.TempDir()

	wal, err := NewWAL(dir, 0)
	if err != nil {
		t.Fatal(err)
	}

	// Append initial entries
	wal.AppendEntry(LogEntry{Term: 1, Index: 1, Data: []byte("entry1")})
	wal.AppendEntry(LogEntry{Term: 1, Index: 2, Data: []byte("entry2")})
	wal.AppendEntry(LogEntry{Term: 1, Index: 3, Data: []byte("uncommitted3")})

	// Truncate at index 3 (discard uncommitted3)
	wal.TruncateLog(3)
	wal.AppendEntry(LogEntry{Term: 2, Index: 3, Data: []byte("committed3")})
	wal.Close()

	// Recover WAL into new node
	wal2, err := NewWAL(dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer wal2.Close()

	commitCh := make(chan LogEntry, 100)
	node := NewRaftNode(0, nil, commitCh, nil, nil, wal2)

	// Verify log length and entry content
	if len(node.raftLog) != 4 { // Index 0..3
		t.Fatalf("Expected log length 4 after recovery, got %d", len(node.raftLog))
	}
	if string(node.raftLog[3].Data) != "committed3" {
		t.Errorf("Expected 'committed3' after truncation recovery, got %q", string(node.raftLog[3].Data))
	}
	if node.commitIndex != 0 {
		t.Errorf("Expected commitIndex=0 on restart safety check, got %d", node.commitIndex)
	}
}

func TestCommitNotificationNoPolling(t *testing.T) {
	cluster := newTestCluster(3)
	defer cluster.stop()
	cluster.start()

	leader := cluster.waitForLeader(3 * time.Second)
	if leader == nil {
		t.Fatal("No leader elected")
	}

	start := time.Now()
	resp := leader.SubmitClientRequest(ClientRequest{Op: "SET", Key: "fast", Value: "commit"})
	elapsed := time.Since(start)

	if !resp.Success {
		t.Fatalf("Client request failed: %s", resp.Error)
	}

	t.Logf("Commit completed in %v", elapsed)
	if elapsed > 200*time.Millisecond {
		t.Errorf("Commit took unexpectedly long (%v), check polling latency", elapsed)
	}
}

func TestInstallSnapshotHandling(t *testing.T) {
	commitCh := make(chan LogEntry, 100)
	node := NewRaftNode(0, []int{1}, commitCh, nil, nil, nil)
	node.Start()
	defer node.Stop()

	// Send InstallSnapshot
	snapReq := InstallSnapshotRequest{
		Term:              1,
		LeaderID:          1,
		LastIncludedIndex: 50,
		LastIncludedTerm:  1,
		Data:              []byte("snapshot_data"),
		Done:              true,
	}

	resp := node.SubmitInstallSnapshot(1, snapReq)
	if resp.Term != 1 {
		t.Errorf("Expected resp.Term=1, got %d", resp.Term)
	}
	if node.GetCommitIndex() != 50 {
		t.Errorf("Expected commitIndex=50 after snapshot install, got %d", node.GetCommitIndex())
	}
}

func clusterWaitForLeader(nodes []*RaftNode, timeout time.Duration) *RaftNode {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, node := range nodes {
			state, _ := node.GetState()
			if state == Leader {
				return node
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	return nil
}

// TestNoopOnLeaderElection verifies that a newly elected leader immediately commits
// entries from prior terms by appending a no-op. Without this, old entries would remain
// uncommitted until the next client write.
func TestNoopOnLeaderElection(t *testing.T) {
	client := newTestClient()
	commitChs := make([]chan LogEntry, 3)
	nodes := make([]*RaftNode, 3)

	for i := range commitChs {
		commitChs[i] = make(chan LogEntry, 100)
	}

	for i := 0; i < 3; i++ {
		peers := []int{}
		for j := 0; j < 3; j++ {
			if i != j {
				peers = append(peers, j)
			}
		}
		nodes[i] = NewRaftNode(i, peers, commitChs[i],
			client.sendRequestVote, client.sendAppendEntries, nil)
		client.register(i, nodes[i])
	}

	// Start all nodes
	for _, n := range nodes {
		n.Start()
	}
	defer func() {
		for _, n := range nodes {
			n.Stop()
		}
		time.Sleep(50 * time.Millisecond)
	}()

	// Wait for leader election
	leader := clusterWaitForLeader(nodes, 3*time.Second)
	if leader == nil {
		t.Fatal("No leader elected")
	}
	leaderID := leader.GetID()

	// Submit an entry from the leader in term 1
	resp := leader.SubmitClientRequest(ClientRequest{Op: "SET", Key: "priorterm", Value: "data"})
	if !resp.Success {
		t.Fatalf("Failed to submit initial entry: %s", resp.Error)
	}
	t.Logf("Initial entry committed by leader %d", leaderID)

	// Stop the leader to force a new election
	leader.Stop()
	time.Sleep(100 * time.Millisecond)

	// Wait for a new leader (different from the stopped one)
	deadline := time.Now().Add(5 * time.Second)
	var newLeader *RaftNode
	for time.Now().Before(deadline) {
		for _, n := range nodes {
			if n.GetID() == leaderID {
				continue
			}
			st, _ := n.GetState()
			if st == Leader {
				newLeader = n
				break
			}
		}
		if newLeader != nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if newLeader == nil {
		t.Fatal("No new leader elected after stopping old leader")
	}

	_, newTerm := newLeader.GetState()
	t.Logf("New leader %d elected at term %d", newLeader.GetID(), newTerm)

	// The new leader should have committed the no-op entry, which as a side effect
	// commits all prior-term entries. Wait briefly for the no-op to be replicated.
	time.Sleep(300 * time.Millisecond)

	// Verify the no-op is in the log (last log index should be > the initial entry index)
	newLeader.mu.Lock()
	lastLogIdx := newLeader.lastLogIndex()
	commitIdx := newLeader.commitIndex
	newLeader.mu.Unlock()

	// The commit index should include the no-op
	if commitIdx < 2 {
		t.Errorf("Expected commitIndex >= 2 (initial entry + no-op), got %d", commitIdx)
	}
	t.Logf("New leader lastLogIndex=%d commitIndex=%d — no-op committed prior-term entries", lastLogIdx, commitIdx)
}

// TestAutoSnapshotCompaction verifies that the log is automatically compacted
// when the number of applied entries exceeds the configured threshold.
func TestAutoSnapshotCompaction(t *testing.T) {
	cluster := newTestCluster(3)
	defer cluster.stop()
	cluster.start()

	leader := cluster.waitForLeader(3 * time.Second)
	if leader == nil {
		t.Fatal("No leader elected")
	}

	// Configure auto-compaction with a low threshold for testing
	snapshotData := []byte(`{"snap":"test"}`)
	leader.SetSnapshotConfig(5, func() ([]byte, error) {
		return snapshotData, nil
	})

	// Submit entries to exceed the threshold (5)
	for i := 0; i < 8; i++ {
		resp := leader.SubmitClientRequest(ClientRequest{
			Op:    "SET",
			Key:   fmt.Sprintf("autosnap%d", i),
			Value: fmt.Sprintf("val%d", i),
		})
		if !resp.Success {
			t.Fatalf("Request %d failed: %s", i, resp.Error)
		}
	}

	// Wait for the auto-snapshot goroutine to trigger
	time.Sleep(500 * time.Millisecond)

	leader.mu.Lock()
	snapIdx := leader.snapshotIndex
	snapTerm := leader.snapshotTerm
	logLen := len(leader.raftLog)
	firstLogIdx := leader.raftLog[0].Index
	leader.mu.Unlock()

	if snapIdx == 0 {
		t.Fatal("Expected snapshot to have been triggered, but snapshotIndex is 0")
	}

	t.Logf("Auto-snapshot: snapshotIndex=%d snapshotTerm=%d logLen=%d firstLogIdx=%d",
		snapIdx, snapTerm, logLen, firstLogIdx)

	// Log should have been compacted
	if firstLogIdx == 0 {
		t.Errorf("Expected log to be compacted (firstLogIdx > 0), but firstLogIdx=0")
	}
}

// TestSnapshotInstallApply verifies that when a follower receives InstallSnapshot,
// the snapshot data is pushed to the snapshot notification channel so the state machine
// can replace its state.
func TestSnapshotInstallApply(t *testing.T) {
	commitCh := make(chan LogEntry, 100)
	snapshotNotifyCh := make(chan SnapshotData, 1)

	n := NewRaftNode(0, []int{1}, commitCh, nil, nil, nil)
	n.SetSnapshotNotifyCh(snapshotNotifyCh)
	n.Start()
	defer n.Stop()

	// Prepare fake snapshot data representing a KV state
	snapData := []byte(`{"hello":"world","foo":"bar"}`)

	// Send InstallSnapshot RPC
	snapReq := InstallSnapshotRequest{
		Term:              1,
		LeaderID:          1,
		LastIncludedIndex: 50,
		LastIncludedTerm:  1,
		Data:              snapData,
		Done:              true,
	}

	resp := n.SubmitInstallSnapshot(1, snapReq)
	if resp.Term != 1 {
		t.Errorf("Expected resp.Term=1, got %d", resp.Term)
	}

	// Verify the snapshot data was pushed to the notify channel
	select {
	case received := <-snapshotNotifyCh:
		if received.Index != 50 {
			t.Errorf("Expected snapshot.Index=50, got %d", received.Index)
		}
		if received.Term != 1 {
			t.Errorf("Expected snapshot.Term=1, got %d", received.Term)
		}
		if string(received.Data) != string(snapData) {
			t.Errorf("Expected snapshot data=%q, got %q", snapData, received.Data)
		}
		t.Log("Snapshot notification received with correct data")
	case <-time.After(1 * time.Second):
		t.Fatal("Timed out waiting for snapshot notification on channel")
	}
}

// TestSnapshotThenReplication verifies that after a snapshot is taken and the log
// is compacted, subsequent client writes still work correctly.
func TestSnapshotThenReplication(t *testing.T) {
	cluster := newTestCluster(3)
	defer cluster.stop()
	cluster.start()

	leader := cluster.waitForLeader(3 * time.Second)
	if leader == nil {
		t.Fatal("No leader elected")
	}

	// Submit some entries
	for i := 0; i < 5; i++ {
		resp := leader.SubmitClientRequest(ClientRequest{
			Op:    "SET",
			Key:   fmt.Sprintf("pre_snap_%d", i),
			Value: fmt.Sprintf("val%d", i),
		})
		if !resp.Success {
			t.Fatalf("Pre-snapshot request %d failed: %s", i, resp.Error)
		}
	}

	// Manually take a snapshot to compact the log
	leader.mu.Lock()
	lastApplied := leader.lastApplied
	term := leader.getEntryAt(lastApplied).Term
	leader.mu.Unlock()

	leader.TakeSnapshot(lastApplied, term, []byte(`{"pre_snap_0":"val0"}`))

	// Verify log was compacted
	leader.mu.Lock()
	firstLogIdx := leader.raftLog[0].Index
	leader.mu.Unlock()
	if firstLogIdx == 0 {
		t.Error("Expected log to be compacted after manual snapshot")
	}
	t.Logf("After snapshot: firstLogIdx=%d", firstLogIdx)

	// Now submit more entries AFTER the snapshot — these should still work
	for i := 0; i < 5; i++ {
		resp := leader.SubmitClientRequest(ClientRequest{
			Op:    "SET",
			Key:   fmt.Sprintf("post_snap_%d", i),
			Value: fmt.Sprintf("val%d", i),
		})
		if !resp.Success {
			t.Fatalf("Post-snapshot request %d failed: %s", i, resp.Error)
		}
	}

	t.Log("Post-snapshot replication succeeded — log compaction did not break consensus")
}

// TestTakeSnapshotIdempotent verifies that taking a snapshot with an older index is ignored.
func TestTakeSnapshotIdempotent(t *testing.T) {
	commitCh := make(chan LogEntry, 100)
	n := NewRaftNode(0, nil, commitCh, nil, nil, nil)

	// Manually set up some log entries
	n.mu.Lock()
	n.raftLog = append(n.raftLog, LogEntry{Term: 1, Index: 1, Data: []byte("e1")})
	n.raftLog = append(n.raftLog, LogEntry{Term: 1, Index: 2, Data: []byte("e2")})
	n.raftLog = append(n.raftLog, LogEntry{Term: 1, Index: 3, Data: []byte("e3")})
	n.mu.Unlock()

	// Take first snapshot at index 2
	n.TakeSnapshot(2, 1, []byte(`{"snap":"v1"}`))

	n.mu.Lock()
	snap1Idx := n.snapshotIndex
	logLen1 := len(n.raftLog)
	n.mu.Unlock()

	if snap1Idx != 2 {
		t.Fatalf("Expected snapshotIndex=2, got %d", snap1Idx)
	}

	// Try to take an older snapshot at index 1 — should be ignored
	n.TakeSnapshot(1, 1, []byte(`{"snap":"old"}`))

	n.mu.Lock()
	snap2Idx := n.snapshotIndex
	logLen2 := len(n.raftLog)
	n.mu.Unlock()

	if snap2Idx != 2 {
		t.Errorf("Expected snapshotIndex to remain 2 after old snapshot, got %d", snap2Idx)
	}
	if logLen2 != logLen1 {
		t.Errorf("Expected log length to remain %d, got %d", logLen1, logLen2)
	}

	t.Logf("Idempotency: snapshotIndex=%d logLen=%d (unchanged)", snap2Idx, logLen2)
}
