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
