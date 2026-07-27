package node

import (	"sync"
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
		return nil, nil
	}

	resp := node.SubmitRequestVote(0, req)
	return &resp, nil
}

func (c *testClient) sendAppendEntries(peerID int, req AppendEntriesRequest) (*AppendEntriesResponse, error) {
	c.mu.RLock()
	node, ok := c.addrMap[peerID]
	c.mu.RUnlock()

	if !ok {
		return nil, nil
	}

	resp := node.SubmitAppendEntries(0, req)
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

	leader := cluster.waitForLeader(2 * time.Second)
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

	leader := cluster.waitForLeader(2 * time.Second)
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

	leader := cluster.waitForLeader(2 * time.Second)
	if leader == nil {
		t.Fatal("No leader elected within timeout")
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

	// Submit to follower - should fail with not leader error
	req := ClientRequest{
		Op:    "SET",
		Key:   "foo",
		Value: "bar",
	}

	resp := follower.SubmitClientRequest(req)
	if resp.Success {
		t.Fatal("Expected follower to reject client request")
	}

	t.Logf("Follower correctly rejected request: %s", resp.Error)
}

func TestConcurrentRequests(t *testing.T) {
	cluster := newTestCluster(3)
	defer cluster.stop()

	cluster.start()

	leader := cluster.waitForLeader(2 * time.Second)
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
				Key:   "key" + string(rune('0'+i)),
				Value: "value" + string(rune('0'+i)),
			}

			resp := leader.SubmitClientRequest(req)
			if !resp.Success {
				t.Errorf("Failed to submit request %d", i)
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
	leader := cluster.waitForLeader(2 * time.Second)
	if leader == nil {
		t.Fatal("No leader elected within timeout")
	}

	_, initialTerm := leader.GetState()
	t.Logf("Initial term: %d (node %d)", initialTerm, leader.GetID())

	// Stop the leader
	leaderID := leader.GetID()
	leader.Stop()

	// Wait for new leader (not the old one)
	deadline := time.Now().Add(3 * time.Second)
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
