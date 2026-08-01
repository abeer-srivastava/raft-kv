package network

import (
	"testing"
	"time"

	"github.com/abeer/raft-kv/kvstore"
	"github.com/abeer/raft-kv/node"
)

func TestGRPCIntegration(t *testing.T) {
	commitCh0 := make(chan node.LogEntry, 100)
	commitCh1 := make(chan node.LogEntry, 100)
	commitCh2 := make(chan node.LogEntry, 100)

	client := NewClient()
	defer client.Close()

	addrMap := map[int]string{
		0: "127.0.0.1:50051",
		1: "127.0.0.1:50052",
		2: "127.0.0.1:50053",
	}

	sendRV := func(peer int, req node.RequestVoteRequest) (*node.RequestVoteResponse, error) {
		return client.SendRequestVoteWithID(addrMap, peer, req)
	}
	sendAE := func(peer int, req node.AppendEntriesRequest) (*node.AppendEntriesResponse, error) {
		return client.SendAppendEntriesWithID(addrMap, peer, req)
	}

	n0 := node.NewRaftNode(0, []int{1, 2}, commitCh0, sendRV, sendAE, nil)
	n1 := node.NewRaftNode(1, []int{0, 2}, commitCh1, sendRV, sendAE, nil)
	n2 := node.NewRaftNode(2, []int{0, 1}, commitCh2, sendRV, sendAE, nil)

	s0 := NewServer(addrMap[0], n0)
	s1 := NewServer(addrMap[1], n1)
	s2 := NewServer(addrMap[2], n2)

	store0 := kvstore.NewStore()
	store1 := kvstore.NewStore()
	store2 := kvstore.NewStore()

	h0 := kvstore.NewClientHandler(store0, n0, addrMap)
	h1 := kvstore.NewClientHandler(store1, n1, addrMap)
	h2 := kvstore.NewClientHandler(store2, n2, addrMap)

	s0.SetClientRequestFunc(h0.HandleRequest)
	s1.SetClientRequestFunc(h1.HandleRequest)
	s2.SetClientRequestFunc(h2.HandleRequest)

	go func() {
		for e := range commitCh0 {
			store0.Apply(e)
		}
	}()
	go func() {
		for e := range commitCh1 {
			store1.Apply(e)
		}
	}()
	go func() {
		for e := range commitCh2 {
			store2.Apply(e)
		}
	}()

	if err := s0.Start(); err != nil {
		t.Fatal(err)
	}
	defer s0.Stop()
	if err := s1.Start(); err != nil {
		t.Fatal(err)
	}
	defer s1.Stop()
	if err := s2.Start(); err != nil {
		t.Fatal(err)
	}
	defer s2.Stop()

	n0.Start()
	defer n0.Stop()
	n1.Start()
	defer n1.Stop()
	n2.Start()
	defer n2.Stop()

	// Wait for leader election over gRPC
	var leaderID int = -1
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for _, n := range []*node.RaftNode{n0, n1, n2} {
			if st, _ := n.GetState(); st == node.Leader {
				leaderID = n.GetID()
				break
			}
		}
		if leaderID >= 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if leaderID < 0 {
		t.Fatal("No leader elected over gRPC transport")
	}
	t.Logf("gRPC Leader elected: node %d", leaderID)

	leaderAddr := addrMap[leaderID]
	req := node.ClientRequest{Op: "SET", Key: "grpc_key", Value: "grpc_val"}
	resp, err := client.SendClientRequest(leaderAddr, req)
	if err != nil {
		t.Fatalf("SendClientRequest over gRPC failed: %v", err)
	}
	if !resp.Success {
		t.Fatalf("gRPC client request failed: %s", resp.Error)
	}

	// Verify linearizable GET
	getReq := node.ClientRequest{Op: "GET", Key: "grpc_key"}
	getResp, err := client.SendClientRequest(leaderAddr, getReq)
	if err != nil {
		t.Fatalf("SendClientRequest GET failed: %v", err)
	}
	if !getResp.Success || getResp.Value != "grpc_val" {
		t.Fatalf("Expected 'grpc_val', got success=%v, val=%q, err=%s", getResp.Success, getResp.Value, getResp.Error)
	}

	t.Log("gRPC End-to-end integration test passed successfully!")
}
