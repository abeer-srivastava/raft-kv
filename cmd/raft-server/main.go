package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/abeer/raft-kv/kvstore"
	"github.com/abeer/raft-kv/network"
	"github.com/abeer/raft-kv/node"
)

func main() {
	nodeID := flag.Int("id", 0, "Node ID")
	addrs := flag.String("addrs", "", "Comma-separated list of node addresses (id:addr)")
	dataDir := flag.String("data", "data", "Data directory for WAL")
	flag.Parse()

	if *nodeID < 0 || *addrs == "" {
		fmt.Println("Usage: raft-kv -id <node_id> -addrs <addr1,addr2,...>")
		fmt.Println("Example: raft-kv -id 0 -addrs 0:localhost:8000,1:localhost:8001,2:localhost:8002")
		os.Exit(1)
	}

	addrMap := make(map[int]string)
	var peers []int
	for _, addr := range strings.Split(*addrs, ",") {
		parts := strings.SplitN(addr, ":", 2)
		if len(parts) != 2 {
			log.Fatalf("Invalid address format: %s (expected id:addr)", addr)
		}
		var id int
		fmt.Sscanf(parts[0], "%d", &id)
		addrMap[id] = parts[1]
		if id != *nodeID {
			peers = append(peers, id)
		}
	}

	if err := os.MkdirAll(*dataDir, 0755); err != nil {
		log.Fatalf("Failed to create data directory: %v", err)
	}

	client := network.NewClient()

	sendRequestVote := func(peerID int, req node.RequestVoteRequest) (*node.RequestVoteResponse, error) {
		return client.SendRequestVoteWithID(addrMap, peerID, req)
	}
	sendAppendEntries := func(peerID int, req node.AppendEntriesRequest) (*node.AppendEntriesResponse, error) {
		return client.SendAppendEntriesWithID(addrMap, peerID, req)
	}
	sendInstallSnapshot := func(peerID int, req node.InstallSnapshotRequest) (*node.InstallSnapshotResponse, error) {
		return client.SendInstallSnapshotWithID(addrMap, peerID, req)
	}

	wal, err := node.NewWAL(*dataDir, *nodeID)
	if err != nil {
		log.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	commitCh := make(chan node.LogEntry, 100)

	raftNode := node.NewRaftNode(
		*nodeID,
		peers,
		commitCh,
		sendRequestVote,
		sendAppendEntries,
		wal,
	)
	raftNode.SetSendInstallSnapshot(sendInstallSnapshot)

	store := kvstore.NewStore()

	// Wire snapshot notification channel for InstallSnapshot → state machine apply
	snapshotNotifyCh := make(chan node.SnapshotData, 1)
	raftNode.SetSnapshotNotifyCh(snapshotNotifyCh)

	// Configure automatic log compaction every 100 applied entries
	raftNode.SetSnapshotConfig(100, func() ([]byte, error) {
		return store.Snapshot()
	})

	// Apply loop: applies committed entries and handles snapshot notifications
	go func() {
		for {
			select {
			case entry, ok := <-commitCh:
				if !ok {
					return
				}
				if err := store.Apply(entry); err != nil {
					log.Printf("Failed to apply entry: %v", err)
				}
			case snap := <-snapshotNotifyCh:
				if err := store.LoadSnapshot(snap.Data); err != nil {
					log.Printf("Failed to load snapshot: %v", err)
				} else {
					log.Printf("Loaded snapshot at index=%d term=%d", snap.Index, snap.Term)
				}
			}
		}
	}()

	addr, exists := addrMap[*nodeID]
	if !exists {
		log.Fatalf("Node ID %d not found in address map", *nodeID)
	}
	server := network.NewServer(addr, raftNode)

	clientHandler := kvstore.NewClientHandler(store, raftNode, addrMap)
	server.SetClientRequestFunc(clientHandler.HandleRequest)

	raftNode.Start()

	if err := server.Start(); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}

	state, term := raftNode.GetState()
	log.Printf("Node %d started on %s (state=%s, term=%d)", *nodeID, addr, state, term)
	log.Printf("Peers: %v", peers)
	log.Printf("Cluster: %v", addrMap)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Printf("Shutting down node %d...", *nodeID)
	raftNode.Stop()
	server.Stop()
	client.Close()

	log.Printf("Node %d shut down gracefully", *nodeID)
}
