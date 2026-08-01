package network

import (
	"context"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/abeer/raft-kv/node"
	pb "github.com/abeer/raft-kv/proto/raftkv/v1"
)

const (
	rpcTimeout = 5 * time.Second
)

type grpcPeerConn struct {
	conn       *grpc.ClientConn
	raftClient pb.RaftServiceClient
	kvClient   pb.KVServiceClient
}

// Client handles outgoing gRPC RPC calls to Raft peers and KV service
type Client struct {
	mu    sync.Mutex
	peers map[string]*grpcPeerConn
}

// NewClient creates a new gRPC network client
func NewClient() *Client {
	return &Client{
		peers: make(map[string]*grpcPeerConn),
	}
}

func (c *Client) getOrCreateConn(addr string) (*grpcPeerConn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if peer, ok := c.peers[addr]; ok {
		return peer, nil
	}

	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to gRPC server at %s: %w", addr, err)
	}

	peer := &grpcPeerConn{
		conn:       conn,
		raftClient: pb.NewRaftServiceClient(conn),
		kvClient:   pb.NewKVServiceClient(conn),
	}
	c.peers[addr] = peer
	return peer, nil
}

func (c *Client) removeConn(addr string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if peer, ok := c.peers[addr]; ok {
		peer.conn.Close()
		delete(c.peers, addr)
	}
}

// SendRequestVote sends a RequestVote RPC to a peer
func (c *Client) SendRequestVote(addr string, req node.RequestVoteRequest) (*node.RequestVoteResponse, error) {
	peer, err := c.getOrCreateConn(addr)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()

	pbReq := &pb.RequestVoteRequest{
		Term:         req.Term,
		CandidateId:  int32(req.CandidateID),
		LastLogIndex: req.LastLogIndex,
		LastLogTerm:  req.LastLogTerm,
	}

	resp, err := peer.raftClient.RequestVote(ctx, pbReq)
	if err != nil {
		c.removeConn(addr)
		return nil, err
	}

	return &node.RequestVoteResponse{
		Term:        resp.Term,
		VoteGranted: resp.VoteGranted,
	}, nil
}

// SendAppendEntries sends an AppendEntries RPC to a peer
func (c *Client) SendAppendEntries(addr string, req node.AppendEntriesRequest) (*node.AppendEntriesResponse, error) {
	peer, err := c.getOrCreateConn(addr)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()

	entries := make([]*pb.LogEntry, len(req.Entries))
	for i, e := range req.Entries {
		entries[i] = &pb.LogEntry{
			Term:  e.Term,
			Index: e.Index,
			Data:  e.Data,
		}
	}

	pbReq := &pb.AppendEntriesRequest{
		Term:         req.Term,
		LeaderId:     int32(req.LeaderID),
		PrevLogIndex: req.PrevLogIndex,
		PrevLogTerm:  req.PrevLogTerm,
		Entries:      entries,
		LeaderCommit: req.LeaderCommit,
	}

	resp, err := peer.raftClient.AppendEntries(ctx, pbReq)
	if err != nil {
		c.removeConn(addr)
		return nil, err
	}

	return &node.AppendEntriesResponse{
		Term:          resp.Term,
		Success:       resp.Success,
		ConflictIndex: resp.ConflictIndex,
		ConflictTerm:  resp.ConflictTerm,
	}, nil
}

// SendInstallSnapshot sends an InstallSnapshot RPC to a peer
func (c *Client) SendInstallSnapshot(addr string, req node.InstallSnapshotRequest) (*node.InstallSnapshotResponse, error) {
	peer, err := c.getOrCreateConn(addr)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()

	pbReq := &pb.InstallSnapshotRequest{
		Term:              req.Term,
		LeaderId:          int32(req.LeaderID),
		LastIncludedIndex: req.LastIncludedIndex,
		LastIncludedTerm:  req.LastIncludedTerm,
		Data:              req.Data,
		Done:              req.Done,
	}

	resp, err := peer.raftClient.InstallSnapshot(ctx, pbReq)
	if err != nil {
		c.removeConn(addr)
		return nil, err
	}

	return &node.InstallSnapshotResponse{
		Term: resp.Term,
	}, nil
}

// SendClientRequest sends a client operation request over gRPC
func (c *Client) SendClientRequest(addr string, req node.ClientRequest) (*node.ClientResponse, error) {
	peer, err := c.getOrCreateConn(addr)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()

	pbReq := &pb.ClientRequest{
		Op:    req.Op,
		Key:   req.Key,
		Value: req.Value,
	}

	resp, err := peer.kvClient.Execute(ctx, pbReq)
	if err != nil {
		c.removeConn(addr)
		return nil, err
	}

	return &node.ClientResponse{
		Success:    resp.Success,
		Value:      resp.Value,
		Error:      resp.Error,
		LeaderAddr: resp.LeaderAddr,
	}, nil
}

// SendRequestVoteWithID sends a RequestVote RPC using target node ID
func (c *Client) SendRequestVoteWithID(addrMap map[int]string, peerID int, req node.RequestVoteRequest) (*node.RequestVoteResponse, error) {
	addr, exists := addrMap[peerID]
	if !exists {
		return nil, fmt.Errorf("unknown peer ID: %d", peerID)
	}
	return c.SendRequestVote(addr, req)
}

// SendAppendEntriesWithID sends an AppendEntries RPC using target node ID
func (c *Client) SendAppendEntriesWithID(addrMap map[int]string, peerID int, req node.AppendEntriesRequest) (*node.AppendEntriesResponse, error) {
	addr, exists := addrMap[peerID]
	if !exists {
		return nil, fmt.Errorf("unknown peer ID: %d", peerID)
	}
	return c.SendAppendEntries(addr, req)
}

// SendInstallSnapshotWithID sends an InstallSnapshot RPC using target node ID
func (c *Client) SendInstallSnapshotWithID(addrMap map[int]string, peerID int, req node.InstallSnapshotRequest) (*node.InstallSnapshotResponse, error) {
	addr, exists := addrMap[peerID]
	if !exists {
		return nil, fmt.Errorf("unknown peer ID: %d", peerID)
	}
	return c.SendInstallSnapshot(addr, req)
}

// Close closes all open gRPC client connections
func (c *Client) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for addr, peer := range c.peers {
		peer.conn.Close()
		delete(c.peers, addr)
	}
}
