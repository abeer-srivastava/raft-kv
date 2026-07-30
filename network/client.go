package network

import (
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/abeer/raft-kv/node"
)

const (
	connectionTimeout = 2 * time.Second
	rpcTimeout        = 5 * time.Second
)

type peerConn struct {
	mu      sync.Mutex
	conn    net.Conn
	encoder *json.Encoder
	decoder *json.Decoder
}

// Client handles outgoing RPC calls to Raft peers
type Client struct {
	mu    sync.Mutex
	peers map[string]*peerConn
}

// NewClient creates a new network client
func NewClient() *Client {
	return &Client{
		peers: make(map[string]*peerConn),
	}
}

// SendRequestVote sends a RequestVote RPC to a peer
func (c *Client) SendRequestVote(addr string, req node.RequestVoteRequest) (*node.RequestVoteResponse, error) {
	data, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal RequestVote: %w", err)
	}

	msg := node.RPCMessage{
		Type: "RequestVote",
		Data: data,
	}

	respMsg, err := c.sendRPC(addr, msg)
	if err != nil {
		return nil, err
	}

	var resp node.RequestVoteResponse
	if err := json.Unmarshal(respMsg.Data, &resp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal RequestVote response: %w", err)
	}

	return &resp, nil
}

// SendAppendEntries sends an AppendEntries RPC to a peer
func (c *Client) SendAppendEntries(addr string, req node.AppendEntriesRequest) (*node.AppendEntriesResponse, error) {
	data, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal AppendEntries: %w", err)
	}

	msg := node.RPCMessage{
		Type: "AppendEntries",
		Data: data,
	}

	respMsg, err := c.sendRPC(addr, msg)
	if err != nil {
		return nil, err
	}

	var resp node.AppendEntriesResponse
	if err := json.Unmarshal(respMsg.Data, &resp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal AppendEntries response: %w", err)
	}

	return &resp, nil
}

// SendClientRequest sends a client request to the cluster (should be sent to leader)
func (c *Client) SendClientRequest(addr string, req node.ClientRequest) (*node.ClientResponse, error) {
	data, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal ClientRequest: %w", err)
	}

	msg := node.RPCMessage{
		Type: "ClientRequest",
		Data: data,
	}

	respMsg, err := c.sendRPC(addr, msg)
	if err != nil {
		return nil, err
	}

	var resp node.ClientResponse
	if err := json.Unmarshal(respMsg.Data, &resp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal ClientRequest response: %w", err)
	}

	return &resp, nil
}

func (c *Client) sendRPC(addr string, msg node.RPCMessage) (*node.RPCMessage, error) {
	pc, err := c.getOrCreatePeerConn(addr)
	if err != nil {
		return nil, err
	}

	pc.mu.Lock()
	defer pc.mu.Unlock()

	pc.conn.SetDeadline(time.Now().Add(rpcTimeout))

	if err := pc.encoder.Encode(msg); err != nil {
		c.removePeerConn(addr)
		return nil, fmt.Errorf("failed to send RPC to %s: %w", addr, err)
	}

	var resp node.RPCMessage
	if err := pc.decoder.Decode(&resp); err != nil {
		c.removePeerConn(addr)
		return nil, fmt.Errorf("failed to receive RPC response from %s: %w", addr, err)
	}

	pc.conn.SetDeadline(time.Time{})

	return &resp, nil
}

func (c *Client) getOrCreatePeerConn(addr string) (*peerConn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if pc, exists := c.peers[addr]; exists {
		return pc, nil
	}

	conn, err := net.DialTimeout("tcp", addr, connectionTimeout)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to %s: %w", addr, err)
	}

	pc := &peerConn{
		conn:    conn,
		encoder: json.NewEncoder(conn),
		decoder: json.NewDecoder(conn),
	}
	c.peers[addr] = pc
	return pc, nil
}

func (c *Client) removePeerConn(addr string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if pc, exists := c.peers[addr]; exists {
		pc.conn.Close()
		delete(c.peers, addr)
	}
}

// Close closes all connections
func (c *Client) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for addr, pc := range c.peers {
		pc.conn.Close()
		delete(c.peers, addr)
	}
}

// SendRequestVoteWithID sends a RequestVote RPC using node ID instead of address
func (c *Client) SendRequestVoteWithID(addrMap map[int]string, peerID int, req node.RequestVoteRequest) (*node.RequestVoteResponse, error) {
	addr, exists := addrMap[peerID]
	if !exists {
		return nil, fmt.Errorf("unknown peer ID: %d", peerID)
	}
	return c.SendRequestVote(addr, req)
}

// SendAppendEntriesWithID sends an AppendEntries RPC using node ID instead of address
func (c *Client) SendAppendEntriesWithID(addrMap map[int]string, peerID int, req node.AppendEntriesRequest) (*node.AppendEntriesResponse, error) {
	addr, exists := addrMap[peerID]
	if !exists {
		return nil, fmt.Errorf("unknown peer ID: %d", peerID)
	}
	return c.SendAppendEntries(addr, req)
}
