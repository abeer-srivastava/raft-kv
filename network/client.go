package network

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/abeer/raft-kv/node"
)

const (
 connectionTimeout = 2 * time.Second
 rpcTimeout       = 5 * time.Second
)

// Client handles outgoing RPC calls to Raft peers
type Client struct {
	mu      sync.RWMutex
	clients map[string]net.Conn
}

// NewClient creates a new network client
func NewClient() *Client {
	return &Client{
		clients: make(map[string]net.Conn),
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

// sendRPC sends an RPC message to a peer and waits for the response
func (c *Client) sendRPC(addr string, msg node.RPCMessage) (*node.RPCMessage, error) {
	conn, err := c.getConnection(addr)
	if err != nil {
		return nil, err
	}

	// Set deadline for the RPC
	conn.SetDeadline(time.Now().Add(rpcTimeout))

	encoder := json.NewEncoder(conn)
	decoder := json.NewDecoder(conn)

	if err := encoder.Encode(msg); err != nil {
		c.removeConnection(addr)
		return nil, fmt.Errorf("failed to send RPC: %w", err)
	}

	var resp node.RPCMessage
	if err := decoder.Decode(&resp); err != nil {
		c.removeConnection(addr)
		return nil, fmt.Errorf("failed to receive RPC response: %w", err)
	}

	// Reset deadline
	conn.SetDeadline(time.Time{})

	return &resp, nil
}

// getConnection gets or creates a connection to a peer
func (c *Client) getConnection(addr string) (net.Conn, error) {
	c.mu.RLock()
	conn, exists := c.clients[addr]
	c.mu.RUnlock()

	if exists && conn != nil {
		// Check if connection is still alive
		_, err := conn.Write([]byte{})
		if err == nil {
			return conn, nil
		}
		// Connection is dead, remove it
		c.removeConnection(addr)
	}

	// Create new connection
	c.mu.Lock()
	defer c.mu.Unlock()

	// Double-check after acquiring write lock
	if conn, exists = c.clients[addr]; exists && conn != nil {
		_, err := conn.Write([]byte{})
		if err == nil {
			return conn, nil
		}
		c.removeConnection(addr)
	}

	newConn, err := net.DialTimeout("tcp", addr, connectionTimeout)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to %s: %w", addr, err)
	}

	c.clients[addr] = newConn
	return newConn, nil
}

// removeConnection removes a connection from the cache
func (c *Client) removeConnection(addr string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if conn, exists := c.clients[addr]; exists {
		conn.Close()
		delete(c.clients, addr)
	}
}

// Close closes all connections
func (c *Client) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for addr, conn := range c.clients {
		conn.Close()
		delete(c.clients, addr)
	}
}

// NodeAddress maps node IDs to their network addresses
type NodeAddress struct {
	ID   int
	Addr string
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

// ReadFull reads exactly n bytes from a connection
func ReadFull(conn net.Conn, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := conn.Read(buf[total:])
		total += n
		if err != nil {
			if err == io.EOF {
				return total, io.ErrUnexpectedEOF
			}
			return total, err
		}
	}
	return total, nil
}
