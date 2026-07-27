package kvstore

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"

	"github.com/abeer/raft-kv/node"
)

// Command represents a KV store command
type Command struct {
	Op    string `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}

// Store is a key-value store that acts as the Raft state machine
type Store struct {
	mu    sync.RWMutex
	data  map[string]string
}

// NewStore creates a new KV store
func NewStore() *Store {
	return &Store{
		data: make(map[string]string),
	}
}

// Apply applies a committed log entry to the KV store
func (s *Store) Apply(entry node.LogEntry) error {
	var cmd Command
	if err := json.Unmarshal(entry.Data, &cmd); err != nil {
		return fmt.Errorf("failed to unmarshal command: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	switch cmd.Op {
	case "SET":
		s.data[cmd.Key] = cmd.Value
		log.Printf("Applied SET %s = %s", cmd.Key, cmd.Value)
	case "DELETE":
		delete(s.data, cmd.Key)
		log.Printf("Applied DELETE %s", cmd.Key)
	case "GET":
		// GET is read-only, no-op for state machine
	default:
		return fmt.Errorf("unknown operation: %s", cmd.Op)
	}

	return nil
}

// Get retrieves a value from the store
func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	val, ok := s.data[key]
	return val, ok
}

// Set sets a value in the store (used for initialization/testing)
func (s *Store) Set(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.data[key] = value
}

// Delete removes a key from the store
func (s *Store) Delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.data, key)
}

// GetAll returns all key-value pairs
func (s *Store) GetAll() map[string]string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make(map[string]string, len(s.data))
	for k, v := range s.data {
		result[k] = v
	}
	return result
}

// Size returns the number of entries in the store
func (s *Store) Size() int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return len(s.data)
}

// ClientHandler handles client requests and forwards them to the Raft cluster
type ClientHandler struct {
	store     *Store
	raftNode  *node.RaftNode
	leaderID  int
}

// NewClientHandler creates a new client handler
func NewClientHandler(store *Store, raftNode *node.RaftNode) *ClientHandler {
	return &ClientHandler{
		store:    store,
		raftNode: raftNode,
	}
}

// HandleRequest processes a client request
func (h *ClientHandler) HandleRequest(req node.ClientRequest) node.ClientResponse {
	switch req.Op {
	case "GET":
		// GET can be served locally
		val, ok := h.store.Get(req.Key)
		if !ok {
			return node.ClientResponse{
				Success: false,
				Error:   fmt.Sprintf("key not found: %s", req.Key),
			}
		}
		return node.ClientResponse{
			Success: true,
			Value:   val,
		}

	case "SET", "DELETE":
		// Write operations must go through Raft consensus
		return h.raftNode.SubmitClientRequest(req)

	default:
		return node.ClientResponse{
			Success: false,
			Error:   fmt.Sprintf("unknown operation: %s", req.Op),
		}
	}
}

// CreateCommand creates a command that can be proposed to Raft
func CreateCommand(op, key, value string) []byte {
	cmd := Command{
		Op:    op,
		Key:   key,
		Value: value,
	}
	data, _ := json.Marshal(cmd)
	return data
}
