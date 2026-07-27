package network

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"sync"

	"github.com/abeer/raft-kv/node"
)

// Server is a TCP server that handles incoming Raft RPCs
type Server struct {
	mu       sync.RWMutex
	listener net.Listener
	node     *node.RaftNode
	addr     string
	quit     chan struct{}
}

// NewServer creates a new network server
func NewServer(addr string, raftNode *node.RaftNode) *Server {
	return &Server{
		addr: addr,
		node: raftNode,
		quit: make(chan struct{}),
	}
}

// Start begins listening for connections
func (s *Server) Start() error {
	var err error
	s.listener, err = net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", s.addr, err)
	}

	log.Printf("Server listening on %s", s.addr)

	go s.acceptLoop()
	return nil
}

// Stop gracefully shuts down the server
func (s *Server) Stop() {
	close(s.quit)
	if s.listener != nil {
		s.listener.Close()
	}
}

// acceptLoop handles incoming connections
func (s *Server) acceptLoop() {
	for {
		select {
		case <-s.quit:
			return
		default:
			conn, err := s.listener.Accept()
			if err != nil {
				select {
				case <-s.quit:
					return
				default:
					log.Printf("Failed to accept connection: %v", err)
					continue
				}
			}
			go s.handleConnection(conn)
		}
	}
}

// handleConnection processes messages from a single connection
func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()

	decoder := json.NewDecoder(conn)
	encoder := json.NewEncoder(conn)

	for {
		var msg node.RPCMessage
		if err := decoder.Decode(&msg); err != nil {
			if err == io.EOF {
				return
			}
			log.Printf("Failed to decode message: %v", err)
			return
		}

		response, err := s.dispatch(msg)
		if err != nil {
			log.Printf("Failed to dispatch message: %v", err)
			continue
		}

		if err := encoder.Encode(response); err != nil {
			log.Printf("Failed to encode response: %v", err)
			return
		}
	}
}

// dispatch routes a message to the appropriate handler
func (s *Server) dispatch(msg node.RPCMessage) (node.RPCMessage, error) {
	var respData json.RawMessage

	switch msg.Type {
	case "RequestVote":
		var req node.RequestVoteRequest
		if err := json.Unmarshal(msg.Data, &req); err != nil {
			return node.RPCMessage{}, fmt.Errorf("failed to unmarshal RequestVote: %w", err)
		}
		resp := s.node.SubmitRequestVote(0, req) // peerID is 0 for incoming
		data, _ := json.Marshal(resp)
		respData = data

	case "AppendEntries":
		var req node.AppendEntriesRequest
		if err := json.Unmarshal(msg.Data, &req); err != nil {
			return node.RPCMessage{}, fmt.Errorf("failed to unmarshal AppendEntries: %w", err)
		}
		resp := s.node.SubmitAppendEntries(0, req) // peerID is 0 for incoming
		data, _ := json.Marshal(resp)
		respData = data

	case "ClientRequest":
		var req node.ClientRequest
		if err := json.Unmarshal(msg.Data, &req); err != nil {
			return node.RPCMessage{}, fmt.Errorf("failed to unmarshal ClientRequest: %w", err)
		}
		resp := s.node.SubmitClientRequest(req)
		data, _ := json.Marshal(resp)
		respData = data

	default:
		return node.RPCMessage{}, fmt.Errorf("unknown message type: %s", msg.Type)
	}

	return node.RPCMessage{
		Type: msg.Type + "Response",
		Data: respData,
	}, nil
}

// GetAddr returns the server's address
func (s *Server) GetAddr() string {
	if s.listener != nil {
		return s.listener.Addr().String()
	}
	return s.addr
}
