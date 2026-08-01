package network

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"

	"google.golang.org/grpc"

	"github.com/abeer/raft-kv/node"
	pb "github.com/abeer/raft-kv/proto/raftkv/v1"
)

// Server is a gRPC server that handles Raft internal RPCs and client requests
type Server struct {
	pb.UnimplementedRaftServiceServer
	pb.UnimplementedKVServiceServer

	mu                sync.RWMutex
	grpcServer        *grpc.Server
	listener          net.Listener
	raftNode          *node.RaftNode
	addr              string
	clientRequestFunc func(node.ClientRequest) node.ClientResponse
}

// NewServer creates a new gRPC server instance
func NewServer(addr string, raftNode *node.RaftNode) *Server {
	return &Server{
		addr:     addr,
		raftNode: raftNode,
	}
}

// SetClientRequestFunc sets the handler for client requests
func (s *Server) SetClientRequestFunc(fn func(node.ClientRequest) node.ClientResponse) {
	s.clientRequestFunc = fn
}

// Start begins listening and serving gRPC requests
func (s *Server) Start() error {
	lis, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", s.addr, err)
	}
	s.listener = lis

	s.grpcServer = grpc.NewServer()
	pb.RegisterRaftServiceServer(s.grpcServer, s)
	pb.RegisterKVServiceServer(s.grpcServer, s)

	log.Printf("gRPC Server listening on %s", s.addr)

	go func() {
		if err := s.grpcServer.Serve(lis); err != nil && err != grpc.ErrServerStopped {
			log.Printf("gRPC server error: %v", err)
		}
	}()

	return nil
}

// Stop gracefully shuts down the gRPC server
func (s *Server) Stop() {
	if s.grpcServer != nil {
		s.grpcServer.GracefulStop()
	}
	if s.listener != nil {
		s.listener.Close()
	}
}

// RequestVote gRPC RPC handler
func (s *Server) RequestVote(ctx context.Context, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	internalReq := node.RequestVoteRequest{
		Term:         req.Term,
		CandidateID:  int(req.CandidateId),
		LastLogIndex: req.LastLogIndex,
		LastLogTerm:  req.LastLogTerm,
	}

	resp := s.raftNode.SubmitRequestVote(int(req.CandidateId), internalReq)

	return &pb.RequestVoteResponse{
		Term:        resp.Term,
		VoteGranted: resp.VoteGranted,
	}, nil
}

// AppendEntries gRPC RPC handler
func (s *Server) AppendEntries(ctx context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	entries := make([]node.LogEntry, len(req.Entries))
	for i, e := range req.Entries {
		entries[i] = node.LogEntry{
			Term:  e.Term,
			Index: e.Index,
			Data:  e.Data,
		}
	}

	internalReq := node.AppendEntriesRequest{
		Term:         req.Term,
		LeaderID:     int(req.LeaderId),
		PrevLogIndex: req.PrevLogIndex,
		PrevLogTerm:  req.PrevLogTerm,
		Entries:      entries,
		LeaderCommit: req.LeaderCommit,
	}

	resp := s.raftNode.SubmitAppendEntries(int(req.LeaderId), internalReq)

	return &pb.AppendEntriesResponse{
		Term:          resp.Term,
		Success:       resp.Success,
		ConflictIndex: resp.ConflictIndex,
		ConflictTerm:  resp.ConflictTerm,
	}, nil
}

// InstallSnapshot gRPC RPC handler
func (s *Server) InstallSnapshot(ctx context.Context, req *pb.InstallSnapshotRequest) (*pb.InstallSnapshotResponse, error) {
	internalReq := node.InstallSnapshotRequest{
		Term:              req.Term,
		LeaderID:          int(req.LeaderId),
		LastIncludedIndex: req.LastIncludedIndex,
		LastIncludedTerm:  req.LastIncludedTerm,
		Data:              req.Data,
		Done:              req.Done,
	}

	resp := s.raftNode.SubmitInstallSnapshot(int(req.LeaderId), internalReq)

	return &pb.InstallSnapshotResponse{
		Term: resp.Term,
	}, nil
}

// Execute gRPC KV service handler
func (s *Server) Execute(ctx context.Context, req *pb.ClientRequest) (*pb.ClientResponse, error) {
	internalReq := node.ClientRequest{
		Op:    req.Op,
		Key:   req.Key,
		Value: req.Value,
	}

	var resp node.ClientResponse
	if s.clientRequestFunc != nil {
		resp = s.clientRequestFunc(internalReq)
	} else {
		resp = s.raftNode.SubmitClientRequest(internalReq)
	}

	return &pb.ClientResponse{
		Success:    resp.Success,
		Value:      resp.Value,
		Error:      resp.Error,
		LeaderAddr: resp.LeaderAddr,
	}, nil
}

// GetAddr returns the listening network address
func (s *Server) GetAddr() string {
	if s.listener != nil {
		return s.listener.Addr().String()
	}
	return s.addr
}
