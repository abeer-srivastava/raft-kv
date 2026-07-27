package node

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"sync"
)

// WALRecord represents a single record in the write-ahead log
type WALRecord struct {
	Type string `json:"type"` // "term", "entry", "entries"
	// For term records
	Term    uint64 `json:"term,omitempty"`
	VotedFor int   `json:"voted_for,omitempty"`
	// For entry records
	Entry  *LogEntry  `json:"entry,omitempty"`
	Entries []LogEntry `json:"entries,omitempty"`
}

// WAL is a simple write-ahead log for persisting Raft state
type WAL struct {
	mu       sync.Mutex
	file     *os.File
	writer   *bufio.Writer
	dir      string
	nodeID   int
}

// NewWAL creates a new WAL for the given node
func NewWAL(dir string, nodeID int) (*WAL, error) {
	filename := fmt.Sprintf("%s/node-%d.wal", dir, nodeID)
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL: %w", err)
	}

	return &WAL{
		file:   file,
		writer: bufio.NewWriter(file),
		dir:    dir,
		nodeID: nodeID,
	}, nil
}

// SaveTerm persists the current term and votedFor
func (w *WAL) SaveTerm(term uint64, votedFor int) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	record := WALRecord{
		Type:      "term",
		Term:      term,
		VotedFor: votedFor,
	}

	return w.writeRecord(record)
}

// AppendEntry appends a single log entry
func (w *WAL) AppendEntry(entry LogEntry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	record := WALRecord{
		Type:  "entry",
		Entry: &entry,
	}

	return w.writeRecord(record)
}

// AppendEntries appends multiple log entries
func (w *WAL) AppendEntries(entries []LogEntry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	record := WALRecord{
		Type:    "entries",
		Entries: entries,
	}

	return w.writeRecord(record)
}

// writeRecord writes a single record to the WAL
func (w *WAL) writeRecord(record WALRecord) error {
	data, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("failed to marshal WAL record: %w", err)
	}

	// Write length prefix (4 bytes, big-endian)
	length := uint32(len(data))
	buf := make([]byte, 4)
	buf[0] = byte(length >> 24)
	buf[1] = byte(length >> 16)
	buf[2] = byte(length >> 8)
	buf[3] = byte(length)

	if _, err := w.writer.Write(buf); err != nil {
		return fmt.Errorf("failed to write WAL length: %w", err)
	}

	if _, err := w.writer.Write(data); err != nil {
		return fmt.Errorf("failed to write WAL data: %w", err)
	}

	return w.writer.Flush()
}

// Recover reads the WAL and restores the node's state
func (w *WAL) Recover(node *RaftNode) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	filename := fmt.Sprintf("%s/node-%d.wal", w.dir, w.nodeID)
	file, err := os.Open(filename)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // No WAL file, start fresh
		}
		return fmt.Errorf("failed to open WAL for recovery: %w", err)
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	for {
		// Read length prefix
		lengthBuf := make([]byte, 4)
		_, err := reader.Read(lengthBuf)
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			return fmt.Errorf("failed to read WAL length: %w", err)
		}

		length := uint32(lengthBuf[0])<<24 |
			uint32(lengthBuf[1])<<16 |
			uint32(lengthBuf[2])<<8 |
			uint32(lengthBuf[3])

		// Read data
		data := make([]byte, length)
		_, err = reader.Read(data)
		if err != nil {
			return fmt.Errorf("failed to read WAL data: %w", err)
		}

		// Parse record
		var record WALRecord
		if err := json.Unmarshal(data, &record); err != nil {
			return fmt.Errorf("failed to parse WAL record: %w", err)
		}

		// Apply record
		switch record.Type {
		case "term":
			node.currentTerm = record.Term
			node.votedFor = record.VotedFor
		case "entry":
			if record.Entry != nil {
				for uint64(len(node.raftLog)) <= record.Entry.Index {
					node.raftLog = append(node.raftLog, LogEntry{})
				}
				node.raftLog[record.Entry.Index] = *record.Entry
			}
		case "entries":
			for _, entry := range record.Entries {
				for uint64(len(node.raftLog)) <= entry.Index {
					node.raftLog = append(node.raftLog, LogEntry{})
				}
				node.raftLog[entry.Index] = entry
			}
		}
	}

	if len(node.raftLog) > 0 {
		node.commitIndex = node.raftLog[len(node.raftLog)-1].Index
		node.lastApplied = node.commitIndex
	}

	return nil
}

// Close closes the WAL file
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.writer.Flush(); err != nil {
		return err
	}
	return w.file.Close()
}
