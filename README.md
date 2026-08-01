# Raft KV: A Scratch Implementation of the Raft Consensus Algorithm

A distributed, fault-tolerant key-value store built on the [Raft consensus algorithm](https://raft.github.io/raft.pdf). Implemented in Go with **gRPC/Protobuf** networking, strict **log compaction**, and robust linearizable reads.

## Table of Contents

1. [What is Raft?](#what-is-raft)
2. [Leader Election](#leader-election)
3. [Log Replication](#log-replication)
4. [Safety Guarantees](#safety-guarantees)
5. [Architecture](#architecture)
6. [Getting Started](#getting-started)
7. [Usage](#usage)

---

## What is Raft?

Raft is a **distributed consensus algorithm** designed to be understandable. It elects a **leader** that manages **log replication** to a set of **followers**, ensuring all nodes agree on the same sequence of state machine commands even in the presence of failures.

### Core Concepts

| Concept | Description |
|---------|-------------|
| **Node** | A process in the Raft cluster (3, 5, or 7 nodes typical) |
| **State** | Each node is either `Follower`, `Candidate`, or `Leader` |
| **Term** | A logical clock; monotonically increasing integer |
| **Log** | A sequence of `LogEntry{Term, Index, Data}` entries |
| **Commit** | An entry is "committed" when replicated to a majority |
| **State Machine** | The application (KV store) that applies committed entries |

### Node States

```
                  ┌─────────────┐
                  │   Follower  │
                  └──────┬──────┘
                         │ election timeout
                         ▼
                  ┌─────────────┐
          ┌───────┤  Candidate  ├───────┐
          │       └──────┬──────┘       │
          │              │              │
   discovers      votes majority  discovers
 current leader    ──────────▶   higher term
          │              │              │
          ▼              ▼              ▼
   ┌─────────────┐  ┌─────────────┐  ┌─────────────┐
   │   Follower  │  │   Leader    │  │   Follower  │
   └─────────────┘  └──────┬──────┘  └─────────────┘
                           │ discovers higher term
                           ▼
                    ┌─────────────┐
                    │   Follower  │
                    └─────────────┘
```

---

## Leader Election

### Algorithm

1. All nodes start as **Followers**
2. A follower becomes a **Candidate** when its **election timeout** fires (150-300ms random)
3. The Candidate increments its term, votes for itself, and sends `RequestVote` RPCs
4. If a Candidate receives votes from a **majority**, it becomes the **Leader**
5. The Leader sends **heartbeats** (empty `AppendEntries`) to maintain authority

### Election Timeout

- Each node has a **randomized timeout** (150-300ms)
- If no heartbeat is received before timeout, an election starts
- Randomization ensures **split votes** are unlikely

### RequestVote RPC

**Request:**
```go
type RequestVoteRequest struct {
    Term         uint64  // Candidate's term
    CandidateID  int     // Candidate requesting vote
    LastLogIndex uint64  // Index of candidate's last log entry
    LastLogTerm  uint64  // Term of candidate's last log entry
}
```

**Response:**
```go
type RequestVoteResponse struct {
    Term        uint64  // Current term (for candidate to update itself)
    VoteGranted bool    // True if candidate received vote
}
```

**Grant vote if:**
- `req.Term >= currentTerm`
- `votedFor` is null or equals `candidateId`
- Candidate's log is at least as up-to-date as voter's log

### Pseudocode

```
// On election timeout:
state = Candidate
currentTerm++
votedFor = self
votesReceived = 1

for each peer:
    send RequestVote{term, candidateId, lastLogIndex, lastLogTerm}

// On receiving RequestVote:
if req.Term < currentTerm:
    reply false
else if req.Term > currentTerm:
    currentTerm = req.Term
    state = Follower

if votedFor is null or candidateId:
    if candidate's log is at least as up-to-date:
        votedFor = candidateId
        reply true
```

---

## Log Replication

### Algorithm

1. Client sends a command to the **Leader**
2. Leader appends the command to its log
3. Leader sends `AppendEntries` RPCs to all followers
4. Once a **majority** acknowledge the entry, it's **committed**
5. Leader applies committed entries to its state machine
6. Leader notifies followers of commit in next heartbeat

### AppendEntries RPC

**Request:**
```go
type AppendEntriesRequest struct {
    Term         uint64     // Leader's term
    LeaderID     int        // Leader's ID
    PrevLogIndex uint64     // Index of entry immediately preceding new ones
    PrevLogTerm  uint64     // Term of PrevLogIndex entry
    Entries      []LogEntry // Log entries to store (empty for heartbeat)
    LeaderCommit uint64     // Leader's commitIndex
}
```

**Response:**
```go
type AppendEntriesResponse struct {
    Term          uint64 // Current term
    Success       bool   // True if follower matched PrevLogIndex/PrevLogTerm
    ConflictIndex uint64 // Fast backtracking hint
    ConflictTerm  uint64 // Fast backtracking hint
}
```

### Log Consistency Check

The Leader maintains `nextIndex[peer]` for each follower. On each RPC:
1. Check if follower's log contains `PrevLogIndex` with `PrevLogTerm`
2. If not, reply false and the leader decrements `nextIndex` and retries
3. Once consistent, append any new entries

```
Leader's log:  [0,1,2,3,4,5,6]
                ─────────────
Follower's log: [0,1,2,3,4]
                 ─────────

Leader sends: PrevLogIndex=4, PrevLogTerm=2, Entries=[5,6]
Follower: has entry at index 4 with term 2? Yes → append 5,6
```

### Commit Advancement

The Leader advances its `commitIndex` when:
1. An entry from the **current term** is replicated to a majority
2. All entries up to that index are also replicated

```go
// Only commit entries from current term
for N = lastLogIndex down to commitIndex+1:
    if log[N].term == currentTerm AND
       count(matchIndex[i] >= N) > majority:
        commitIndex = N
```

**Critical:** Never commit entries from previous terms by counting replicas. They commit indirectly when a current-term entry is committed.

### Pseudocode

```
// Leader receives client command:
append entry to log
for each peer:
    send AppendEntries{term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit}

// On receiving AppendEntries:
if req.Term < currentTerm:
    reply false

if prevLogIndex > lastLogIndex OR log[prevLogIndex].term != prevLogTerm:
    reply false  // Log inconsistency

for each entry in entries[]:
    if log[entry.index].term != entry.term:
        truncate log from entry.index onward
    append entry to log

if leaderCommit > commitIndex:
    commitIndex = min(leaderCommit, lastLogIndex)
    apply entries up to commitIndex to state machine

reply true
```

---

## Safety Guarantees

### Invariants (from Figure 3 of the Raft paper)

| Property | Description |
|----------|-------------|
| **Election Safety** | At most one leader per term |
| **Leader Append-Only** | Leader never deletes/overwrites entries, only appends |
| **Log Matching** | If two entries have same index+term, all preceding entries are identical |
| **Leader Completeness** | If entry committed in term T, all leaders for terms >T have it |
| **State Machine Safety** | If server applies entry at index i, no server applies different entry at i |

### Election Restriction

To ensure Leader Completeness, a Candidate must have a log at least as up-to-date as a majority of nodes. A vote is denied if:

```
voter.lastLogTerm > candidate.lastLogTerm
OR
(voter.lastLogTerm == candidate.lastLogTerm AND
 voter.lastLogIndex > candidate.lastLogIndex)
```

### Commit Rules

1. Leader only commits entries from the **current term** by counting replicas
2. Entries from previous terms commit **indirectly** when a current-term entry after them is committed
3. This prevents the "Figure 8 problem" where committed entries could be overwritten

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        Raft KV Cluster                                  │
│                                                                         │
│  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐       │
│  │   Node 0         │  │   Node 1         │  │   Node 2         │       │
│  │  (Leader)        │  │  (Follower)      │  │  (Follower)      │       │
│  │                  │  │                  │  │                  │       │
│  │ ┌──────────────┐ │  │ ┌──────────────┐ │  │ ┌──────────────┐ │       │
│  │ │  Raft Node   │ │  │ │  Raft Node   │ │  │ │  Raft Node   │ │       │
│  │ │              │ │  │ │              │ │  │ │              │ │       │
│  │ │ • State      │ │  │ │ • State      │ │  │ │ • State      │ │       │
│  │ │ • Term       │ │  │ │ • Term       │ │  │ │ • Term       │ │       │
│  │ │ • Log        │ │  │ │ • Log        │ │  │ │ • Log        │ │       │
│  │ │ • Commit Idx │ │  │ │ • Commit Idx │ │  │ │ • Commit Idx │ │       │
│  │ └──────┬───────┘ │  │ └──────┬───────┘ │  │ └──────┬───────┘ │       │
│  │        │         │  │        │         │  │        │         │       │
│  │ ┌──────▼───────┐ │  │ ┌──────▼───────┐ │  │ ┌──────▼───────┐ │       │
│  │ │  Network     │ │  │ │  Network     │ │  │ │  Network     │ │       │
│  │ │  Layer       │ │  │ │  Layer       │ │  │ │  Layer       │ │       │
│  │ │  (gRPC)      │ │  │ │  (gRPC)      │ │  │ │  (gRPC)      │ │       │
│  │ └──────┬───────┘ │  │ └──────┬───────┘ │  │ └──────┬───────┘ │       │
│  │        │         │  │        │         │  │        │         │       │
│  │ ┌──────▼───────┐ │  │ ┌──────▼───────┐ │  │ ┌──────▼───────┐ │       │
│  │ │  KV Store    │ │  │ │  KV Store    │ │  │ │  KV Store    │ │       │
│  │ │  (State      │ │  │ │  (State      │ │  │ │  (State      │ │       │
│  │ │   Machine)   │ │  │ │   Machine)   │ │  │ │   Machine)   │ │       │
│  │ └──────────────┘ │  │ └──────────────┘ │  │ └──────────────┘ │       │
│  │                  │  │                  │  │                  │       │
│  │ ┌──────────────┐ │  │ ┌──────────────┐ │  │ ┌──────────────┐ │       │
│  │ │ Persistence  │ │  │ │ Persistence  │ │  │ │ Persistence  │ │       │
│  │ │ (WAL File)   │ │  │ │ (WAL File)   │ │  │ │ (WAL File)   │ │       │
│  │ └──────────────┘ │  │ └──────────────┘ │  │ └──────────────┘ │       │
│  └──────────────────┘  └──────────────────┘  └──────────────────┘       │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### Message Flow: Client Request

```
  Client Request (SET foo bar)
         │
         ▼
  ┌─────────────────┐
  │  Any Node       │ ──── If not leader, redirect
  └────────┬────────┘
           │ (if leader)
           ▼
  ┌─────────────────┐
  │  Append to Log  │
  └────────┬────────┘
           │
           ├───(send RPCs)───┐
           ▼                 ▼
  ┌─────────────────┐     ┌─────────────────┐
  │  AppendEntries  │     │  AppendEntries  │
  │  RPC to Node 1  │     │  RPC to Node 2  │
  └────────┬────────┘     └────────┬────────┘
           │                       │
           ▼                       ▼
  ┌─────────────────┐     ┌─────────────────┐
  │  Validate &     │     │  Validate &     │
  │  Append to Log  │     │  Append to Log  │
  └────────┬────────┘     └────────┬────────┘
           │                       │
           ▼                       ▼
  ┌─────────────────┐     ┌─────────────────┐
  │  Reply Success  │     │  Reply Success  │
  └────────┬────────┘     └────────┬────────┘
           │                       │
           └───────────┬───────────┘
                       │
                       ▼
  ┌─────────────────┐
  │  Update         │
  │  matchIndex     │
  │  Check Quorum   │
  └────────┬────────┘
           │ (majority replicated?)
           ▼
  ┌─────────────────┐
  │  Advance        │
  │  commitIndex    │
  └────────┬────────┘
           │
           ▼
  ┌─────────────────┐
  │  Apply to       │
  │  State Machine  │
  │  (KV Store)     │
  └────────┬────────┘
           │
           ▼
  ┌─────────────────┐
  │  Return Result  │
  │  to Client      │
  └─────────────────┘
```

### Leader Election Flow

```
  Election Timeout Fires
         │
         ▼
  ┌─────────────────┐
  │  Become         │
  │  Candidate      │
  │  (term++)       │
  │  (vote for self)│
  └────────┬────────┘
           │
           ├───(send RPCs)───┐
           ▼                 ▼
  ┌─────────────────┐     ┌─────────────────┐
  │  RequestVote    │     │  RequestVote    │
  │  RPC to Node 1  │     │  RPC to Node 2  │
  └────────┬────────┘     └────────┬────────┘
           │                       │
           ▼                       ▼
  ┌─────────────────┐     ┌─────────────────┐
  │  Check:         │     │  Check:         │
  │  • term >= mine?│     │  • term >= mine?│
  │  • voted already│     │  • voted already│
  │  • up-to-date?  │     │  • up-to-date?  │
  └────────┬────────┘     └────────┬────────┘
           │                       │
           ▼                       ▼
  ┌─────────────────┐     ┌─────────────────┐
  │  Grant Vote       │     │  Grant Vote       │
  └────────┬────────┘     └────────┬────────┘
           │                       │
           └───────────┬───────────┘
                       │ (majority votes?)
                       ▼
  ┌─────────────────┐
  │ Become Leader   │
  │ Start Heartbeats│
  └─────────────────┘
```

---

## Getting Started

### Prerequisites

- Go 1.21+

### Build

```bash
go build -o bin/raft-server ./cmd/raft-server/
go build -o bin/raft-kv-cli ./cmd/raft-kv/
```

### Run a 3-Node Cluster

```bash
# Terminal 1
./bin/raft-server -id 0 -addrs 0:localhost:8000,1:localhost:8001,2:localhost:8002 -data ./data0

# Terminal 2
./bin/raft-server -id 1 -addrs 0:localhost:8000,1:localhost:8001,2:localhost:8002 -data ./data1

# Terminal 3
./bin/raft-server -id 2 -addrs 0:localhost:8000,1:localhost:8001,2:localhost:8002 -data ./data2
```

### Run Tests

```bash
go test ./node/ -v
```

---

## Usage

### CLI Client

```bash
# Connect to any node in the cluster
./raft-kv-cli localhost:8000

# Set a value
> SET foo bar
OK

# Get a value
> GET foo
bar

# Delete a value
> DELETE foo
OK
```

### Supported Operations

| Command | Description | Example |
|---------|-------------|---------|
| `SET <key> <value>` | Set a key-value pair | `SET name Alice` |
| `GET <key>` | Get a value by key | `GET name` |
| `DELETE <key>` | Delete a key | `DELETE name` |
| `QUIT` | Exit the client | `QUIT` |

---

## Project Structure

```
raft-kv/
├── cmd/
│   ├── raft-server/
│   │   └── main.go          # Entry point, cluster setup
│   └── raft-kv/
│       └── main.go          # Interactive CLI client
├── proto/
│   └── raftkv/v1/           # Protobuf definitions and generated gRPC code
├── node/
│   ├── types.go             # Message types, log entry, node state
│   ├── raft.go              # Core Raft logic, leader election, log compaction
│   ├── raft_test.go         # Extensive unit & stress tests
│   └── persistence.go       # WAL-based disk persistence
├── network/
│   ├── server.go            # gRPC server implementation
│   ├── client.go            # gRPC client for peer RPC calls
│   └── network_test.go      # End-to-end cluster integration tests
├── kvstore/
│   └── store.go             # In-memory KV state machine with snapshot support
└── go.mod
```

---

## References

- [In Search of an Understandable Consensus Algorithm](https://raft.github.io/raft.pdf) - Diego Ongaro, Stanford PhD Thesis, 2014
- [Raft Visualization](https://raft.github.io/) - Interactive Raft visualization
- [The Raft Authors](https://raft.github.io/) - Official Raft resources

---

## License

MIT
