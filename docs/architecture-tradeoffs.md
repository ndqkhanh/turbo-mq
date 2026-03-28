# TurboMQ — Architecture Trade-offs

Every architectural decision involves a trade-off. This document explains the six most consequential design choices in TurboMQ, what was sacrificed in each case, and when a different approach would be better.

---

## 1. Per-partition Raft vs Single Controller

**What was chosen.** Each partition runs an independent `RaftNode` instance with its own `RaftLog` (in-memory `ArrayList<LogEntry>`), its own `currentTerm`/`votedFor` state, and its own leader state maps (`nextIndex`, `matchIndex`). Partition-to-broker assignment is managed separately by `PartitionManager`.

**What's sacrificed.**
- **O(P x R) memory:** P partitions with R replicas each maintain a full Raft state machine, log, and peer maps.
- **Election storms:** A network blip triggers 10,000 concurrent election rounds. Pre-vote (`RaftNode.java:110-132`) mitigates disruption but not CPU cost.
- **No cross-partition transactions:** Each Raft group is independent. Atomic commits across partitions require external coordination (e.g., two-phase commit on top of Raft).
- **No batched heartbeats:** Each `RaftNode.sendHeartbeats()` sends individual `AppendEntries` RPCs. 1,000 partitions = 1,000 separate RPCs per heartbeat interval per peer.

**When the alternative wins.**
- **Single controller (Kafka KRaft):** O(1) Raft groups regardless of partition count. Batched metadata updates. Simpler failure detection. Disadvantage: single point of metadata bottleneck.
- **Multi-Raft with batched transport (TiKV-style):** Multiple Raft groups share a transport layer that batches messages to the same peer. Retains per-partition independence while amortizing network overhead.

**Engineering judgment.** Architecturally clean — each partition is a fully independent replicated state machine. The `RaftNode` class is beautifully testable: "Designed for deterministic testing: no threads, no timers — the caller drives ticks and message delivery." Could be extended with batched transport without changing `RaftNode` itself.

---

## 2. SWIM Gossip vs Centralized Membership

**What was chosen.** `GossipProtocol` implements SWIM-lite with direct ping, indirect probe via intermediaries, suspect/dead state transitions, incarnation-based conflict resolution, and push-pull gossip rounds for state dissemination.

**What's sacrificed.**
- **Eventual consistency:** After a node failure, different brokers may disagree on membership for `suspectTimeout` + propagation delay. Producers might send to a broker others consider dead.
- **Split-brain during partitions:** If a network partition isolates two groups, each declares the other DEAD and operates independently. No fencing mechanism prevents both groups from serving the same partition.
- **Partition assignment via gossip:** `updatePartitions()` propagates ownership through gossip, meaning two brokers could temporarily believe they own the same partition leadership.

**When the alternative wins.**
- **Raft for membership:** Strongly consistent membership via consensus. Kafka KRaft does this. Cost: latency (changes require Raft consensus) and complexity (joint consensus for membership changes).
- **ZooKeeper:** Linearizable membership with watches/notifications. Cost: external dependency and single point of failure.

**Engineering judgment.** Gossip is right when availability trumps strict membership consistency. SWIM provides O(log N) convergence and constant per-node network overhead. The key insight: TurboMQ separates concerns — gossip handles *failure detection* (fast, available), Raft handles *data replication* (strong, consistent). Raft's term-based fencing prevents two leaders from making conflicting commits even if gossip temporarily disagrees.

---

## 3. RocksDB vs Custom Log-structured Storage

**What was chosen.** `RocksDBEngine` wraps RocksDB with tuned options: 64MB memtable, 3 write buffers, dynamic level compaction. `SnapshotManager` uses RocksDB's native `Checkpoint` API for efficient point-in-time snapshots via hard-linked SST files.

**What's sacrificed.**
- **Write amplification:** RocksDB's LSM compaction has 10-30x write amplification. For append-mostly message queue data, a custom store could achieve ~1x.
- **Tail latency:** Compaction causes latency spikes. Unpredictable at high percentiles.
- **JNI overhead:** Every RocksDB call crosses the JNI boundary. At millions of operations/sec, the per-call cost adds up.
- **No access pattern exploitation:** Message queues have append-at-tail, read-from-head, delete-old-segments patterns. RocksDB's general-purpose SST layout doesn't exploit this.

**When the alternative wins.**
- **Custom log-segment storage (Kafka-style):** Append-only segment files with index. Near-zero write amplification, predictable sequential I/O, trivial segment deletion for retention. Cost: implementing compaction, recovery, and range queries from scratch.
- **Direct mmap-based storage:** Memory-mapped files avoid JNI entirely. Cost: managing mapped regions and page faults.

**Engineering judgment.** Pragmatic choice. RocksDB provides battle-tested durability, bloom filters, `WriteBatch` atomicity, and the `Checkpoint` API that `SnapshotManager` leverages for hard-linked snapshots. Engineering time better spent on the Raft implementation and gossip protocol.

---

## 4. Virtual-Thread-per-Partition

**What was chosen.** `GrpcServer` uses `Executors.newVirtualThreadPerTaskExecutor()` for request handling. The architecture is one virtual thread per gRPC request.

**What's sacrificed.**
- **GC pressure:** Each virtual thread allocates a continuation stack (~1KB initially). 10,000 concurrent requests = 10MB of continuation stacks the GC must trace.
- **Carrier thread pinning:** Virtual threads pin their carrier thread inside `synchronized` blocks. RocksDB JNI calls may hold native locks, reducing effective parallelism during I/O.
- **No backpressure:** `newVirtualThreadPerTaskExecutor()` creates unlimited virtual threads. A burst of 100K concurrent requests creates 100K virtual threads, potentially overwhelming RocksDB.

**When the alternative wins.**
- **Bounded thread pool with queue:** Natural backpressure. When saturated, new requests queue rather than creating more threads.
- **Event-loop (Netty-style):** For I/O-dominated workloads, event loops avoid thread-per-request overhead entirely. Kafka uses this approach.

**Engineering judgment.** Right modern Java choice. Virtual threads eliminate the thread-pool sizing problem and simplify the programming model (blocking I/O without callback hell). The pinning risk with RocksDB JNI is real but manageable — operations are microsecond-scale. Missing backpressure is the main production concern, fixable with a `Semaphore`-based admission control.

---

## 5. In-memory Raft Log vs Persistent Log

**What was chosen.** `RaftLog` stores entries in an `ArrayList<LogEntry>`. `RaftNode` stores `currentTerm` and `votedFor` as plain fields, not persisted to disk.

**What's sacrificed.**
- **Complete state loss on restart.** A restarting node loses its entire Raft log. Recovery depends on snapshot transfer from the leader via `InstallSnapshot`.
- **Safety violation risk.** After restart, a node could re-vote for a different candidate in the same term, violating Raft's safety guarantee. `currentTerm`/`votedFor` must be persisted before responding to RPCs.
- **Increased snapshot frequency.** Without persistent logs, snapshots must be frequent enough that restarting nodes can catch up from the leader's latest snapshot.

**When the alternative wins.**
- **Persistent Raft log (etcd, TiKV):** Every entry written to WAL before acknowledgment. On restart, replay the log. Required for correctness in production.
- **Hybrid:** Persist only metadata (`currentTerm`, `votedFor`) and recent entries. Use snapshots for bulk state.

**Engineering judgment.** Deliberate scope decision. The code acknowledges: "would be persisted to disk in production" (`RaftNode.java:28`). `SnapshotManager` with RocksDB `Checkpoint` provides the foundation for snapshot-based recovery. Adding log persistence is mechanical — write entries to RocksDB before appending to the `ArrayList` — without architectural impact.

---

## 6. gRPC vs Custom Binary Protocol

**What was chosen.** `GrpcServer` uses gRPC with Protobuf for the client-facing Producer and Consumer APIs. Virtual threads handle request processing.

**What's sacrificed.**
- **Protobuf serialization cost.** Every message goes through encoding/decoding. Kafka's custom protocol uses zero-copy `ByteBuffer` access.
- **HTTP/2 framing overhead.** HPACK header compression, stream multiplexing, and flow control add bytes-on-wire and CPU for long-lived connections where messages are small.
- **No batched pipelining.** gRPC's request-response model doesn't naturally batch multiple produce requests into a single network round-trip. Kafka allows multi-partition-produce in one API call.

**When the alternative wins.**
- **Custom binary protocol (Kafka-style):** Purpose-built for message queues. Batched requests, zero-copy to disk, minimal framing. Cost: implementing the protocol from scratch including versioning, error handling, and client libraries.

**Engineering judgment.** Right for a portfolio project. gRPC provides schema-first API design, automatic client generation in multiple languages, built-in deadlines/cancellation, and a well-understood operational model. Performance overhead is real but acceptable for demonstrating distributed systems concepts.

---

## Summary

| Dimension | Choice | Key Trade-off | Risk Level |
|-----------|--------|---------------|------------|
| Consensus | Per-partition Raft | Memory per group vs fault isolation | Medium |
| Membership | SWIM gossip | Eventual consistency vs availability | Medium |
| Storage | RocksDB | Write amplification vs engineering velocity | Low |
| Threading | Virtual threads | Unlimited creation vs simplified model | Low |
| Raft log | In-memory ArrayList | State loss vs simplicity | High |
| API | gRPC/Protobuf | Serialization overhead vs schema-first design | Low |
