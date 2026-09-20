# Dev Notes

challenges, design considerations

## day 1

- did some reading and prompting around `protoc` vs `buf`; decided to pivot to `buf` to get some familiarity with it
- had some minor frustration dealing with `buf lint`; could have lowered the lint level, but opted to conform to `STANDARD`
- messages were named precisely as they are in the paper, but have now changed to conform to gRPC standards, per `buf`
- followed the example of `etcd` in how `raftpb` import is named and handled, over `raftv1` - unlikely to have a v2 in this case
- realized I hadn't fully separated `node.go` from the transport layer — only ingestion was decoupled
- decided full transport decoupling was preferable, with separate inbound and outbound transport adapters:
  - `transport.go` — transport interface
  - `rpc_server.go` — inbound gRPC adapter
  - `grpc_transport.go` — outbound gRPC adapter
- creates a nice injection point for manufactured failures - wrap the `transport` interface


## day 2

- renamed `rpc_server.go` to `grpc_server.go` - matching transport.

## day 3

- weighed benefits of upgrading to `1.27.x` for golang, opted for `1.25.x` floor instead.
- our election snapshot is starting to resemble etcd/raft's `Ready` boundary:
  consensus transitions produce durable state plus external effects, which are
  executed outside the core transition. Cockroach and TiKV extend this pattern
  to Multi-Raft by multiplexing many independent Raft groups over shared
  persistence and transport. Keeping the abstraction local for now rather than
  introducing a general `Ready` type prematurely.
- considered folding candidate vote bookkeeping into general peer tracking, as upstream etcd/raft does. Current Cockroach Raft instead separates election tracking from replication progress. Keeping stranded simpler and role-explicit: candidate votes live in CandidateState, while the immutable election value remains context for asynchronous RPCs.
- checked timer/message ownership against etcd/raft, Cockroach, and HashiCorp Raft. All serialize incoming consensus events with timeout/tick processing at some layer; Cockroach explicitly processes queued Raft messages before ticks to avoid spurious elections. Rather than letting RPC goroutines mutate Raft state and separately signal timer resets, route inbound Raft events through Run; keep network I/O concurrent, but serialize consensus decisions and time.

## day 4

- made stable-storage ordering explicit as prepare → persist → publish:
  consensus state changes that affect externally visible behavior are not
  published in memory until the corresponding persistent state has been saved.
  A local persistence failure is treated as fatal to the running node rather
  than as an ordinary peer/network failure.
- `fileStorage` remains mechanism-only: `Load` reports a missing state file as
  an error; `New` owns the lifecycle policy that missing state means a fresh
  node. Corrupt or otherwise unreadable persisted state prevents startup.
- began routing inbound Raft work through `Run` rather than letting transport
  goroutines mutate consensus state directly. Network I/O may remain concurrent,
  but term/role/vote decisions and time-related events should be serialized
  through one consensus-processing path.
- keeping the event seam typed for now (`requestVoteCall` /
  `requestVoteResult`) rather than introducing a generic event type before more
  event kinds justify it.
- keeping physical `time.Timer` election timing for now. etcd/raft's logical
  tick model is attractive because it turns time into another deterministic
  state-machine input, but first establishing `Run` as the serialization point;
  revisit logical ticks once RPCs and vote replies flow through that path.

### Apply-path prior art

State-machine application should remain outside the core Raft state transition and outside `n.mu`.

etcd/raft exposes committed entries to the embedding application through its Ready/Advance boundary; its asynchronous mode makes log persistence and state-machine application separate local execution paths. HashiCorp Raft similarly feeds committed logs to a dedicated `runFSM` path so application work does not block internal Raft processing. CockroachDB follows the same broad separation when integrating etcd/raft.

For `stranded`, follow the HashiCorp-style boundary at the smallest useful scale:

* `processAppendEntries` owns replication and commitment only.
* After `processAppendEntries` returns and releases `n.mu`, `Run` hands entries in `(LastApplied, CommitIndex]` to `applyCh` in log-index order.
* `LastApplied` advances only after successful application handoff, never merely when `CommitIndex` advances.
* For now, application handoff may synchronously backpressure `Run`. Do not introduce a larger Ready/Advance or dedicated-applier abstraction until tests demonstrate that this matters for liveness or ordering.
* If that pressure appears, the natural next step is a dedicated ordered applier/ack path rather than allowing state-machine work to run under the Raft mutex.

### Leader transition / outbound work: 

Follow the etcd/HashiCorp separation rather than their full machinery. Consensus transitions, including processing vote replies and becoming leader, should be serialized through Run. Becoming leader initializes leader replication state but performs no network I/O while holding Raft state. After the transition, Run schedules outbound AppendEntries work. Start with the paper’s initial empty heartbeat; when general leader-side log replication exists, add the current-term no-op entry used by mature implementations to establish commitment in the new term.

## day 5

Final stretch for RAFT MVP.

Today's goal: v0 core complete: a 3-node cluster can elect a leader, accept a client command, durably replicate and commit it to a majority, apply it in order, survive leader loss, elect a replacement, and continue.

### Timing: logical ticks with the heartbeat slice

After pending-client cleanup on `Run` exit, introduce logical ticks alongside
periodic heartbeats. Keep timing decisions serialized in `Run`: one production
ticker supplies ticks; Raft tracks election and heartbeat elapsed counts, with
randomized election deadlines. Tests can supply ticks explicitly.

Leaders send periodic AppendEntries without starting elections on their own
timeout. Role transitions and qualifying RPCs reset the relevant counters.
Keep this small—no generalized scheduler or `Ready` abstraction. Logical ticks
do not remove synchronous application backpressure.

First invariant: an idle leader sends periodic AppendEntries and remains leader
past an election timeout. Explicit ticks will also support the later deterministic
multi-node failure harness.

### v0 core — COMPLETE

The day-5 goal is met: a three-node cluster elects a leader, accepts client
commands, persists and replicates them to a majority, applies committed entries
in order, survives leader loss, elects a replacement, and continues.

Integration coverage now includes:

- Reliable replication and ordered application across three nodes.
- Continued writes after leader loss.
- No client success, commitment, or application without quorum.
- Partition healing: the former leader steps down, fails its pending client,
  and replaces its uncommitted suffix without applying the abandoned command.
- File-backed restart through New: persisted term, vote, and log are restored;
  the restarted node catches up and replays committed history to a fresh
  application.

Validation passed:

- `go test ./...`
- `go test -race ./node -run '^TestCluster'`

Scope: consensus-core integration with controlled logical ticks, in-memory RPC
routing, and file-backed storage. Client success means application-channel
handoff. Restart coverage uses orderly node shutdown and reconstruction, not
power-loss fault injection; replay assumes a fresh application.

CLI/process wiring, production hardening, and exactly-once external effects
remain outside this milestone. Today's planned v0 work is complete.
