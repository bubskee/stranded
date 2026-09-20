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
