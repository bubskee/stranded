# stranded

[![CI](https://github.com/bubskee/stranded/actions/workflows/ci.yml/badge.svg)](https://github.com/bubskee/stranded/actions/workflows/ci.yml)

> What do you need when you're stranded? A raft.

An educational implementation of the [Raft consensus algorithm](https://raft.github.io/) in Go, built to explore the algorithm from the protocol messages up through leader election, durable log replication, commitment, application, failure recovery, and restart.

**Status:** v0.1. The consensus core is complete, static clusters can run as separate gRPC processes, and deterministic integration tests cover elections, replication, quorum loss, partition healing, leader replacement, and file-backed restart.

## Usage

Requires Go 1.25.14.

Clone and test:

```sh
git clone https://github.com/bubskee/stranded.git
cd stranded
go test ./...
```

Run the cluster scenarios under the race detector:

```sh
go test -race ./node -run '^TestCluster'
```

CI runs both checks on pushes and pull requests. The regular test suite also includes an in-memory gRPC smoke test crossing the generated client, gRPC server, `Node.Run`, and Raft request handling.

### Run a local cluster

Start three nodes in separate terminals:

```sh
go run ./cmd/node \
  --id node-a \
  --listen 127.0.0.1:5001 \
  --peers node-b=127.0.0.1:5002,node-c=127.0.0.1:5003 \
  --data-dir .data/node-a
```

```sh
go run ./cmd/node \
  --id node-b \
  --listen 127.0.0.1:5002 \
  --peers node-a=127.0.0.1:5001,node-c=127.0.0.1:5003 \
  --data-dir .data/node-b
```

```sh
go run ./cmd/node \
  --id node-c \
  --listen 127.0.0.1:5003 \
  --peers node-a=127.0.0.1:5001,node-b=127.0.0.1:5002 \
  --data-dir .data/node-c
```

The node process exposes Raft and command submission over gRPC. There is no interactive client CLI.

See [DEV.md](DEV.md) for the implementation diary, design decisions, prior-art notes, and the original v0 milestone.

## Scope and assumptions

`stranded` is an educational implementation, not a production Raft library. In particular:

* Cluster membership is static and supplied at startup.
* Consensus transitions are serialized through `Node.Run`; durable state changes follow a prepare → persist → publish boundary.
* Leaders append a current-term no-op on election; prior-term entries are not committed from replica count alone.
* Application handoff is synchronous. Client success means the committed entry reached the application channel, not that an arbitrary external side effect is durable.
* Restart restores persisted term, vote, and log state; replay assumes a fresh application. The application state itself is not persisted by Raft.
* Client request deduplication / exactly-once semantics, snapshots, membership changes, and power-loss fault injection are outside v0.1.

The command-line process also uses plaintext gRPC; authentication, TLS, operational hardening, and production observability are deliberately out of scope.

## Potential future work

* Richer unreliable-transport, partition, and crash/power-loss scenarios.
* Client request identity and deduplication.
* Snapshots, log compaction, and dynamic membership.
* A small visualization or replay UI for elections, replication, and failures.

*Might continue. Had fun!*

## AI disclosure

Development of `stranded` included conversations with Claude and ChatGPT to discuss design tradeoffs, compare prior art, red-team tests, and work through Raft's protocol details. All code was written and is understood by the author — these tools were used the way one might use a study partner, not as a code generator.

---

> We said there warn’t no home like a raft, after all. 
Other places do seem so cramped up and smothery, but a raft don’t. 
You feel mighty free and easy and comfortable on a raft.  

&nbsp;&nbsp;&nbsp;&nbsp; —_The Adventures of Huckleberry Finn_ by Mark Twain
