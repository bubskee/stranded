# stranded

> What do you need when you're stranded? A raft.

An educational implementation of the [Raft consensus algorithm](https://raft.github.io/) in Go, built to explore the algorithm from the protocol messages up through leader election, durable log replication, commitment, application, and failure recovery.

**Status:** v0 core complete. The consensus engine is exercised through deterministic multi-node integration tests; the standalone process CLI is not wired yet.

*Might continue. Had fun!*

## Usage

Requires Go 1.25.14.

```sh
git clone https://github.com/bubskee/stranded.git
cd stranded
go test ./...
```

Run the cluster integration tests under the race detector:

```sh
go test -race ./node -run '^TestCluster'
```

See [DEV.md](DEV.md) for the implementation diary, design decisions, and exact v0 scope.

## Potential future work

* Wire the CLI and run real nodes as separate gRPC processes.
* Add unreliable transport, richer partition scenarios, and power-loss fault injection.
* Add client request identity / exactly-once semantics.
* Build a small visualization or replay UI for elections, replication, and failures.

## AI disclosure

Development of `stranded` included conversations with Claude and ChatGPT to discuss design tradeoffs (e.g. transport choice, idempotency strategy) and work through Raft's protocol details. All code was written and is understood by the author — these tools were used the way one might use a study partner, not as a code generator.

---

> We said there warn’t no home like a raft, after all. 
Other places do seem so cramped up and smothery, but a raft don’t. 
You feel mighty free and easy and comfortable on a raft.  

&nbsp;&nbsp;&nbsp;&nbsp; —_The Adventures of Huckleberry Finn_ by Mark Twain
