# stranded

 > What do you need when you're stranded? A raft.

An educational implementation of the [Raft consensus algorithm](https://raft.github.io/) in Go, built to explore the algorithm from the protocol messages up through leader election, log replication, and failure recovery.

## Usage

<!-- TODO: usage guide -->

## Development Plan

```
proto 
→ node skeleton (config/state/rpc plumbing, persist.go stubbed but called in the right places) 
→ election (real logic + real persist) 
→ replication (real logic + real persist) 
→ client 
→ failure/recovery + disasters 
→ visualization
```

<!-- TODO: link to DEV.md -->

## AI disclosure

Development of `stranded` included conversations with Claude and ChatGPT to
discuss design tradeoffs (e.g. transport choice, idempotency strategy) and
work through Raft's protocol details. All code was written and is understood
by the author — these tools were used the way one might use a study partner,
not as a code generator.

---

> We said there warn’t no home like a raft, after all. Other places do seem so cramped up and smothery, but a raft don’t. You feel mighty free and easy and comfortable on a raft.
	—_The Adventures of Huckleberry Finn_, by Mark Twain