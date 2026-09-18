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
