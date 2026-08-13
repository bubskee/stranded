# Dev Notes

## day 1 - 8/12/2026

- did some reading and prompting around `protoc` vs `buf`; decided to pivot to `buf` to get some familiarity with it
- had some minor frustration dealing with `buf lint`; could have lowered the lint level, but opted to conform to `STANDARD`
- realized I hadn't fully separated `node.go` from the transport layer — only ingestion was decoupled
- decided full transport decoupling was preferable, with separate inbound and outbound transport adapters:
  - `transport.go` — transport interface
  - `rpc_server.go` — inbound gRPC adapter
  - `grpc_transport.go` — outbound gRPC adapter
- creates a nice injection point for manufactured failures - wrap the `transport` interface
