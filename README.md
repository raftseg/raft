# raft

A fork of [etcd raft](https://github.com/etcd-io/raft) at v3.6.0, kept free
of dependencies.

- **Module:** `github.com/raftseg/raft`.
- **No dependencies:** `go.mod` requires nothing, and every package, tests
  included, imports only the Go standard library and this module.
  `internal/depcheck` fails the tests otherwise.
- **Wire-compatible with etcd raft:** messages encode byte for byte as the
  original's. `raftpb/testdata` holds 3,609 messages encoded by the
  original and 21,654 damaged or hostile inputs, and the tests check both.
- **Fixes:** safety, linearizable-read and liveness fixes, and validation
  of messages from peers. [FORK.md](FORK.md) explains each change from
  upstream, and each has a regression test that fails on upstream's code.
- **64-bit platforms only:** `raftpb` refuses to build elsewhere (see
  `raftpb/platform.go`).

Upstream's README is [UPSTREAM-README.md](UPSTREAM-README.md). The license
is upstream's, Apache 2.0 ([LICENSE](LICENSE)); [NOTICE](NOTICE) lists what
this fork changed.

## Relation to upstream

This fork's history starts at upstream's tag v3.6.0 (commit `f35a022`, on
upstream's `release-3.6` branch), with upstream's full history below it.
Upstream fixes are taken with `git cherry-pick -x`, so each commit names
the upstream commit it came from. Upstream's switch to
`google.golang.org/protobuf` in v3.7.0, which turned message fields into
pointers, is not taken: this fork keeps the v3.6.0 API.

## Testing

```
go test ./...
go test -short -race ./...
```
