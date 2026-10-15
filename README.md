# raft

A fork of [etcd raft](https://github.com/etcd-io/raft) at upstream's
`main` (commit `638f427`, which holds every change of v3.7.0), with
safety, linearizable-read and liveness fixes and validation of messages
from peers.

- **Module:** `github.com/raftseg/raft`, Go 1.26 or later.
- **One dependency:** google.golang.org/protobuf, Google's protobuf
  runtime, which upstream's `raftpb` is generated for. `go.mod` requires
  nothing else, and every package, tests included, imports only the Go
  standard library, this module and protobuf. `internal/depcheck` fails
  the tests otherwise, and `TestGeneratedCode` checks that `raft.pb.go` is
  protoc-gen-go's output.
- **Wire-compatible with etcd raft:** `raftpb/testdata` holds 3,609
  messages encoded by v3.6.0, which decode and re-encode byte for byte,
  and 21,654 damaged or hostile inputs, and the tests check both.
- **Fixes:** [FORK.md](FORK.md) explains each change from upstream, and
  each has a regression test that fails on upstream's code.

Upstream's README is [UPSTREAM-README.md](UPSTREAM-README.md). The license
is upstream's, Apache 2.0 ([LICENSE](LICENSE)); [NOTICE](NOTICE) lists what
this fork changed.

## Relation to upstream

The branch `upstream-main` mirrors upstream's `main`. This fork's history
starts at upstream's tag v3.6.0, and upstream's `main` is merged into it,
so both histories are kept whole. Upstream changes are taken by merging
`upstream-main`, or for a single fix by `git cherry-pick -x`, so that each
commit names the upstream commit it came from. [upstream.json](upstream.json),
which SBOM tools read for this fork's pedigree, records the upstream commit
this fork is based on and any cherry-picks since.

## Testing

```
go test ./...
go test -short -race ./...
go test -run XXX -fuzz FuzzStepHostile .           # hostile peer messages
go test -run XXX -fuzz FuzzUnmarshal ./raftpb      # and FuzzUnmarshalInto, FuzzConfChangesFromString
GOARCH=386 go test -short ./...                    # 32-bit
```

FORK.md names the regression test for each change. Cluster-level testing
(a deterministic simulator with raft's safety and liveness checks, soaks,
and mutation testing of this fork) lives in
[raftseg](https://github.com/raftseg/raftseg), which builds on this module.
