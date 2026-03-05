# How this raft differs from etcd raft v3.6.0

This package is a fork of [etcd raft](https://github.com/etcd-io/raft) at
tag v3.6.0 (Apache License 2.0, see LICENSE). This file lists every change
in behaviour and why it was made. Every modified file says so in its
header, and each change is marked "Fork" in a comment where it is made.

Most of these changes fix defects found in the upstream code by the
deterministic cluster simulator of the fabric project (`internal/raftsim`).
That simulator runs randomized clusters under message loss, duplication and
reordering; partitions; crashes that lose unsynced writes; configuration
changes; snapshots; leadership transfers; and reads. It checks raft's
safety and liveness properties as it goes. Each fix has a regression
test (in `fork_test.go`, `validate_test.go` or `raftpb/hostile_test.go`),
and each of those tests fails on the upstream code.

Raft tolerates crashes, not lies. A peer that lies about its log can break
safety, and nothing here changes that. What the fork adds is that a faulty,
outdated or misconfigured peer cannot crash a node with a malformed
message.

## No dependencies

`raftpb` no longer imports `github.com/gogo/protobuf` or
`github.com/golang/protobuf`:
- The generated `Marshal`, `Unmarshal` and `Size` code is unchanged.
  Descriptor registration and the `XXX_` methods were removed.
- `String()` and enum JSON decoding are reimplemented in `compat.go`.
- The tests use `internal/testutil` and `internal/datadriven` in place of
  testify and cockroachdb/datadriven.

Compatibility is checked against the original package:
- `raftpb/testdata/golden.jsonl` holds 3,609 messages encoded by the
  original. Each must decode and re-encode byte for byte, with the same
  `Size`, `String` and JSON.
- `raftpb/testdata/malformed.jsonl.gz` holds 21,654 damaged and hostile
  inputs. The fork must reject each one with the original's error, or decode
  it into the same message.
- The generators are kept as `.go.txt` files next to the data.

## Safety

**Configuration changes known to be committed (`inferConfChangeCommit`).**
etcd raft applies a configuration when its entry is applied, not when it is
appended. Upstream's argument for why that is safe ("the old and new
configurations are guaranteed to overlap") holds only between consecutive
configurations. A node that held two configuration changes in its log, but
had not learned that the first was committed, still counted votes with the
configuration from before both. Configurations two steps apart need not
share a majority: going from (1 2 3 4) through a joint configuration to (1)
is enough. Such a node could be elected by a majority of the old
configuration while n1 led the new one. Two leaders in the same term then
committed different entries at the same index.

A leader appends a configuration change only after it has applied every
earlier one in its log. So when a later change follows a change in a node's
log, the earlier one is committed. The node now advances its commit index
to it, and it must apply that change before it may campaign. Its
configuration is then at most one step behind the latest.

The inference relies on raft refusing a configuration change while another
is pending, so it is skipped when `DisableConfChangeValidation` is set.
Test: `TestStaleConfigurationCannotElect`.

**Snapshots wait for entries being applied.** With `AsyncStorageWrites`, or
with `Node` when a snapshot arrives between `Ready` and `Advance`, a
follower restored a snapshot while committed entries were still with the
application. The application then called `ApplyConfChange` for older
configuration changes on top of the snapshot's configuration, which
corrupted it silently. A follower now refuses a snapshot until those
entries are applied, and the leader retries. Test:
`TestSnapshotWaitsForApplying`.

**`Ready.MustSync` covers snapshots.** Upstream left `MustSync` false for a
`Ready` that held only a snapshot and a new commit index. An application
that honoured it could acknowledge a snapshot, lose it in a crash, and
restart with an empty log. The leader's next heartbeat then panicked the
node. Test: `TestMustSyncSnapshot`.

## Linearizable reads

**A single voter answers reads only when it is that voter.** A leader that
had removed itself, leaving one voter, still answered reads from its own
state, because its configuration had a single voter. Test:
`TestReadIndexAfterLeaderRemovedItself`.

**A single voter waits for a commit in its own term.** Upstream answered a
single voter's reads at once, with a commit index that can lag behind
entries already acknowledged:
- after a restart, because the commit index is persisted lazily;
- after leadership has just been handed to it.

Such a leader now waits, as multi-voter leaders always did, until it has
committed an entry in its own term. Test: `TestSingleVoterReadAfterRestart`.

**Lease-based reads are backed by acknowledged heartbeat rounds
(`leaseValid`).** Upstream trusted CheckQuorum. CheckQuorum tests a
quorum's activity only once per election timeout, and it counts responses
to heartbeats sent at any time. As a result, a leader cut off from its
followers served lease reads for up to two election timeouts, while the
followers could elect a new leader and commit after one.

The lease now works as follows:
- Heartbeats carry a round number, and followers echo it.
- A leader serves a lease read only while a quorum of voters has
  acknowledged a round recent enough that each of them still refuses to
  vote for anyone else.
- Without such a quorum, the read is confirmed through a quorum, as with
  `ReadOnlySafe`.
- A leader gives up its lease once it has sent `MsgTimeoutNow`, because the
  recipient's election ignores leases.
- A node that restarts with a persisted term ignores vote requests for an
  election timeout (`startGuard`), to keep a promise it may have made before
  the restart.

Tick rates must not drift apart by more than two ticks per election
timeout. Tests: `TestLeaseNeedsRecentRound`, `TestLeaseStartGuard`,
`TestNoLeaseReadAfterTimeoutNow`.

**Retried read requests (upstream fix, with a check added).** Upstream
fixed this after v3.6.0 (etcd-io/raft commits 9b9d6ee and 63903dd), and
this fork takes the fix by cherry-pick. A leader matched heartbeat
responses to read requests by the request's own context. A client that
retried a read with the same context, while heartbeat responses were
delayed, could get its read confirmed by acknowledgements of an earlier
request: a leader already cut off from the cluster then answered with a
stale read index. Reads are now numbered by the leader, heartbeats carry
the latest number, and a read is confirmed once a quorum has acknowledged a
number at least as high.

As upstream wrote it, a heartbeat response whose context was not 8 bytes
long panicked the leader (`TestHostilePanicClasses` caught it). The fork
ignores such contexts, and positions beyond the reads registered so far,
which only a faulty peer sends. Tests: `TestReadOnlyDuplicateRequest`
(upstream's), `TestHeartbeatAckContextChecked`.

## Liveness

**A leader that leaves the voters hands over (`handOff`).** This was
upstream's own TODO. Upstream, a leader that removed itself either:
- stayed leader for good, dropping every proposal; or
- with `StepDownOnRemoval`, stepped down at once. It could then be the only
  node holding entries the voters needed, and it could neither campaign nor
  give them its vote. The group then had no leader permanently.

Now such a leader does the following:
- It keeps replicating and drops new proposals.
- It sends `MsgTimeoutNow` to the voter holding the most of its log, once
  that voter holds all of it, and retries every election timeout.
- It is exempt from CheckQuorum, and it confirms reads through a quorum.
- With `StepDownOnRemoval`, it steps down once it has sent `MsgTimeoutNow`.

Tests: `TestDemotedLeaderHandsOff`, and the updated
`testdata/confchange_*_leader*.txt` scenarios.

**A transfer waits for pending configuration changes.** The
`MsgTimeoutNow` of a hand-off often arrives together with the commit of the
configuration change that prompted it, before the recipient has applied
that change. Upstream, the recipient dropped such a message, so the group
waited out a full election timeout. Now it campaigns as soon as the change
is applied. `TestDemotedLeaderHandsOff` requires the hand-off to complete
within a heartbeat.

**A node that is not a voter can campaign to hand its log over
(`mayRescue`).** A leader that removed or demoted itself can be deposed
before it hands over. It may hold entries no voter has, while voters that
have not applied its removal still count it, need its vote and cannot get
it. Such a node now campaigns through a pre-vote. The pre-vote wins only if
no voter hears from a leader and the node's log is at least as up to date
as theirs. Once elected, the node replicates its log and hands leadership
over. Votes are counted in its configuration, as for any candidate. Test:
`TestRemovedLeaderRescues`.

**Nodes answer a stale leader.** Without CheckQuorum or PreVote, upstream
relied on `MsgVote` to carry a higher term to a stale leader. But a node
sends `MsgVote` only to the voters of its own configuration, and a learner
sends none. A leader whose configuration needed such a node waited for it
indefinitely. A node now answers appends from a lower term with its own term,
as the Raft paper prescribes. Test: `TestAnswersStaleLeader`.

**`MaxSizePerMsg` 0 no longer breaks application.** With
`MaxCommittedSizePerReady` left at its default (`MaxSizePerMsg`), a limit
of 0 caused one of two failures:
- the first `Ready` panicked; or
- application paused for good, because 0 >= 0.

A limit of 0 now applies one entry at a time. Test:
`TestApplyWithZeroSizeLimits`.

## Hostile and malformed input

**`ValidateMessage`.** `RawNode.Step` and `Node.Step` now check every
message from a peer. A message from a peer is one with a sender other than
the node itself or its storage threads. The check rejects:
- local message types (including `MsgForgetLeader`);
- reserved or missing node IDs;
- local response lists;
- entries out of order, or from a term above the message's term;
- proposals whose configuration change does not decode. Upstream, such a
  proposal panicked the leader.
- snapshots whose configuration raft would panic on;
- messages addressed to another node.

A transport must set `From` to the authenticated sender, with one
exception. `MsgProp`, `MsgReadIndex` and `MsgTransferLeader` are forwarded
unchanged by followers, so their `From` is the node that made the request.
Tests: `TestValidateMessage`, `TestStepValidates`,
`TestUndecodableConfChangeDoesNotPanicLeader`, `FuzzStepHostile`.

**Bounded message nesting.** Decoding recursed once per level of
`Message.Responses`. About 40 MB of crafted input (8 million levels)
exhausted the goroutine stack, which Go cannot recover from. That killed any
process that decoded the input. `Message.Unmarshal` now rejects nesting
deeper than `MaxMessageNesting` (16) with `ErrNestingTooDeep`. Tests:
`TestNestingLimit`, `TestNestingCannotExhaustStack`.

**Decoding cost.** Small encodings expand in memory. For example, an empty
`Message` in `Responses` is 2 bytes on the wire and a full struct in
memory. `TestDecodeAmplification` measures the worst shapes so that a
transport can set its message size limit: at most about 490 bytes allocated
per input byte, counting outgrown slices.

## Platforms

`raftpb` refuses to build where `int` has fewer than 64 bits. The generated decoder, unchanged from upstream, accumulates field lengths in an `int`. With 32-bit ints, a crafted length such as 2^33 wraps to 0, so a 32-bit build accepts malformed messages that 64-bit builds reject; for example, `0800100018004a808080808020` decodes as a `ConfChange` with an empty context. The golden corpora record the 64-bit behaviour of the original. Nodes of one cluster must agree on which messages are valid, so this fork supports 64-bit platforms only. (Upstream etcd raft has the same behaviour on 32-bit platforms.)

## Additions to the API

- `Config.ElectionRand`: the source of election-timeout jitter, for
  deterministic simulation. When nil, the default is unchanged.
- `ValidateMessage`, `ErrInvalidMessage`: see above.
- `ValidateConfChange(cs, cc)`: whether `ApplyConfChange` can apply `cc` to
  `cs`. An application can treat a committed change that fails this check
  as a no-op on every node.
- `raftpb.MaxMessageNesting`, `raftpb.ErrNestingTooDeep`.

## Contracts for applications

These held upstream too, but upstream did not spell them out. The
simulator relies on them.

- **Storage.** On restart, `InitialState` must report a commit index of at
  least the snapshot index, even if a crash kept a snapshot and lost the
  hard state saved with it.
- **Membership.** Shut down a removed node only after every voter of the new
  configuration has applied its removal. Until then, they may need its vote.
- **Transport.** Authenticate `From` as described above. Bound message
  sizes, with the amplification factors above in mind.
- **`DisableConfChangeValidation`.** Configuration changes must then be
  serialized above raft. Wait for each change to be applied by every member
  before proposing the next one; the commit inference above is off in this
  mode.
