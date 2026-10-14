// Copyright 2026 The raftseg authors. Licensed under the Apache License,
// Version 2.0.

package raft

import (
	"fmt"
	"testing"

	pb "github.com/raftseg/raft/raftpb"
)

// Tests for behaviour this fork adds to etcd raft.

type fixedRand []int

func (f *fixedRand) Intn(n int) int {
	v := (*f)[0]
	*f = (*f)[1:]
	return v
}

func TestElectionRand(t *testing.T) {
	for _, tc := range []struct {
		v, want int
	}{
		{0, 10}, {3, 13}, {9, 19}, {10, 10}, {23, 13}, {-1, 19}, {-11, 19},
	} {
		src := &fixedRand{tc.v, tc.v, tc.v, tc.v}
		cfg := newTestConfig(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)))
		cfg.ElectionRand = src
		r := newRaft(cfg)
		if r.randomizedElectionTimeout != tc.want {
			t.Errorf("Intn returned %d: timeout %d, want %d", tc.v, r.randomizedElectionTimeout, tc.want)
		}
	}
}

// Without ElectionRand, timeouts still fall in [electionTimeout,
// 2*electionTimeout) and vary.
func TestElectionRandDefault(t *testing.T) {
	seen := map[int]bool{}
	for i := 0; i < 200; i++ {
		r := newRaft(newTestConfig(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3))))
		if r.randomizedElectionTimeout < 10 || r.randomizedElectionTimeout >= 20 {
			t.Fatalf("timeout %d", r.randomizedElectionTimeout)
		}
		seen[r.randomizedElectionTimeout] = true
	}
	if len(seen) < 5 {
		t.Fatalf("only %d distinct timeouts in 200 tries", len(seen))
	}
}

// With MaxSizePerMsg 0 ("at most one entry per message") and
// MaxCommittedSizePerReady left at its default (MaxSizePerMsg), upstream
// paused entry application for good after the first batch: the pause
// condition applyingEntsSize >= maxApplyingEntsSize holds at 0 >= 0. Found
// by the simulator in internal/raftsim.
func TestApplyWithZeroSizeLimits(t *testing.T) {
	for _, async := range []bool{false, true} {
		s := newTestMemoryStorage(withPeers(1))
		cfg := newTestConfig(1, 10, 1, s)
		cfg.MaxSizePerMsg = 0
		cfg.MaxCommittedSizePerReady = 0
		cfg.AsyncStorageWrites = async
		rn, err := NewRawNode(cfg)
		if err != nil {
			t.Fatal(err)
		}
		if err := rn.Campaign(); err != nil {
			t.Fatal(err)
		}
		var applied []pb.Entry
		handle := func() {
			for i := 0; i < 100 && rn.HasReady(); i++ {
				rd := rn.Ready()
				if !async {
					s.Append(rd.Entries)
					if !IsEmptyHardState(rd.HardState) {
						s.SetHardState(rd.HardState)
					}
					applied = append(applied, rd.CommittedEntries...)
					rn.Advance(rd)
					continue
				}
				for _, m := range rd.Messages {
					switch m.To {
					case LocalAppendThread:
						s.Append(m.Entries)
						if m.Term != 0 {
							s.SetHardState(pb.HardState{Term: m.Term, Vote: m.Vote, Commit: m.Commit})
						}
					case LocalApplyThread:
						applied = append(applied, m.Entries...)
					default:
						continue
					}
					for _, r := range m.Responses {
						if err := rn.Step(r); err != nil {
							t.Fatal(err)
						}
					}
				}
			}
		}
		handle()
		for i := 0; i < 5; i++ {
			if err := rn.Propose([]byte(fmt.Sprintf("x%d", i))); err != nil {
				t.Fatal(err)
			}
			handle()
		}
		var data []string
		for _, e := range applied {
			if len(e.Data) > 0 {
				data = append(data, string(e.Data))
			}
		}
		if len(data) != 5 {
			t.Errorf("async=%v: applied %v, want x0..x4", async, data)
		}
	}
}

// A leader that has removed itself, leaving one voter, must not answer
// linearizable reads from its own state: the remaining voter may have
// elected itself and committed more. Upstream answered at once, because
// the configuration had a single voter. Found by internal/raftsim.
func TestReadIndexAfterLeaderRemovedItself(t *testing.T) {
	for _, option := range []ReadOnlyOption{ReadOnlySafe, ReadOnlyLeaseBased} {
		cfg := newTestConfig(1, 10, 1, newTestMemoryStorage(withPeers(1, 2)))
		cfg.ReadOnlyOption = option
		cfg.CheckQuorum = option == ReadOnlyLeaseBased
		r := newRaft(cfg)
		r.becomeCandidate()
		r.becomeLeader()
		r.readMessages()
		// n2 acknowledges the leader's empty entry, which commits it.
		if err := r.Step(pb.Message{From: 2, To: 1, Term: r.Term, Type: pb.MsgAppResp, Index: r.raftLog.lastIndex()}); err != nil {
			t.Fatal(err)
		}
		if !r.committedEntryInCurrentTerm() {
			t.Fatal("leader's entry not committed")
		}
		r.applyConfChange(pb.ConfChange{Type: pb.ConfChangeRemoveNode, NodeID: 1}.AsV2())
		r.readMessages()
		ctx := []byte("ctx")
		if err := r.Step(pb.Message{From: 1, To: 1, Type: pb.MsgReadIndex, Entries: []pb.Entry{{Data: ctx}}}); err != nil {
			t.Fatal(err)
		}
		if len(r.readStates) != 0 {
			t.Fatalf("%v: removed leader answered a read without asking the remaining voter: %v", option, r.readStates)
		}
		var asked []byte
		for _, m := range r.readMessages() {
			if m.Type == pb.MsgHeartbeat && m.To == 2 && len(m.Context) > 0 {
				asked = m.Context
			}
		}
		if asked == nil {
			t.Fatalf("%v: no heartbeat carrying the read to n2", option)
		}
		// Once n2 confirms the leadership, the read is answered.
		if err := r.Step(pb.Message{From: 2, To: 1, Term: r.Term, Type: pb.MsgHeartbeatResp, Context: asked}); err != nil {
			t.Fatal(err)
		}
		if len(r.readStates) != 1 || r.readStates[0].Index != r.raftLog.committed {
			t.Fatalf("%v: read states %v after confirmation", option, r.readStates)
		}
	}
}

// A Ready that carries a snapshot must be synced even when the hard state
// changes only its commit index. Upstream set MustSync false, so a node
// could acknowledge a snapshot it then lost in a crash. Found by
// internal/raftsim.
func TestMustSyncSnapshot(t *testing.T) {
	s := newTestMemoryStorage(withPeers(1, 2))
	rn, err := NewRawNode(newTestConfig(2, 10, 1, s))
	if err != nil {
		t.Fatal(err)
	}
	// Learn term 1 from the leader first, so the snapshot changes only the
	// commit index.
	if err := rn.Step(pb.Message{From: 1, To: 2, Term: 1, Type: pb.MsgHeartbeat}); err != nil {
		t.Fatal(err)
	}
	rd := rn.Ready()
	s.SetHardState(rd.HardState)
	rn.Advance(rd)
	snap := pb.Snapshot{Metadata: pb.SnapshotMetadata{Index: 10, Term: 1, ConfState: pb.ConfState{Voters: []uint64{1, 2}}}}
	if err := rn.Step(pb.Message{From: 1, To: 2, Term: 1, Type: pb.MsgSnap, Snapshot: &snap}); err != nil {
		t.Fatal(err)
	}
	rd = rn.Ready()
	if IsEmptySnap(rd.Snapshot) {
		t.Fatal("no snapshot in Ready")
	}
	if rd.HardState.Term != 1 || rd.HardState.Vote != 0 {
		t.Fatalf("hard state %v: test needs a commit-only change", rd.HardState)
	}
	if !rd.MustSync {
		t.Fatal("Ready with a snapshot does not require a sync")
	}
}

// With AsyncStorageWrites, a follower must not restore a snapshot while
// committed entries are still with the apply thread: the application would
// apply their configuration changes on top of the snapshot's configuration.
// Found by internal/raftsim.
func TestSnapshotWaitsForApplying(t *testing.T) {
	s := newTestMemoryStorage(withPeers(1, 2))
	cfg := newTestConfig(2, 10, 1, s)
	cfg.AsyncStorageWrites = true
	rn, err := NewRawNode(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ents := []pb.Entry{{Term: 1, Index: 1}, {Term: 1, Index: 2}}
	if err := rn.Step(pb.Message{From: 1, To: 2, Term: 1, Type: pb.MsgApp, Entries: ents, Commit: 2}); err != nil {
		t.Fatal(err)
	}
	// Persist, then hand the committed entries to the apply thread and
	// leave them there.
	var apply []pb.Message
	for i := 0; i < 10 && rn.HasReady(); i++ {
		rd := rn.Ready()
		for _, m := range rd.Messages {
			switch m.To {
			case LocalAppendThread:
				s.Append(m.Entries)
				for _, r := range m.Responses {
					if r.To == 2 {
						if err := rn.Step(r); err != nil {
							t.Fatal(err)
						}
					}
				}
			case LocalApplyThread:
				apply = append(apply, m)
			}
		}
	}
	if len(apply) == 0 {
		t.Fatal("nothing handed to the apply thread")
	}
	snap := pb.Snapshot{Metadata: pb.SnapshotMetadata{Index: 10, Term: 1, ConfState: pb.ConfState{Voters: []uint64{1, 2}}}}
	msnap := pb.Message{From: 1, To: 2, Term: 1, Type: pb.MsgSnap, Snapshot: &snap}
	if err := rn.Step(msnap); err != nil {
		t.Fatal(err)
	}
	if rd := rn.Ready(); !IsEmptySnap(rd.Snapshot) {
		t.Fatal("snapshot restored while entries were being applied")
	}
	// Once applied, the snapshot is accepted.
	for _, m := range apply {
		for _, r := range m.Responses {
			if err := rn.Step(r); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := rn.Step(msnap); err != nil {
		t.Fatal(err)
	}
	if rd := rn.Ready(); IsEmptySnap(rd.Snapshot) {
		t.Fatal("snapshot refused after entries were applied")
	}
}

// A node must tell a leader at a lower term about its term, even without
// CheckQuorum or PreVote, or that leader can wait for it for ever: upstream
// relied on the node's MsgVote, which goes only to the voters of the node's
// own configuration (none, for a learner). Found by internal/raftsim.
func TestAnswersStaleLeader(t *testing.T) {
	for _, s := range []*MemoryStorage{
		newTestMemoryStorage(withPeers(1), withLearners(2)), // a learner
		newTestMemoryStorage(withPeers(1, 2)),               // a voter
	} {
		r := newTestRaft(2, 10, 1, s)
		r.becomeFollower(5, None)
		if err := r.Step(pb.Message{From: 1, To: 2, Term: 3, Type: pb.MsgApp}); err != nil {
			t.Fatal(err)
		}
		msgs := r.readMessages()
		if len(msgs) != 1 || msgs[0].Type != pb.MsgAppResp || msgs[0].Term != 5 || msgs[0].To != 1 {
			t.Fatalf("got %v, want a MsgAppResp at term 5", msgs)
		}
	}
}

// A leader that demotes itself while holding an entry the other voter lacks,
// and whose commit notice is lost, must not leave the group leaderless.
// Upstream, with StepDownOnRemoval, it stepped down at once: n2 still
// counted n1 as a voter and needed its vote, n1 refused it (its log was
// longer) and, as a learner, could not campaign. Found by internal/raftsim.
func TestDemotedLeaderHandsOff(t *testing.T) {
	nt := newNetworkWithConfig(func(c *Config) { c.StepDownOnRemoval = true }, nil, nil)
	n1, n2 := nt.peers[1].(*raft), nt.peers[2].(*raft)
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})
	if n1.state != StateLeader {
		t.Fatal("n1 not leader")
	}
	cc := pb.ConfChange{Type: pb.ConfChangeAddLearnerNode, NodeID: 1}
	data, err := cc.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	ccIndex := n1.raftLog.lastIndex() + 1
	// n2 receives the demotion, but no message telling it that it
	// committed, and not the entry after it.
	nt.msgHook = func(m pb.Message) bool {
		return !(m.From == 1 && m.To == 2 && (m.Commit >= ccIndex || m.Index >= ccIndex))
	}
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{Type: pb.EntryConfChange, Data: data}}})
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{Data: []byte("x")}}})
	if n1.raftLog.committed < ccIndex || n2.raftLog.committed >= ccIndex || n2.raftLog.lastIndex() != ccIndex {
		t.Fatalf("setup: n1 commit %d, n2 commit %d last %d", n1.raftLog.committed, n2.raftLog.committed, n2.raftLog.lastIndex())
	}
	// Each node applies what it knows to be committed, and tells raft, as
	// RawNode does (nextEnts bypasses raft's own bookkeeping).
	apply := func(r *raft) {
		ents := nextEntsApplied(r, nt.storage[r.id])
		for _, e := range ents {
			if e.Type == pb.EntryConfChange {
				var cc pb.ConfChange
				if err := cc.Unmarshal(e.Data); err != nil {
					t.Fatal(err)
				}
				r.applyConfChange(cc.AsV2())
			}
		}
		if len(ents) > 0 {
			r.appliedTo(ents[len(ents)-1].Index, 0)
		}
		nt.send(r.readMessages()...)
	}
	apply(n1)
	apply(n2)
	if !n1.isLearner {
		t.Fatal("n1 not demoted")
	}
	// The network heals; time passes.
	nt.msgHook = nil
	if testing.Verbose() {
		nt.t = t
	}
	ticks := 0
	for ; ticks < 200 && n2.state != StateLeader; ticks++ {
		for _, r := range []*raft{n1, n2} {
			r.tick()
			nt.send(r.readMessages()...)
			apply(r)
		}
	}
	if n2.state != StateLeader {
		t.Fatalf("no leader: n1 %s (log %d), n2 %s (log %d)", n1.state, n1.raftLog.lastIndex(), n2.state, n2.raftLog.lastIndex())
	}
	// The hand-off takes a heartbeat; an election (or a rescue campaign by
	// n1) would take an election timeout.
	if ticks > n1.heartbeatTimeout+2 {
		t.Fatalf("n2 took %d ticks to lead: not handed over", ticks)
	}
	if n1.state == StateLeader {
		t.Fatal("n1 still leader")
	}
}

// A single voter must not answer a read with a commit index that lags behind
// entries acknowledged in an earlier term. Here the node restarts with its
// commit index persisted lazily (MustSync was false for the commit-only
// change), so entry 3, committed and applied before the crash, is not
// covered by the commit index it restarts with. Upstream answered the read
// at index 2. Found by internal/raftsim.
func TestSingleVoterReadAfterRestart(t *testing.T) {
	s := newTestMemoryStorage(withPeers(1))
	if err := s.Append([]pb.Entry{{Term: 1, Index: 1}, {Term: 1, Index: 2}, {Term: 1, Index: 3, Data: []byte("x")}}); err != nil {
		t.Fatal(err)
	}
	if err := s.SetHardState(pb.HardState{Term: 1, Vote: 1, Commit: 2}); err != nil {
		t.Fatal(err)
	}
	rn := newTestRawNode(1, 10, 1, s)
	var rss []ReadState
	handleOne := func() {
		rd := rn.Ready()
		s.Append(rd.Entries)
		if !IsEmptyHardState(rd.HardState) {
			s.SetHardState(rd.HardState)
		}
		rss = append(rss, rd.ReadStates...)
		rn.Advance(rd)
	}
	if err := rn.Campaign(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10 && rn.BasicStatus().RaftState != StateLeader; i++ {
		handleOne()
	}
	if st := rn.BasicStatus(); st.RaftState != StateLeader || st.Commit != 2 {
		t.Fatalf("setup: %s with commit %d, want a leader with commit 2", st.RaftState, st.Commit)
	}
	// Ask before the new leader commits its first entry, then after.
	rn.ReadIndex([]byte("early"))
	for i := 0; i < 10 && rn.HasReady(); i++ {
		handleOne()
	}
	rn.ReadIndex([]byte("late"))
	for i := 0; i < 10 && rn.HasReady(); i++ {
		handleOne()
	}
	if len(rss) != 2 {
		t.Fatalf("read states %v, want two", rss)
	}
	for _, rs := range rss {
		if rs.Index < 3 {
			t.Errorf("read %q at index %d misses entry 3", rs.RequestCtx, rs.Index)
		}
	}
}

// Once a leader has sent MsgTimeoutNow, the recipient may win an election
// that ignores the leader's lease, so the leader must stop serving
// lease-based reads. Upstream kept serving them and could return stale
// data. Found by internal/raftsim.
func TestNoLeaseReadAfterTimeoutNow(t *testing.T) {
	nt := newNetworkWithConfig(func(c *Config) {
		c.CheckQuorum = true
		c.ReadOnlyOption = ReadOnlyLeaseBased
	}, nil, nil, nil)
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})
	n1 := nt.peers[1].(*raft)
	if n1.state != StateLeader || !n1.committedEntryInCurrentTerm() {
		t.Fatal("n1 not an established leader")
	}
	// A heartbeat round acknowledged by a quorum establishes the lease.
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgBeat})
	read := func() []ReadState {
		n1.readStates = nil
		if err := n1.Step(pb.Message{From: 1, To: 1, Type: pb.MsgReadIndex, Entries: []pb.Entry{{Data: []byte("ctx")}}}); err != nil {
			t.Fatal(err)
		}
		n1.readMessages()
		return n1.readStates
	}
	if len(read()) != 1 {
		t.Fatal("lease read not served before the transfer")
	}
	// The transfer starts; the MsgTimeoutNow is in flight.
	nt.ignore(pb.MsgTimeoutNow)
	nt.send(pb.Message{From: 2, To: 1, Type: pb.MsgTransferLeader})
	if n1.state != StateLeader || n1.leadTransferee != 2 {
		t.Fatal("transfer not started")
	}
	if rs := read(); len(rs) != 0 {
		t.Fatalf("lease read served after MsgTimeoutNow: %v", rs)
	}
	// The lease stays revoked after the transfer is abandoned...
	for i := 0; i < n1.electionTimeout; i++ {
		n1.tick()
	}
	n1.readMessages()
	if n1.state != StateLeader || n1.leadTransferee != None {
		t.Fatalf("setup: %s, transferee %d", n1.state, n1.leadTransferee)
	}
	if rs := read(); len(rs) != 0 {
		t.Fatalf("lease read served after an abandoned transfer: %v", rs)
	}
	// ...until a new term.
	n1.becomeFollower(n1.Term+1, None)
	if n1.leaseRevoked {
		t.Fatal("lease still revoked in a new term")
	}
}

// A lease rests on a quorum's acknowledgement of a recent heartbeat round.
// Upstream relied on CheckQuorum, so a leader cut off from its followers
// kept serving lease reads for up to two election timeouts, while they
// could elect a new leader after one. Found by internal/raftsim.
func TestLeaseNeedsRecentRound(t *testing.T) {
	nt := newNetworkWithConfig(func(c *Config) {
		c.CheckQuorum = true
		c.ReadOnlyOption = ReadOnlyLeaseBased
	}, nil, nil, nil)
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgHup})
	n1 := nt.peers[1].(*raft)
	if n1.leaseValid() {
		t.Fatal("lease before any heartbeat round")
	}
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgBeat})
	if !n1.leaseValid() {
		t.Fatal("no lease after a round acknowledged by all")
	}
	// Cut n1 off. Its lease must end before the followers would vote.
	nt.isolate(1)
	ticks := 0
	for ; n1.leaseValid(); ticks++ {
		n1.tick()
		nt.send(n1.readMessages()...)
		if n1.state != StateLeader {
			t.Fatal("n1 stepped down")
		}
	}
	if ticks >= n1.electionTimeout-1 {
		t.Fatalf("lease lasted %d ticks after the last acknowledged round; followers vote again after %d", ticks, n1.electionTimeout)
	}
	// A read now goes through a quorum, which cannot answer.
	n1.readStates = nil
	if err := n1.Step(pb.Message{From: 1, To: 1, Type: pb.MsgReadIndex, Entries: []pb.Entry{{Data: []byte("ctx")}}}); err != nil {
		t.Fatal(err)
	}
	nt.send(n1.readMessages()...)
	if len(n1.readStates) != 0 {
		t.Fatalf("read served without a lease: %v", n1.readStates)
	}
	// An acknowledgement of an old round does not renew the lease.
	nt.recover()
	n1.leaseAck(2, 1)
	n1.leaseAck(3, 1)
	if n1.leaseValid() {
		t.Fatal("old round renewed the lease")
	}
	nt.send(pb.Message{From: 1, To: 1, Type: pb.MsgBeat})
	if !n1.leaseValid() {
		t.Fatal("fresh round did not renew the lease")
	}
}

// After a restart, a node in lease mode keeps any promise it may have made
// to a leader before: it ignores vote requests for an election timeout.
// A node that never persisted a term promised nothing.
func TestLeaseStartGuard(t *testing.T) {
	for _, term := range []uint64{0, 3} {
		s := newTestMemoryStorage(withPeers(1, 2, 3))
		if term > 0 {
			s.SetHardState(pb.HardState{Term: term})
		}
		cfg := newTestConfig(2, 10, 1, s)
		cfg.CheckQuorum = true
		cfg.ReadOnlyOption = ReadOnlyLeaseBased
		r := newRaft(cfg)
		vote := pb.Message{From: 3, To: 2, Term: term + 1, Type: pb.MsgVote}
		if err := r.Step(vote); err != nil {
			t.Fatal(err)
		}
		granted := false
		for _, m := range r.readMessages() {
			granted = granted || (m.Type == pb.MsgVoteResp && !m.Reject)
		}
		if term == 0 && !granted {
			t.Fatal("fresh node refused a vote")
		}
		if term > 0 && granted {
			t.Fatal("restarted node voted within an election timeout")
		}
		if term > 0 {
			for i := 0; i < r.electionTimeout; i++ {
				r.tick()
			}
			r.readMessages()
			if r.state != StateFollower {
				// It may campaign itself; what matters is that it would vote.
				r.becomeFollower(r.Term, None)
			}
			vote.Term = r.Term + 1
			if err := r.Step(vote); err != nil {
				t.Fatal(err)
			}
			granted = false
			for _, m := range r.readMessages() {
				granted = granted || (m.Type == pb.MsgVoteResp && !m.Reject)
			}
			if !granted {
				t.Fatal("restarted node still refuses votes after an election timeout")
			}
		}
	}
}

// forkNet is a small in-memory network for scenarios that need to rewrite
// messages in flight.
type forkNet struct {
	t       *testing.T
	peers   map[uint64]*raft
	storage map[uint64]*MemoryStorage
	queue   []pb.Message
	// transform may rewrite or drop (false) a message before delivery.
	transform func(pb.Message) (pb.Message, bool)
}

func newForkNet(t *testing.T, ids ...uint64) *forkNet {
	n := &forkNet{t: t, peers: map[uint64]*raft{}, storage: map[uint64]*MemoryStorage{}}
	for _, id := range ids {
		s := newTestMemoryStorage(withPeers(ids...))
		cfg := newTestConfig(id, 10, 1, s)
		cfg.Logger = quietLogger{}
		n.storage[id] = s
		n.peers[id] = newRaft(cfg)
	}
	return n
}

// settle delivers messages and applies committed entries until quiet.
func (n *forkNet) settle() {
	for round := 0; round < 1000; round++ {
		for _, id := range sortedIDs(n.peers) {
			n.apply(id)
		}
		if len(n.queue) == 0 {
			return
		}
		m := n.queue[0]
		n.queue = n.queue[1:]
		if n.transform != nil {
			var ok bool
			if m, ok = n.transform(m); !ok {
				continue
			}
		}
		if p := n.peers[m.To]; p != nil {
			_ = p.Step(m)
		}
	}
	n.t.Fatal("network did not settle")
}

func (n *forkNet) apply(id uint64) {
	r := n.peers[id]
	for _, e := range nextEnts(r, n.storage[id]) {
		var cc pb.ConfChangeI
		switch e.Type {
		case pb.EntryConfChange:
			var c pb.ConfChange
			if err := c.Unmarshal(e.Data); err != nil {
				n.t.Fatal(err)
			}
			cc = c
		case pb.EntryConfChangeV2:
			var c pb.ConfChangeV2
			if err := c.Unmarshal(e.Data); err != nil {
				n.t.Fatal(err)
			}
			cc = c
		default:
			continue
		}
		r.applyConfChange(cc.AsV2())
	}
	n.queue = append(n.queue, r.readMessages()...)
}

func (n *forkNet) tick(id uint64) {
	n.peers[id].tick()
	n.queue = append(n.queue, n.peers[id].readMessages()...)
}

func sortedIDs(m map[uint64]*raft) []uint64 {
	var ids []uint64
	for id := range m {
		ids = append(ids, id)
	}
	for i := range ids {
		for j := i + 1; j < len(ids); j++ {
			if ids[j] < ids[i] {
				ids[i], ids[j] = ids[j], ids[i]
			}
		}
	}
	return ids
}

// A node that holds two configuration changes but has not learned that the
// first committed must not campaign with the configuration before both.
// Here the group goes from (1 2 3 4) through a joint configuration to (1).
// n2 receives both changes but never their commit. n1 is then elected alone
// and commits. Upstream, n2 then counted votes in (1 2 3 4) and could be
// elected by n2, n3 and n4 and commit a different entry at the same index.
// Now n2 knows the first change committed, must apply it first, and then
// needs n1's vote. Found by internal/raftsim.
func TestStaleConfigurationCannotElect(t *testing.T) {
	n := newForkNet(t, 1, 2, 3, 4)
	_ = n.peers[4].Step(pb.Message{From: 4, To: 4, Type: pb.MsgHup})
	n.queue = append(n.queue, n.peers[4].readMessages()...)
	n.settle()
	if n.peers[4].state != StateLeader {
		t.Fatal("n4 not leader")
	}
	k1 := n.peers[4].raftLog.lastIndex() + 1
	// n2 gets every entry, but never learns a commit index at or past the
	// first configuration change.
	n.transform = func(m pb.Message) (pb.Message, bool) {
		if m.To == 2 && (m.Type == pb.MsgApp || m.Type == pb.MsgHeartbeat) && m.Commit >= k1 {
			m.Commit = k1 - 1
		}
		return m, true
	}
	propose := func(cc pb.ConfChangeV2) {
		data, err := cc.Marshal()
		if err != nil {
			t.Fatal(err)
		}
		_ = n.peers[4].Step(pb.Message{From: 4, To: 4, Type: pb.MsgProp, Entries: []pb.Entry{{Type: pb.EntryConfChangeV2, Data: data}}})
		n.queue = append(n.queue, n.peers[4].readMessages()...)
		n.settle()
	}
	propose(pb.ConfChangeV2{Transition: pb.ConfChangeTransitionJointExplicit, Changes: []pb.ConfChangeSingle{
		{Type: pb.ConfChangeRemoveNode, NodeID: 2},
		{Type: pb.ConfChangeRemoveNode, NodeID: 3},
		{Type: pb.ConfChangeRemoveNode, NodeID: 4},
	}})
	propose(pb.ConfChangeV2{}) // leave the joint configuration
	// n1 now has the configuration (1); n4 has handed leadership to it.
	for i := 0; i < 50 && n.peers[1].state != StateLeader; i++ {
		for _, id := range []uint64{1, 3, 4} {
			n.tick(id)
		}
		n.settle()
	}
	n1, n2 := n.peers[1], n.peers[2]
	if n1.state != StateLeader || len(n1.trk.Voters[0]) != 1 {
		t.Fatalf("n1 %s with %s, want sole voter and leader", n1.state, n1.trk.Config)
	}
	if n2.raftLog.lastIndex() < k1+1 {
		t.Fatalf("setup: n2 holds up to %d, want both changes (%d, %d)", n2.raftLog.lastIndex(), k1, k1+1)
	}
	// n1 commits an entry of its own, alone.
	_ = n1.Step(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{Data: []byte("by n1")}}})
	n.queue = append(n.queue, n1.readMessages()...)
	n.settle()
	committedByN1 := n1.raftLog.committed
	// Cut n1 off; the others try to elect someone.
	n.transform = func(m pb.Message) (pb.Message, bool) { return m, m.To != 1 && m.From != 1 }
	for i := 0; i < 300; i++ {
		for _, id := range []uint64{2, 3, 4} {
			n.tick(id)
		}
		n.settle()
		for _, id := range []uint64{2, 3, 4} {
			p := n.peers[id]
			// n4 may still believe it leads term 1, which it handed to
			// n1; it cannot commit. Any later leader would be a second
			// leader beside n1.
			if p.state == StateLeader && p.Term >= n1.Term {
				t.Fatalf("n%d elected in term %d with configuration %s while n1 leads %s and committed %d",
					id, p.Term, p.trk.Config, n1.trk.Config, committedByN1)
			}
		}
	}
}

// A leader that removes itself can be deposed before it hands over, while
// it holds an entry the remaining voter lacks and that voter, not knowing
// the removal committed, still needs its vote. Upstream the removed node
// refused that vote (its log was longer) and could not campaign: no leader
// for good. Now it campaigns to hand its log over. Found by
// internal/raftsim.
func TestRemovedLeaderRescues(t *testing.T) {
	n := newForkNet(t, 1, 2)
	_ = n.peers[1].Step(pb.Message{From: 1, To: 1, Type: pb.MsgHup})
	n.queue = append(n.queue, n.peers[1].readMessages()...)
	n.settle()
	n1, n2 := n.peers[1], n.peers[2]
	if n1.state != StateLeader {
		t.Fatal("n1 not leader")
	}
	k1 := n1.raftLog.lastIndex() + 1
	var hold uint64 // entries from this index on are kept from n2
	n.transform = func(m pb.Message) (pb.Message, bool) {
		if m.To == 2 && (m.Type == pb.MsgApp || m.Type == pb.MsgHeartbeat) {
			if hold != 0 && len(m.Entries) > 0 && m.Entries[len(m.Entries)-1].Index >= hold {
				return m, false
			}
			m.Commit = min(m.Commit, k1-1)
		}
		return m, true
	}
	propose := func(e pb.Entry) {
		_ = n1.Step(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{e}})
		n.queue = append(n.queue, n1.readMessages()...)
	}
	data := func(cc pb.ConfChangeV2) []byte {
		b, err := cc.Marshal()
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	propose(pb.Entry{Type: pb.EntryConfChangeV2, Data: data(pb.ConfChangeV2{
		Transition: pb.ConfChangeTransitionJointExplicit,
		Changes:    []pb.ConfChangeSingle{{Type: pb.ConfChangeRemoveNode, NodeID: 1}},
	})})
	n.settle()
	// The leave entry reaches n2; an entry proposed right after it does not.
	propose(pb.Entry{Type: pb.EntryConfChangeV2, Data: data(pb.ConfChangeV2{})})
	hold = n1.raftLog.lastIndex() + 1
	propose(pb.Entry{Data: []byte("x")})
	n.settle()
	if n1.trk.Progress[1] != nil {
		t.Fatalf("setup: n1 not removed: %s", n1.trk.Config)
	}
	if n1.raftLog.lastIndex() != hold || n2.raftLog.lastIndex() != hold-1 {
		t.Fatalf("setup: n1 holds %d, n2 %d", n1.raftLog.lastIndex(), n2.raftLog.lastIndex())
	}
	// n2 campaigns and deposes n1 before n1 has handed over.
	for i := 0; i < 100 && n1.state == StateLeader; i++ {
		n.tick(2)
		n.settle()
	}
	if n1.state == StateLeader {
		t.Fatal("setup: n1 not deposed")
	}
	// From here on the network is healthy.
	n.transform = nil
	for i := 0; i < 500 && n2.state != StateLeader; i++ {
		n.tick(1)
		n.tick(2)
		n.settle()
	}
	if n2.state != StateLeader {
		t.Fatalf("no leader: n1 %s log %d, n2 %s log %d config %s",
			n1.state, n1.raftLog.lastIndex(), n2.state, n2.raftLog.lastIndex(), n2.trk.Config)
	}
	if n2.raftLog.lastIndex() < hold {
		t.Fatal("n2 leads without n1's entries")
	}
}

// A crash can keep a node's entries and lose the hard state saved with them
// (nothing was acknowledged before both were synced). The node then
// campaigns at a term below that of its last entry; its vote requests must
// pass validation. Found by internal/raftsim.
func TestVoteFromNodeThatLostItsTerm(t *testing.T) {
	s := newTestMemoryStorage(withPeers(1, 2, 3))
	if err := s.Append([]pb.Entry{{Index: 1, Term: 2}, {Index: 2, Term: 4}}); err != nil {
		t.Fatal(err)
	}
	if err := s.SetHardState(pb.HardState{Term: 2, Vote: 3, Commit: 1}); err != nil {
		t.Fatal(err)
	}
	r := newTestRaft(1, 10, 1, s)
	r.campaign(campaignElection)
	for _, m := range r.readMessages() {
		if m.Type == pb.MsgVote {
			if m.LogTerm <= m.Term {
				t.Fatalf("setup: vote at term %d for log term %d", m.Term, m.LogTerm)
			}
			if err := ValidateMessage(m); err != nil {
				t.Fatalf("vote request rejected: %v", err)
			}
		}
	}
}

// A follower forwards a transfer request that already carries a term (one a
// peer forwarded before) without panicking. Found by FuzzStepHostile.
func TestForwardTransferWithTerm(t *testing.T) {
	r := newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)))
	r.becomeFollower(3, 3)
	if err := r.Step(pb.Message{From: 2, To: 1, Term: 3, Type: pb.MsgTransferLeader}); err != nil {
		t.Fatal(err)
	}
	msgs := r.readMessages()
	if len(msgs) != 1 || msgs[0].Type != pb.MsgTransferLeader || msgs[0].To != 3 || msgs[0].From != 2 || msgs[0].Term != 3 {
		t.Fatalf("got %v, want the request forwarded to n3", msgs)
	}
}

// nextEntsApplied is nextEnts, but reports the application to raft as
// RawNode does, so that what raft does on application happens.
func nextEntsApplied(r *raft, s *MemoryStorage) []pb.Entry {
	s.Append(r.raftLog.nextUnstableEnts())
	r.raftLog.stableTo(r.raftLog.lastEntryID())
	r.advanceMessagesAfterAppend()
	ents := r.raftLog.nextCommittedEnts(true)
	if len(ents) > 0 {
		r.raftLog.acceptApplying(ents[len(ents)-1].Index, 0, true)
	}
	return ents
}

// Raft's storage contract persists entries before the hard state, so a
// crash between the two can leave a node with entries of a term but no
// term of its own (0). Such a node rejects a pre-vote from a candidate with
// a shorter log. Upstream answered with its term, 0, and send panicked on
// it; the candidate would have ignored the answer as stale anyway. Found by
// internal/raftsim (seed 14664, once retried reads changed its schedules).
func TestPreVoteRejectedWithoutTerm(t *testing.T) {
	s := newTestMemoryStorage(withPeers(1, 2, 3))
	if err := s.Append([]pb.Entry{{Term: 1, Index: 1}, {Term: 1, Index: 2}, {Term: 1, Index: 3}}); err != nil {
		t.Fatal(err)
	}
	cfg := newTestConfig(1, 10, 1, s)
	cfg.PreVote = true
	r := newRaft(cfg)
	if r.Term != 0 {
		t.Fatalf("term %d", r.Term)
	}
	// A candidate with a shorter log is rejected, without an answer.
	if err := r.Step(pb.Message{From: 2, To: 1, Term: 2, Type: pb.MsgPreVote, LogTerm: 1, Index: 2}); err != nil {
		t.Fatal(err)
	}
	if msgs := r.readMessages(); len(msgs) != 0 {
		t.Fatalf("answered %v", msgs)
	}
	// One whose log is at least as up to date gets the pre-vote.
	if err := r.Step(pb.Message{From: 3, To: 1, Term: 2, Type: pb.MsgPreVote, LogTerm: 1, Index: 3}); err != nil {
		t.Fatal(err)
	}
	msgs := r.readMessages()
	if len(msgs) != 1 || msgs[0].Type != pb.MsgPreVoteResp || msgs[0].Reject || msgs[0].Term != 2 {
		t.Fatalf("got %v", msgs)
	}
}

// A candidate whose term is behind a voter it needs must learn the higher
// term, or it can never win. Upstream ignored MsgVote from a lower term. In
// a joint configuration (1)&&(6), n1 campaigned for ever at terms below
// n6's, which a third node kept raising with campaigns of its own that
// never reached n1: no leader for good. A node that knows no leader now
// rejects such a vote request with its term, as the Raft paper prescribes;
// one that knows a leader still ignores it, so that a removed node does not
// learn the current term from a healthy cluster. Found by internal/raftsim
// (seed 39352).
func TestCandidateLearnsHigherTerm(t *testing.T) {
	newNode := func(id uint64) *raft {
		return newTestRaft(id, 10, 1, newTestMemoryStorage(withPeers(1, 6)))
	}
	n6 := newNode(6)
	n6.becomeFollower(10, None)
	n1 := newNode(1)
	n1.becomeFollower(4, None)
	n1.becomeCandidate() // term 5
	if err := n6.Step(pb.Message{From: 1, To: 6, Term: n1.Term, Type: pb.MsgVote}); err != nil {
		t.Fatal(err)
	}
	msgs := n6.readMessages()
	if len(msgs) != 1 || msgs[0].Type != pb.MsgVoteResp || !msgs[0].Reject || msgs[0].Term != 10 {
		t.Fatalf("n6 answered %v", msgs)
	}
	if err := n1.Step(msgs[0]); err != nil {
		t.Fatal(err)
	}
	if n1.Term != 10 || n1.state != StateFollower {
		t.Fatalf("n1 at term %d, %v", n1.Term, n1.state)
	}
	// Its next campaign wins n6's vote.
	n1.becomeCandidate()
	if err := n6.Step(pb.Message{From: 1, To: 6, Term: n1.Term, Type: pb.MsgVote}); err != nil {
		t.Fatal(err)
	}
	msgs = n6.readMessages()
	if len(msgs) != 1 || msgs[0].Type != pb.MsgVoteResp || msgs[0].Reject {
		t.Fatalf("n6 answered %v", msgs)
	}
	// A node that knows a leader ignores a vote request from a lower term.
	n6.becomeFollower(20, 1)
	if err := n6.Step(pb.Message{From: 9, To: 6, Term: 15, Type: pb.MsgVote}); err != nil {
		t.Fatal(err)
	}
	if msgs := n6.readMessages(); len(msgs) != 0 {
		t.Fatalf("answered %v while it knows a leader", msgs)
	}
}
