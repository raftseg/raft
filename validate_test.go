// Copyright 2026 The raftseg authors. Licensed under the Apache License,
// Version 2.0.

package raft

import (
	"errors"
	"testing"

	pb "github.com/raftseg/raft/raftpb"
)

func TestValidateMessage(t *testing.T) {
	ccMsg := pb.ConfChange{Type: pb.ConfChangeAddNode, NodeID: 4}
	cc, _ := ccMsg.Marshal()
	cs := pb.ConfState{Voters: []uint64{1, 2, 3}}
	snap := func(md pb.SnapshotMetadata) *pb.Snapshot { return &pb.Snapshot{Metadata: md} }
	ents := func(es ...pb.Entry) []pb.Entry { return es }
	valid := []pb.Message{
		{Type: pb.MsgProp, From: 2, To: 1, Entries: ents(pb.Entry{Data: []byte("x")}, pb.Entry{Type: pb.EntryConfChange, Data: cc})},
		{Type: pb.MsgProp, From: 1, To: 1, Entries: ents(pb.Entry{})}, // forwarded back to its originator
		{Type: pb.MsgApp, From: 1, To: 2, Term: 3, Index: 5, LogTerm: 2, Commit: 4, Entries: ents(pb.Entry{Index: 6, Term: 2}, pb.Entry{Index: 7, Term: 3})},
		{Type: pb.MsgApp, From: 1, To: 2, Term: 3}, // probe at index 0
		{Type: pb.MsgAppResp, From: 2, To: 1, Term: 3, Index: 9, Reject: true, RejectHint: 4, LogTerm: 2},
		{Type: pb.MsgVote, From: 2, To: 1, Term: 4, Index: 9, LogTerm: 3},
		{Type: pb.MsgVote, From: 2, To: 1, Term: 4, Context: []byte(campaignTransfer)},
		{Type: pb.MsgPreVote, From: 2, To: 1, Term: 5, Index: 9, LogTerm: 3},
		{Type: pb.MsgVoteResp, From: 2, To: 1, Term: 4, Reject: true},
		{Type: pb.MsgPreVoteResp, From: 2, To: 1, Term: 4},
		{Type: pb.MsgSnap, From: 1, To: 2, Term: 3, Snapshot: snap(pb.SnapshotMetadata{Index: 10, Term: 3, ConfState: cs})},
		{Type: pb.MsgSnap, From: 1, To: 2, Term: 3, Snapshot: snap(pb.SnapshotMetadata{Index: 10, Term: 2, ConfState: pb.ConfState{
			Voters: []uint64{1, 2}, VotersOutgoing: []uint64{1, 2, 3}, LearnersNext: []uint64{3}, Learners: []uint64{4}, AutoLeave: true}})},
		{Type: pb.MsgHeartbeat, From: 1, To: 2, Term: 3, Commit: 9, Index: 7, Context: []byte("ctx")},
		{Type: pb.MsgHeartbeatResp, From: 2, To: 1, Term: 3, Index: 7},
		{Type: pb.MsgTransferLeader, From: 3, To: 1, Term: 3},
		{Type: pb.MsgTimeoutNow, From: 1, To: 2, Term: 3},
		{Type: pb.MsgReadIndex, From: 2, To: 1, Entries: ents(pb.Entry{Data: []byte("ctx")})},
		{Type: pb.MsgReadIndexResp, From: 1, To: 2, Term: 3, Index: 9, Entries: ents(pb.Entry{Data: []byte("ctx")})},
	}
	for _, m := range valid {
		if err := ValidateMessage(m); err != nil {
			t.Errorf("rejected a correct message %s: %v", DescribeMessage(m, nil), err)
		}
	}

	base := pb.Message{Type: pb.MsgHeartbeat, From: 1, To: 2, Term: 3}
	with := func(f func(*pb.Message)) pb.Message {
		m := base
		f(&m)
		return m
	}
	invalid := map[string]pb.Message{
		"local type MsgHup":            with(func(m *pb.Message) { m.Type = pb.MsgHup }),
		"local type MsgStorageAppend":  with(func(m *pb.Message) { m.Type = pb.MsgStorageAppend }),
		"local type MsgForgetLeader":   with(func(m *pb.Message) { m.Type = pb.MsgForgetLeader }),
		"unknown type":                 with(func(m *pb.Message) { m.Type = 99 }),
		"negative type":                with(func(m *pb.Message) { m.Type = -1 }),
		"no sender":                    with(func(m *pb.Message) { m.From = 0 }),
		"no recipient":                 with(func(m *pb.Message) { m.To = 0 }),
		"sender is a storage thread":   with(func(m *pb.Message) { m.From = LocalAppendThread }),
		"recipient is a storage":       with(func(m *pb.Message) { m.To = LocalApplyThread }),
		"to itself":                    with(func(m *pb.Message) { m.To = 1 }),
		"no term":                      with(func(m *pb.Message) { m.Term = 0 }),
		"local responses":              with(func(m *pb.Message) { m.Responses = []pb.Message{{}} }),
		"snapshot on a heartbeat":      with(func(m *pb.Message) { m.Snapshot = &pb.Snapshot{} }),
		"entries on a heartbeat":       with(func(m *pb.Message) { m.Entries = ents(pb.Entry{}) }),
		"proposal with a term":         {Type: pb.MsgProp, From: 2, To: 1, Term: 3, Entries: ents(pb.Entry{})},
		"empty proposal":               {Type: pb.MsgProp, From: 2, To: 1},
		"proposal with an index":       {Type: pb.MsgProp, From: 2, To: 1, Entries: ents(pb.Entry{Index: 4})},
		"undecodable conf change":      {Type: pb.MsgProp, From: 2, To: 1, Entries: ents(pb.Entry{Type: pb.EntryConfChange, Data: []byte{0xff}})},
		"undecodable conf change v2":   {Type: pb.MsgProp, From: 2, To: 1, Entries: ents(pb.Entry{Type: pb.EntryConfChangeV2, Data: []byte{0x0a, 0x09}})},
		"unknown entry type":           {Type: pb.MsgProp, From: 2, To: 1, Entries: ents(pb.Entry{Type: 7})},
		"unknown transition":           {Type: pb.MsgProp, From: 2, To: 1, Entries: ents(pb.Entry{Type: pb.EntryConfChangeV2, Data: mustMarshal(pb.ConfChangeV2{Transition: 3})})},
		"unknown change type":          {Type: pb.MsgProp, From: 2, To: 1, Entries: ents(pb.Entry{Type: pb.EntryConfChangeV2, Data: mustMarshal(pb.ConfChangeV2{Changes: []pb.ConfChangeSingle{{Type: 9, NodeID: 2}}})})},
		"change of a storage thread":   {Type: pb.MsgProp, From: 2, To: 1, Entries: ents(pb.Entry{Type: pb.EntryConfChangeV2, Data: mustMarshal(pb.ConfChangeV2{Changes: []pb.ConfChangeSingle{{Type: pb.ConfChangeAddNode, NodeID: LocalApplyThread}}})})},
		"append bad conf change":       {Type: pb.MsgApp, From: 1, To: 2, Term: 3, Index: 5, LogTerm: 2, Entries: ents(pb.Entry{Index: 6, Term: 3, Type: pb.EntryConfChangeV2, Data: mustMarshal(pb.ConfChangeV2{Transition: 7})})},
		"append log term from future":  {Type: pb.MsgApp, From: 1, To: 2, Term: 3, Index: 5, LogTerm: 4},
		"append term for index 0":      {Type: pb.MsgApp, From: 1, To: 2, Term: 3, LogTerm: 1},
		"append gap":                   {Type: pb.MsgApp, From: 1, To: 2, Term: 3, Index: 5, LogTerm: 2, Entries: ents(pb.Entry{Index: 7, Term: 2})},
		"append index repeated":        {Type: pb.MsgApp, From: 1, To: 2, Term: 3, Index: 5, LogTerm: 2, Entries: ents(pb.Entry{Index: 6, Term: 2}, pb.Entry{Index: 6, Term: 2})},
		"append term decreases":        {Type: pb.MsgApp, From: 1, To: 2, Term: 3, Index: 5, LogTerm: 2, Entries: ents(pb.Entry{Index: 6, Term: 1})},
		"append entry from future":     {Type: pb.MsgApp, From: 1, To: 2, Term: 3, Index: 5, LogTerm: 2, Entries: ents(pb.Entry{Index: 6, Term: 4})},
		"append overflows":             {Type: pb.MsgApp, From: 1, To: 2, Term: 3, Index: ^uint64(0), LogTerm: 2, Entries: ents(pb.Entry{Index: 0, Term: 2})},
		"snapshot missing":             {Type: pb.MsgSnap, From: 1, To: 2, Term: 3},
		"snapshot at index 0":          {Type: pb.MsgSnap, From: 1, To: 2, Term: 3, Snapshot: snap(pb.SnapshotMetadata{Term: 3, ConfState: cs})},
		"snapshot from future":         {Type: pb.MsgSnap, From: 1, To: 2, Term: 3, Snapshot: snap(pb.SnapshotMetadata{Index: 4, Term: 4, ConfState: cs})},
		"snapshot without voters":      {Type: pb.MsgSnap, From: 1, To: 2, Term: 3, Snapshot: snap(pb.SnapshotMetadata{Index: 4, Term: 3, ConfState: pb.ConfState{Learners: []uint64{2}}})},
		"snapshot voter and learner":   {Type: pb.MsgSnap, From: 1, To: 2, Term: 3, Snapshot: snap(pb.SnapshotMetadata{Index: 4, Term: 3, ConfState: pb.ConfState{Voters: []uint64{1, 2}, Learners: []uint64{2}}})},
		"snapshot duplicate voter":     {Type: pb.MsgSnap, From: 1, To: 2, Term: 3, Snapshot: snap(pb.SnapshotMetadata{Index: 4, Term: 3, ConfState: pb.ConfState{Voters: []uint64{1, 2, 2}}})},
		"snapshot node 0":              {Type: pb.MsgSnap, From: 1, To: 2, Term: 3, Snapshot: snap(pb.SnapshotMetadata{Index: 4, Term: 3, ConfState: pb.ConfState{Voters: []uint64{0, 1}}})},
		"snapshot storage thread":      {Type: pb.MsgSnap, From: 1, To: 2, Term: 3, Snapshot: snap(pb.SnapshotMetadata{Index: 4, Term: 3, ConfState: pb.ConfState{Voters: []uint64{1, LocalAppendThread}}})},
		"snapshot learners next alone": {Type: pb.MsgSnap, From: 1, To: 2, Term: 3, Snapshot: snap(pb.SnapshotMetadata{Index: 4, Term: 3, ConfState: pb.ConfState{Voters: []uint64{1}, LearnersNext: []uint64{2}}})},
		"snapshot auto leave alone":    {Type: pb.MsgSnap, From: 1, To: 2, Term: 3, Snapshot: snap(pb.SnapshotMetadata{Index: 4, Term: 3, ConfState: pb.ConfState{Voters: []uint64{1}, AutoLeave: true}})},
		"read without context":         {Type: pb.MsgReadIndex, From: 2, To: 1},
		"read response two contexts":   {Type: pb.MsgReadIndexResp, From: 1, To: 2, Term: 3, Entries: ents(pb.Entry{}, pb.Entry{})},
	}
	for name, m := range invalid {
		if err := ValidateMessage(m); !errors.Is(err, ErrInvalidMessage) {
			t.Errorf("%s: accepted %s", name, DescribeMessage(m, nil))
		}
	}
}

// RawNode.Step and Node.Step reject what ValidateMessage rejects, from
// peers only.
func TestStepValidates(t *testing.T) {
	rn := newTestRawNode(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)))
	bad := pb.Message{Type: pb.MsgProp, From: 2, To: 1, Entries: []pb.Entry{{Type: pb.EntryConfChange, Data: []byte{0xff}}}}
	if err := rn.Step(bad); !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("RawNode.Step: %v", err)
	}
	misrouted := pb.Message{Type: pb.MsgHeartbeat, From: 2, To: 3, Term: 1}
	if err := rn.Step(misrouted); !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("RawNode.Step accepted a message for another node: %v", err)
	}
	// A storage thread's own messages, and the node's, are not checked.
	local := pb.Message{Type: pb.MsgProp, From: 1, To: 1, Entries: []pb.Entry{{Data: []byte("x")}}}
	if err := rn.Step(local); err != nil && errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("local message rejected: %v", err)
	}
	n := &node{rn: rn, propc: make(chan msgWithResult, 1), recvc: make(chan pb.Message, 1)}
	if err := n.Step(t.Context(), bad); !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("Node.Step: %v", err)
	}
}

// Upstream, a forwarded proposal whose configuration change did not decode
// panicked the leader. Validation turns it into an error.
func TestUndecodableConfChangeDoesNotPanicLeader(t *testing.T) {
	s := newTestMemoryStorage(withPeers(1, 2))
	rn := newTestRawNode(1, 10, 1, s)
	if err := rn.Campaign(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5 && rn.HasReady(); i++ {
		rd := rn.Ready()
		s.Append(rd.Entries)
		s.SetHardState(rd.HardState)
		rn.Advance(rd)
	}
	_ = rn.Step(pb.Message{From: 2, To: 1, Term: 1, Type: pb.MsgVoteResp})
	if rn.BasicStatus().RaftState != StateLeader {
		t.Fatal("not leader")
	}
	m := pb.Message{Type: pb.MsgProp, From: 2, To: 1, Entries: []pb.Entry{{Type: pb.EntryConfChangeV2, Data: []byte{0x0a, 0x09}}}}
	if err := rn.Step(m); !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("got %v", err)
	}
}

func mustMarshal(cc pb.ConfChangeV2) []byte {
	b, err := cc.Marshal()
	if err != nil {
		panic(err)
	}
	return b
}

// A local proposal of a configuration change raft cannot apply is dropped;
// upstream appended it, and every node panicked applying it.
func TestInvalidLocalConfChangeDropped(t *testing.T) {
	r := newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1)))
	r.becomeCandidate()
	r.becomeLeader()
	last := r.raftLog.lastIndex()
	err := r.Step(pb.Message{From: 1, To: 1, Type: pb.MsgProp, Entries: []pb.Entry{{Type: pb.EntryConfChangeV2, Data: mustMarshal(pb.ConfChangeV2{Transition: 3})}}})
	if !errors.Is(err, ErrProposalDropped) {
		t.Fatalf("got %v", err)
	}
	if r.raftLog.lastIndex() != last {
		t.Fatal("invalid change appended")
	}
	if err := ValidateConfChange(pb.ConfState{Voters: []uint64{1}}, pb.ConfChangeV2{Transition: 3}); err == nil {
		t.Fatal("ValidateConfChange accepted an unknown transition")
	}
}

// Read acknowledgements are positions a leader numbers its read requests
// with, sent in heartbeats and echoed back (see readOnly.heartbeatCtx).
// A heartbeat response whose context is not such a position must neither
// panic the leader nor confirm a read: upstream's decoder read 8 bytes from
// any non-empty context, and a value beyond the positions issued would
// confirm reads no heartbeat was sent for.
func TestHeartbeatAckContextChecked(t *testing.T) {
	r := newTestRaft(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)))
	r.becomeCandidate()
	r.becomeLeader()
	r.raftLog.commitTo(r.raftLog.lastIndex())
	r.Step(pb.Message{From: 2, To: 1, Type: pb.MsgReadIndex, Entries: []pb.Entry{{Data: []byte("ctx")}}})
	hb := r.readMessages()
	if len(hb) == 0 || hb[0].Type != pb.MsgHeartbeat || len(hb[0].Context) != 8 {
		t.Fatalf("no numbered heartbeat: %+v", hb)
	}
	confirmed := func() bool {
		for _, m := range r.readMessages() {
			if m.Type == pb.MsgReadIndexResp {
				return true
			}
		}
		return false
	}
	beyond := append([]byte(nil), hb[0].Context...)
	beyond[7] = 0xff
	for _, ctx := range [][]byte{{1}, {1, 2, 3, 4, 5, 6, 7}, make([]byte, 9), beyond} {
		if err := r.Step(pb.Message{From: 2, To: 1, Term: r.Term, Type: pb.MsgHeartbeatResp, Context: ctx}); err != nil {
			t.Fatalf("context %x: %v", ctx, err)
		}
		if confirmed() {
			t.Fatalf("context %x confirmed the read", ctx)
		}
	}
	r.Step(pb.Message{From: 2, To: 1, Term: r.Term, Type: pb.MsgHeartbeatResp, Context: hb[0].Context})
	if !confirmed() {
		t.Fatal("a valid acknowledgement did not confirm the read")
	}
}
