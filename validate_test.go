// Copyright 2026 The raftseg authors. Licensed under the Apache License,
// Version 2.0.

package raft

import (
	"context"
	"errors"
	"testing"

	"google.golang.org/protobuf/proto"

	pb "github.com/raftseg/raft/raftpb"
)

func TestValidateMessage(t *testing.T) {
	ccMsg := &pb.ConfChange{Type: pb.ConfChangeAddNode.Enum(), NodeId: new(uint64(4))}
	cc, _ := proto.Marshal(ccMsg)
	cs := &pb.ConfState{Voters: []uint64{1, 2, 3}}
	snap := func(md *pb.SnapshotMetadata) *pb.Snapshot { return &pb.Snapshot{Metadata: md} }
	ents := func(es ...*pb.Entry) []*pb.Entry { return es }
	valid := []*pb.Message{
		{Type: pb.MsgProp.Enum(), From: new(uint64(2)), To: new(uint64(1)), Entries: ents(&pb.Entry{Data: []byte("x")}, &pb.Entry{Type: pb.EntryConfChange.Enum(), Data: cc})},
		{Type: pb.MsgProp.Enum(), From: new(uint64(1)), To: new(uint64(1)), Entries: ents(&pb.Entry{})}, // forwarded back to its originator
		{Type: pb.MsgApp.Enum(), From: new(uint64(1)), To: new(uint64(2)), Term: new(uint64(3)), Index: new(uint64(5)), LogTerm: new(uint64(2)), Commit: new(uint64(4)), Entries: ents(&pb.Entry{Index: new(uint64(6)), Term: new(uint64(2))}, &pb.Entry{Index: new(uint64(7)), Term: new(uint64(3))})},
		{Type: pb.MsgApp.Enum(), From: new(uint64(1)), To: new(uint64(2)), Term: new(uint64(3))}, // probe at index 0
		{Type: pb.MsgAppResp.Enum(), From: new(uint64(2)), To: new(uint64(1)), Term: new(uint64(3)), Index: new(uint64(9)), Reject: new(true), RejectHint: new(uint64(4)), LogTerm: new(uint64(2))},
		{Type: pb.MsgVote.Enum(), From: new(uint64(2)), To: new(uint64(1)), Term: new(uint64(4)), Index: new(uint64(9)), LogTerm: new(uint64(3))},
		{Type: pb.MsgVote.Enum(), From: new(uint64(2)), To: new(uint64(1)), Term: new(uint64(4)), Context: []byte(campaignTransfer)},
		{Type: pb.MsgPreVote.Enum(), From: new(uint64(2)), To: new(uint64(1)), Term: new(uint64(5)), Index: new(uint64(9)), LogTerm: new(uint64(3))},
		{Type: pb.MsgVoteResp.Enum(), From: new(uint64(2)), To: new(uint64(1)), Term: new(uint64(4)), Reject: new(true)},
		{Type: pb.MsgPreVoteResp.Enum(), From: new(uint64(2)), To: new(uint64(1)), Term: new(uint64(4))},
		{Type: pb.MsgSnap.Enum(), From: new(uint64(1)), To: new(uint64(2)), Term: new(uint64(3)), Snapshot: snap(&pb.SnapshotMetadata{Index: new(uint64(10)), Term: new(uint64(3)), ConfState: cs})},
		{Type: pb.MsgSnap.Enum(), From: new(uint64(1)), To: new(uint64(2)), Term: new(uint64(3)), Snapshot: snap(&pb.SnapshotMetadata{Index: new(uint64(10)), Term: new(uint64(2)), ConfState: &pb.ConfState{
			Voters: []uint64{1, 2}, VotersOutgoing: []uint64{1, 2, 3}, LearnersNext: []uint64{3}, Learners: []uint64{4}, AutoLeave: new(true)}})},
		{Type: pb.MsgHeartbeat.Enum(), From: new(uint64(1)), To: new(uint64(2)), Term: new(uint64(3)), Commit: new(uint64(9)), Index: new(uint64(7)), Context: []byte("ctx")},
		{Type: pb.MsgHeartbeatResp.Enum(), From: new(uint64(2)), To: new(uint64(1)), Term: new(uint64(3)), Index: new(uint64(7))},
		{Type: pb.MsgTransferLeader.Enum(), From: new(uint64(3)), To: new(uint64(1)), Term: new(uint64(3))},
		{Type: pb.MsgTimeoutNow.Enum(), From: new(uint64(1)), To: new(uint64(2)), Term: new(uint64(3))},
		{Type: pb.MsgReadIndex.Enum(), From: new(uint64(2)), To: new(uint64(1)), Entries: ents(&pb.Entry{Data: []byte("ctx")})},
		{Type: pb.MsgReadIndexResp.Enum(), From: new(uint64(1)), To: new(uint64(2)), Term: new(uint64(3)), Index: new(uint64(9)), Entries: ents(&pb.Entry{Data: []byte("ctx")})},
	}
	for _, m := range valid {
		if err := ValidateMessage(m); err != nil {
			t.Errorf("rejected a correct message %s: %v", DescribeMessage(m, nil), err)
		}
	}

	base := &pb.Message{Type: pb.MsgHeartbeat.Enum(), From: new(uint64(1)), To: new(uint64(2)), Term: new(uint64(3))}
	with := func(f func(*pb.Message)) *pb.Message {
		m := proto.Clone(base).(*pb.Message)
		f(m)
		return m
	}
	invalid := map[string]*pb.Message{
		"local type MsgHup":            with(func(m *pb.Message) { m.Type = pb.MsgHup.Enum() }),
		"local type MsgStorageAppend":  with(func(m *pb.Message) { m.Type = pb.MsgStorageAppend.Enum() }),
		"local type MsgForgetLeader":   with(func(m *pb.Message) { m.Type = pb.MsgForgetLeader.Enum() }),
		"unknown type":                 with(func(m *pb.Message) { m.Type = pb.MessageType(99).Enum() }),
		"negative type":                with(func(m *pb.Message) { m.Type = pb.MessageType(-1).Enum() }),
		"no sender":                    with(func(m *pb.Message) { m.From = new(uint64(0)) }),
		"no recipient":                 with(func(m *pb.Message) { m.To = new(uint64(0)) }),
		"sender is a storage thread":   with(func(m *pb.Message) { m.From = new(uint64(LocalAppendThread)) }),
		"recipient is a storage":       with(func(m *pb.Message) { m.To = new(uint64(LocalApplyThread)) }),
		"to itself":                    with(func(m *pb.Message) { m.To = new(uint64(1)) }),
		"no term":                      with(func(m *pb.Message) { m.Term = new(uint64(0)) }),
		"local responses":              with(func(m *pb.Message) { m.Responses = []*pb.Message{{}} }),
		"snapshot on a heartbeat":      with(func(m *pb.Message) { m.Snapshot = &pb.Snapshot{} }),
		"entries on a heartbeat":       with(func(m *pb.Message) { m.Entries = ents(&pb.Entry{}) }),
		"proposal with a term":         {Type: pb.MsgProp.Enum(), From: new(uint64(2)), To: new(uint64(1)), Term: new(uint64(3)), Entries: ents(&pb.Entry{})},
		"empty proposal":               {Type: pb.MsgProp.Enum(), From: new(uint64(2)), To: new(uint64(1))},
		"proposal with an index":       {Type: pb.MsgProp.Enum(), From: new(uint64(2)), To: new(uint64(1)), Entries: ents(&pb.Entry{Index: new(uint64(4))})},
		"undecodable conf change":      {Type: pb.MsgProp.Enum(), From: new(uint64(2)), To: new(uint64(1)), Entries: ents(&pb.Entry{Type: pb.EntryConfChange.Enum(), Data: []byte{0xff}})},
		"undecodable conf change v2":   {Type: pb.MsgProp.Enum(), From: new(uint64(2)), To: new(uint64(1)), Entries: ents(&pb.Entry{Type: pb.EntryConfChangeV2.Enum(), Data: []byte{0x0a, 0x09}})},
		"unknown entry type":           {Type: pb.MsgProp.Enum(), From: new(uint64(2)), To: new(uint64(1)), Entries: ents(&pb.Entry{Type: pb.EntryType(7).Enum()})},
		"unknown transition":           {Type: pb.MsgProp.Enum(), From: new(uint64(2)), To: new(uint64(1)), Entries: ents(&pb.Entry{Type: pb.EntryConfChangeV2.Enum(), Data: mustMarshal(&pb.ConfChangeV2{Transition: pb.ConfChangeTransition(3).Enum()})})},
		"unknown change type":          {Type: pb.MsgProp.Enum(), From: new(uint64(2)), To: new(uint64(1)), Entries: ents(&pb.Entry{Type: pb.EntryConfChangeV2.Enum(), Data: mustMarshal(&pb.ConfChangeV2{Changes: []*pb.ConfChangeSingle{{Type: pb.ConfChangeType(9).Enum(), NodeId: new(uint64(2))}}})})},
		"change of a storage thread":   {Type: pb.MsgProp.Enum(), From: new(uint64(2)), To: new(uint64(1)), Entries: ents(&pb.Entry{Type: pb.EntryConfChangeV2.Enum(), Data: mustMarshal(&pb.ConfChangeV2{Changes: []*pb.ConfChangeSingle{{Type: pb.ConfChangeAddNode.Enum(), NodeId: new(uint64(LocalApplyThread))}}})})},
		"append bad conf change":       {Type: pb.MsgApp.Enum(), From: new(uint64(1)), To: new(uint64(2)), Term: new(uint64(3)), Index: new(uint64(5)), LogTerm: new(uint64(2)), Entries: ents(&pb.Entry{Index: new(uint64(6)), Term: new(uint64(3)), Type: pb.EntryConfChangeV2.Enum(), Data: mustMarshal(&pb.ConfChangeV2{Transition: pb.ConfChangeTransition(7).Enum()})})},
		"append log term from future":  {Type: pb.MsgApp.Enum(), From: new(uint64(1)), To: new(uint64(2)), Term: new(uint64(3)), Index: new(uint64(5)), LogTerm: new(uint64(4))},
		"append term for index 0":      {Type: pb.MsgApp.Enum(), From: new(uint64(1)), To: new(uint64(2)), Term: new(uint64(3)), LogTerm: new(uint64(1))},
		"append gap":                   {Type: pb.MsgApp.Enum(), From: new(uint64(1)), To: new(uint64(2)), Term: new(uint64(3)), Index: new(uint64(5)), LogTerm: new(uint64(2)), Entries: ents(&pb.Entry{Index: new(uint64(7)), Term: new(uint64(2))})},
		"append index repeated":        {Type: pb.MsgApp.Enum(), From: new(uint64(1)), To: new(uint64(2)), Term: new(uint64(3)), Index: new(uint64(5)), LogTerm: new(uint64(2)), Entries: ents(&pb.Entry{Index: new(uint64(6)), Term: new(uint64(2))}, &pb.Entry{Index: new(uint64(6)), Term: new(uint64(2))})},
		"append term decreases":        {Type: pb.MsgApp.Enum(), From: new(uint64(1)), To: new(uint64(2)), Term: new(uint64(3)), Index: new(uint64(5)), LogTerm: new(uint64(2)), Entries: ents(&pb.Entry{Index: new(uint64(6)), Term: new(uint64(1))})},
		"append entry from future":     {Type: pb.MsgApp.Enum(), From: new(uint64(1)), To: new(uint64(2)), Term: new(uint64(3)), Index: new(uint64(5)), LogTerm: new(uint64(2)), Entries: ents(&pb.Entry{Index: new(uint64(6)), Term: new(uint64(4))})},
		"append overflows":             {Type: pb.MsgApp.Enum(), From: new(uint64(1)), To: new(uint64(2)), Term: new(uint64(3)), Index: new(uint64(^uint64(0))), LogTerm: new(uint64(2)), Entries: ents(&pb.Entry{Index: new(uint64(0)), Term: new(uint64(2))})},
		"snapshot missing":             {Type: pb.MsgSnap.Enum(), From: new(uint64(1)), To: new(uint64(2)), Term: new(uint64(3))},
		"snapshot at index 0":          {Type: pb.MsgSnap.Enum(), From: new(uint64(1)), To: new(uint64(2)), Term: new(uint64(3)), Snapshot: snap(&pb.SnapshotMetadata{Term: new(uint64(3)), ConfState: cs})},
		"snapshot from future":         {Type: pb.MsgSnap.Enum(), From: new(uint64(1)), To: new(uint64(2)), Term: new(uint64(3)), Snapshot: snap(&pb.SnapshotMetadata{Index: new(uint64(4)), Term: new(uint64(4)), ConfState: cs})},
		"snapshot without voters":      {Type: pb.MsgSnap.Enum(), From: new(uint64(1)), To: new(uint64(2)), Term: new(uint64(3)), Snapshot: snap(&pb.SnapshotMetadata{Index: new(uint64(4)), Term: new(uint64(3)), ConfState: &pb.ConfState{Learners: []uint64{2}}})},
		"snapshot voter and learner":   {Type: pb.MsgSnap.Enum(), From: new(uint64(1)), To: new(uint64(2)), Term: new(uint64(3)), Snapshot: snap(&pb.SnapshotMetadata{Index: new(uint64(4)), Term: new(uint64(3)), ConfState: &pb.ConfState{Voters: []uint64{1, 2}, Learners: []uint64{2}}})},
		"snapshot duplicate voter":     {Type: pb.MsgSnap.Enum(), From: new(uint64(1)), To: new(uint64(2)), Term: new(uint64(3)), Snapshot: snap(&pb.SnapshotMetadata{Index: new(uint64(4)), Term: new(uint64(3)), ConfState: &pb.ConfState{Voters: []uint64{1, 2, 2}}})},
		"snapshot node 0":              {Type: pb.MsgSnap.Enum(), From: new(uint64(1)), To: new(uint64(2)), Term: new(uint64(3)), Snapshot: snap(&pb.SnapshotMetadata{Index: new(uint64(4)), Term: new(uint64(3)), ConfState: &pb.ConfState{Voters: []uint64{0, 1}}})},
		"snapshot storage thread":      {Type: pb.MsgSnap.Enum(), From: new(uint64(1)), To: new(uint64(2)), Term: new(uint64(3)), Snapshot: snap(&pb.SnapshotMetadata{Index: new(uint64(4)), Term: new(uint64(3)), ConfState: &pb.ConfState{Voters: []uint64{1, LocalAppendThread}}})},
		"snapshot learners next alone": {Type: pb.MsgSnap.Enum(), From: new(uint64(1)), To: new(uint64(2)), Term: new(uint64(3)), Snapshot: snap(&pb.SnapshotMetadata{Index: new(uint64(4)), Term: new(uint64(3)), ConfState: &pb.ConfState{Voters: []uint64{1}, LearnersNext: []uint64{2}}})},
		"snapshot auto leave alone":    {Type: pb.MsgSnap.Enum(), From: new(uint64(1)), To: new(uint64(2)), Term: new(uint64(3)), Snapshot: snap(&pb.SnapshotMetadata{Index: new(uint64(4)), Term: new(uint64(3)), ConfState: &pb.ConfState{Voters: []uint64{1}, AutoLeave: new(true)}})},
		"read without context":         {Type: pb.MsgReadIndex.Enum(), From: new(uint64(2)), To: new(uint64(1))},
		"read response two contexts":   {Type: pb.MsgReadIndexResp.Enum(), From: new(uint64(1)), To: new(uint64(2)), Term: new(uint64(3)), Entries: ents(&pb.Entry{}, &pb.Entry{})},
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
	bad := &pb.Message{Type: pb.MsgProp.Enum(), From: new(uint64(2)), To: new(uint64(1)), Entries: []*pb.Entry{{Type: pb.EntryConfChange.Enum(), Data: []byte{0xff}}}}
	if err := rn.Step(bad); !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("RawNode.Step: %v", err)
	}
	misrouted := &pb.Message{Type: pb.MsgHeartbeat.Enum(), From: new(uint64(2)), To: new(uint64(3)), Term: new(uint64(1))}
	if err := rn.Step(misrouted); !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("RawNode.Step accepted a message for another node: %v", err)
	}
	// A storage thread's own messages, and the node's, are not checked.
	local := &pb.Message{Type: pb.MsgProp.Enum(), From: new(uint64(1)), To: new(uint64(1)), Entries: []*pb.Entry{{Data: []byte("x")}}}
	if err := rn.Step(local); err != nil && errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("local message rejected: %v", err)
	}
	n := &node{rn: rn, propc: make(chan msgWithResult, 1), recvc: make(chan *pb.Message, 1)}
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
	_ = rn.Step(&pb.Message{From: new(uint64(2)), To: new(uint64(1)), Term: new(uint64(1)), Type: pb.MsgVoteResp.Enum()})
	if rn.BasicStatus().RaftState != StateLeader {
		t.Fatal("not leader")
	}
	m := &pb.Message{Type: pb.MsgProp.Enum(), From: new(uint64(2)), To: new(uint64(1)), Entries: []*pb.Entry{{Type: pb.EntryConfChangeV2.Enum(), Data: []byte{0x0a, 0x09}}}}
	if err := rn.Step(m); !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("got %v", err)
	}
}

func mustMarshal(cc *pb.ConfChangeV2) []byte {
	b, err := proto.Marshal(cc)
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
	err := r.Step(&pb.Message{From: new(uint64(1)), To: new(uint64(1)), Type: pb.MsgProp.Enum(), Entries: []*pb.Entry{{Type: pb.EntryConfChangeV2.Enum(), Data: mustMarshal(&pb.ConfChangeV2{Transition: pb.ConfChangeTransition(3).Enum()})}}})
	if !errors.Is(err, ErrProposalDropped) {
		t.Fatalf("got %v", err)
	}
	if r.raftLog.lastIndex() != last {
		t.Fatal("invalid change appended")
	}
	if err := ValidateConfChange(&pb.ConfState{Voters: []uint64{1}}, &pb.ConfChangeV2{Transition: pb.ConfChangeTransition(3).Enum()}); err == nil {
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
	r.Step(&pb.Message{From: new(uint64(2)), To: new(uint64(1)), Type: pb.MsgReadIndex.Enum(), Entries: []*pb.Entry{{Data: []byte("ctx")}}})
	hb := r.readMessages()
	if len(hb) == 0 || hb[0].GetType() != pb.MsgHeartbeat || len(hb[0].Context) != 8 {
		t.Fatalf("no numbered heartbeat: %+v", hb)
	}
	confirmed := func() bool {
		for _, m := range r.readMessages() {
			if m.GetType() == pb.MsgReadIndexResp {
				return true
			}
		}
		return false
	}
	beyond := append([]byte(nil), hb[0].Context...)
	beyond[7] = 0xff
	for _, ctx := range [][]byte{{1}, {1, 2, 3, 4, 5, 6, 7}, make([]byte, 9), beyond} {
		if err := r.Step(&pb.Message{From: new(uint64(2)), To: new(uint64(1)), Term: new(r.Term), Type: pb.MsgHeartbeatResp.Enum(), Context: ctx}); err != nil {
			t.Fatalf("context %x: %v", ctx, err)
		}
		if confirmed() {
			t.Fatalf("context %x confirmed the read", ctx)
		}
	}
	r.Step(&pb.Message{From: new(uint64(2)), To: new(uint64(1)), Term: new(r.Term), Type: pb.MsgHeartbeatResp.Enum(), Context: hb[0].Context})
	if !confirmed() {
		t.Fatal("a valid acknowledgement did not confirm the read")
	}
}

// Since messages are pointers, a message can be nil, or hold nil entries,
// changes or snapshot parts, and a decoded entry can carry fields this
// version does not know. None of that panics, and peers' messages with any
// of it are rejected.
func TestValidateMessagePointers(t *testing.T) {
	if err := ValidateMessage(nil); !errors.Is(err, ErrInvalidMessage) {
		t.Errorf("nil message: %v", err)
	}
	unknown := &pb.Entry{}
	unknown.ProtoReflect().SetUnknown([]byte{0xf8, 0x01, 0x01}) // field 31, varint 1
	cs := &pb.ConfState{Voters: []uint64{1, 2}}
	cases := map[string]*pb.Message{
		"nil entry":              {Type: pb.MsgProp.Enum(), From: new(uint64(2)), To: new(uint64(1)), Entries: []*pb.Entry{nil}},
		"nil entry among others": {Type: pb.MsgApp.Enum(), From: new(uint64(1)), To: new(uint64(2)), Term: new(uint64(3)), Index: new(uint64(5)), LogTerm: new(uint64(2)), Entries: []*pb.Entry{{Index: new(uint64(6)), Term: new(uint64(2))}, nil}},
		"entry, unknown fields":  {Type: pb.MsgProp.Enum(), From: new(uint64(2)), To: new(uint64(1)), Entries: []*pb.Entry{unknown}},
		"snapshot, no metadata":  {Type: pb.MsgSnap.Enum(), From: new(uint64(1)), To: new(uint64(2)), Term: new(uint64(3)), Snapshot: &pb.Snapshot{}},
		"snapshot, no conf":      {Type: pb.MsgSnap.Enum(), From: new(uint64(1)), To: new(uint64(2)), Term: new(uint64(3)), Snapshot: &pb.Snapshot{Metadata: &pb.SnapshotMetadata{Index: new(uint64(4)), Term: new(uint64(3))}}},
		"read with nil entry":    {Type: pb.MsgReadIndex.Enum(), From: new(uint64(2)), To: new(uint64(1)), Entries: []*pb.Entry{nil}},
		"all fields unset":       {},
		"type only":              {Type: pb.MsgHeartbeat.Enum()},
	}
	for name, m := range cases {
		if err := ValidateMessage(m); !errors.Is(err, ErrInvalidMessage) {
			t.Errorf("%s: accepted %s", name, DescribeMessage(m, nil))
		}
	}
	// The same snapshot with its configuration is valid.
	ok := &pb.Message{Type: pb.MsgSnap.Enum(), From: new(uint64(1)), To: new(uint64(2)), Term: new(uint64(3)),
		Snapshot: &pb.Snapshot{Metadata: &pb.SnapshotMetadata{Index: new(uint64(4)), Term: new(uint64(3)), ConfState: cs}}}
	if err := ValidateMessage(ok); err != nil {
		t.Errorf("valid snapshot rejected: %v", err)
	}
	// Nil changes, configurations and change lists do not panic.
	for name, f := range map[string]func() error{
		"nil change":      func() error { return ValidateConfChange(cs, &pb.ConfChangeV2{Changes: []*pb.ConfChangeSingle{nil}}) },
		"nil conf change": func() error { return ValidateConfChange(cs, nil) },
		"nil conf state":  func() error { return ValidateConfChange(nil, &pb.ConfChangeV2{}) },
		"both nil":        func() error { return ValidateConfChange(nil, nil) },
		"nil voter in joint": func() error {
			return ValidateConfChange(cs, &pb.ConfChangeV2{Changes: []*pb.ConfChangeSingle{{}, nil}})
		},
	} {
		func() {
			defer func() {
				if p := recover(); p != nil {
					t.Errorf("%s: panic: %v", name, p)
				}
			}()
			_ = f()
		}()
	}
}

// RawNode.Step and Node.Step refuse a nil message without panicking.
func TestStepNil(t *testing.T) {
	rn := newTestRawNode(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)))
	if err := rn.Step(nil); err == nil {
		t.Error("RawNode.Step(nil) succeeded")
	}
	n := newNode(newTestRawNode(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3))))
	if err := n.Step(context.Background(), nil); err == nil {
		t.Error("Node.Step(nil) succeeded")
	}
}

// A message type outside the enum, as protobuf decodes any int32 into it,
// is refused without a panic. Upstream's IsLocalMsg and IsResponseMsg
// indexed an array with a negative type, so RawNode.Step panicked on such
// a message from a peer before validating it. Found by FuzzStepHostile.
func TestNegativeMessageType(t *testing.T) {
	for _, typ := range []pb.MessageType{-1, -1751120486, 24, 1 << 30} {
		if IsLocalMsg(typ) || IsResponseMsg(typ) {
			t.Errorf("type %d is local or a response", typ)
		}
		rn := newTestRawNode(1, 10, 1, newTestMemoryStorage(withPeers(1, 2, 3)))
		m := &pb.Message{Type: typ.Enum(), From: new(uint64(2)), To: new(uint64(1)), Term: new(uint64(1))}
		if err := rn.Step(m); !errors.Is(err, ErrInvalidMessage) {
			t.Errorf("type %d: %v", typ, err)
		}
	}
}
