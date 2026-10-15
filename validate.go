// Copyright 2026 The raftseg authors. Licensed under the Apache License,
// Version 2.0.

package raft

import (
	"errors"
	"fmt"
	"math"

	"google.golang.org/protobuf/proto"

	"github.com/raftseg/raft/confchange"
	pb "github.com/raftseg/raft/raftpb"
	"github.com/raftseg/raft/tracker"
)

// ErrInvalidMessage is returned (wrapped) by ValidateMessage, and by
// RawNode.Step for messages from peers that fail it.
var ErrInvalidMessage = errors.New("raft: invalid message")

func invalid(m *pb.Message, format string, args ...any) error {
	return fmt.Errorf("%w: %s from %x to %x: %s", ErrInvalidMessage, m.GetType(), m.GetFrom(), m.GetTo(), fmt.Sprintf(format, args...))
}

// peerMessage reports the message types a peer may send over the network.
var peerMessage = [...]bool{
	pb.MsgProp:           true, // forwarded proposals
	pb.MsgApp:            true,
	pb.MsgAppResp:        true,
	pb.MsgVote:           true,
	pb.MsgVoteResp:       true,
	pb.MsgSnap:           true,
	pb.MsgHeartbeat:      true,
	pb.MsgHeartbeatResp:  true,
	pb.MsgTransferLeader: true, // forwarded transfer requests
	pb.MsgTimeoutNow:     true,
	pb.MsgReadIndex:      true, // forwarded reads
	pb.MsgReadIndexResp:  true,
	pb.MsgPreVote:        true,
	pb.MsgPreVoteResp:    true,
}

// ValidateMessage checks a message received from a peer for what no correct
// peer sends: local message types, missing or reserved node IDs, local
// response lists, entries out of order or from the future, proposals that
// do not decode, snapshots with impossible configurations. It does not and
// cannot check that the sender tells the truth: raft tolerates crashes, not
// lying peers. Its purpose is that a faulty, outdated or misconfigured peer
// cannot crash a node or slip it messages meant for its own storage threads.
//
// RawNode.Step and Node.Step apply it to every message from a peer: one
// with a sender other than the node itself or its storage threads.
//
// A transport must check that From is the authenticated node the message
// came from, except for MsgProp, MsgReadIndex and MsgTransferLeader, which
// followers forward to the leader unchanged: their From is the node that
// made the request. Fork addition; upstream accepted anything, and for
// example panicked on a forwarded proposal whose configuration change did
// not decode.
//
// Entries must not carry fields this version does not know: protobuf keeps
// them, so they would be stored and replicated with the log without being
// counted in its size limits.
func ValidateMessage(m *pb.Message) error {
	if m == nil {
		return fmt.Errorf("%w: nil message", ErrInvalidMessage)
	}
	if t := m.GetType(); int(t) < 0 || int(t) >= len(peerMessage) || !peerMessage[t] {
		return invalid(m, "not a message type peers exchange")
	}
	if m.GetFrom() == None || m.GetTo() == None || IsLocalMsgTarget(m.GetFrom()) || IsLocalMsgTarget(m.GetTo()) {
		return invalid(m, "reserved node ID")
	}
	switch m.GetType() {
	case pb.MsgProp, pb.MsgReadIndex, pb.MsgTransferLeader:
		// Followers forward these to the leader with the original sender,
		// which can be the leader they reach.
	default:
		if m.GetFrom() == m.GetTo() {
			return invalid(m, "sent to itself")
		}
	}
	if len(m.GetResponses()) > 0 {
		return invalid(m, "carries local responses")
	}
	if m.Snapshot != nil && m.GetType() != pb.MsgSnap {
		return invalid(m, "unexpected snapshot")
	}
	switch m.GetType() {
	case pb.MsgProp, pb.MsgReadIndex:
		// Forwarded as local messages, without a term.
		if m.GetTerm() != 0 {
			return invalid(m, "term %d on a forwarded request", m.GetTerm())
		}
	default:
		if m.GetTerm() == 0 {
			return invalid(m, "no term")
		}
	}
	for i, e := range m.GetEntries() {
		if e == nil {
			return invalid(m, "entry %d is nil", i)
		}
		if e.GetType() < pb.EntryNormal || e.GetType() > pb.EntryConfChangeV2 {
			return invalid(m, "entry %d has type %d", i, e.GetType())
		}
		if len(e.ProtoReflect().GetUnknown()) > 0 {
			return invalid(m, "entry %d has fields this version does not know", i)
		}
	}
	switch m.GetType() {
	case pb.MsgProp:
		if len(m.GetEntries()) == 0 {
			return invalid(m, "no entries")
		}
		for i, e := range m.GetEntries() {
			if e.GetTerm() != 0 || e.GetIndex() != 0 {
				return invalid(m, "proposed entry %d already has index %d term %d", i, e.GetIndex(), e.GetTerm())
			}
			if err := checkConfChange(e); err != nil {
				return invalid(m, "proposed entry %d: %v", i, err)
			}
		}
	case pb.MsgApp:
		if m.GetLogTerm() > m.GetTerm() {
			return invalid(m, "log term %d above term %d", m.GetLogTerm(), m.GetTerm())
		}
		if m.GetIndex() == 0 && m.GetLogTerm() != 0 {
			return invalid(m, "term %d for index 0", m.GetLogTerm())
		}
		if uint64(len(m.GetEntries())) > math.MaxUint64-m.GetIndex() {
			return invalid(m, "entries overflow the index space")
		}
		prev := m.GetLogTerm()
		for i, e := range m.GetEntries() {
			if e.GetIndex() != m.GetIndex()+1+uint64(i) {
				return invalid(m, "entry %d has index %d, want %d", i, e.GetIndex(), m.GetIndex()+1+uint64(i))
			}
			if e.GetTerm() < prev || e.GetTerm() > m.GetTerm() {
				return invalid(m, "entry %d has term %d, outside [%d, %d]", i, e.GetTerm(), prev, m.GetTerm())
			}
			prev = e.GetTerm()
			// A leader accepts only configuration changes raft can apply.
			if err := checkConfChange(e); err != nil {
				return invalid(m, "entry %d: %v", i, err)
			}
		}
	case pb.MsgSnap:
		if m.Snapshot == nil {
			return invalid(m, "no snapshot")
		}
		md := m.GetSnapshot().GetMetadata()
		if md.GetIndex() == 0 || md.GetTerm() == 0 || md.GetTerm() > m.GetTerm() {
			return invalid(m, "snapshot at index %d term %d (message term %d)", md.GetIndex(), md.GetTerm(), m.GetTerm())
		}
		if err := checkConfState(md.GetConfState()); err != nil {
			return invalid(m, "snapshot configuration: %v", err)
		}
	case pb.MsgReadIndex, pb.MsgReadIndexResp:
		if len(m.GetEntries()) != 1 {
			return invalid(m, "%d entries, want the request context", len(m.GetEntries()))
		}
	}
	switch m.GetType() {
	case pb.MsgProp, pb.MsgApp, pb.MsgReadIndex, pb.MsgReadIndexResp:
	default:
		if len(m.GetEntries()) > 0 {
			return invalid(m, "unexpected entries")
		}
	}
	return nil
}

// isPeerMessage reports whether m, stepped into node self, comes from a peer.
// Messages from the node itself or its storage threads are its own, and so
// are those without a sender, which applications step for local requests:
// a transport must set From to the authenticated sender.
func isPeerMessage(self uint64, m *pb.Message) bool {
	from := m.GetFrom()
	return from != None && from != self && !IsLocalMsgTarget(from)
}

// checkConfChange checks that a configuration change entry decodes into a
// change raft knows how to apply.
func checkConfChange(e *pb.Entry) error {
	switch e.GetType() {
	case pb.EntryConfChange:
		var cc pb.ConfChange
		if err := proto.Unmarshal(e.GetData(), &cc); err != nil {
			return err
		}
		return checkConfChangeV2(cc.AsV2())
	case pb.EntryConfChangeV2:
		var cc pb.ConfChangeV2
		if err := proto.Unmarshal(e.GetData(), &cc); err != nil {
			return err
		}
		return checkConfChangeV2(&cc)
	}
	return nil
}

// checkConfChangeV2 checks a change's enumerations, on which raft panics,
// and its node IDs.
func checkConfChangeV2(cc *pb.ConfChangeV2) error {
	if t := cc.GetTransition(); t < pb.ConfChangeTransitionAuto || t > pb.ConfChangeTransitionJointExplicit {
		return fmt.Errorf("unknown transition %d", t)
	}
	for _, c := range cc.GetChanges() {
		if c == nil {
			return errors.New("nil change")
		}
		if t := c.GetType(); t < pb.ConfChangeAddNode || t > pb.ConfChangeAddLearnerNode {
			return fmt.Errorf("unknown change type %d", t)
		}
		// Node 0 marks a change the application decided not to apply,
		// which raft skips; the storage threads' IDs are reserved.
		if IsLocalMsgTarget(c.GetNodeId()) {
			return fmt.Errorf("reserved node ID %x", c.GetNodeId())
		}
	}
	return nil
}

// checkConfState checks that cs is a configuration raft can switch to.
func checkConfState(cs *pb.ConfState) error {
	for _, ids := range [][]uint64{cs.GetVoters(), cs.GetLearners(), cs.GetVotersOutgoing(), cs.GetLearnersNext()} {
		for _, id := range ids {
			if id == None || IsLocalMsgTarget(id) {
				return fmt.Errorf("reserved node ID %x", id)
			}
		}
	}
	if len(cs.GetVoters()) == 0 {
		return errors.New("no voters")
	}
	cfg, trk, err := confchange.Restore(confchange.Changer{Tracker: tracker.MakeProgressTracker(1, 0), LastIndex: 1}, cs)
	if err != nil {
		return err
	}
	// Restore builds a configuration from cs without rejecting overlaps or
	// duplicates (a node listed as voter and learner ends up a learner);
	// the result must be cs itself.
	pt := tracker.ProgressTracker{Config: cfg, Progress: trk}
	return pt.ConfState().Equivalent(cs)
}

// ValidateConfChange reports whether cc can be applied to the configuration
// cs, as ApplyConfChange would apply it: ApplyConfChange panics on a change
// it cannot apply. An application that receives a committed change it
// cannot apply (a leader that skipped validation, or one of the rare cases
// in which validation at proposal time misses a conflict) can treat it as
// a no-op instead: every node decides the same way, as they all hold the
// same configuration at that index. Fork addition.
func ValidateConfChange(cs *pb.ConfState, cc *pb.ConfChangeV2) error {
	if err := checkConfChangeV2(cc); err != nil {
		return err
	}
	if cs == nil {
		cs = &pb.ConfState{} // as protobuf reads a missing message
	}
	cfg, trk, err := confchange.Restore(confchange.Changer{Tracker: tracker.MakeProgressTracker(1, 0), LastIndex: 1}, cs)
	if err != nil {
		return err
	}
	pt := tracker.MakeProgressTracker(1, 0)
	pt.Config, pt.Progress = cfg, trk
	chg := confchange.Changer{Tracker: pt, LastIndex: 1}
	if cc.LeaveJoint() {
		_, _, err = chg.LeaveJoint()
	} else if auto, ok := cc.EnterJoint(); ok {
		_, _, err = chg.EnterJoint(auto, cc.GetChanges()...)
	} else {
		_, _, err = chg.Simple(cc.GetChanges()...)
	}
	return err
}
