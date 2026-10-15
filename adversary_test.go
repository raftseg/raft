// Copyright 2026 The raftseg authors. Licensed under the Apache License,
// Version 2.0.

package raft

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	pb "github.com/raftseg/raft/raftpb"
)

// Hostile and malformed messages from peers. Raft tolerates crashes, not
// lies: a peer that lies about its log can break safety, and raft rightly
// panics when a message proves the log it holds is inconsistent (a
// tripwire). What a faulty peer must not be able to do is crash a node
// with a message whose only fault is its shape, or one that raft should
// simply ignore. These fuzzers drive a node in each state with arbitrary
// message sequences and fail on any panic that is not a tripwire.

// tripwires are panics raised when a message, well-formed on its own,
// contradicts the node's durable log: only a peer that lies (or a node
// that lost its log) produces them, and crashing is the safe response.
var tripwires = []string{
	"is out of range [lastIndex(",    // commit index beyond our log
	"conflict with committed entry",  // a leader rewriting committed entries
	"is out of range [prevApplying(", // follows from the above
}

func isTripwire(p any) bool {
	s := fmt.Sprint(p)
	for _, w := range tripwires {
		if strings.Contains(s, w) {
			return true
		}
	}
	return false
}

// byteSource turns fuzz input into choices.
type byteSource struct{ b []byte }

func (s *byteSource) byte() byte {
	if len(s.b) == 0 {
		return 0
	}
	v := s.b[0]
	s.b = s.b[1:]
	return v
}

func (s *byteSource) intn(n int) int { return int(s.byte()) % n }

func (s *byteSource) u64() uint64 {
	var buf [8]byte
	for i := range buf {
		buf[i] = s.byte()
	}
	return binary.LittleEndian.Uint64(buf[:])
}

func (s *byteSource) empty() bool { return len(s.b) == 0 }

// near picks a value around base: often equal or close, sometimes extreme.
func (s *byteSource) near(base uint64) uint64 {
	switch s.intn(8) {
	case 0:
		return 0
	case 1:
		return s.u64()
	case 2:
		if base > 0 {
			return base - 1
		}
		return 0
	case 3, 4:
		return base
	default:
		return base + uint64(s.intn(4))
	}
}

// adversaryNode is a node in one of several states, with a log, a snapshot
// and a configuration of voters 1, 2, 3 and learner 4.
type adversaryNode struct {
	rn    *RawNode
	s     *MemoryStorage
	async bool
}

func newAdversaryNode(t *testing.T, src *byteSource) *adversaryNode {
	cs := &pb.ConfState{Voters: []uint64{1, 2, 3}, Learners: []uint64{4}}
	s := NewMemoryStorage()
	snap := &pb.Snapshot{Metadata: &pb.SnapshotMetadata{Index: new(uint64(5)), Term: new(uint64(1)), ConfState: cs}}
	if err := s.ApplySnapshot(snap); err != nil {
		t.Fatal(err)
	}
	var ents []*pb.Entry
	for i := uint64(6); i <= 12; i++ {
		ents = append(ents, &pb.Entry{Index: new(i), Term: new(1 + (i-6)/3)})
	}
	if err := s.Append(ents); err != nil {
		t.Fatal(err)
	}
	if err := s.SetHardState(&pb.HardState{Term: new(uint64(3)), Commit: new(uint64(9))}); err != nil {
		t.Fatal(err)
	}
	id := []uint64{1, 1, 1, 4}[src.intn(4)] // mostly a voter, sometimes the learner
	cfg := newTestConfig(id, 10, 1, s)
	cfg.Applied = 5
	cfg.PreVote = src.intn(2) == 0
	cfg.CheckQuorum = src.intn(2) == 0
	cfg.AsyncStorageWrites = src.intn(2) == 0
	if cfg.CheckQuorum && src.intn(2) == 0 {
		cfg.ReadOnlyOption = ReadOnlyLeaseBased
	}
	cfg.StepDownOnRemoval = src.intn(2) == 0
	cfg.Logger = quietLogger{}
	rn, err := NewRawNode(cfg)
	if err != nil {
		t.Fatal(err)
	}
	n := &adversaryNode{rn: rn, s: s, async: cfg.AsyncStorageWrites}
	switch src.intn(4) {
	case 0: // follower
	case 1: // candidate or pre-candidate
		_ = rn.Campaign()
		n.handle()
	default: // leader, if it can be
		_ = rn.Campaign()
		n.handle()
		for _, from := range []uint64{2, 3} {
			_ = rn.Step(&pb.Message{From: new(from), To: new(id), Term: new(rn.raft.Term), Type: pb.MsgVoteResp.Enum()})
			_ = rn.Step(&pb.Message{From: new(from), To: new(id), Term: new(rn.raft.Term), Type: pb.MsgPreVoteResp.Enum()})
		}
		n.handle()
	}
	return n
}

// handle processes Ready as a correct application would.
func (n *adversaryNode) handle() {
	for i := 0; i < 50 && n.rn.HasReady(); i++ {
		rd := n.rn.Ready()
		if n.async {
			for _, m := range rd.Messages {
				switch m.GetTo() {
				case LocalAppendThread:
					if m.Snapshot != nil {
						_ = n.s.ApplySnapshot(m.Snapshot)
					}
					_ = n.s.Append(m.Entries)
					if m.GetTerm() != 0 || m.GetVote() != 0 || m.GetCommit() != 0 {
						_ = n.s.SetHardState(&pb.HardState{Term: m.Term, Vote: m.Vote, Commit: m.Commit})
					}
				case LocalApplyThread:
					n.applyConfChanges(m.Entries)
				default:
					continue
				}
				for _, r := range m.Responses {
					if r.GetTo() == n.rn.raft.id || IsLocalMsgTarget(r.GetTo()) {
						_ = n.rn.Step(r)
					}
				}
			}
			continue
		}
		if !IsEmptySnap(rd.Snapshot) {
			_ = n.s.ApplySnapshot(rd.Snapshot)
		}
		_ = n.s.Append(rd.Entries)
		if !IsEmptyHardState(rd.HardState) {
			_ = n.s.SetHardState(rd.HardState)
		}
		n.applyConfChanges(rd.CommittedEntries)
		n.rn.Advance(rd)
	}
}

func (n *adversaryNode) applyConfChanges(ents []*pb.Entry) {
	for _, e := range ents {
		var cc pb.ConfChangeI
		switch e.GetType() {
		case pb.EntryConfChange:
			c := &pb.ConfChange{}
			if proto.Unmarshal(e.Data, c) != nil {
				continue
			}
			cc = c
		case pb.EntryConfChangeV2:
			c := &pb.ConfChangeV2{}
			if proto.Unmarshal(e.Data, c) != nil {
				continue
			}
			cc = c
		default:
			continue
		}
		// A correct application validates committed configuration
		// changes as raft would; raft panics on one it cannot apply.
		if err := checkApplicable(n.rn, cc.AsV2()); err != nil {
			continue
		}
		n.rn.ApplyConfChange(cc)
	}
}

func checkApplicable(rn *RawNode, cc *pb.ConfChangeV2) error {
	cs := rn.raft.trk.ConfState()
	return ValidateConfChange(cs, cc)
}

// hostileMessage builds a message from fuzz input, near the node's state so
// that it gets past the term checks often.
func hostileMessage(src *byteSource, rn *RawNode) *pb.Message {
	r := rn.raft
	// A transport sets From to the authenticated sender: another node.
	var peers []uint64
	for _, id := range []uint64{1, 2, 3, 4, 5} {
		if id != r.id {
			peers = append(peers, id)
		}
	}
	ids := []uint64{1, 2, 3, 4, 5, 0, LocalAppendThread, LocalApplyThread}
	m := &pb.Message{
		Type:       pb.MessageType(pb.MessageType(src.intn(25))).Enum(),
		From:       new(peers[src.intn(len(peers))]),
		To:         new(r.id),
		Term:       new(src.near(r.Term)),
		LogTerm:    new(src.near(r.raftLog.lastEntryID().term)),
		Index:      new(src.near(r.raftLog.lastIndex())),
		Commit:     new(src.near(r.raftLog.committed)),
		Vote:       new(uint64(src.intn(5))),
		Reject:     new(bool(src.intn(3) == 0)),
		RejectHint: new(src.near(r.raftLog.lastIndex())),
	}
	if src.intn(8) == 0 {
		m.To = new(ids[src.intn(len(ids))])
	}
	if src.intn(3) == 0 {
		m.Context = []byte{src.byte()}
	}
	nents := src.intn(4)
	idx, term := m.GetIndex(), m.GetLogTerm()
	for i := 0; i < nents; i++ {
		e := &pb.Entry{Type: pb.EntryType(pb.EntryType(src.intn(4))).Enum()}
		if src.intn(4) == 0 {
			e.Index, e.Term = new(src.near(idx+1)), new(src.near(term))
		} else {
			e.Index, e.Term = new(idx+1), new(term+uint64(src.intn(2)))
		}
		idx, term = e.GetIndex(), e.GetTerm()
		switch e.GetType() {
		case pb.EntryConfChange:
			cc := &pb.ConfChange{Type: pb.ConfChangeType(pb.ConfChangeType(src.intn(5))).Enum(), NodeId: new(uint64(src.intn(7)))}
			e.Data, _ = proto.Marshal(cc)
		case pb.EntryConfChangeV2:
			cc := &pb.ConfChangeV2{Transition: pb.ConfChangeTransition(pb.ConfChangeTransition(src.intn(4))).Enum()}
			for j := src.intn(3); j > 0; j-- {
				cc.Changes = append(cc.Changes, &pb.ConfChangeSingle{Type: pb.ConfChangeType(pb.ConfChangeType(src.intn(5))).Enum(), NodeId: new(uint64(src.intn(7)))})
			}
			e.Data, _ = proto.Marshal(cc)
		}
		if src.intn(6) == 0 {
			e.Data = []byte{src.byte(), src.byte()}
		}
		m.Entries = append(m.Entries, e)
	}
	if m.GetType() == pb.MsgSnap || src.intn(10) == 0 {
		cs := &pb.ConfState{}
		for j := src.intn(4); j > 0; j-- {
			cs.Voters = append(cs.Voters, uint64(src.intn(6)))
		}
		for j := src.intn(3); j > 0; j-- {
			cs.Learners = append(cs.Learners, uint64(src.intn(6)))
		}
		if src.intn(4) == 0 {
			cs.VotersOutgoing = []uint64{uint64(src.intn(6))}
			cs.LearnersNext = []uint64{uint64(src.intn(6))}
		}
		m.Snapshot = &pb.Snapshot{Metadata: &pb.SnapshotMetadata{
			Index:     new(src.near(r.raftLog.lastIndex() + 5)),
			Term:      new(src.near(r.Term)),
			ConfState: cs,
		}}
		if src.intn(5) == 0 {
			m.Snapshot = nil
		}
	}
	if src.intn(12) == 0 {
		m.Responses = []*pb.Message{{Type: pb.MsgAppResp.Enum(), From: new(r.id), To: new(r.id)}}
	}
	return m
}

func runAdversary(t *testing.T, data []byte) {
	src := &byteSource{b: data}
	n := newAdversaryNode(t, src)
	for steps := 0; steps < 64 && !src.empty(); steps++ {
		m := hostileMessage(src, n.rn)
		func() {
			defer func() {
				if p := recover(); p != nil {
					if isTripwire(p) {
						return
					}
					t.Fatalf("panic stepping %s:\n%v", DescribeMessage(m, nil), p)
				}
			}()
			_ = n.rn.Step(m)
			n.handle()
			if src.intn(4) == 0 {
				n.rn.Tick()
				n.handle()
			}
		}()
	}
}

func FuzzStepHostile(f *testing.F) {
	for i := 0; i < 64; i++ {
		seed := make([]byte, 0, 200)
		for j := 0; j < 200; j++ {
			seed = append(seed, byte(i*31+j*17+j*j))
		}
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		// No message sequence may keep a node busy: a stuck input is
		// saved, then fails the run.
		done := make(chan struct{})
		defer close(done)
		go func() {
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				_ = os.WriteFile(filepath.Join(os.TempDir(), "raft-hostile-stuck.bin"), data, 0o644)
				panic(fmt.Sprintf("input %x has run for 10s", data))
			}
		}()
		runAdversary(t, data)
	})
}

type quietLogger struct{}

func (quietLogger) Debug(...any)              {}
func (quietLogger) Debugf(string, ...any)     {}
func (quietLogger) Error(...any)              {}
func (quietLogger) Errorf(string, ...any)     {}
func (quietLogger) Info(...any)               {}
func (quietLogger) Infof(string, ...any)      {}
func (quietLogger) Warning(...any)            {}
func (quietLogger) Warningf(string, ...any)   {}
func (quietLogger) Fatal(v ...any)            { panic(fmt.Sprint(v...)) }
func (quietLogger) Fatalf(f string, v ...any) { panic(fmt.Sprintf(f, v...)) }
func (quietLogger) Panic(v ...any)            { panic(fmt.Sprint(v...)) }
func (quietLogger) Panicf(f string, v ...any) { panic(fmt.Sprintf(f, v...)) }

// TestHostilePanicClasses drives nodes with random hostile message
// sequences, once through RawNode.Step and once around its validation,
// and reports which panics each path reaches. Through Step, only tripwires
// may remain.
func TestHostilePanicClasses(t *testing.T) {
	if testing.Short() {
		t.Skip("slow")
	}
	r := newRandSource(20261013)
	classes := map[bool]map[string]int{true: {}, false: {}}
	for i := 0; i < 20000; i++ {
		data := make([]byte, 64+r.Intn(400))
		r.Read(data)
		for _, validated := range []bool{true, false} {
			src := &byteSource{b: append([]byte(nil), data...)}
			n := newAdversaryNode(t, src)
			for steps := 0; steps < 64 && !src.empty(); steps++ {
				m := hostileMessage(src, n.rn)
				p := func() (p any) {
					defer func() { p = recover() }()
					if validated {
						_ = n.rn.Step(m)
					} else {
						_ = n.rn.raft.Step(m)
					}
					n.handle()
					return nil
				}()
				if p != nil {
					msg := fmt.Sprint(p)
					if i := strings.IndexAny(msg, "0123456789"); i > 0 {
						msg = msg[:i]
					}
					classes[validated][msg]++
					if validated && !isTripwire(p) {
						t.Errorf("panic through Step on %s: %v", DescribeMessage(m, nil), p)
					}
					break
				}
			}
		}
	}
	for _, validated := range []bool{false, true} {
		name := map[bool]string{false: "without validation", true: "through Step"}[validated]
		t.Logf("%s: %d panic classes", name, len(classes[validated]))
		for msg, n := range classes[validated] {
			t.Logf("  %6d  %s", n, msg)
		}
	}
}

func newRandSource(seed int64) *rand.Rand { return rand.New(rand.NewSource(seed)) }
