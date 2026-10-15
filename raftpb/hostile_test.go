// Copyright 2026 The raftseg authors. Licensed under the Apache License,
// Version 2.0.

package raftpb

// Decoding is the first thing that touches bytes from a peer or from disk.
// These tests bound what damaged and hostile input can cost, and fuzz the
// decoder. Decoding is google.golang.org/protobuf's (proto.Unmarshal).

import (
	"bytes"
	"encoding/binary"
	"math"
	"runtime"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
)

// nestedMessage encodes a Message whose Responses nest depth levels deep.
func nestedMessage(depth int) []byte {
	buf := make([]byte, depth*11)
	pos, size := len(buf), 0
	var tmp [binary.MaxVarintLen64]byte
	for i := 0; i < depth; i++ {
		n := binary.PutUvarint(tmp[:], uint64(size))
		pos -= n
		copy(buf[pos:], tmp[:n])
		pos--
		buf[pos] = 14<<3 | 2 // field 14 (responses), length-delimited
		size += n + 1
	}
	return buf[pos:]
}

func depthOf(m *Message) int {
	d := 0
	for p := m; len(p.GetResponses()) > 0; p = p.GetResponses()[0] {
		d++
	}
	return d
}

// Decoding is recursive in the nesting of Responses. protobuf bounds the
// recursion (proto.UnmarshalOptions.RecursionLimit, 10,000 by default), so
// that nesting cannot run a decoder out of stack, which Go cannot recover
// from: the gogo code of v3.6.0 recursed without bound, and about 40 MB of
// nesting killed any process that decoded it. raft nests one level
// (MsgStorageAppend and MsgStorageApply carry the messages to send), and a
// peer sends none: ValidateMessage refuses Responses from peers.
func TestNestingLimit(t *testing.T) {
	for _, d := range []int{0, 1, 2, 100, 9_000} {
		var m Message
		if err := proto.Unmarshal(nestedMessage(d), &m); err != nil {
			t.Fatalf("depth %d: %v", d, err)
		}
		if got := depthOf(&m); got != d {
			t.Fatalf("depth %d decoded as %d", d, got)
		}
	}
	for _, d := range []int{10_001, 1_000_000} {
		var m Message
		if err := proto.Unmarshal(nestedMessage(d), &m); err == nil {
			t.Fatalf("depth %d: accepted", d)
		}
	}
	// A transport can set a tighter limit.
	opts := proto.UnmarshalOptions{RecursionLimit: 4}
	var m Message
	if err := opts.Unmarshal(nestedMessage(3), &m); err != nil {
		t.Fatalf("depth 3 with limit 4: %v", err)
	}
	if err := opts.Unmarshal(nestedMessage(4), &m); err == nil {
		t.Fatal("depth 4 with limit 4: accepted")
	}
}

// 88 MB of nesting, eight million levels, is rejected without running out
// of stack.
func TestNestingCannotExhaustStack(t *testing.T) {
	if testing.Short() {
		t.Skip("allocates 88 MB")
	}
	b := nestedMessage(8_000_000)
	done := make(chan error)
	go func() {
		var m Message
		done <- proto.Unmarshal(b, &m)
	}()
	if err := <-done; err == nil {
		t.Fatal("accepted")
	}
}

// A length prefix larger than the input fails before allocating.
func TestHugeLengthsDoNotAllocate(t *testing.T) {
	type field struct {
		typ   string
		field uint64
	}
	fields := []field{
		{"Entry", 4}, {"Message", 7}, {"Message", 9}, {"Message", 12}, {"Message", 14},
		{"Snapshot", 1}, {"Snapshot", 2}, {"SnapshotMetadata", 1}, {"ConfState", 1},
		{"ConfState", 2}, {"ConfState", 3}, {"ConfState", 4}, {"ConfChange", 4},
		{"ConfChangeV2", 2}, {"ConfChangeV2", 3},
	}
	for _, f := range fields {
		for _, n := range []uint64{1 << 20, 1 << 31, 1<<62 + 1, 1<<63 - 1, 1 << 63, ^uint64(0)} {
			b := binary.AppendUvarint(nil, f.field<<3|2)
			b = binary.AppendUvarint(b, n)
			b = append(b, 1, 2, 3)
			allocated := allocatedBy(func() {
				if err := proto.Unmarshal(b, newGolden(f.typ)); err == nil {
					t.Fatalf("%s field %d length %d: accepted", f.typ, f.field, n)
				}
			})
			if allocated > 1024 {
				t.Fatalf("%s field %d length %d: allocated %d bytes", f.typ, f.field, n, allocated)
			}
		}
	}
}

// allocatedBy returns the bytes f allocates: the least of a few runs, as
// the runtime itself allocates now and then.
func allocatedBy(f func()) uint64 {
	best := uint64(math.MaxUint64)
	for i := 0; i < 5; i++ {
		var before, after runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&before)
		f()
		runtime.ReadMemStats(&after)
		best = min(best, after.TotalAlloc-before.TotalAlloc)
	}
	return best
}

// Decoding allocates in proportion to its input, but small encodings expand
// into large structs: an empty Message in Responses is 2 bytes on the wire
// and a whole Message in memory, a scalar field is a separate allocation
// behind its pointer, and slices grow by doubling. This measures the worst
// shapes, counting every allocation including outgrown slices. The limits
// sit about 15% above what the decoder allocates today, to catch
// regressions. A transport must size its message limit with these factors
// in mind; messages from peers never carry Responses.
func TestDecodeAmplification(t *testing.T) {
	if raceEnabled {
		t.Skip("the race detector changes how much decoding allocates")
	}
	const n = 100_000
	repeat := func(field uint64, elem []byte) []byte {
		var b []byte
		for i := 0; i < n; i++ {
			b = binary.AppendUvarint(b, field<<3|2)
			b = binary.AppendUvarint(b, uint64(len(elem)))
			b = append(b, elem...)
		}
		return b
	}
	packed := func(field uint64) []byte {
		body := bytes.Repeat([]byte{1}, n)
		b := binary.AppendUvarint(nil, field<<3|2)
		b = binary.AppendUvarint(b, uint64(len(body)))
		return append(b, body...)
	}
	unpacked := func(field uint64) []byte {
		var b []byte
		for i := 0; i < n; i++ {
			b = binary.AppendUvarint(b, field<<3)
			b = append(b, 1)
		}
		return b
	}
	cases := []struct {
		name  string
		typ   string
		input []byte
		limit float64 // allocated bytes per input byte
	}{
		{"Message: empty entries", "Message", repeat(7, nil), amplification["Message: empty entries"]},
		{"Message: empty responses", "Message", repeat(14, nil), amplification["Message: empty responses"]},
		{"ConfState: packed voters", "ConfState", packed(1), amplification["ConfState: packed voters"]},
		{"ConfState: unpacked voters", "ConfState", unpacked(1), amplification["ConfState: unpacked voters"]},
		{"ConfChangeV2: empty changes", "ConfChangeV2", repeat(2, nil), amplification["ConfChangeV2: empty changes"]},
		{"Entry: data", "Entry", repeat(4, []byte("x")), amplification["Entry: data"]},
		{"Message: entries with data", "Message", repeat(7, []byte{4<<3 | 2, 1, 'x'}), amplification["Message: entries with data"]},
		{"Message: entries with scalars", "Message", repeat(7, []byte{1 << 3, 1, 2 << 3, 1, 3 << 3, 1}), amplification["Message: entries with scalars"]},
	}
	for _, c := range cases {
		var err error
		allocated := allocatedBy(func() { err = proto.Unmarshal(c.input, newGolden(c.typ)) })
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		ratio := float64(allocated) / float64(len(c.input))
		t.Logf("%-30s %8d input bytes, %10d allocated, %6.1fx", c.name, len(c.input), allocated, ratio)
		if ratio > c.limit {
			t.Errorf("%s: %.1f bytes allocated per input byte, limit %.0f", c.name, ratio, c.limit)
		}
	}
}

// amplification holds the limits for TestDecodeAmplification: bytes
// allocated per input byte, about 15% above what the decoder allocates.
var amplification = map[string]float64{
	"Message: empty entries":        82,  // measured 70.5
	"Message: empty responses":      146, // 126.5; the gogo code of v3.6.0 allocated about 490
	"ConfState: packed voters":      10,  // 8.0
	"ConfState: unpacked voters":    24,  // 20.5
	"ConfChangeV2: empty changes":   63,  // 54.5
	"Entry: data":                   4,   // 2.7
	"Message: entries with data":    35,  // 29.8
	"Message: entries with scalars": 24,  // 20.6
}

// fuzzMessage maps the fuzzer's first byte to a message type.
func fuzzMessage(sel byte) (string, proto.Message) {
	typ := goldenTypes[int(sel)%len(goldenTypes)]
	return typ, newGolden(typ)
}

func addFuzzSeeds(f *testing.F) {
	for i, g := range readGolden(f) {
		if i%7 != 0 {
			continue
		}
		for s, typ := range goldenTypes {
			if typ == g.Type {
				f.Add(byte(s), mustHex(f, g.Hex))
			}
		}
	}
	for i, c := range readMalformed(f) {
		if i%13 != 0 {
			continue
		}
		for s, typ := range goldenTypes {
			if typ == c.Type {
				f.Add(byte(s), mustHex(f, c.Hex))
			}
		}
	}
	f.Add(byte(1), nestedMessage(9_999))
	f.Add(byte(1), nestedMessage(10_001))
}

func FuzzUnmarshal(f *testing.F) {
	addFuzzSeeds(f)
	f.Fuzz(func(t *testing.T, sel byte, data []byte) {
		typ, m := fuzzMessage(sel)
		if err := proto.Unmarshal(data, m); err != nil {
			return
		}
		checkDecoded(t, typ, m)
	})
}

// Decoding into a message that already holds data (merging) appends to
// repeated fields and overwrites the rest; it must never fail differently
// from decoding into an empty message.
func FuzzUnmarshalInto(f *testing.F) {
	addFuzzSeeds(f)
	merge := proto.UnmarshalOptions{Merge: true}
	f.Fuzz(func(t *testing.T, sel byte, data []byte) {
		typ, m := fuzzMessage(sel)
		half := len(data) / 2
		err1 := merge.Unmarshal(data[:half], m)
		err2 := merge.Unmarshal(data[half:], m)
		err3 := proto.Unmarshal(data[half:], newGolden(typ))
		if (err2 == nil) != (err3 == nil) {
			t.Fatalf("%s: into used message: %v, into empty: %v", typ, err2, err3)
		}
		if err1 == nil && err2 == nil {
			checkDecoded(t, typ, m)
		}
	})
}

func FuzzConfChangesFromString(f *testing.F) {
	for _, s := range []string{"", "v1", "v1 v2 l3 r4", "u5", "v18446744073709551615", "v1  v2", "x1", "v", "v-1", "l1 r1 v1"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		ccs, err := ConfChangesFromString(s)
		if err != nil {
			return
		}
		again, err := ConfChangesFromString(ConfChangesToString(ccs))
		if err != nil {
			t.Fatalf("%q: rendering %q does not parse: %v", s, ConfChangesToString(ccs), err)
		}
		if len(again) != len(ccs) {
			t.Fatalf("%q: round trip %v != %v", s, again, ccs)
		}
		for i := range ccs {
			if !proto.Equal(again[i], ccs[i]) {
				t.Fatalf("%q: round trip %v != %v", s, again, ccs)
			}
		}
		if !strings.Contains(s, " ") && len(ccs) > 1 {
			t.Fatalf("%q: %d changes from one token", s, len(ccs))
		}
	})
}
