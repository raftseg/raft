// Copyright 2026 The raftseg authors. Licensed under the Apache License,
// Version 2.0.

package raftpb

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"runtime"
	"runtime/debug"
	"strings"
	"testing"
)

// Decoding is the first thing that touches bytes from a peer or from disk.
// These tests hold the fork's decoder to the original's behaviour on damaged
// and hostile input, and bound what such input can cost.

var goldenTypes = []string{"Entry", "Message", "HardState", "ConfState", "Snapshot",
	"SnapshotMetadata", "ConfChange", "ConfChangeSingle", "ConfChangeV2"}

type malformedLine struct {
	Type string `json:"type"`
	Kind string `json:"kind"`
	Hex  string `json:"hex"`
	Err  string `json:"err"`
	Re   string `json:"re"`
	Text string `json:"text"`
	Size int    `json:"size"`
}

func readMalformed(t testing.TB) []malformedLine {
	f, err := os.Open("testdata/malformed.jsonl.gz")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	zr, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	var out []malformedLine
	sc := bufio.NewScanner(zr)
	sc.Buffer(make([]byte, 1<<20), 1<<24)
	for sc.Scan() {
		var m malformedLine
		if err := json.Unmarshal(sc.Bytes(), &m); err != nil {
			t.Fatal(err)
		}
		out = append(out, m)
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// For about 21,000 damaged and hostile inputs (truncations, bit flips,
// unknown fields of every wire type, huge and negative lengths, overlong and
// padded varints, wrong wire types, random bytes), the fork returns the
// original's error, or decodes to the same message.
func TestMalformedMatchesOriginal(t *testing.T) {
	lines := readMalformed(t)
	if len(lines) < 20000 {
		t.Fatalf("only %d malformed inputs", len(lines))
	}
	rejected, accepted := 0, 0
	for i, c := range lines {
		raw, err := hex.DecodeString(c.Hex)
		if err != nil {
			t.Fatal(err)
		}
		m := newGolden(c.Type)
		err = m.Unmarshal(raw)
		if c.Err != "" {
			if err == nil {
				t.Fatalf("line %d (%s, %s): original failed with %q, fork accepted %x", i+1, c.Type, c.Kind, c.Err, raw)
			}
			if err.Error() != c.Err {
				t.Fatalf("line %d (%s, %s): error %q, original %q", i+1, c.Type, c.Kind, err, c.Err)
			}
			rejected++
			continue
		}
		if err != nil {
			t.Fatalf("line %d (%s, %s): original accepted, fork failed: %v", i+1, c.Type, c.Kind, err)
		}
		re, err := m.Marshal()
		if err != nil {
			t.Fatal(err)
		}
		if hex.EncodeToString(re) != c.Re || m.Size() != c.Size || m.String() != c.Text {
			t.Fatalf("line %d (%s, %s): decoded differently\n got %x %q\nwant %s %q", i+1, c.Type, c.Kind, re, m.String(), c.Re, c.Text)
		}
		accepted++
	}
	t.Logf("%d inputs: %d rejected with the original's error, %d accepted identically", len(lines), rejected, accepted)
}

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

func TestNestingLimit(t *testing.T) {
	for _, d := range []int{0, 1, 2, MaxMessageNesting} {
		var m Message
		if err := m.Unmarshal(nestedMessage(d)); err != nil {
			t.Fatalf("depth %d: %v", d, err)
		}
		got := 0
		for p := &m; len(p.Responses) > 0; p = &p.Responses[0] {
			got++
		}
		if got != d {
			t.Fatalf("depth %d decoded as %d", d, got)
		}
	}
	for _, d := range []int{MaxMessageNesting + 1, 1000, 1_000_000} {
		var m Message
		if err := m.Unmarshal(nestedMessage(d)); !errors.Is(err, ErrNestingTooDeep) {
			t.Fatalf("depth %d: got %v, want ErrNestingTooDeep", d, err)
		}
	}
	// Responses nested inside a valid sibling list are counted too.
	inner := nestedMessage(MaxMessageNesting)
	var outer []byte
	outer = append(outer, 14<<3|2)
	outer = binary.AppendUvarint(outer, 0)
	outer = append(outer, 14<<3|2)
	outer = binary.AppendUvarint(outer, uint64(len(inner)))
	outer = append(outer, inner...)
	var m Message
	if err := m.Unmarshal(outer); !errors.Is(err, ErrNestingTooDeep) {
		t.Fatalf("sibling then deep: got %v", err)
	}
}

// The original decoder recursed once per nesting level, and Go cannot
// recover from a stack overflow. With the limit, 40 MB of nesting (the
// amount that used to kill the process) is rejected on a 1 MiB stack.
func TestNestingCannotExhaustStack(t *testing.T) {
	if testing.Short() {
		t.Skip("allocates 40 MB")
	}
	b := nestedMessage(8_000_000)
	if len(b) < 30<<20 {
		t.Fatalf("input only %d bytes", len(b))
	}
	old := debug.SetMaxStack(1 << 20)
	defer debug.SetMaxStack(old)
	done := make(chan error)
	go func() {
		var m Message
		done <- m.Unmarshal(b)
	}()
	if err := <-done; !errors.Is(err, ErrNestingTooDeep) {
		t.Fatalf("got %v", err)
	}
}

// A length prefix larger than the input must fail before allocating.
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
				if err := newGolden(f.typ).Unmarshal(b); err == nil {
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
// and a whole Message in memory, and slices grow by doubling. This measures
// the worst shapes, counting every allocation including outgrown slices
// (about 490 bytes per input byte for empty Responses, 140 for empty
// Entries). The limits sit about 15% above what this decoder, the original
// generated code, allocates today, to catch regressions. A transport must
// size its message limit with these factors in mind; messages from peers
// never carry Responses.
func TestDecodeAmplification(t *testing.T) {
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
		{"Message: empty entries", "Message", repeat(7, nil), 160},
		{"Message: empty responses", "Message", repeat(14, nil), 560},
		{"ConfState: packed voters", "ConfState", packed(1), 16},
		{"ConfState: unpacked voters", "ConfState", unpacked(1), 24},
		{"ConfChangeV2: empty changes", "ConfChangeV2", repeat(2, nil), 52},
		{"Entry: data", "Entry", repeat(4, []byte("x")), 4},
		{"Message: entries with data", "Message", repeat(7, []byte{4<<3 | 2, 1, 'x'}), 66},
	}
	for _, c := range cases {
		var err error
		allocated := allocatedBy(func() { err = newGolden(c.typ).Unmarshal(c.input) })
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

// fuzzTypes maps the fuzzer's first byte to a message type.
func fuzzMessage(sel byte) (string, goldenMsg) {
	typ := goldenTypes[int(sel)%len(goldenTypes)]
	return typ, newGolden(typ)
}

// checkDecoded holds for anything Unmarshal accepts: it encodes without
// error to exactly Size() bytes, that encoding is canonical (decoding and
// encoding it again changes nothing), and String and encoding/json work.
func checkDecoded(t *testing.T, typ string, m goldenMsg) {
	t.Helper()
	b, err := m.Marshal()
	if err != nil {
		t.Fatalf("%s: marshal after successful unmarshal: %v", typ, err)
	}
	if len(b) != m.Size() {
		t.Fatalf("%s: Size %d, encoded %d bytes", typ, m.Size(), len(b))
	}
	m2 := newGolden(typ)
	if err := m2.Unmarshal(b); err != nil {
		t.Fatalf("%s: re-encoding does not decode: %v", typ, err)
	}
	b2, err := m2.Marshal()
	if err != nil || !bytes.Equal(b, b2) {
		t.Fatalf("%s: encoding not canonical\n%x\n%x (%v)", typ, b, b2, err)
	}
	if m.String() != m2.String() {
		t.Fatalf("%s: String changed across a round trip\n%s\n%s", typ, m.String(), m2.String())
	}
	if _, err := json.Marshal(m); err != nil {
		t.Fatalf("%s: json: %v", typ, err)
	}
}

func addFuzzSeeds(f *testing.F) {
	for i, g := range readGolden(f) {
		if i%7 != 0 {
			continue
		}
		raw, _ := hex.DecodeString(g.Hex)
		for s, typ := range goldenTypes {
			if typ == g.Type {
				f.Add(byte(s), raw)
			}
		}
	}
	for i, c := range readMalformed(f) {
		if i%13 != 0 {
			continue
		}
		raw, _ := hex.DecodeString(c.Hex)
		for s, typ := range goldenTypes {
			if typ == c.Type {
				f.Add(byte(s), raw)
			}
		}
	}
	f.Add(byte(1), nestedMessage(MaxMessageNesting))
	f.Add(byte(1), nestedMessage(MaxMessageNesting+1))
}

func FuzzUnmarshal(f *testing.F) {
	addFuzzSeeds(f)
	f.Fuzz(func(t *testing.T, sel byte, data []byte) {
		typ, m := fuzzMessage(sel)
		if err := m.Unmarshal(data); err != nil {
			return
		}
		checkDecoded(t, typ, m)
	})
}

// Decoding into a message that already holds data appends to repeated
// fields and overwrites the rest, as the original does; it must never fail
// differently from decoding into an empty message.
func FuzzUnmarshalInto(f *testing.F) {
	addFuzzSeeds(f)
	f.Fuzz(func(t *testing.T, sel byte, data []byte) {
		typ, m := fuzzMessage(sel)
		half := len(data) / 2
		err1 := m.Unmarshal(data[:half])
		err2 := m.Unmarshal(data[half:])
		fresh := newGolden(typ)
		err3 := fresh.Unmarshal(data[half:])
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
		if fmt.Sprint(again) != fmt.Sprint(ccs) {
			t.Fatalf("%q: round trip %v != %v", s, again, ccs)
		}
		if !strings.Contains(s, " ") && len(ccs) > 1 {
			t.Fatalf("%q: %d changes from one token", s, len(ccs))
		}
	})
}
