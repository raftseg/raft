// Copyright 2026 The raftseg authors. Licensed under the Apache License,
// Version 2.0.

package raftpb

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"encoding/json"
	"os"
	"testing"
)

// golden.jsonl was produced by the original go.etcd.io/raft/v3 v3.6.0
// raftpb (built on gogo/protobuf), with the generator kept in
// testdata/gengolden.go.txt. For every message it holds the encoding, the
// String() output, the encoding/json output and Size().

type goldenMsg interface {
	Marshal() ([]byte, error)
	Unmarshal([]byte) error
	Size() int
	String() string
}

func newGolden(typ string) goldenMsg {
	switch typ {
	case "Entry":
		return &Entry{}
	case "Message":
		return &Message{}
	case "HardState":
		return &HardState{}
	case "ConfState":
		return &ConfState{}
	case "Snapshot":
		return &Snapshot{}
	case "SnapshotMetadata":
		return &SnapshotMetadata{}
	case "ConfChange":
		return &ConfChange{}
	case "ConfChangeSingle":
		return &ConfChangeSingle{}
	case "ConfChangeV2":
		return &ConfChangeV2{}
	}
	return nil
}

type goldenLine struct {
	Type string `json:"type"`
	Hex  string `json:"hex"`
	Text string `json:"text"`
	JSON string `json:"json"`
	Size int    `json:"size"`
	// Encoding, by the original, of the message decoded from JSON.
	JSONHex string `json:"jsonhex"`
}

func readGolden(t testing.TB) []goldenLine {
	f, err := os.Open("testdata/golden.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var out []goldenLine
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<24)
	for sc.Scan() {
		var g goldenLine
		if err := json.Unmarshal(sc.Bytes(), &g); err != nil {
			t.Fatal(err)
		}
		out = append(out, g)
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// Decoding the original's bytes, then encoding again, gives the same bytes,
// the same text, the same JSON and the same size.
func TestGoldenCompatibility(t *testing.T) {
	lines := readGolden(t)
	if len(lines) < 3000 {
		t.Fatalf("only %d golden messages", len(lines))
	}
	counts := map[string]int{}
	for i, g := range lines {
		m := newGolden(g.Type)
		if m == nil {
			t.Fatalf("line %d: unknown type %q", i+1, g.Type)
		}
		raw, err := hex.DecodeString(g.Hex)
		if err != nil {
			t.Fatal(err)
		}
		if err := m.Unmarshal(raw); err != nil {
			t.Fatalf("line %d (%s): unmarshal: %v", i+1, g.Type, err)
		}
		again, err := m.Marshal()
		if err != nil {
			t.Fatalf("line %d (%s): marshal: %v", i+1, g.Type, err)
		}
		if !bytes.Equal(again, raw) {
			t.Fatalf("line %d (%s): encoding differs\n got %x\nwant %x", i+1, g.Type, again, raw)
		}
		if m.Size() != g.Size || m.Size() != len(raw) {
			t.Fatalf("line %d (%s): Size %d, original %d, encoded %d", i+1, g.Type, m.Size(), g.Size, len(raw))
		}
		if s := m.String(); s != g.Text {
			t.Fatalf("line %d (%s): String differs\n got %q\nwant %q", i+1, g.Type, s, g.Text)
		}
		j, err := json.Marshal(m)
		if err != nil {
			t.Fatal(err)
		}
		if string(j) != g.JSON {
			t.Fatalf("line %d (%s): JSON differs\n got %s\nwant %s", i+1, g.Type, j, g.JSON)
		}
		counts[g.Type]++
	}
	t.Logf("%d golden messages: %v", len(lines), counts)
}

// String() of in-memory messages, including ones that encoding would
// normalise (empty versus nil slices, unset optional pointers), matches the
// original's text format.
func TestGoldenText(t *testing.T) {
	if len(goldenText) < 500 {
		t.Fatalf("only %d text cases", len(goldenText))
	}
	for i, c := range goldenText {
		if s := c.m.String(); s != c.text {
			t.Fatalf("case %d: String differs\n got %q\nwant %q", i, s, c.text)
		}
	}
}

// The original's JSON decodes into the message the original decodes it into
// (enum fields go through UnmarshalJSON). JSON omits empty bytes fields, so
// that is not always the message the JSON was made from.
func TestGoldenJSONRoundTrip(t *testing.T) {
	for i, g := range readGolden(t) {
		m := newGolden(g.Type)
		if err := json.Unmarshal([]byte(g.JSON), m); err != nil {
			t.Fatalf("line %d (%s): %v", i+1, g.Type, err)
		}
		raw, _ := hex.DecodeString(g.JSONHex)
		b, err := m.Marshal()
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(b, raw) {
			t.Fatalf("line %d (%s): JSON round trip changed the encoding\n got %x\nwant %x", i+1, g.Type, b, raw)
		}
	}
}
