// Copyright 2026 The raftseg authors. Licensed under the Apache License,
// Version 2.0.

package raftpb

// Compatibility with the encoding etcd raft v3.6.0 wrote (gogo/protobuf
// generated code, which this fork carried until it took upstream's switch to
// google.golang.org/protobuf), and behaviour on damaged and hostile input.
//
// testdata/golden.jsonl holds 3,609 messages encoded by v3.6.0, each with
// its encoding/json rendering; testdata/malformed.jsonl.gz holds 21,654
// damaged and hostile inputs with what v3.6.0 made of them (the generators
// are kept next to the data as .go.txt files). The text format of String()
// and the encoding/json rendering changed with upstream's switch, and are
// not compared.

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

var goldenTypes = []string{"Entry", "Message", "HardState", "ConfState", "Snapshot",
	"SnapshotMetadata", "ConfChange", "ConfChangeSingle", "ConfChangeV2"}

func newGolden(typ string) proto.Message {
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
	JSON string `json:"json"`
	Size int    `json:"size"`
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

type malformedLine struct {
	Type string `json:"type"`
	Kind string `json:"kind"`
	Hex  string `json:"hex"`
	Err  string `json:"err"`
	Re   string `json:"re"`
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

func mustHex(t testing.TB, s string) []byte {
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// fieldFor finds the field a v3.6.0 encoding/json key names: the Go field
// name (Entry had no json tags) or the proto name.
func fieldFor(md protoreflect.MessageDescriptor, key string) protoreflect.FieldDescriptor {
	norm := func(s string) string { return strings.ToLower(strings.ReplaceAll(s, "_", "")) }
	fields := md.Fields()
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		if norm(string(fd.Name())) == norm(key) || norm(fd.JSONName()) == norm(key) {
			return fd
		}
	}
	return nil
}

// matchJSON checks that m holds what v3.6.0's encoding/json rendering obj
// says its message held, and nothing else. An enum value outside its enum
// is kept in the field, as v3.6.0 kept it (google.golang.org/protobuf, like
// gogo, does not enforce that proto2 enums are closed).
func matchJSON(path string, m protoreflect.Message, obj map[string]any) error {
	md := m.Descriptor()
	seen := map[protoreflect.FieldNumber]bool{}
	for key, raw := range obj {
		fd := fieldFor(md, key)
		if fd == nil {
			return fmt.Errorf("%s: no field for %q", path, key)
		}
		seen[fd.Number()] = true
		p := path + "." + string(fd.Name())
		switch {
		case raw == nil:
			if fd.IsList() && m.Get(fd).List().Len() != 0 {
				return fmt.Errorf("%s: want empty", p)
			}
		case fd.IsList():
			arr := raw.([]any)
			l := m.Get(fd).List()
			if l.Len() != len(arr) {
				return fmt.Errorf("%s: %d elements, want %d", p, l.Len(), len(arr))
			}
			for i, x := range arr {
				if fd.Kind() == protoreflect.MessageKind {
					if err := matchJSON(fmt.Sprintf("%s[%d]", p, i), l.Get(i).Message(), x.(map[string]any)); err != nil {
						return err
					}
				} else if got, want := l.Get(i).Uint(), mustUint(x); got != want {
					return fmt.Errorf("%s[%d] = %d, want %d", p, i, got, want)
				}
			}
		case fd.Kind() == protoreflect.MessageKind:
			if err := matchJSON(p, m.Get(fd).Message(), raw.(map[string]any)); err != nil {
				return err
			}
		case fd.Kind() == protoreflect.EnumKind:
			if got, want := uint64(m.Get(fd).Enum()), mustUint(raw); got != want {
				return fmt.Errorf("%s = %d, want %d", p, got, want)
			}
		case fd.Kind() == protoreflect.BoolKind:
			if got := m.Get(fd).Bool(); got != raw.(bool) {
				return fmt.Errorf("%s = %v, want %v", p, got, raw)
			}
		case fd.Kind() == protoreflect.BytesKind:
			want, err := base64.StdEncoding.DecodeString(raw.(string))
			if err != nil {
				return err
			}
			if got := m.Get(fd).Bytes(); !bytes.Equal(got, want) {
				return fmt.Errorf("%s = %x, want %x", p, got, want)
			}
		default:
			if got, want := m.Get(fd).Uint(), mustUint(raw); got != want {
				return fmt.Errorf("%s = %d, want %d", p, got, want)
			}
		}
	}
	// Whatever the JSON leaves out must be empty (encoding/json omitted
	// empty bytes, and nil pointers to messages).
	var err error
	m.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		if seen[fd.Number()] {
			return true
		}
		if (fd.Kind() == protoreflect.BytesKind && len(v.Bytes()) == 0) ||
			(fd.Kind() == protoreflect.MessageKind && !fd.IsList() && proto.Size(v.Message().Interface()) == 0) {
			return true
		}
		err = fmt.Errorf("%s.%s is set but absent from the JSON", path, fd.Name())
		return false
	})
	return err
}

func mustUint(x any) uint64 {
	var u uint64
	if _, err := fmt.Sscan(string(x.(json.Number)), &u); err != nil {
		panic(err)
	}
	return u
}

// hasUnknown reports whether m, or a message in it, holds unknown fields.
func hasUnknown(m protoreflect.Message) bool {
	found := len(m.GetUnknown()) > 0
	m.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		switch {
		case fd.IsList() && fd.Kind() == protoreflect.MessageKind:
			for i := 0; i < v.List().Len() && !found; i++ {
				found = hasUnknown(v.List().Get(i).Message())
			}
		case fd.Kind() == protoreflect.MessageKind:
			found = found || hasUnknown(v.Message())
		}
		return !found
	})
	return found
}

// Every message v3.6.0 encoded decodes, holds what v3.6.0 said it held, and
// encodes again to the same bytes, with the same size.
func TestGoldenCompatibility(t *testing.T) {
	lines := readGolden(t)
	if len(lines) < 3000 {
		t.Fatalf("only %d golden messages", len(lines))
	}
	counts := map[string]int{}
	for i, g := range lines {
		m := newGolden(g.Type)
		raw := mustHex(t, g.Hex)
		if err := proto.Unmarshal(raw, m); err != nil {
			t.Fatalf("line %d (%s): unmarshal: %v", i+1, g.Type, err)
		}
		dec := json.NewDecoder(strings.NewReader(g.JSON))
		dec.UseNumber()
		var obj map[string]any
		if err := dec.Decode(&obj); err != nil {
			t.Fatal(err)
		}
		if err := matchJSON(g.Type, m.ProtoReflect(), obj); err != nil {
			t.Fatalf("line %d: %v", i+1, err)
		}
		again, err := proto.Marshal(m)
		if err != nil {
			t.Fatalf("line %d (%s): marshal: %v", i+1, g.Type, err)
		}
		if proto.Size(m) != len(again) || len(again) != g.Size {
			t.Fatalf("line %d (%s): Size %d, encoded %d, original %d", i+1, g.Type, proto.Size(m), len(again), g.Size)
		}
		if !bytes.Equal(again, raw) {
			t.Fatalf("line %d (%s): encoding differs\n got %x\nwant %x", i+1, g.Type, again, raw)
		}
		counts[g.Type]++
	}
	t.Logf("%d golden messages decode as v3.6.0 decoded them and re-encode byte for byte: %v", len(lines), counts)
}

// stripUnknown removes unknown fields from m and the messages in it.
func stripUnknown(m protoreflect.Message) {
	m.SetUnknown(nil)
	m.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		switch {
		case fd.IsList() && fd.Kind() == protoreflect.MessageKind:
			for i := 0; i < v.List().Len(); i++ {
				stripUnknown(v.List().Get(i).Message())
			}
		case fd.Kind() == protoreflect.MessageKind:
			stripUnknown(v.Message())
		}
		return true
	})
}

// normalize clears fields holding zero values, and messages left empty, so
// that a field v3.6.0 always wrote compares equal to one left out.
func normalize(m protoreflect.Message) {
	m.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		switch {
		case fd.IsList():
			if fd.Kind() == protoreflect.MessageKind {
				for i := 0; i < v.List().Len(); i++ {
					normalize(v.List().Get(i).Message())
				}
			}
			if v.List().Len() == 0 {
				m.Clear(fd)
			}
		case fd.Kind() == protoreflect.MessageKind:
			normalize(v.Message())
			if proto.Size(v.Message().Interface()) == 0 && len(v.Message().GetUnknown()) == 0 {
				m.Clear(fd)
			}
		case fd.Kind() == protoreflect.BytesKind:
			if len(v.Bytes()) == 0 {
				m.Clear(fd)
			}
		case fd.Kind() == protoreflect.BoolKind:
			if !v.Bool() {
				m.Clear(fd)
			}
		case fd.Kind() == protoreflect.EnumKind:
			if v.Enum() == 0 {
				m.Clear(fd)
			}
		default:
			if v.Uint() == 0 {
				m.Clear(fd)
			}
		}
		return true
	})
}

// For 21,654 damaged and hostile inputs (truncations, bit flips, unknown
// fields of every wire type, huge and negative lengths, overlong and padded
// varints, wrong wire types, random bytes):
//   - what v3.6.0 accepted decodes, to what v3.6.0 decoded it to, apart from
//     the unknown fields protobuf now keeps where v3.6.0 dropped them; or,
//     if it is not valid wire format (a field number above 2^29-1, a varint
//     longer than 64 bits), which v3.6.0 let through, it is rejected;
//   - what v3.6.0 rejected is rejected, or decodes with unknown fields:
//     google.golang.org/protobuf keeps a field of the wrong wire type as an
//     unknown field where gogo's generated code failed. No message decodes
//     from such input with known fields v3.6.0 would not have set.
func TestMalformedAgainstOriginal(t *testing.T) {
	lines := readMalformed(t)
	if len(lines) < 20000 {
		t.Fatalf("only %d malformed inputs", len(lines))
	}
	var bothAccept, bothReject, nowAccepted int
	nowRejected := map[string]int{}
	kinds := map[string]int{}
	for i, c := range lines {
		raw := mustHex(t, c.Hex)
		m := newGolden(c.Type)
		err := proto.Unmarshal(raw, m)
		if c.Err == "" {
			if err != nil {
				if why := wireError(m.ProtoReflect().Descriptor(), raw); why != "" {
					// Stricter than v3.6.0, which accepted some invalid
					// wire format: field numbers above 2^29-1, which it
					// skipped as unknown fields, and varints longer than
					// 64 bits, whose extra bits it dropped.
					nowRejected[why]++
					continue
				}
				t.Fatalf("line %d (%s, %s): v3.6.0 accepted %x, this fails: %v", i+1, c.Type, c.Kind, raw, err)
			}
			want := newGolden(c.Type)
			if err := proto.Unmarshal(mustHex(t, c.Re), want); err != nil {
				t.Fatalf("line %d: v3.6.0's re-encoding does not decode: %v", i+1, err)
			}
			got := proto.Clone(m)
			for _, x := range []proto.Message{got, want} {
				stripUnknown(x.ProtoReflect())
				normalize(x.ProtoReflect())
			}
			if !proto.Equal(got, want) {
				t.Fatalf("line %d (%s, %s): decoded differently from v3.6.0\n got %v\nwant %v", i+1, c.Type, c.Kind, got, want)
			}
			bothAccept++
			continue
		}
		if err != nil {
			bothReject++
			continue
		}
		if !hasUnknown(m.ProtoReflect()) {
			t.Fatalf("line %d (%s, %s): v3.6.0 rejected %x (%s); this decodes it, without unknown fields, as %v", i+1, c.Type, c.Kind, raw, c.Err, m)
		}
		nowAccepted++
		kinds[c.Kind]++
	}
	t.Logf("%d inputs: %d accepted by both alike, %d rejected by both, %d now decoded with unknown fields (by kind: %v), now rejected as invalid wire format: %v",
		len(lines), bothAccept, bothReject, nowAccepted, kinds, nowRejected)
}

// wireError returns why b, read as message md, is not valid protobuf wire
// format (a field number outside protobuf's range, a varint longer than 64
// bits, ...) at any depth of known message fields, or "" if it is valid.
func wireError(md protoreflect.MessageDescriptor, b []byte) string {
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return protowire.ParseError(n).Error()
		}
		if num > protowire.MaxValidNumber {
			return "field number above 2^29-1"
		}
		l := protowire.ConsumeFieldValue(num, typ, b[n:])
		if l < 0 {
			return protowire.ParseError(l).Error()
		}
		if fd := md.Fields().ByNumber(num); fd != nil && fd.Kind() == protoreflect.MessageKind && typ == protowire.BytesType {
			inner, _ := protowire.ConsumeBytes(b[n:])
			if e := wireError(fd.Message(), inner); e != "" {
				return e
			}
		}
		b = b[n+l:]
	}
	return ""
}

// checkDecoded holds for anything Unmarshal accepts: it encodes without
// error to exactly Size bytes, encoding again after decoding that changes
// nothing, and the text and JSON formats work.
func checkDecoded(t *testing.T, typ string, m proto.Message) {
	t.Helper()
	b, err := proto.Marshal(m)
	if err != nil {
		t.Fatalf("%s: marshal after successful unmarshal: %v", typ, err)
	}
	if len(b) != proto.Size(m) {
		t.Fatalf("%s: Size %d, encoded %d bytes", typ, proto.Size(m), len(b))
	}
	m2 := newGolden(typ)
	if err := proto.Unmarshal(b, m2); err != nil {
		t.Fatalf("%s: re-encoding does not decode: %v", typ, err)
	}
	if !proto.Equal(m, m2) {
		t.Fatalf("%s: round trip changed the message", typ)
	}
	b2, err := proto.Marshal(m2)
	if err != nil || !bytes.Equal(b, b2) {
		t.Fatalf("%s: encoding not stable\n%x\n%x (%v)", typ, b, b2, err)
	}
	_ = fmt.Sprint(m)
	if _, err := protojson.Marshal(m); err != nil {
		t.Fatalf("%s: json: %v", typ, err)
	}
}
