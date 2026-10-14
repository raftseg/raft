// Copyright 2026 The raftseg authors. Licensed under the Apache License,
// Version 2.0.

package raftpb

// Standard-library replacements for the protobuf runtime helpers the
// generated code used: enum names, enum JSON decoding and the compact text
// format of String(). Their output is identical to the original's; the
// golden corpus in testdata checks it.

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

// enumName is proto.EnumName: the value's name, or its number.
func enumName(names map[int32]string, v int32) string {
	if s, ok := names[v]; ok {
		return s
	}
	return strconv.Itoa(int(v))
}

// unmarshalJSONEnum is proto.UnmarshalJSONEnum: a JSON string naming a
// value, or a JSON number.
func unmarshalJSONEnum(values map[string]int32, data []byte, enumName string) (int32, error) {
	if len(data) > 0 && data[0] == '"' {
		var name string
		if err := json.Unmarshal(data, &name); err != nil {
			return 0, err
		}
		v, ok := values[name]
		if !ok {
			return 0, fmt.Errorf("unrecognized enum %s value %q", enumName, name)
		}
		return v, nil
	}
	var v int32
	if err := json.Unmarshal(data, &v); err != nil {
		return 0, fmt.Errorf("cannot unmarshal %#q into enum %s", data, enumName)
	}
	return v, nil
}

// compactTextString renders a message in the protobuf compact text format,
// as golang/protobuf's CompactTextString did for these types: fields in
// declaration order, each followed by a space; scalars and enums only when
// non-zero; bytes when non-nil; repeated fields once per element; nested
// messages as <...> when non-zero (or non-nil, for pointers).
func compactTextString(m any) string {
	v := reflect.ValueOf(m)
	if !v.IsValid() || (v.Kind() == reflect.Pointer && v.IsNil()) {
		return "<nil>"
	}
	var b strings.Builder
	writeTextMessage(&b, reflect.Indirect(v))
	return b.String()
}

var stringerType = reflect.TypeOf((*fmt.Stringer)(nil)).Elem()

func writeTextMessage(b *strings.Builder, v reflect.Value) {
	t := v.Type()
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		tag := f.Tag.Get("protobuf")
		if tag == "" {
			continue
		}
		name := ""
		for _, part := range strings.Split(tag, ",") {
			if n, ok := strings.CutPrefix(part, "name="); ok {
				name = n
			}
		}
		fv := v.Field(i)
		switch {
		case fv.Kind() == reflect.Slice && f.Type.Elem().Kind() == reflect.Uint8:
			if fv.IsNil() {
				continue
			}
			b.WriteString(name)
			b.WriteByte(':')
			writeQuoted(b, fv.Bytes())
			b.WriteByte(' ')
		case fv.Kind() == reflect.Slice:
			for j := 0; j < fv.Len(); j++ {
				b.WriteString(name)
				b.WriteByte(':')
				writeTextValue(b, fv.Index(j))
				b.WriteByte(' ')
			}
		case fv.Kind() == reflect.Pointer:
			if fv.IsNil() {
				continue
			}
			b.WriteString(name)
			b.WriteByte(':')
			writeTextValue(b, fv.Elem())
			b.WriteByte(' ')
		default:
			if fv.IsZero() {
				continue
			}
			b.WriteString(name)
			b.WriteByte(':')
			writeTextValue(b, fv)
			b.WriteByte(' ')
		}
	}
}

func writeTextValue(b *strings.Builder, v reflect.Value) {
	switch {
	case v.Kind() == reflect.Struct:
		b.WriteByte('<')
		writeTextMessage(b, v)
		b.WriteByte('>')
	case v.Kind() == reflect.Int32 && v.Type().Implements(stringerType):
		b.WriteString(v.Interface().(fmt.Stringer).String())
	case v.Kind() == reflect.Bool:
		b.WriteString(strconv.FormatBool(v.Bool()))
	case v.Kind() == reflect.Uint64 || v.Kind() == reflect.Uint32:
		b.WriteString(strconv.FormatUint(v.Uint(), 10))
	default:
		fmt.Fprint(b, v.Interface())
	}
}

// writeQuoted quotes bytes like the protobuf text format: \n \r \t \" \\
// escaped, other bytes outside printable ASCII as three-digit octal.
func writeQuoted(b *strings.Builder, p []byte) {
	b.WriteByte('"')
	for _, c := range p {
		switch c {
		case '\n':
			b.WriteString(`\n`)
		case '\r':
			b.WriteString(`\r`)
		case '\t':
			b.WriteString(`\t`)
		case '"':
			b.WriteString(`\"`)
		case '\\':
			b.WriteString(`\\`)
		default:
			if c >= 0x20 && c < 0x7f {
				b.WriteByte(c)
			} else {
				fmt.Fprintf(b, `\%03o`, c)
			}
		}
	}
	b.WriteByte('"')
}
