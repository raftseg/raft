// Copyright 2026 The raftseg authors. Licensed under the Apache License,
// Version 2.0.

package raftpb_test

import (
	"bytes"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/pluginpb"

	"github.com/raftseg/raft/raftpb"
)

const goPackage = "github.com/raftseg/raft/raftpb"

// TestGoPackage checks that raft.proto and the descriptor compiled into
// raft.pb.go name this fork's package.
func TestGoPackage(t *testing.T) {
	fdp := protodesc.ToFileDescriptorProto(raftpb.File_raft_proto)
	if got := fdp.GetOptions().GetGoPackage(); got != goPackage {
		t.Errorf("raft.pb.go's descriptor has go_package %q; want %q", got, goPackage)
	}
	src, err := os.ReadFile("raft.proto")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(src, []byte(`option go_package="`+goPackage+`";`)) {
		t.Errorf("raft.proto does not set go_package %q", goPackage)
	}
}

// TestGeneratedCode checks that raft.pb.go is what protoc-gen-go, from the
// google.golang.org/protobuf module this module requires, generates for
// the descriptor compiled into it, apart from comments: the descriptor
// carries no comments, which come from raft.proto. So raft.pb.go holds no
// code a person wrote. It runs `go build`, which needs the module cache.
//
// To regenerate raft.pb.go after changing raft.proto without protoc, set
// RAFTPB_REGENERATE to a file name: the generated code is written there,
// to merge with the current file's comments.
func TestGeneratedCode(t *testing.T) {
	if testing.Short() {
		t.Skip("builds protoc-gen-go")
	}
	current, err := os.ReadFile("raft.pb.go")
	if err != nil {
		t.Fatal(err)
	}
	// The compiler version in the header is a comment; use the same.
	var compiler *pluginpb.Version
	if m := regexp.MustCompile(`(?m)^// \tprotoc +v(\d+)\.(\d+)\.(\d+)$`).FindSubmatch(current); m != nil {
		compiler = &pluginpb.Version{Major: atoi(m[1]), Minor: atoi(m[2]), Patch: atoi(m[3])}
	}
	bin := filepath.Join(t.TempDir(), "protoc-gen-go")
	build := exec.Command("go", "build", "-o", bin, "google.golang.org/protobuf/cmd/protoc-gen-go")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("building protoc-gen-go: %v\n%s", err, out)
	}
	req := &pluginpb.CodeGeneratorRequest{
		FileToGenerate:  []string{"raft.proto"},
		ProtoFile:       []*descriptorpb.FileDescriptorProto{protodesc.ToFileDescriptorProto(raftpb.File_raft_proto)},
		CompilerVersion: compiler,
	}
	in, err := proto.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	gen := exec.Command(bin)
	gen.Stdin = bytes.NewReader(in)
	out, err := gen.Output()
	if err != nil {
		t.Fatalf("protoc-gen-go: %v", err)
	}
	var resp pluginpb.CodeGeneratorResponse
	if err := proto.Unmarshal(out, &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Error != nil || len(resp.File) != 1 {
		t.Fatalf("protoc-gen-go: %q, %d files", resp.GetError(), len(resp.File))
	}
	generated := []byte(resp.File[0].GetContent())
	if name := os.Getenv("RAFTPB_REGENERATE"); name != "" {
		if err := os.WriteFile(name, generated, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, got := withoutComments(t, generated), withoutComments(t, current)
	if want != got {
		wl, gl := strings.Split(want, "\n"), strings.Split(got, "\n")
		for i := 0; i < len(wl) || i < len(gl); i++ {
			var w, g string
			if i < len(wl) {
				w = wl[i]
			}
			if i < len(gl) {
				g = gl[i]
			}
			if w != g {
				t.Fatalf("raft.pb.go differs from protoc-gen-go's output at line %d (comments removed):\ngenerated: %s\nfile:      %s", i+1, w, g)
			}
		}
	}
}

// withoutComments formats Go source with its comments removed.
func withoutComments(t *testing.T, src []byte) string {
	f, err := parser.ParseFile(token.NewFileSet(), "raft.pb.go", src, parser.SkipObjectResolution)
	if err != nil {
		t.Fatal(err)
	}
	var b bytes.Buffer
	if err := format.Node(&b, token.NewFileSet(), f); err != nil {
		t.Fatal(err)
	}
	return b.String()
}

func atoi(b []byte) *int32 {
	n := int32(0)
	for _, c := range b {
		n = n*10 + int32(c-'0')
	}
	return &n
}
