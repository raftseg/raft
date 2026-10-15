// Copyright 2026 The raftseg authors. Licensed under the Apache License,
// Version 2.0.

// Package depcheck holds tests that keep the module's dependencies to the
// one it needs: google.golang.org/protobuf (Google's protobuf runtime, which
// upstream's raftpb is generated for). go.mod requires nothing else, go.sum
// lists nothing else but what that module's own go.mod requires, every Go
// file imports only the standard library, this module and protobuf, and so
// does every package that a build of this module, tests included, compiles.
package depcheck

import (
	"bufio"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

const modulePath = "github.com/raftseg/raft"

// allowed lists the modules this one may require, with the modules their
// go.mod files require in turn, which go.sum lists without compiling them.
var allowed = map[string][]string{
	"google.golang.org/protobuf": {"github.com/google/go-cmp", "github.com/golang/protobuf"},
}

func moduleRoot(t *testing.T) string {
	dir, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "go.mod")); err != nil {
		t.Fatal(err)
	}
	return dir
}

// goModLines returns the significant lines of go.mod: comments and blank
// lines dropped, require blocks flattened to "require <path> <version>".
func goModLines(t *testing.T) []string {
	f, err := os.Open(filepath.Join(moduleRoot(t), "go.mod"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var out []string
	block := ""
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if i := strings.Index(line, "//"); i >= 0 {
			line = strings.TrimSpace(line[:i])
		}
		switch {
		case line == "":
		case block != "" && line == ")":
			block = ""
		case block != "":
			out = append(out, block+" "+line)
		case strings.HasSuffix(line, "("):
			block = strings.TrimSpace(strings.TrimSuffix(line, "("))
		default:
			out = append(out, line)
		}
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// go.mod declares the module, a Go version and requirements on allowed
// modules, and nothing else: no replace, exclude, retract, tool or
// toolchain lines.
func TestGoModRequiresAllowedOnly(t *testing.T) {
	required := map[string]bool{}
	for _, line := range goModLines(t) {
		f := strings.Fields(line)
		switch f[0] {
		case "module":
			if line != "module "+modulePath {
				t.Errorf("go.mod: %q", line)
			}
		case "go":
		case "require":
			if len(f) != 3 {
				t.Errorf("go.mod: %q", line)
				continue
			}
			if _, ok := allowed[f[1]]; !ok {
				t.Errorf("go.mod requires %s, which is not allowed", f[1])
			}
			required[f[1]] = true
		default:
			t.Errorf("go.mod: %q is not allowed", line)
		}
	}
	for m := range allowed {
		if !required[m] {
			t.Errorf("go.mod does not require %s; drop it from the allowed list", m)
		}
	}
}

// go.sum lists only allowed modules and what their go.mod files require.
func TestGoSumAllowedOnly(t *testing.T) {
	ok := map[string]bool{}
	for m, deps := range allowed {
		ok[m] = true
		for _, d := range deps {
			ok[d] = true
		}
	}
	b, err := os.ReadFile(filepath.Join(moduleRoot(t), "go.sum"))
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		if f := strings.Fields(line); len(f) != 3 || !ok[f[0]] {
			t.Errorf("go.sum: %q", line)
		}
	}
}

// importAllowed reports whether an import path is in the standard library
// (no dot in its first element), this module or an allowed module.
func importAllowed(p string) bool {
	first, _, _ := strings.Cut(p, "/")
	return inAllowedModule(p) || !strings.Contains(first, ".")
}

// inAllowedModule reports whether a package path is in this module or an
// allowed module.
func inAllowedModule(p string) bool {
	if p == modulePath || strings.HasPrefix(p, modulePath+"/") {
		return true
	}
	for m := range allowed {
		if p == m || strings.HasPrefix(p, m+"/") {
			return true
		}
	}
	return false
}

// Every Go file, tests included and whatever its build tags, imports only
// the standard library, this module and allowed modules.
func TestImportsAllowedOnly(t *testing.T) {
	root := moduleRoot(t)
	files := 0
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			// Go ignores testdata, and directories starting with . or _.
			if path != root && (d.Name() == "testdata" || strings.HasPrefix(d.Name(), ".") || strings.HasPrefix(d.Name(), "_")) {
				return filepath.SkipDir
			}
			// A nested module is not part of this one.
			if path != root {
				if _, err := os.Stat(filepath.Join(path, "go.mod")); err == nil {
					return filepath.SkipDir
				}
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		files++
		f, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		for _, imp := range f.Imports {
			p, err := strconv.Unquote(imp.Path.Value)
			if err != nil {
				return err
			}
			if !importAllowed(p) {
				rel, _ := filepath.Rel(root, path)
				t.Errorf("%s imports %q", rel, p)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if files < 100 {
		t.Fatalf("only %d Go files found under %s", files, root)
	}
	t.Logf("%d Go files import only the standard library, %s and allowed modules", files, modulePath)
}

// Every package a build of this module compiles, tests included, comes from
// the standard library, this module or an allowed module: what the allowed
// modules import counts too. The go command says which packages are the
// standard library's, so any other package must be in this module or an
// allowed one, dot in its path or not (a go.work workspace can supply a
// module named "tools").
func TestCompiledPackagesAllowedOnly(t *testing.T) {
	cmd := exec.Command("go", "list", "-deps", "-test", "-f", "{{if not .Standard}}{{.ImportPath}}{{end}}", "./...")
	cmd.Dir = moduleRoot(t)
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("go list: %v (the module cache must hold the required modules)", err)
	}
	n := 0
	for _, p := range strings.Fields(string(out)) {
		p, _, _ = strings.Cut(p, " ") // test variants: "pkg [pkg.test]"
		if p == "" || strings.HasPrefix(p, "[") {
			continue
		}
		p = strings.TrimSuffix(p, ".test") // a package's test binary
		p = strings.TrimSuffix(p, "_test") // a package's external tests
		n++
		if !inAllowedModule(p) {
			t.Errorf("a build compiles %s", p)
		}
	}
	if n < 10 {
		t.Fatalf("only %d packages listed", n)
	}
}
