//go:build architecture

package kubemq

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestNoProtoInPublicAPI verifies that no public API file in the root
// package imports gRPC or protobuf packages directly. This enforces
// the layer boundary: only internal/transport/ may import those packages.
func TestNoProtoInPublicAPI(t *testing.T) {
	forbiddenPrefixes := []string{
		"google.golang.org/grpc",
		"google.golang.org/protobuf",
		"github.com/kubemq-io/kubemq-go/v2/pb",
		"github.com/gogo/protobuf",
		"go.opencensus.io",
		"github.com/go-resty/resty",
		"github.com/gorilla/websocket",
		"go.uber.org/atomic",
	}

	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("failed to read directory: %v", err)
	}

	fset := token.NewFileSet()
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasSuffix(name, ".go") {
			continue
		}
		if strings.HasSuffix(name, "_test.go") {
			continue
		}

		f, err := parser.ParseFile(fset, name, nil, parser.ImportsOnly)
		if err != nil {
			t.Errorf("failed to parse %s: %v", name, err)
			continue
		}

		for _, imp := range f.Imports {
			importPath := strings.Trim(imp.Path.Value, `"`)
			for _, prefix := range forbiddenPrefixes {
				if strings.HasPrefix(importPath, prefix) {
					t.Errorf("file %s imports forbidden package %q (violates layer boundary)", name, importPath)
				}
			}
		}
	}
}

// TestInternalDirectoryExists verifies the internal package structure.
func TestInternalDirectoryExists(t *testing.T) {
	dirs := []string{
		"internal/transport",
		"internal/middleware",
		"internal/types",
		"internal/testutil",
		"internal/otel",
	}
	for _, dir := range dirs {
		if _, err := os.Stat(dir); os.IsNotExist(err) {
			t.Errorf("expected directory %s to exist", dir)
		}
	}
}

// TestTransportInterfaceExists verifies that the Transport interface is defined.
func TestTransportInterfaceExists(t *testing.T) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "internal/transport/transport.go", nil, parser.ParseComments)
	if err != nil {
		t.Fatalf("failed to parse internal/transport/transport.go: %v", err)
	}

	found := false
	ast.Inspect(f, func(n ast.Node) bool {
		ts, ok := n.(*ast.TypeSpec)
		if ok && ts.Name.Name == "Transport" {
			if _, isIface := ts.Type.(*ast.InterfaceType); isIface {
				found = true
			}
		}
		return true
	})

	if !found {
		t.Error("Transport interface not found in internal/transport/transport.go")
	}
}

// TestNoPublicAPIImportsGRPC scans all .go files in the root directory
// (excluding test files and internal/) and verifies none import gRPC.
func TestNoPublicAPIImportsGRPC(t *testing.T) {
	matches, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatalf("failed to glob: %v", err)
	}

	fset := token.NewFileSet()
	for _, name := range matches {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}

		f, err := parser.ParseFile(fset, name, nil, parser.ImportsOnly)
		if err != nil {
			t.Errorf("failed to parse %s: %v", name, err)
			continue
		}

		for _, imp := range f.Imports {
			importPath := strings.Trim(imp.Path.Value, `"`)
			if strings.HasPrefix(importPath, "google.golang.org/grpc") {
				t.Errorf("%s: imports gRPC package %q — public API must not import gRPC", name, importPath)
			}
		}
	}
}

// TestFileMaxLines checks that no .go file in the root package exceeds
// the 500-line hard limit.
func TestFileMaxLines(t *testing.T) {
	const hardLimit = 500
	matches, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatalf("failed to glob: %v", err)
	}

	for _, name := range matches {
		data, err := os.ReadFile(name)
		if err != nil {
			t.Errorf("failed to read %s: %v", name, err)
			continue
		}
		lines := strings.Count(string(data), "\n") + 1
		if lines > hardLimit {
			t.Errorf("%s has %d lines, exceeding the %d-line hard limit", name, lines, hardLimit)
		}
	}
}
