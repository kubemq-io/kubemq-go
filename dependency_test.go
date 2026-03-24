//go:build architecture

package kubemq

import (
	"os"
	"strings"
	"testing"
)

// TestDependencyCount parses go.mod and counts direct non-test dependencies
// excluding gRPC, protobuf, and OTel. The target is 0 extra dependencies.
func TestDependencyCount(t *testing.T) {
	data, err := os.ReadFile("go.mod")
	if err != nil {
		t.Fatalf("failed to read go.mod: %v", err)
	}

	allowedPrefixes := []string{
		"google.golang.org/grpc",
		"google.golang.org/protobuf",
		"github.com/kubemq-io/protobuf",
		"go.opentelemetry.io/otel",
		"github.com/stretchr/testify",
	}

	lines := strings.Split(string(data), "\n")
	inRequire := false
	directDeps := 0
	extraDeps := []string{}

	for _, line := range lines {
		trimmed := strings.TrimSpace(line)

		if trimmed == "require (" {
			inRequire = true
			continue
		}
		if trimmed == ")" {
			inRequire = false
			continue
		}
		if !inRequire {
			continue
		}
		if strings.Contains(trimmed, "// indirect") {
			continue
		}

		parts := strings.Fields(trimmed)
		if len(parts) < 2 {
			continue
		}
		dep := parts[0]

		allowed := false
		for _, prefix := range allowedPrefixes {
			if strings.HasPrefix(dep, prefix) {
				allowed = true
				break
			}
		}
		if !allowed {
			directDeps++
			extraDeps = append(extraDeps, dep)
		}
	}

	const maxExtraDeps = 5
	if directDeps > maxExtraDeps {
		t.Errorf("too many extra direct dependencies (%d > %d): %v", directDeps, maxExtraDeps, extraDeps)
	}
}
