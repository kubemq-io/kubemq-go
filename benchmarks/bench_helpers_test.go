package benchmarks_test

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	kubemq "github.com/kubemq-io/kubemq-go/v2"
)

const (
	defaultBenchHost = "localhost"
	defaultBenchPort = 50000
	benchPayload     = 1024 // 1KB
	benchTimeout     = 10 * time.Second
)

func getBenchHost() string {
	if v := os.Getenv("KUBEMQ_HOST"); v != "" {
		return v
	}
	return defaultBenchHost
}

func getBenchPort() int {
	if v := os.Getenv("KUBEMQ_PORT"); v != "" {
		if p, err := strconv.Atoi(v); err == nil {
			return p
		}
	}
	return defaultBenchPort
}

// newBenchClient creates a KubeMQ client for benchmarks.
// Uses KUBEMQ_HOST and KUBEMQ_PORT env vars when set.
// Registers Cleanup to close the client when the benchmark ends.
func newBenchClient(tb testing.TB) *kubemq.Client {
	ctx, cancel := context.WithTimeout(context.Background(), benchTimeout)
	defer cancel()

	client, err := kubemq.NewClient(ctx,
		kubemq.WithAddress(getBenchHost(), getBenchPort()),
		kubemq.WithClientId("bench-"+tb.Name()),
	)
	if err != nil {
		tb.Fatalf("failed to create client: %v", err)
	}
	tb.Cleanup(func() { _ = client.Close() })
	return client
}

// makePayload creates a deterministic payload of the given size.
func makePayload(size int) []byte {
	b := make([]byte, size)
	for i := range b {
		b[i] = byte(i % 256)
	}
	return b
}

// TestBenchmarksMD_Exists verifies BENCHMARKS.md exists in repo root.
func TestBenchmarksMD_Exists(t *testing.T) {
	for _, path := range []string{"BENCHMARKS.md", filepath.Join("..", "BENCHMARKS.md")} {
		if _, err := os.Stat(path); err == nil {
			return
		}
	}
	t.Fatal("BENCHMARKS.md must exist in repo root")
}
