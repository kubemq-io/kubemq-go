# KubeMQ Go SDK — Benchmarks

## Running Benchmarks

**Prerequisites:**
- KubeMQ server running on `localhost:50000` (or set via `KUBEMQ_HOST` / `KUBEMQ_PORT` env vars)
- Go 1.22+

**Run all benchmarks:**

```bash
go test -bench=. -benchmem -count=5 -timeout=10m ./benchmarks/
```

**Run a specific benchmark:**

```bash
go test -bench=BenchmarkPublishThroughput -benchmem -count=5 ./benchmarks/
```

**Generate CPU profile:**

```bash
go test -bench=BenchmarkPublishThroughput -cpuprofile=cpu.prof ./benchmarks/
go tool pprof cpu.prof
```

## Methodology

- **Hardware:** Results below measured on [document actual hardware].
- **Server:** KubeMQ server [version], single node, default configuration.
- **Payload:** 1KB unless noted.
- **Iterations:** `go test -bench` auto-calibrates iteration count. Reported as `-count=5` runs.
- **Warmup:** First 100 operations discarded (Go benchmark framework handles warmup internally).
- **Measurement:** All times are wall-clock. `testing.B` reports ns/op, MB/s, and allocs/op.

## Baseline Results

> Replace with actual numbers after running benchmarks on reference hardware.

| Benchmark | Payload | Metric | Result | Allocs/op |
|-----------|---------|--------|--------|-----------|
| PublishThroughput | 1KB | msgs/sec | TBD | TBD |
| PublishLatency | 1KB | mean ns/op | TBD | TBD |
| QueueRoundtrip | 1KB | mean ns/op | TBD | TBD |
| ConnectionSetup | N/A | mean ms | TBD | TBD |

### Optional Benchmarks

| Benchmark | Payload | Metric | Result |
|-----------|---------|--------|--------|
| PublishThroughput | 64B | msgs/sec | TBD |
| PublishThroughput | 64KB | msgs/sec | TBD |

## Interpreting Results

- **ns/op:** Nanoseconds per operation. Lower is better.
- **MB/s:** Throughput in megabytes per second. Higher is better. Only reported for benchmarks that call `b.SetBytes()`.
- **allocs/op:** Heap allocations per operation. Lower is better.
- **p50/p99:** Go's `testing.B` reports only mean ns/op. For true percentile analysis (p50, p99), use a histogram library (e.g., `github.com/HdrHistogram/hdrhistogram-go`) inside the benchmark, or use `benchstat` for statistical distribution analysis across `-count=N` runs. `benchstat` reports mean and confidence intervals, not true percentiles.

### Getting Statistical Latency Distribution

Go's `testing.B` reports average ns/op, not percentiles. For distribution analysis:

```bash
go test -bench=BenchmarkPublishLatency -benchtime=1000x -count=10 ./benchmarks/ | tee bench.txt
go install golang.org/x/perf/cmd/benchstat@latest
benchstat bench.txt
```

> **Note:** `benchstat` reports mean and confidence intervals across runs, not p50/p99 percentiles within a single run. For true percentile analysis, embed a histogram library in the benchmark and record per-operation latencies.

## Regression Detection

To compare benchmark results between versions:

```bash
# Baseline (current version)
go test -bench=. -count=5 ./benchmarks/ > old.txt

# After changes
go test -bench=. -count=5 ./benchmarks/ > new.txt

# Compare
benchstat old.txt new.txt
```

A regression is defined as a >10% increase in ns/op or allocs/op with statistical significance (p < 0.05).

## Batch Operations Guidance

| Parameter | Guidance |
|-----------|----------|
| Batch size | 10–100 messages per batch for optimal throughput |
| Max batch size | Limited by `MaxSendMessageSize` (default 100MB). In practice, 1000+ messages may exceed this with large payloads. |
| Optimal payload | Batch benefits are greatest with small messages (≤1KB). For large messages (>64KB), single sends may be equivalent. |
| Error handling | A batch failure may be partial. Check individual message results. |

## Buffer Pooling Policy

Per GS: Buffer pooling is recommended only when benchmarks demonstrate allocation pressure. The v2 SDK does NOT introduce `sync.Pool` at launch. If REQ-PERF-1 benchmarks reveal significant allocation pressure (e.g., >2 allocs/op for publish operations), buffer pooling can be added in a future minor version.
