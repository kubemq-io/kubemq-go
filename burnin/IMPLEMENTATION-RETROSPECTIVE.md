# Burn-In Test Implementation Retrospective

> **Purpose**: Lessons learned from the Go SDK burn-in implementation. Feed this document to the next SDK implementation (Java, Python, JS, C#) to avoid repeating the same mistakes.
>
> **Reference Implementation**: `kubemq-go/burnin/`
> **Spec Version**: `sdk-burnin-test-requirements.md` v1.2

---

## Critical Issues Found During Development

### 1. Prometheus Counter Double-Counting (Severity: Critical)

**What happened**: The gap detection method returned *cumulative* lost counts. The engine added this value to a Prometheus counter on every periodic tick, causing the counter to grow geometrically (5 losses became 10, then 15, etc.).

**Root cause**: Confusion between "current total" and "delta since last report" semantics.

**Fix**: `DetectGaps()` must track `lastReportedLost` per producer and return only the delta.

**Rule for other SDKs**: Any periodic task that feeds a Prometheus counter must compute and add **deltas**, never cumulative totals. Prometheus counters are monotonically increasing — calling `.Add(total)` repeatedly doubles the count.

---

### 2. 2-Phase Shutdown: Null Pointer on StopProducers (Severity: Critical)

**What happened**: The `Worker` interface required `StopProducers()` and `StopConsumers()` for 2-phase shutdown. `BaseWorker` declared `producerCancel` but no worker's `Start()` method ever initialized it. Calling `StopProducers()` invoked a nil function pointer.

**Root cause**: Interface contract defined at the base level but initialization left to each concrete worker. Every worker forgot.

**Fix**: Every worker's `Start()` must create `producerCtx, producerCancel = context.WithCancel(ctx)` and pass `producerCtx` to producer goroutines.

**Rule for other SDKs**: When adding 2-phase shutdown, ensure **every** pattern worker initializes the producer-specific cancellation context. Add a compilation check or constructor enforcement. In languages with constructors (Java, C#), do it in the base class. In Python, do it in `__init__`. Don't leave it to each subclass.

---

### 3. Memory Baseline Captured Too Early (Severity: Critical)

**What happened**: The spec says baseline RSS is measured at the 5-minute mark. The code had a fallback condition `if baselineRSS == 0 { set it now }` that fired on the very first tick (10 seconds in), so the 5-minute condition never activated.

**Rule for other SDKs**: The memory baseline must ONLY be set at the 5-minute mark. If the test is shorter than 5 minutes, set the baseline at shutdown as a fallback. Never use "set on first sample" as it defeats the purpose of letting the runtime stabilize.

---

## High-Severity Issues

### 4. Metrics Declared But Never Incremented

**What happened**: Several Prometheus metrics were registered in `metrics.go` with helper functions, but the helper functions were never called anywhere:
- `burnin_reconnection_duplicates_total` — helper existed, never called
- `burnin_downtime_seconds_total` — helper existed, never called
- `burnin_active_connections` — gauge declared, never set
- `burnin_consumer_group_balance_ratio` — gauge declared, never set

**Rule for other SDKs**: After declaring all metrics, **grep for each helper function** to verify it has at least one call site. A metric that is registered but always returns 0 is worse than not having it — it gives false confidence. Add a verification step to the implementation checklist: "grep for every metric helper and confirm call sites."

---

### 5. RPC-Specific Fields Not Populated in Summary JSON

**What happened**: The `/summary` JSON and final report had fields for `responses_success`, `responses_timeout`, `responses_error`, and RPC latency percentiles. These fields existed in the struct but were never populated — always zero.

**Root cause**: Workers tracked RPC outcomes via Prometheus counters but not via in-process atomics. The summary builder only had access to in-process counters, not Prometheus.

**Rule for other SDKs**: For every field in the `/summary` JSON structure, trace the data path from the worker to the report builder. If a field exists in the struct, it must be populated. Maintain **dual tracking** (Prometheus + in-process atomic) for any metric that appears in both `/metrics` and `/summary`.

---

### 6. Warmup Only Verified Events, Not All 6 Patterns

**What happened**: The spec requires sending 10 warmup messages per pattern and verifying receipt. The initial implementation only did this for Events and Events Store. Queue and RPC patterns fell back to a simple `Ping()`.

**Rule for other SDKs**: Implement warmup verification for **every** pattern:
- Events/Events Store: temporary subscription + send + verify receipt
- Queue Stream/Simple: send queue messages + receive them back
- Commands: temporary responder + send command + verify response
- Queries: temporary responder + send query + verify response

---

### 7. Sent Counter Incremented Before Error Check (Commands/Queries)

**What happened**: In the RPC sender loop, `sent.Add(1)` was called before checking whether the `SendCommand`/`SendQuery` call returned an error. Failed or timed-out commands were counted as "sent", inflating the sent count.

**Rule for other SDKs**: The sent counter must ONLY be incremented **after** confirming the transport call succeeded. For RPC patterns: increment after `err == nil`. For Events Store: increment after `Result.Sent == true`.

---

### 8. `--cleanup-only` Used Run-Specific Prefix Instead of Broad SDK Prefix

**What happened**: The cleanup-only mode used `go_burnin_{runID}_` as the channel prefix, meaning it could only clean channels from the current run. The spec requires cleaning all channels matching `{sdk}_burnin_*`.

**Rule for other SDKs**: `--cleanup-only` must use the broad prefix `{sdk}_burnin_` (no run ID) to catch channels from **any** previous run. The startup stale channel cleanup should also use the broad prefix.

---

## Medium-Severity Issues

### 9. Error Rate Formula Used Wrong Denominator

**Spec**: `errors / (sent + received) * 100`
**Bug**: Code used `errors / sent * 100` (missing `+ received`)

**Rule**: Copy the formula exactly from the spec. Don't simplify.

---

### 10. Shutdown Did Not Separate Producer and Consumer Phases

**Spec**: Stop producers → wait drain → stop consumers.
**Bug**: All workers were stopped simultaneously via a single `cancel()`.

**Rule**: Every SDK must implement 2-phase shutdown: producer cancellation first, drain timeout, then consumer cancellation. This requires separate cancellation mechanisms for producers vs consumers.

---

### 11. Periodic Status Format Did Not Match Spec for RPC Patterns

**Spec**: Commands/queries status lines use `resp=` and `tout=` instead of `recv=`.
**Bug**: All patterns used the same `sent=/recv=/lost=/dup=` format.

**Rule**: Check the spec's Section 8.1 format **per pattern type**. RPC patterns have a different column layout than pub/sub patterns.

---

### 12. Report File Written After Console Print (Wrong Order)

**Spec Section 12.2**: Step 6 = write file, Step 7 = print console.
**Bug**: Console was printed first, file written second.

**Rule**: Follow the spec's shutdown sequence numbering exactly.

---

### 13. `PASSED_WITH_WARNINGS` Verdict State Missing

**Bug**: Only `PASSED` and `FAILED` were implemented. No advisory checks existed.

**Fix**: Add at least one advisory check (e.g., memory growth trending toward threshold) and implement the three-state verdict logic.

**Rule**: Implement all three verdict states from the start. Define at least one advisory check so the code path is exercised.

---

### 14. Reconnection Tracking Missing for Events/Commands/Queries Patterns

**Bug**: `burnin_reconnections_total` was only incremented for Queue Stream (on stream errors). Events, Commands, and Queries subscription closures were logged but not counted.

**Rule**: Every pattern's consumer/responder monitor must call both `metrics.IncReconnection()` AND the in-process reconnection counter when a subscription or stream closes unexpectedly.

---

### 15. Peak 10-Second Throughput Tracker Built But Never Wired

**Bug**: `PeakRateTracker` struct with `Record()`, `Advance()`, and `Peak()` was implemented in the metrics package but never instantiated in any worker. The peak throughput was never reported.

**Rule**: After building utility structs, immediately wire them into the worker/engine lifecycle. Don't leave them as dead code. The `PeakRateTracker` needs:
1. Instantiation in the worker constructor
2. `Record()` called on each send
3. `Advance()` called every 1 second from a ticker goroutine
4. `Peak()` read in the summary builder

---

## Implementation Checklist Additions for Other SDKs

Based on these findings, add these verification steps **after implementation, before declaring done**:

```
[ ] Every Prometheus metric helper function has at least one call site (grep verify)
[ ] Every field in the /summary JSON struct is populated (no zero-value fields for active patterns)
[ ] Sent counter only incremented after transport success confirmation
[ ] Gap detection returns deltas, not cumulative totals
[ ] Memory baseline set at 5-minute mark, not on first sample
[ ] 2-phase shutdown: producer stop → drain → consumer stop (verify with logging)
[ ] Warmup sends messages for ALL 6 patterns (not just events)
[ ] Periodic status format matches spec per pattern type (RPC vs pub/sub)
[ ] Error rate formula uses (sent + received) as denominator
[ ] --cleanup-only uses broad {sdk}_burnin_ prefix
[ ] Report file written before console print (spec Section 12.2 order)
[ ] PASSED_WITH_WARNINGS verdict state exists with at least one advisory check
[ ] All 3 exit codes work: 0 (PASSED), 1 (FAILED), 2 (config error)
[ ] Reconnection counter incremented for ALL patterns, not just queue stream
[ ] RPC counter (success/timeout/error) tracked both in Prometheus AND in-process
```

---

## Architecture Notes for Other SDKs

### Dual Tracking Pattern
Every metric that appears in both `/metrics` (Prometheus) AND `/summary` (JSON) needs dual tracking:
- Prometheus counter/histogram for scraping
- In-process atomic/accumulator for the report builder

Don't try to read Prometheus counters back — it's error-prone and creates import dependencies.

### Worker Interface Completeness
Define the Worker interface with ALL methods upfront before implementing any pattern. The Go implementation had to add methods (`ErrorCount`, `RPCSuccess`, `PeakRate`, etc.) incrementally, causing repeated compile-fix cycles.

### Context Hierarchy for Shutdown
```
rootCtx (from signal handler)
  └── workerCtx (w.cancel — cancels everything)
        └── producerCtx (w.producerCancel — cancels only producers)
```
Both contexts must be initialized in `Start()`. Consumers use `workerCtx`, producers use `producerCtx`.

---

## Issues Found During Live Broker Testing

### 16. Warmup Messages Sent with Wrong API

**What happened**: Warmup for events_store used `SendEvent` (fire-and-forget) instead of `SendEventStore` (persistent). The events_store subscriber never received warmup messages, causing warmup verification to time out and the test to abort.

**Rule**: Warmup messages MUST use the same API as the pattern being tested. Events → `SendEvent`, Events Store → `SendEventStore`, Queue → `SendQueueMessage`, Commands → `SendCommand`, Queries → `SendQuery`.

### 17. Warmup Messages Missing CRC Tag

**What happened**: `SendWarmupMessages` included `tags["warmup"]="true"` but no `tags["content_hash"]`. When warmup messages reached real consumers (who were already subscribed), the CRC check failed because the tag was missing.

**Rule**: ALL messages — including warmup — must include the `content_hash` tag. Consumer `RecordReceive` should skip warmup messages (check `producer_id == "warmup"` or `tags["warmup"] == "true"`), but defense-in-depth requires the CRC tag to be present anyway.

### 18. Warmup Race Condition: Real Responders See Warmup RPC Messages

**What happened**: For commands/queries warmup, the engine subscribes a temporary auto-responder, then sends a command/query. But the real worker responder is already subscribed on the same channel. Both receive the warmup message. The real responder must detect and skip warmup messages.

**Rule**: All responder callbacks (commands, queries) must check for `tags["warmup"] == "true"` and auto-respond without CRC validation. This must be the FIRST check in the callback, before any payload decoding or CRC verification.

### 19. Queue Stream Consumer Sending Get After Every Message (Batch Pileup)

**What happened**: The queue downstream returns up to `MaxItems` messages per Get request. Each message arrives individually on the `Messages` channel. The consumer was sending a new Get request after each individual message, creating a pileup of overlapping Get requests and causing massive lag (sent=4485, recv=834 at 30s).

**Fix**: After receiving the first message from a Get, drain all remaining messages from the batch (non-blocking channel read), THEN send the next Get request.

**Rule**: Queue Stream consumer must drain the entire batch before issuing the next Get. Pattern: `receive first msg → drain remaining → ack all → send next Get`.

### 20. Memory Baseline 0 Until 5-Minute Mark

**What happened**: The memory baseline was only set at the 5-minute mark. Before that, `baselineRSS=0`, making `peakRSS/baselineRSS` infinite. The memory stability check always failed in short tests.

**Fix**: Seed `baselineRSS` with the first sample value (initial estimate), then update it to the 5-minute value when available.

**Rule**: Always seed the baseline with an initial value. The 5-minute update refines it, but the initial seed prevents division by zero.

### 21. Queue Patterns: At-Least-Once Duplicates Are Normal

**Observation**: Queue patterns showed 54-139 duplicates over 15 minutes. These are visibility timeout redeliveries — expected at-least-once behavior, not application bugs.

**Rule**: The default `BURNIN_MAX_DUPLICATION_PCT` threshold of 0.1% may be too tight for queue patterns with at-least-once delivery. Consider pattern-specific thresholds or document that queue duplicates at low rates are expected.
