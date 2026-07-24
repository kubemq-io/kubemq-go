# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [2.0.4] - 2026-07-24

### Security
- Update `google.golang.org/grpc` 1.81.1 → 1.82.1 — fixes GHSA-hrxh-6v49-42gf.
- Update `golang.org/x/net` 0.55.0 → 0.56.0 (CVE-2026-46600) and `golang.org/x/text` 0.37.0 → 0.39.0 (CVE-2026-56852) — DoS advisories.
- `go mod tidy` also bumped transitive `golang.org/x/sys` 0.45.0 → 0.46.0 and `google.golang.org/genproto/googleapis/rpc`. Burn-in Dockerfile builder image bumped `golang:1.23-alpine` → `golang:1.25-alpine`.

## [2.0.3] - 2026-05-31

### Added
- Environment variable override and API support for broker address (51837ed)

### Improvements
- Update gRPC 1.79.3 → 1.81.1, OpenTelemetry 1.42 → 1.44, and golang.org/x packages for security advisories (a22b72a)
- Refactor examples: subscription delay for stability, refined queue peeking logic (006654e)

## [2.0.0] - 2026-03-11

### Added
- v2 clean break: new module path `github.com/kubemq-io/kubemq-go/v2`
- Layered architecture: public API, protocol, transport layers
- `KubeMQError` structured error type with error codes and retryability
- Exponential backoff reconnection with `ReconnectPolicy`
- `CredentialProvider` interface for pluggable authentication
- OpenTelemetry tracing and metrics integration
- `RetryPolicy` with configurable backoff for transient failures
- Runtime version constant (`kubemq.Version`)
- Functional options API (`With*` pattern)

### Changed
- Module path: `github.com/kubemq-io/kubemq-go` → `github.com/kubemq-io/kubemq-go/v2`
- Protobuf: `gogo/protobuf` → `google.golang.org/protobuf`
- gRPC and protobuf types moved to `internal/transport/` (no longer leaked to public API)

### Removed
- REST transport (`rest.go`)
- WebSocket transport
- `TransportType` enum and related configuration
- Direct protobuf type embedding in public API types

[Unreleased]: https://github.com/kubemq-io/kubemq-go/compare/v2.0.0...HEAD
[2.0.0]: https://github.com/kubemq-io/kubemq-go/releases/tag/v2.0.0
