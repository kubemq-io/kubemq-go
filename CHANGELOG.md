# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
