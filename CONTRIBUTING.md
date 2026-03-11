# Contributing to KubeMQ Go SDK

Thank you for your interest in contributing to the KubeMQ Go SDK!

## Development Setup

### Prerequisites

- Go 1.23 or later
- KubeMQ server (for integration tests):
  `docker run -d -p 50000:50000 kubemq/kubemq`
- golangci-lint: `go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest`

### Building

```bash
git clone https://github.com/kubemq-io/kubemq-go.git
cd kubemq-go
go build ./...
```

### Running Tests

```bash
# Unit tests only (no server required — default)
go test ./...

# Unit tests with race detector
go test -race ./...

# Integration tests (requires running KubeMQ server)
go test -tags integration -race -timeout 5m -v ./...
```

### Linting

```bash
golangci-lint run
```

## Coding Standards

- Follow [Effective Go](https://go.dev/doc/effective_go) and
  [Go Code Review Comments](https://github.com/golang/go/wiki/CodeReviewComments)
- All exported symbols must have GoDoc comments (enforced by CI)
- Use `gofmt` for formatting (enforced by CI)
- Write tests for new functionality (target: ≥80% coverage)
- All examples in `examples/` must compile

## Pull Request Process

1. Fork the repository and create your branch from `master`
2. Add tests for any new functionality
3. Ensure unit tests pass: `go test ./...`
4. Ensure integration tests pass (if applicable): `go test -tags integration -race -timeout 5m ./...`
5. Ensure linting passes: `golangci-lint run`
6. Ensure examples compile: `go build ./examples/...`
7. Update documentation if you changed public APIs
8. Create a pull request with a clear description

### PR Template

```
## Summary
Brief description of the change.

## Type of Change
- [ ] Bug fix
- [ ] New feature
- [ ] Breaking change
- [ ] Documentation
- [ ] Refactoring

## Breaking Changes
{describe what breaks and the migration path, or "N/A"}

## Testing
- [ ] Unit tests added/updated
- [ ] Integration tests added/updated
- [ ] All existing tests pass
```

### Commit Messages

Follow [Conventional Commits](https://www.conventionalcommits.org/) — see
[CONVENTIONAL_COMMITS.md](https://github.com/kubemq-io/kubemq-go/blob/master/CONVENTIONAL_COMMITS.md) for project-specific guidance:

```
feat: add mTLS support
fix: prevent nil pointer in response handling
docs: update quick start for v2 API
test: add unit tests for retry interceptor
refactor: move gRPC transport to internal package
```

## Reporting Issues

Please file issues on
[GitHub Issues](https://github.com/kubemq-io/kubemq-go/issues) with:

1. SDK version (`go list -m github.com/kubemq-io/kubemq-go/v2`)
2. Go version (`go version`)
3. KubeMQ server version
4. Steps to reproduce
5. Expected vs actual behavior
6. Error messages (full output)

## Code of Conduct

Be respectful and constructive. We follow the
[Contributor Covenant](https://www.contributor-covenant.org/version/2/0/code_of_conduct/).
