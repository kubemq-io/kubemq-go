# Conventional Commits

This project uses the [Conventional Commits](https://www.conventionalcommits.org/) format for commit messages. This convention helps maintain a clear history and enables automated changelog generation.

## Format

```
type(scope): description

[optional body]

[optional footer(s)]
```

## Types

| Type | SemVer Impact | Example |
|------|---------------|---------|
| `feat` | MINOR | `feat(queues): add batch receive` |
| `fix` | PATCH | `fix(transport): handle reconnect race` |
| `docs` | none | `docs: update CHANGELOG for v2.1.0` |
| `test` | none | `test(events): add store subscribe test` |
| `chore` | none | `chore: update CI Go version matrix` |
| `refactor` | none | `refactor(middleware): simplify retry loop` |
| `perf` | none | `perf(transport): reduce allocation in send` |
| `BREAKING CHANGE` | MAJOR | Footer `BREAKING CHANGE: remove Event.Id field` |

## Scopes

Optional scopes for this SDK:

- `transport` — connection, gRPC, reconnection
- `middleware` — retry, auth, otel, errmap
- `queues` — queue operations
- `events` — event pub/sub
- `commands` — command pub/sub
- `queries` — query pub/sub
- `auth` — credential provider, TLS
- `otel` — tracing, metrics
- `ci` — workflows, tooling

## Examples

```
feat(queues): add batch receive
fix(transport): handle reconnect race
docs: update CHANGELOG for v2.1.0
test(events): add store subscribe test
chore: update CI Go version matrix
refactor(middleware): simplify retry loop
perf(transport): reduce allocation in send
```

### Breaking Changes

Use the `BREAKING CHANGE` footer for MAJOR version bumps:

```
feat(queues): remove Message.ID field

BREAKING CHANGE: remove Event.Id field in favor of Event.MessageId
```

## Enforcement

This convention is **recommended**, not enforced by CI. Enforcement is left to code review. Adding commit linting would require Node.js tooling, which violates the minimal-dependency principle for this Go SDK.

## Reference

- **Owning spec:** 11-packaging-spec.md (REQ-PKG-4)
- **Inclusion:** Spec 06 (documentation) includes this in CONTRIBUTING.md when that file is created.
