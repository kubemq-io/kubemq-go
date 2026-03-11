# KubeMQ Go SDK — Compatibility Matrix

## Client SDK ↔ Server Version

| SDK Version | Server ≥ 2.2 | Server ≥ 2.4 | Notes |
|-------------|:------------:|:------------:|-------|
| v1.x        | ✅           | ✅           | Legacy, EOL 12 months after v2 GA |
| v2.0.x      | ✅           | ✅           | Initial v2 release |

> **Legend:** ✅ = tested and supported, ⚠️ = untested (may work), ❌ = known incompatible
>
> **Note:** Server 2.2+ is the minimum tested version. Earlier server versions (2.0, 2.1) may work but are not included in the CI test matrix.

## Go Version Support

| SDK Version | Go 1.23 | Go 1.24 | Notes |
|-------------|:-------:|:-------:|-------|
| v2.0.x      | ✅      | ✅      | Latest 2 releases per Go policy |

See also: [Go Release Policy](https://go.dev/doc/devel/release#policy)

## How We Test

- CI runs against the Go version matrix listed above.
- Integration tests run against the oldest and newest supported KubeMQ server versions.
- The SDK warns (but does not fail) when connecting to a server outside the tested range.

## Updating This Matrix

When a new server version or Go release is published:
1. Update the matrix table above.
2. Update CI workflows to include the new version.
3. Run the full test suite against the new version.
4. Tag a patch release if compatibility adjustments are needed.

## Version Support & End-of-Life

### Policy

When a new major SDK version reaches General Availability (GA):
- The **previous major version** receives security patches for **12 months**.
- After 12 months, the previous major version is **End-of-Life (EOL)**.
- EOL versions receive no further updates, including security fixes.

### Current Status

| SDK Version | Status | Support Ends | Notes |
|-------------|--------|-------------|-------|
| v2.x        | **Active** | — | Current major version |
| v1.x        | **Security patches only** | 12 months after v2.0.0 GA | See [Migration Guide](./MIGRATION.md) |

### What "Security Patches Only" Means

- Critical and high-severity CVEs in SDK code or direct dependencies are patched.
- No new features, enhancements, or non-security bug fixes.
- Patches are released as v1.x.y patch versions.

### Migration Resources

- [Migration Guide (v1 → v2)](./MIGRATION.md) — owned by 06-documentation-spec.md
- [CHANGELOG](./CHANGELOG.md) — owned by 11-packaging-spec.md
