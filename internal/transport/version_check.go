package transport

import (
	"context"
	"strconv"
	"strings"
)

const (
	minTestedServerVersion = "2.2.0"
	maxTestedServerVersion = "2.99.99"
)

// checkServerVersion pings the server and logs a warning if the server version
// is outside the tested compatibility range. Runs asynchronously; connection proceeds normally.
func (t *grpcTransport) checkServerVersion(ctx context.Context) {
	info, err := t.Ping(ctx)
	if err != nil {
		t.logger.Warn("could not retrieve server version for compatibility check",
			"error", err,
		)
		return
	}

	sv := info.Version
	if sv == "" {
		t.logger.Warn("server did not report a version; compatibility unknown")
		return
	}

	if !isVersionInRange(sv, minTestedServerVersion, maxTestedServerVersion) {
		t.logger.Warn("server version outside tested compatibility range",
			"server_version", sv,
			"tested_min", minTestedServerVersion,
			"tested_max", maxTestedServerVersion,
			"action", "connection proceeds; consult COMPATIBILITY.md",
		)
	}
}

func isVersionInRange(version, minVer, maxVer string) bool {
	v := parseSimpleVersion(version)
	lo := parseSimpleVersion(minVer)
	hi := parseSimpleVersion(maxVer)
	if v == nil || lo == nil || hi == nil {
		return true // can't parse → don't warn
	}
	return !versionLessThan(v, lo) && !versionLessThan(hi, v)
}

type semverTriple struct {
	major, minor, patch int
}

func parseSimpleVersion(s string) *semverTriple {
	// Strip leading 'v' if present
	if s != "" && s[0] == 'v' {
		s = s[1:]
	}
	parts := strings.Split(s, ".")
	if len(parts) < 2 {
		return nil
	}
	maj, err1 := strconv.Atoi(parts[0])
	minor, err2 := strconv.Atoi(parts[1])
	pat := 0
	if len(parts) >= 3 {
		// Ignore pre-release suffix (e.g., "2.4.1-beta")
		numPart := parts[2]
		for i, c := range numPart {
			if c < '0' || c > '9' {
				numPart = numPart[:i]
				break
			}
		}
		pat, _ = strconv.Atoi(numPart)
	}
	if err1 != nil || err2 != nil {
		return nil
	}
	return &semverTriple{maj, minor, pat}
}

func versionLessThan(a, b *semverTriple) bool {
	if a.major != b.major {
		return a.major < b.major
	}
	if a.minor != b.minor {
		return a.minor < b.minor
	}
	return a.patch < b.patch
}
