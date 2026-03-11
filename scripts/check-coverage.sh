#!/usr/bin/env bash
# scripts/check-coverage.sh — Coverage threshold enforcement
# Owned by: 04-testing-spec.md (REQ-TEST-5)
set -euo pipefail

THRESHOLD="${1:-40}"

echo "Running tests with coverage..."
go test -race -count=1 -timeout 30s -coverprofile=coverage.out -coverpkg=./... ./...

echo "Filtering generated code..."
grep -v '\.pb\.go' coverage.out > coverage-filtered.out || cp coverage.out coverage-filtered.out
grep -v '_gen\.go' coverage-filtered.out > coverage-clean.out || cp coverage-filtered.out coverage-clean.out
mv coverage-clean.out coverage.out

echo ""
go tool cover -func=coverage.out

TOTAL=$(go tool cover -func=coverage.out | grep total | awk '{print substr($3, 1, length($3)-1)}')
echo ""
echo "Total coverage: ${TOTAL}%"
echo "Threshold: ${THRESHOLD}%"

if awk "BEGIN {exit !($TOTAL < $THRESHOLD)}"; then
    echo "FAIL: Coverage ${TOTAL}% is below ${THRESHOLD}% threshold"
    exit 1
fi

echo "PASS: Coverage ${TOTAL}% meets ${THRESHOLD}% threshold"
