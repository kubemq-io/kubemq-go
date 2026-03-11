# Makefile — KubeMQ Go SDK v2
# Owned by: 04-testing-spec.md (REQ-TEST-4)

.PHONY: test test-race test-integration test-all lint coverage coverage-html clean

# Run unit tests (default — no integration tests)
test:
	go test -count=1 -timeout 30s ./...

# Run unit tests with race detector
test-race:
	go test -race -count=1 -timeout 30s ./...

# Run integration tests (requires running KubeMQ server)
test-integration:
	go test -race -count=1 -timeout 5m -tags integration -v ./...

# Run all tests
test-all: test-race test-integration

# Lint
lint:
	golangci-lint run ./...

# Coverage report
coverage:
	go test -race -count=1 -timeout 30s -coverprofile=coverage.out -coverpkg=./... ./...
	@grep -v '\.pb\.go' coverage.out > coverage-filtered.out || true
	@grep -v '_gen\.go' coverage-filtered.out > coverage-clean.out || true
	@mv coverage-clean.out coverage.out
	go tool cover -func=coverage.out
	@echo ""
	@TOTAL=$$(go tool cover -func=coverage.out | grep total | awk '{print substr($$3, 1, length($$3)-1)}'); \
	echo "Total coverage: $${TOTAL}%"; \
	if awk "BEGIN {exit !($${TOTAL} < 40)}"; then \
		echo "FAIL: Coverage $${TOTAL}% is below 40% threshold"; \
		exit 1; \
	fi; \
	echo "PASS: Coverage $${TOTAL}% meets 40% threshold"

# Coverage HTML report
coverage-html: coverage
	go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report: coverage.html"

# Clean build artifacts
clean:
	rm -f coverage.out coverage.html coverage-filtered.out coverage-clean.out
