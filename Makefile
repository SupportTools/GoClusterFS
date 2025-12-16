# GoClusterFS Makefile
# Build and test automation for the distributed filesystem

.PHONY: build build-mount test test-unit test-integration test-all clean fmt vet check-fuse lint help

# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOTEST=$(GOCMD) test
GOFMT=$(GOCMD) fmt
GOVET=$(GOCMD) vet
GOMOD=$(GOCMD) mod

# Binary names
MOUNT_BINARY=goclusterfs-mount

# Default target
.DEFAULT_GOAL := help

# Build all binaries
build: build-mount

build-mount:
	@echo "Building $(MOUNT_BINARY)..."
	$(GOBUILD) -o $(MOUNT_BINARY) ./cmd/mount
	@echo "Built $(MOUNT_BINARY) successfully"

# Unit tests (fast, no FUSE needed)
test: test-unit

test-unit:
	@echo "Running unit tests..."
	$(GOTEST) -v ./internal/...

# Integration tests (requires FUSE and built binary)
test-integration: check-fuse build-mount
	@echo "Running integration tests..."
	$(GOTEST) -v -timeout 5m ./tests/integration/...

# All tests
test-all: test-unit test-integration
	@echo "All tests completed"

# Check FUSE is available
check-fuse:
	@which fusermount > /dev/null 2>&1 || (echo "Error: fusermount not found. Install FUSE first." && exit 1)
	@echo "FUSE is available"

# Run linter (if golangci-lint is installed)
lint:
	@which golangci-lint > /dev/null 2>&1 && golangci-lint run ./... || echo "golangci-lint not installed, skipping"

# Utilities
clean:
	@echo "Cleaning build artifacts..."
	rm -f $(MOUNT_BINARY) GoClusterFS
	rm -f *.img
	@echo "Clean complete"

fmt:
	@echo "Formatting code..."
	$(GOFMT) ./...

vet:
	@echo "Running go vet..."
	$(GOVET) ./...

# Module management
mod-tidy:
	$(GOMOD) tidy

mod-download:
	$(GOMOD) download

# Help target
help:
	@echo "GoClusterFS Makefile targets:"
	@echo ""
	@echo "  Build:"
	@echo "    build          - Build all binaries"
	@echo "    build-mount    - Build goclusterfs-mount binary"
	@echo ""
	@echo "  Test:"
	@echo "    test           - Run unit tests (alias for test-unit)"
	@echo "    test-unit      - Run unit tests only (fast, no FUSE needed)"
	@echo "    test-integration - Run integration tests (requires FUSE)"
	@echo "    test-all       - Run all tests (unit + integration)"
	@echo ""
	@echo "  Code Quality:"
	@echo "    fmt            - Format code with go fmt"
	@echo "    vet            - Run go vet"
	@echo "    lint           - Run golangci-lint (if installed)"
	@echo ""
	@echo "  Utilities:"
	@echo "    clean          - Remove build artifacts"
	@echo "    mod-tidy       - Run go mod tidy"
	@echo "    mod-download   - Download dependencies"
	@echo "    check-fuse     - Verify FUSE is available"
	@echo "    help           - Show this help message"
