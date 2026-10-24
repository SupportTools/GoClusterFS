# GoClusterFS Makefile
# Build and test automation for the distributed filesystem

.PHONY: build build-mount build-fsck test test-unit test-integration test-all test-parity test-multinode-docker docker-build-test docker-clean-test test-multinode-vm vm-clean clean fmt vet check-fuse check-qemu lint vuln help

# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOTEST=$(GOCMD) test
GOFMT=$(GOCMD) fmt
GOVET=$(GOCMD) vet
GOMOD=$(GOCMD) mod

# Binary names
MOUNT_BINARY=goclusterfs-mount
FSCK_BINARY=goclusterfs-fsck

GOLANGCI_LINT_VERSION=v2.15.0
GOVULNCHECK_VERSION=v1.9.0
COVER_PROFILE=coverage.out

# Default target
.DEFAULT_GOAL := help

# Build all binaries
build: build-mount build-fsck

build-mount:
	@echo "Building $(MOUNT_BINARY)..."
	$(GOBUILD) -o $(MOUNT_BINARY) ./cmd/mount
	@echo "Built $(MOUNT_BINARY) successfully"

build-fsck:
	@echo "Building $(FSCK_BINARY)..."
	$(GOBUILD) -o $(FSCK_BINARY) ./cmd/fsck
	@echo "Built $(FSCK_BINARY) successfully"

# Unit tests (fast, no FUSE needed)
test: test-unit

test-unit:
	@echo "Running unit tests with race detector..."
	$(GOTEST) -v -race -covermode=atomic -coverprofile=$(COVER_PROFILE) ./internal/...

# Integration tests (requires FUSE and built binary)
test-integration: check-fuse build-mount
	@echo "Running integration tests..."
	$(GOTEST) -v -timeout 5m ./tests/integration/...

# All tests
test-all: test-unit test-integration
	@echo "All tests completed"

# Test parity validation - ensures tests exist across all environments
test-parity:
	@echo "Validating test parity across environments..."
	@./tests/scripts/validate-test-parity.sh

# Test parity with detailed report
test-parity-report:
	@echo "Generating test parity report..."
	@./tests/scripts/validate-test-parity.sh --report

# Docker Compose configuration (use docker-compose for compatibility)
DOCKER_COMPOSE_TEST=docker-compose -f tests/docker/docker-compose.test.yml

# Build Docker test images
docker-build-test:
	@echo "Building Docker test images..."
	$(DOCKER_COMPOSE_TEST) build
	@echo "Docker images built successfully"

# Run multi-node tests in Docker containers
# Two-phase approach with detached mode to allow container kill/restart tests
test-multinode-docker: docker-build-test
	@echo "Running multi-node Docker tests..."
	@mkdir -p tests/docker/results
	@echo "Phase 1: Initializing shared storage..."
	$(DOCKER_COMPOSE_TEST) run --rm storage-init
	@echo "Phase 2: Starting nodes in background..."
	$(DOCKER_COMPOSE_TEST) up --no-deps -d node1 node2 node3 node4
	@echo "Phase 3: Waiting for nodes to be healthy..."
	@sleep 5
	@for node in goclusterfs-node1 goclusterfs-node2 goclusterfs-node3 goclusterfs-node4; do \
		echo "Checking $$node..."; \
		timeout 60 sh -c "until docker exec $$node /scripts/healthcheck.sh > /dev/null 2>&1; do sleep 1; done" || \
		(echo "$$node failed to become healthy" && $(DOCKER_COMPOSE_TEST) logs $$node && exit 1); \
	done
	@echo "Phase 4: Running test orchestrator..."
	$(DOCKER_COMPOSE_TEST) run --rm orchestrator; \
	EXIT_CODE=$$?; \
	echo "Orchestrator exited with code $$EXIT_CODE"; \
	$(DOCKER_COMPOSE_TEST) stop node1 node2 node3 node4 2>/dev/null || true; \
	exit $$EXIT_CODE

# Clean up Docker test resources
docker-clean-test:
	@echo "Cleaning up Docker test resources..."
	$(DOCKER_COMPOSE_TEST) down -v --remove-orphans || true
	docker image prune -f --filter "label=goclusterfs-test" || true
	rm -rf tests/docker/results/*
	@echo "Docker cleanup complete"

# Run multi-node tests with larger storage (10GB) - for stress testing
test-multinode-docker-10gb: docker-build-test
	@echo "Running multi-node Docker tests with 10GB storage..."
	CLUSTER_SIZE_MB=10240 $(MAKE) test-multinode-docker

# Run multi-node tests with very large storage (100GB) - for production-scale testing
test-multinode-docker-100gb: docker-build-test
	@echo "Running multi-node Docker tests with 100GB storage..."
	@echo "WARNING: This will create a 100GB sparse file and may take a long time"
	CLUSTER_SIZE_MB=102400 $(MAKE) test-multinode-docker

# =============================================================================
# VM Multi-Node Tests (QEMU-based)
# =============================================================================

VM_TEST_HARNESS=tests/vm/vm-test-harness.sh

# Check QEMU is available
check-qemu:
	@which qemu-system-x86_64 > /dev/null 2>&1 || (echo "Error: qemu-system-x86_64 not found. Install QEMU first." && exit 1)
	@echo "QEMU is available"

# Run multi-node tests in QEMU VMs
test-multinode-vm: check-qemu build-mount
	@echo "Running multi-node VM tests..."
	@mkdir -p tests/vm/results
	$(VM_TEST_HARNESS) --run-tests --nodes=2
	@echo "VM tests completed"

# Run VM tests with more nodes
test-multinode-vm-3: check-qemu build-mount
	@echo "Running multi-node VM tests (3 nodes)..."
	@mkdir -p tests/vm/results
	$(VM_TEST_HARNESS) --run-tests --nodes=3
	@echo "VM tests completed"

# Clean up VM test resources
vm-clean:
	@echo "Cleaning up VM test resources..."
	$(VM_TEST_HARNESS) --cleanup
	@echo "VM cleanup complete"

# Full VM cleanup including built images
vm-clean-all:
	@echo "Cleaning up all VM resources..."
	$(VM_TEST_HARNESS) --cleanup-all
	@echo "Full VM cleanup complete"

# =============================================================================
# Environment Checks
# =============================================================================

# Check FUSE is available
check-fuse:
	@which fusermount > /dev/null 2>&1 || (echo "Error: fusermount not found. Install FUSE first." && exit 1)
	@echo "FUSE is available"

lint:
	$(GOCMD) run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION) run ./...

vuln:
	$(GOCMD) run golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION) ./...

# Utilities
clean:
	@echo "Cleaning build artifacts..."
	rm -f $(MOUNT_BINARY) $(FSCK_BINARY) $(COVER_PROFILE)
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
	@echo "    build-fsck     - Build goclusterfs-fsck binary"
	@echo ""
	@echo "  Test:"
	@echo "    test           - Run unit tests (alias for test-unit)"
	@echo "    test-unit      - Run unit tests with race detector and coverage (no FUSE needed)"
	@echo "    test-integration - Run integration tests (requires FUSE)"
	@echo "    test-all       - Run all tests (unit + integration)"
	@echo "    test-parity    - Validate test coverage across environments"
	@echo "    test-parity-report - Detailed test coverage report"
	@echo ""
	@echo "  Docker Multi-Node Tests:"
	@echo "    test-multinode-docker      - Run multi-node tests in Docker (500MB)"
	@echo "    test-multinode-docker-10gb - Run multi-node tests with 10GB storage"
	@echo "    test-multinode-docker-100gb- Run multi-node tests with 100GB storage"
	@echo "    docker-build-test          - Build Docker test images only"
	@echo "    docker-clean-test          - Clean up Docker test resources"
	@echo ""
	@echo "  VM Multi-Node Tests (QEMU):"
	@echo "    test-multinode-vm     - Run multi-node tests in QEMU VMs (2 nodes)"
	@echo "    test-multinode-vm-3   - Run multi-node tests in QEMU VMs (3 nodes)"
	@echo "    vm-clean              - Clean up VM runtime files"
	@echo "    vm-clean-all          - Clean up all VM resources including images"
	@echo "    check-qemu            - Verify QEMU is available"
	@echo ""
	@echo "  Code Quality:"
	@echo "    fmt            - Format code with go fmt"
	@echo "    vet            - Run go vet"
	@echo "    lint           - Run golangci-lint $(GOLANGCI_LINT_VERSION) with .golangci.yml"
	@echo "    vuln           - Run govulncheck $(GOVULNCHECK_VERSION)"
	@echo ""
	@echo "  Utilities:"
	@echo "    clean          - Remove build artifacts"
	@echo "    mod-tidy       - Run go mod tidy"
	@echo "    mod-download   - Download dependencies"
	@echo "    check-fuse     - Verify FUSE is available"
	@echo "    help           - Show this help message"
