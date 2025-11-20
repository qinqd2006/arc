# Arc Core Go Makefile

# Variables
APP_NAME := arc-core
VERSION := 1.0.0
BUILD_DIR := build
DIST_DIR := dist
GO_FILES := $(shell find . -type f -name '*.go')
CONFIG_FILES := arc.conf
LDFLAGS := -ldflags "-X main.AppVersion=$(VERSION) -X main.buildTime=$(shell date -u +'%Y-%m-%dT%H:%M:%SZ')"

# Default target
.PHONY: all
all: clean build

# Build
.PHONY: build
build:
	@echo "Building $(APP_NAME)..."
	@mkdir -p $(BUILD_DIR)
	go build $(LDFLAGS) -o $(BUILD_DIR)/$(APP_NAME) ./cmd/arc-core

# Build for multiple platforms
.PHONY: build-all
build-all:
	@echo "Building $(APP_NAME) for multiple platforms..."
	@mkdir -p $(DIST_DIR)

	# Linux AMD64
	GOOS=linux GOARCH=amd64 go build $(LDFLAGS) -o $(DIST_DIR)/$(APP_NAME)-linux-amd64 ./cmd/arc-core

	# Linux ARM64
	GOOS=linux GOARCH=arm64 go build $(LDFLAGS) -o $(DIST_DIR)/$(APP_NAME)-linux-arm64 ./cmd/arc-core

	# Darwin AMD64
	GOOS=darwin GOARCH=amd64 go build $(LDFLAGS) -o $(DIST_DIR)/$(APP_NAME)-darwin-amd64 ./cmd/arc-core

	# Darwin ARM64
	GOOS=darwin GOARCH=arm64 go build $(LDFLAGS) -o $(DIST_DIR)/$(APP_NAME)-darwin-arm64 ./cmd/arc-core

	# Windows AMD64
	GOOS=windows GOARCH=amd64 go build $(LDFLAGS) -o $(DIST_DIR)/$(APP_NAME)-windows-amd64.exe ./cmd/arc-core

# Install dependencies
.PHONY: deps
deps:
	@echo "Installing dependencies..."
	go mod download
	go mod tidy

# Test
.PHONY: test
test:
	@echo "Running tests..."
	go test -v ./...

# Test with coverage
.PHONY: test-coverage
test-coverage:
	@echo "Running tests with coverage..."
	go test -v -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html

# Benchmarks
.PHONY: bench
bench:
	@echo "Running benchmarks..."
	go test -bench=. -benchmem ./test/benchmarks/...

# Lint
.PHONY: lint
lint:
	@echo "Running linter..."
	golangci-lint run

# Format code
.PHONY: fmt
fmt:
	@echo "Formatting code..."
	go fmt ./...

# Vet
.PHONY: vet
vet:
	@echo "Vetting code..."
	go vet ./...

# Clean
.PHONY: clean
clean:
	@echo "Cleaning..."
	rm -rf $(BUILD_DIR) $(DIST_DIR)
	rm -f coverage.out coverage.html
	go clean -cache

# Run
.PHONY: run
run: build
	@echo "Running $(APP_NAME)..."
	./$(BUILD_DIR)/$(APP_NAME)

# Run with config
.PHONY: run-dev
run-dev: build
	@echo "Running $(APP_NAME) in development mode..."
	./$(BUILD_DIR)/$(APP_NAME) -config arc.conf

# Docker build
.PHONY: docker-build
docker-build:
	@echo "Building Docker image..."
	docker build -t arc-core:$(VERSION) .
	docker tag arc-core:$(VERSION) arc-core:latest

# Docker run
.PHONY: docker-run
docker-run:
	@echo "Running Docker container..."
	docker run --rm -p 8000:8000 -v $(PWD)/arc.conf:/app/arc.conf arc-core:latest

# Docker compose up
.PHONY: docker-up
docker-up:
	@echo "Starting Docker Compose..."
	docker-compose up -d

# Docker compose down
.PHONY: docker-down
docker-down:
	@echo "Stopping Docker Compose..."
	docker-compose down

# Generate documentation
.PHONY: docs
docs:
	@echo "Generating documentation..."
	swag init -g cmd/arc-core/main.go -o docs/

# Install development tools
.PHONY: install-tools
install-tools:
	@echo "Installing development tools..."
	go install github.com/swaggo/swag/cmd/swag@latest
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
	go install github.com/air-verse/air@latest

# Development with hot reload
.PHONY: dev
dev:
	@echo "Starting development server with hot reload..."
	air

# Release
.PHONY: release
release: clean test build-all
	@echo "Creating release..."
	@mkdir -p $(DIST_DIR)/release
	@cp $(CONFIG_FILES) $(DIST_DIR)/release/
	@cp README_GO.md $(DIST_DIR)/release/README.md
	@cp LICENSE $(DIST_DIR)/release/ 2>/dev/null || true
	@for binary in $(DIST_DIR)/$(APP_NAME)-*; do \
		basename=$$(basename $$binary); \
		mkdir -p $(DIST_DIR)/release/$${basename%.*}; \
		cp $$binary $(DIST_DIR)/release/$${basename%.*}/$(APP_NAME); \
		cp $(CONFIG_FILES) $(DIST_DIR)/release/$${basename%.*}/; \
		cp README_GO.md $(DIST_DIR)/release/$${basename%.*}/README.md; \
		cp LICENSE $(DIST_DIR)/release/$${basename%.*}/ 2>/dev/null || true; \
		cd $(DIST_DIR)/release && tar -czf $$basename.tar.gz $${basename%.*}; \
		cd -; \
	done

# Install to system
.PHONY: install
install: build
	@echo "Installing $(APP_NAME) to /usr/local/bin..."
	sudo cp $(BUILD_DIR)/$(APP_NAME) /usr/local/bin/

# Uninstall from system
.PHONY: uninstall
uninstall:
	@echo "Uninstalling $(APP_NAME) from /usr/local/bin..."
	sudo rm -f /usr/local/bin/$(APP_NAME)

# Check for required tools
.PHONY: check-tools
check-tools:
	@echo "Checking for required tools..."
	@command -v go >/dev/null 2>&1 || { echo "Go is required but not installed. Aborting." >&2; exit 1; }
	@command -v docker >/dev/null 2>&1 || { echo "Docker is required but not installed. Aborting." >&2; exit 1; }
	@command -v docker-compose >/dev/null 2>&1 || { echo "Docker Compose is required but not installed. Aborting." >&2; exit 1; }

# Help
.PHONY: help
help:
	@echo "Available targets:"
	@echo "  all          - Clean and build"
	@echo "  build        - Build the application"
	@echo "  build-all    - Build for multiple platforms"
	@echo "  deps         - Install dependencies"
	@echo "  test         - Run tests"
	@echo "  test-coverage- Run tests with coverage"
	@echo "  bench        - Run benchmarks"
	@echo "  lint         - Run linter"
	@echo "  fmt          - Format code"
	@echo "  vet          - Vet code"
	@echo "  clean        - Clean build artifacts"
	@echo "  run          - Build and run"
	@echo "  run-dev      - Build and run with config"
	@echo "  docker-build - Build Docker image"
	@echo "  docker-run   - Run Docker container"
	@echo "  docker-up    - Start Docker Compose"
	@echo "  docker-down  - Stop Docker Compose"
	@echo "  docs         - Generate documentation"
	@echo "  install-tools- Install development tools"
	@echo "  dev          - Development with hot reload"
	@echo "  release      - Create release"
	@echo "  install      - Install to system"
	@echo "  uninstall    - Uninstall from system"
	@echo "  check-tools  - Check for required tools"
	@echo "  help         - Show this help"