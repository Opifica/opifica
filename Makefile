BUF_VERSION            := v1.75.0
PROTOC_GEN_GO_VERSION  := v1.36.12
CONNECT_GO_VERSION     := v1.21.1

GOBIN   := $(shell go env GOPATH)/bin
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -X github.com/opifica/opifica/internal/version.Version=$(VERSION)

.PHONY: all build test lint generate tools clean

all: lint test build

build: ## Build every binary into bin/
	go build -ldflags "$(LDFLAGS)" -o bin/ ./cmd/...

test: ## Run tests with the race detector
	go test -race ./...

lint: ## gofmt, go vet and buf lint
	@test -z "$$(gofmt -l .)" || (echo "Files need gofmt:"; gofmt -l .; exit 1)
	go vet ./...
	$(GOBIN)/buf lint

generate: ## Regenerate Go code from proto/
	rm -rf gen
	PATH="$(GOBIN):$$PATH" $(GOBIN)/buf generate

tools: ## Install buf and the protoc plugins
	go install github.com/bufbuild/buf/cmd/buf@$(BUF_VERSION)
	go install google.golang.org/protobuf/cmd/protoc-gen-go@$(PROTOC_GEN_GO_VERSION)
	go install connectrpc.com/connect/cmd/protoc-gen-connect-go@$(CONNECT_GO_VERSION)

clean:
	rm -rf bin
