# Use bash so we can rely on pipefail in recipes.
SHELL := /bin/bash

V ?= 0
ifeq ($(V), 1)
override VERBOSE_TAG := -v
endif

TEST_TIMEOUT := 5m
GOLANGCI     ?= golangci-lint
# The service module, the producer SDK, the SDK's bus-test module (T-35) and the simulator (T-38);
# `./...` does not cross a module boundary, so every Go target visits each.
# All but the service run with GOWORK=off so each resolves its own go.mod, never the service's graph through go.work.
MODULES      := . producer producer/internal/bustest
in_module     = for m in $(MODULES); do w=; [ $$m != . ] && w=off; (cd $$m && GOWORK=$$w $(1)) || exit 1; done

.DEFAULT_GOAL := help

##@ General

help: ## Print this help
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m [VAR=value ...]\n"} \
		/^[a-zA-Z0-9_.-]+:.*?##/ { printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2 } \
		/^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) }' $(MAKEFILE_LIST)

##@ Lint

lint: ## Run golangci-lint in every module (version pinned in mise.toml)
	@$(call in_module,$(GOLANGCI) run)

fmt: ## Apply golangci-lint fmt in every module
	@$(call in_module,$(GOLANGCI) fmt)

vet: ## Run go vet in every module
	@$(call in_module,go vet ./...)

check: lint vet ## Run lint + vet

docs-check: ## Check docs/ against .agents/rules/450-doc-lifecycle.md
	@python3 scripts/docs-check.py

data-check: ## Check that git tracks nothing under data/ but its README.md and .gitignore
	@python3 scripts/data-check.py

work: ## Create the local go.work joining the service and the SDK (gitignored, T-27)
	@go work init . ./producer

hooks: ## Enable the repository's git hooks (data-check before every commit); once per clone
	@git config core.hooksPath scripts/hooks

##@ Test

test: ## Run tests in every module with -short and -race
	@$(call in_module,CGO_ENABLED=1 go test ./... -short -race -timeout=$(TEST_TIMEOUT) $(VERBOSE_TAG))

# The pinned go.mod is the SDK's minimum resolved graph; this is the latest one, on a copy of the tracked tree,
# since producers are usually newer than the SDK's floor (producer-sdk.md §4.7 *Versions*).
test-latest: ## Run the SDK's and bustest's tests with the latest nats.go
	@set -e; d=$$(mktemp -d); trap 'rm -rf "$$d"' EXIT; \
	git ls-files -z | tar --null -T - -cf - | tar -xf - -C "$$d"; \
	for m in producer producer/internal/bustest; do \
		(cd "$$d/$$m" && export GOWORK=off && go get github.com/nats-io/nats.go@latest && go mod tidy && \
			echo "$$m resolves:" && go list -m all && \
			CGO_ENABLED=1 go test ./... -short -race -timeout=$(TEST_TIMEOUT) $(VERBOSE_TAG)); \
	done

test-s3: ## Run the s3 store's tests against MinIO and Ceph RGW in containers (needs docker)
	@scripts/test-s3.sh

test-catalog: ## Run the catalog's tests on MariaDB in a container (needs docker); make test runs them on SQLite
	@scripts/catalog.sh test

bench: ## Run benchmarks in every module
	@$(call in_module,go test ./... -run=^$$ -bench=. -benchmem)

##@ Catalog

migrate-diff: ## Generate both engines' migration from schema.hcl: make migrate-diff NAME=<change> (needs docker and atlas)
	@scripts/catalog.sh diff $(NAME)

migrate-check: ## Validate both engines' migrations and check schema.hcl for drift (needs docker and atlas)
	@scripts/catalog.sh check

##@ CI

ci: check docs-check data-check test ## Every gate, plus the service built without the workspace
	@GOWORK=off go build ./...

.PHONY: help lint fmt vet check docs-check data-check work hooks test test-latest test-s3 test-catalog migrate-diff migrate-check bench ci
