.DEFAULT_GOAL := help

.PHONY: db
db: ## Start postgres container
	docker run -d --name postgres -e POSTGRES_HOST_AUTH_METHOD=trust -p 5432:5432 postgres:13-alpine

.PHONY: db-down
db-down: ## Stop and rm postgres container
	docker stop postgres && docker rm postgres

.PHONY: test
test: ## Run tests
	@go test -v -race -timeout=30s ./...

.PHONY: lint
lint: ## Run linter
	golangci-lint --version
	golangci-lint run -v

.PHONY: help
help: ## Display this help screen
	@grep -h -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
