-include .env
export

.PHONY: up down clean test test-integration e2e lint run-api run-worker

up:
	docker compose up -d --wait postgres minio elasticmq

down:
	docker compose down

clean:
	docker compose down -v

test:
	go test -race ./...

test-integration:
	go test -race -tags integration ./...

e2e:
	./scripts/e2e.sh

lint:
	go vet ./...

run-api:
	go run ./cmd/api

run-worker:
	go run ./cmd/worker
