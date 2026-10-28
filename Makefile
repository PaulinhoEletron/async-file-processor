-include .env
export

.PHONY: up down clean test test-integration lint run-api run-worker

up:
	docker compose up -d --wait postgres minio elasticmq

down:
	docker compose down

clean:
	docker compose down -v

test:
	go test ./...

test-integration:
	go test -race -tags integration ./...

lint:
	go vet ./...

run-api:
	go run ./cmd/api

run-worker:
	go run ./cmd/worker
