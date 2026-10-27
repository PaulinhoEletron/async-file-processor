-include .env
export

.PHONY: up down test lint run-api run-worker

up:
	docker compose up -d --wait postgres minio elasticmq
	docker compose run --rm --no-deps minio-init

down:
	docker compose down

test:
	go test ./...

lint:
	go vet ./...

run-api:
	go run ./cmd/api

run-worker:
	go run ./cmd/worker
