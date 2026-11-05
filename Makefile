-include .env
export

DEV_COMPOSE = docker compose -f docker-compose.yml -f docker-compose.dev.yml

.PHONY: up down clean infra test test-integration e2e lint run-api run-worker

# Full stack in containers: Postgres, MinIO, ElasticMQ, the API and two workers.
# The UI is at http://localhost:8080. Extra arguments reach compose: `make down ARGS=-v`.
up:
	docker compose up -d --build --wait

down:
	docker compose down $(ARGS)

# Stop everything and delete local data (same as `make down ARGS=-v`).
clean:
	docker compose down -v

# Only the infrastructure, with host ports published, for run-api, run-worker and test-integration.
# Do not mix with `make up`: stop one before starting the other.
infra:
	$(DEV_COMPOSE) up -d --wait postgres minio elasticmq

test:
	go test -race ./...

test-integration:
	go test -race -tags integration ./...

# Brings up the containerised stack and checks it end to end.
e2e:
	./scripts/e2e.sh

lint:
	go vet ./...
	go vet -tags integration ./...

run-api:
	go run ./cmd/api

run-worker:
	go run ./cmd/worker
