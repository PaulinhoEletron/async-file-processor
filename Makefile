-include .env
export

DEV_COMPOSE = docker compose -f docker-compose.yml -f docker-compose.dev.yml

.PHONY: up public down clean infra test test-integration e2e lint run-api run-worker

# Full stack in containers: Postgres, MinIO, ElasticMQ, the API and two workers.
# The UI is at http://localhost:8080. Extra arguments reach compose: `make down ARGS=-v`.
up:
	docker compose up -d --build --wait

# Full stack plus the Cloudflare Tunnel connector (token in .env as CLOUDFLARE_TUNNEL_TOKEN).
# Trusts CF-Connecting-IP and marks cookies Secure, which is right only when public traffic
# arrives through the tunnel.
public:
	@test -n "$(CLOUDFLARE_TUNNEL_TOKEN)" || { echo "CLOUDFLARE_TUNNEL_TOKEN is not set (add it to .env)"; exit 1; }
	TRUST_CF_CONNECTING_IP=true COOKIE_SECURE=true docker compose --profile public up -d --build --wait

# --profile public also stops the tunnel connector if `make public` started it.
down:
	docker compose --profile public down $(ARGS)

# Stop everything and delete local data (same as `make down ARGS=-v`).
clean:
	docker compose --profile public down -v

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
