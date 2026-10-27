# Async File Processor

A Go service that accepts CSV uploads, validates records asynchronously, and generates
downloadable reports. Built on S3, SQS, and ECS Fargate; runs fully locally with Docker.

> **Status:** early development. The application skeleton and local infrastructure are in place;
> upload, processing, and reporting are in progress.

Requires Go 1.26, Docker with Compose, and Make.

```sh
cp .env.example .env
make up
make run-api
```

In another terminal:

```sh
curl -i http://localhost:8080/healthz
```

Returns HTTP 200 with `{"status":"ok"}`. The API handles SIGINT and SIGTERM,
allowing active requests up to `SHUTDOWN_TIMEOUT` to finish.
The health endpoint checks the API process; a dependency readiness check is planned.

`make up` starts Postgres, MinIO, and ElasticMQ and waits for their health checks.
The MinIO initialization container creates the `uploads` bucket and exits successfully.
ElasticMQ creates `jobs` and `jobs-dlq`, with redrive after three receives.
Services bind only to localhost. Credentials in `.env.example` are for local development.
MinIO and `mc` use public [Chainguard builds](https://images.chainguard.dev/directory/image/minio/overview),
because the original MinIO Docker Hub and Quay images no longer allow anonymous pulls.

| Service | Address |
| --- | --- |
| API | http://localhost:8080 |
| Postgres | localhost:5433 |
| MinIO S3 | http://localhost:9000 |
| MinIO console | http://localhost:9001 |
| ElasticMQ SQS | http://localhost:9324 |
| ElasticMQ statistics | http://localhost:9325 |

Application defaults are defined in `internal/config` and shown in `.env.example`.
Make loads `.env`; when running Go directly, export the environment variables yourself.
AWS credentials use the standard `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` variables.
Postgres uses host port 5433 to coexist with a local installation. If you change
`POSTGRES_PORT`, update the port in `DATABASE_URL` to match.

```sh
make test
make lint
make run-worker
make down
```

The worker currently waits for a shutdown signal; queue processing is in progress.
`make down` preserves stored data. Use `docker compose down -v` to remove local data.
