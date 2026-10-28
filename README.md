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
ElasticMQ creates `jobs` and `jobs-dlq`, with redrive after three receives.
The API applies database migrations and creates the S3 bucket if it is missing when it starts.
Services bind only to localhost. Credentials in `.env.example` are for local development.
MinIO uses a public [Chainguard build](https://images.chainguard.dev/directory/image/minio/overview),
pinned by digest, because the original MinIO Docker Hub and Quay images no longer allow anonymous pulls.

| Service | Address |
| --- | --- |
| API | http://localhost:8080 |
| Postgres | localhost:5433 |
| MinIO S3 | http://localhost:9000 |
| MinIO console | http://localhost:9001 |
| ElasticMQ SQS | http://localhost:9324 |
| ElasticMQ statistics | http://localhost:9325 |

## API

Errors are JSON: `{"error":"message"}`. Every response carries an `X-Request-ID` header
(a client-supplied value is echoed if it is short and alphanumeric).

**Upload a CSV** - `POST /api/jobs`, multipart field `file`. The file is streamed to S3 without
being held in memory, so large files are fine. Returns `202` with the new job.
Responses: `400` (not a `.csv` file, missing `file` field, malformed body), `413` (larger than
`MAX_UPLOAD_BYTES`, default 500 MB).

```sh
curl -i -F file=@scripts/sample.csv http://localhost:8080/api/jobs
```

**Get a job** - `GET /api/jobs/{id}`. Returns `404` for an unknown id and `400` for a malformed id.

```sh
curl http://localhost:8080/api/jobs/<id>
```

**List jobs** - `GET /api/jobs?limit=50`, newest first. `limit` defaults to 50 and is capped at 200.

```sh
curl "http://localhost:8080/api/jobs?limit=10"
```

A job has the fields `id`, `filename`, `status` (`PENDING`, `PROCESSING`, `COMPLETED`, `FAILED`),
`total_rows`, `valid_rows`, `invalid_rows`, `attempts`, `error_msg`, `created_at`, `updated_at`,
`started_at` and `finished_at`.

## Worker

`make run-worker` starts the queue consumer. It long-polls SQS and feeds a fixed pool of
`WORKER_CONCURRENCY` goroutines (default 4). Messages are only requested when a pool slot is
free, so work never waits in memory while its visibility timeout runs. For each message the
worker claims the job (`PROCESSING`), runs the processor, marks the job `COMPLETED`, and deletes
the message. Malformed messages, unknown jobs, and jobs that are already finished are discarded;
a failed run leaves the message on the queue for redelivery. Log lines carry `job_id` and
`worker`. Until CSV validation is implemented, the processor is a placeholder that waits
`STUB_PROCESS_DELAY` (default 500ms) and reports zero rows.

On SIGINT or SIGTERM the worker stops receiving, lets jobs already running finish for up to
`SHUTDOWN_TIMEOUT`, and exits. Messages received but not yet started are left on the queue and
become visible again. The worker does not run migrations: start the API once first.

Application defaults are defined in `internal/config` and shown in `.env.example`.
Make loads `.env`; when running Go directly, export the environment variables yourself.
AWS credentials use the standard `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` variables.
Postgres uses host port 5433 to coexist with a local installation. If you change
`POSTGRES_PORT`, update the port in `DATABASE_URL` to match.

```sh
make test
make test-integration   # needs `make up`; exercises Postgres, S3, and SQS
make lint
make run-worker
make down
make clean
```

`make down` preserves stored data. `make clean` stops the services and removes local data.
