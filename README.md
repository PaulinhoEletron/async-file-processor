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
worker claims the job (`PROCESSING`), validates the file, marks the job `COMPLETED`, and deletes
the message. Malformed messages, unknown jobs, and jobs that are already finished are discarded.
A file that can never be processed (empty, or with a wrong header) marks the job `FAILED` with
an explanatory `error_msg` and deletes the message. Any other failure leaves the message on the
queue for redelivery. Log lines carry `job_id` and `worker`.

On SIGINT or SIGTERM the worker stops receiving, lets jobs already running finish for up to
`SHUTDOWN_TIMEOUT`, and exits. Messages received but not yet started are left on the queue and
become visible again. The worker does not run migrations: start the API once first.

## CSV validation

Files are read as a stream (`encoding/csv`, one record at a time), so memory use does not depend
on file size. Invalid rows are written to `{job id}/errors.csv` as they are found, and the
file is uploaded to S3 concurrently through a pipe.

### Schema

The header must be exactly `id,name,email,amount,date,country`, in that order. Surrounding spaces
and a leading UTF-8 byte order mark are ignored. Spaces around values are ignored too.

| Column | Rule |
| --- | --- |
| `id` | Required, a base-10 integer, unique within the file. |
| `name` | Required, at most 100 characters. |
| `email` | A bare address accepted by `net/mail`; `Name <a@b.com>` is rejected. |
| `amount` | A plain decimal, 0 or greater: `10`, `10.50`, `0`. A sign, an exponent (`1e3`), `NaN` and `Inf` are rejected. |
| `date` | `YYYY-MM-DD` and a real calendar date (`2024-02-30` is invalid). |
| `country` | Exactly two uppercase ASCII letters. |

A row with the wrong number of fields is a single error on the pseudo-field `row`. A row that
is not well-formed CSV (for example a stray quote inside an unquoted field) is also reported on
`row`, and processing continues with the next line. The header is stricter: if it is empty,
wrong, or malformed, the job fails.

Duplicate detection keeps every id seen so far in a set, a few tens of bytes per distinct
id, so a file with tens of millions of unique ids needs gigabytes of memory. That is the one part of
processing whose memory grows with the input. A row claims its id whenever the id parses, even if
other columns are invalid; rows of the wrong width claim nothing.

### errors.csv

`{job id}/errors.csv` is always written, with only the header when every row is valid. Columns:
`row,field,error,raw`. There is one line per failed rule, so a row with three problems
appears three times. `row` is the 1-based data row number (the header is not counted), and
`raw` is the original record re-encoded as CSV. For a row that is not well-formed CSV, `raw`
is empty. Progress is written to the job (`total_rows`, `valid_rows`, `invalid_rows`)
every 5000 rows and at completion.

```csv
row,field,error,raw
5,email,must be a valid email address,"5,Eva Novak,not-an-email,10.00,2024-05-05,CZ"
10,id,is a duplicate of an earlier row,"2,Duplicate Id,dup@example.com,9.99,2024-09-30,PT"
```

### Large test files

`scripts/gencsv` writes a file in this schema without holding it in memory. `-bad` is the
fraction of invalid rows, spread over every rule. It prints the exact number it generated to
stderr, so job counts can be compared.

```sh
go run ./scripts/gencsv -rows 1000000 -bad 0.05 -seed 1 -out big.csv   # prints rows=1000000 bad=...
curl -F file=@big.csv http://localhost:8080/api/jobs
```

Benchmark (local, MinIO and Postgres in Docker): a 1,000,000 row, 69 MB file with 49,751
invalid rows processed in about 6 seconds (roughly 165,000 rows/s). The worker's resident memory
peaked at about 85 MB with one such job, and about 280 MB with four running concurrently
(`WORKER_CONCURRENCY=4`). Per-job memory is mostly the duplicate-id set and the upload buffer,
so size the worker for `WORKER_CONCURRENCY` times the largest expected file.

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
