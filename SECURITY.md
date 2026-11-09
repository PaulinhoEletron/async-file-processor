# Security

This project is a portfolio demo: a public, authentication-free web service that anyone on the internet can use
to upload a CSV and get a validation report. It is meant to run on a single laptop, exposed through a Cloudflare
Tunnel. This document describes the threat model, what is deliberately open, the safeguards in place, the issues a
pre-launch self-review found and how each was fixed, and the risks that are accepted.

## Threat model

**Assets worth protecting**

- The host machine: its memory, CPU and disk must not be exhausted by a visitor.
- The availability of the demo for other visitors (one visitor must not monopolise the workers).
- Visitors' uploaded files and reports, for as long as they exist (24 hours by default).
- The internal services (Postgres, object storage, the queue) and their credentials.

**Who is assumed to attack:** anonymous internet users who can reach the public URL, send arbitrary HTTP, and
upload arbitrary bytes. Attackers are not assumed to have access to the host or the Docker network.

**Out of scope:** a compromised host, a compromised Cloudflare account, attacks on Cloudflare itself, and
denial-of-service volumes that Cloudflare would have to absorb at the edge.

## Intentionally open

- **No authentication.** Anyone can upload. The demo's purpose is to be tried instantly; abuse is bounded by the
  safeguards below instead.
- **Job ids are capability links.** `GET /api/jobs/{id}`, `/report` and `/errors.csv` work for anyone who holds
  the job's UUID (random, version 4). Ids are never listed to anyone but the browser that created the job.
- **Do not upload real or personal data.** The UI says so. Files are deleted after 24 hours, but the service is
  not designed for confidential content.

## Safeguards

| Area | Safeguard |
| --- | --- |
| Upload size | `MAX_UPLOAD_BYTES` is enforced while streaming (`413`); the body is never buffered whole. |
| Rate limiting | Token bucket per client on `POST /api/jobs` (`429` with `Retry-After`). Clients are keyed by the connection's remote address; `CF-Connecting-IP` is honoured only with `TRUST_CF_CONNECTING_IP=true`, which is correct only when the API is reachable solely through Cloudflare. IPv4-mapped addresses are unmapped, IPv6 zones dropped and IPv6 clients keyed by /64. When the client table is full, new clients share an overflow bucket instead of being refused. `X-Forwarded-For` is never used. |
| Upload deadlines | Every body read refreshes a read deadline (`UPLOAD_IDLE_TIMEOUT`, 30s), and an upload may not exceed `UPLOAD_MAX_DURATION` (10m). A stalled upload gets `408`, its multipart upload is aborted and no job row or queue message is created. |
| Upload concurrency | At most `MAX_CONCURRENT_UPLOADS` (16) uploads stream at once; others get `503`. The slot is taken before the body is read. |
| Queue caps | At most `MAX_UNFINISHED_JOBS` (50) unfinished jobs overall (`503`) and `MAX_UNFINISHED_PER_CLIENT` (3) per client (`429`), checked before the body is read. Order of admission: rate limit, upload slot, queue caps, read body. The client key stored on a job is a SHA-256 hash, never a raw address. |
| Error file cap | `errors.csv` holds at most `MAX_ERROR_LINES` (50,000) lines per job, shows the raw row once per row and truncates it to 1 KiB. Counts in the summary stay exact and `errors_truncated` says when the cap applied. |
| Record size cap | A single CSV record larger than `MAX_RECORD_BYTES` (1 MiB) fails the job without retry, so one enormous line or an unterminated quote cannot grow the worker's memory. |
| Spreadsheet formulas | Any `errors.csv` cell starting with `=`, `+`, `-`, `@`, tab or carriage return is prefixed with `'`. |
| Filenames | Directory parts are dropped, control and bidirectional-override characters are removed, invalid UTF-8 and names over 255 characters are rejected, and the name must end in `.csv`. Filenames never become object keys. |
| Per-browser job list | `GET /api/jobs` returns only the jobs uploaded from the same browser. The `owner` cookie is 32 random bytes, `HttpOnly`, `SameSite=Lax`, `Secure` when `COOKIE_SECURE=true`; only its SHA-256 hash is stored. |
| Retention | Finished jobs and their objects are deleted after `RETENTION` (24h). |
| Browser hardening | `Content-Security-Policy: default-src 'self'; object-src 'none'; base-uri 'none'; form-action 'self'; frame-ancestors 'none'`, `X-Content-Type-Options`, `X-Frame-Options`, `Referrer-Policy`, `Permissions-Policy`, `Cross-Origin-Opener-Policy`, `Cross-Origin-Resource-Policy`, and `Cache-Control: no-store` on every `/api/` response. The UI builds the DOM with `textContent` only and has no inline scripts. |
| Error hygiene | Responses never include stack traces, object keys or driver errors. `/readyz` reports `unavailable` per failing check and logs the cause. After retries are exhausted a job stores a generic message; the detail stays in the logs. |
| Internal-only infrastructure | Postgres, MinIO and ElasticMQ are not published. API metrics are on a separate listener (`API_METRICS_ADDR`) that is not published; the public port answers `404` for `/metrics`. |
| Containers | Memory, CPU and process limits on every container and `GOMEMLIMIT` on the Go services. The API and workers run as a non-root user in distroless images, with a read-only root filesystem, all capabilities dropped and `no-new-privileges`. Base images are pinned by digest. |
| Supply chain | Built with Go 1.26.10; `govulncheck` reports no vulnerabilities in code the binaries call. `.dockerignore` keeps `.env` and workflow files out of the images; no secrets are baked in. |

## Findings from the pre-launch self-review

Before going public, the stack was probed from the outside (black box, then with the source) across uploads,
resource exhaustion, the API surface, injection and parsing, the web UI and the containers. Everything below was
fixed and has a regression test where practical.

| Severity | Finding | Fix |
| --- | --- | --- |
| High | `errors.csv` amplification: the raw row was repeated on every error line with no cap, so a 49 MB file of invalid rows produced an 846 MB error file. | Raw row once per row, truncated to 1 KiB; line cap per job with exact counts and an `errors_truncated` flag. |
| Medium | `CF-Connecting-IP` was trusted unconditionally, so a direct client could rotate it to dodge the rate limit. | Trusted only with `TRUST_CF_CONNECTING_IP=true`; addresses normalised (unmapped, zone stripped, IPv6 by /64). |
| Medium | The job list exposed every visitor's jobs, and with them their error rows. | Per-browser owner cookie; list filtered by its hash. |
| Medium | A stalled upload held its connection open indefinitely, and nothing capped how many uploads could stream at once. | Per-read idle deadline, overall cap, concurrent-upload semaphore. |
| Medium | A single huge record or an unterminated quote could balloon worker memory; containers had no limits. | Record size cap; memory, CPU and pids limits; `GOMEMLIMIT`. |
| Medium | 17 reachable vulnerabilities in the Go standard library (go1.26.5). | Go 1.26.10 toolchain and builder image; `govulncheck` is clean. |
| Medium | Unbounded filenames and control characters stored and logged verbatim. | Sanitised, length-limited, UTF-8-validated names. |
| Medium | One client could fill every worker with jobs. | Global and per-client caps on unfinished jobs. |
| Low | Formula injection through `errors.csv` cells. | Leading `'` on risky cells. |
| Low | `/metrics` on the public listener. | Moved to an internal listener. |
| Low | Internal error text stored on jobs and returned by `/readyz`. | Generic stored message; `unavailable` in the readiness body. |
| Low | A truncated multipart body produced `500`. | `400 incomplete upload`. |
| Low | Missing `object-src`, `base-uri` and `form-action` in the CSP; no `Permissions-Policy`, COOP or CORP; API responses cacheable. | Headers added; `Cache-Control: no-store` on `/api/`. |
| Low | API and worker containers ran with default capabilities and a writable filesystem; some images were tag-pinned only. | Read-only root, `cap_drop: ALL`, `no-new-privileges`; digests pinned. |
| Low | Rate limiter refused new clients outright when its table was full. | Shared overflow bucket. |

## Accepted risks

- **Default local credentials.** Postgres, MinIO and ElasticMQ use the development credentials from
  `.env.example`. They are not published and sit on a private Docker network, so they are unreachable from
  outside. Anyone with access to the host already controls the data.
- **`HEAD` on `GET` routes.** The standard router answers `HEAD` for `GET` routes. It returns headers only and
  exposes nothing a `GET` would not.
- **Non-UTF-8 cell content is accepted.** Cell values are validated by the schema rules; encoding is not
  enforced, and the UI renders values with `textContent`. Filenames, by contrast, must be valid UTF-8.
- **The rate limiter runs before validation.** A request that is later rejected (wrong extension, bad body) still
  consumes a token. This keeps rejected traffic cheap and the limiter simple.
- **HSTS is the edge's job.** The origin serves plain HTTP on loopback; Cloudflare terminates TLS and should send
  `Strict-Transport-Security`.
- **Ingress must target only the API service.** The tunnel must point at the API container and nothing else. A
  catch-all that returns `404` for any other hostname is recommended. The compose network is flat, so exposing
  any other service through the tunnel would defeat the isolation described above.
- **Queue caps are approximate.** The count and the insert are separate statements, so concurrent requests can
  overshoot a cap by a small amount.
- **Capability links.** A job id is a bearer secret for that job. Sharing the link shares the report.

## Reporting a vulnerability

Please report security problems privately to the repository owner rather than posting details publicly: open a
GitHub issue that asks for a private contact channel (without technical details), or use the owner's contact
information on their GitHub profile. Include what you found, how to reproduce it and the impact. Reports are
answered on a best-effort basis.
