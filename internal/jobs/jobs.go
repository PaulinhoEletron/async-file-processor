// Package jobs defines job states and the Postgres repository.
package jobs

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Status is the lifecycle state of a job.
type Status string

const (
	StatusPending    Status = "PENDING"
	StatusProcessing Status = "PROCESSING"
	StatusCompleted  Status = "COMPLETED"
	StatusFailed     Status = "FAILED"
)

var (
	// ErrNotFound is returned when no job has the requested id.
	ErrNotFound = errors.New("job not found")
	// ErrNotClaimable is returned by MarkProcessing when the job is already finished.
	ErrNotClaimable = errors.New("job is not claimable")
)

// Job is a processing request for one uploaded file.
type Job struct {
	ID          string
	Filename    string
	Status      Status
	InputKey    string
	ReportKey   *string
	ErrorsKey   *string
	TotalRows   int64
	ValidRows   int64
	InvalidRows int64
	Attempts    int
	ErrorMsg    *string
	CreatedAt   time.Time
	UpdatedAt   time.Time
	StartedAt   *time.Time
	FinishedAt  *time.Time
}

// Repo stores jobs in Postgres.
type Repo struct {
	pool *pgxpool.Pool
}

// NewRepo returns a Repo backed by pool.
func NewRepo(pool *pgxpool.Pool) *Repo {
	return &Repo{pool: pool}
}

const columns = `id, filename, status, input_key, report_key, errors_key, total_rows, valid_rows,
	invalid_rows, attempts, error_msg, created_at, updated_at, started_at, finished_at`

func scan(row pgx.Row) (Job, error) {
	var j Job
	err := row.Scan(&j.ID, &j.Filename, &j.Status, &j.InputKey, &j.ReportKey, &j.ErrorsKey, &j.TotalRows,
		&j.ValidRows, &j.InvalidRows, &j.Attempts, &j.ErrorMsg, &j.CreatedAt, &j.UpdatedAt, &j.StartedAt, &j.FinishedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Job{}, ErrNotFound
	}
	return j, err
}

// NewJob holds the values supplied when a job is created.
type NewJob struct {
	ID       string // chosen by the caller so it can be used in the object key
	Filename string
	InputKey string
	// OwnerHash is the SHA-256 of the uploading browser's owner token. Nil leaves the job
	// visible to nobody in the per-owner list.
	OwnerHash []byte
	// ClientKey identifies the uploading client (a hash, never a raw address) for the
	// per-client cap on unfinished jobs.
	ClientKey string
}

// Create inserts a PENDING job.
func (r *Repo) Create(ctx context.Context, n NewJob) (Job, error) {
	j, err := scan(r.pool.QueryRow(ctx,
		`INSERT INTO jobs (id, filename, input_key, owner_hash, client_key)
		 VALUES ($1, $2, $3, $4, NULLIF($5, '')) RETURNING `+columns,
		n.ID, n.Filename, n.InputKey, n.OwnerHash, n.ClientKey))
	if err != nil {
		return Job{}, fmt.Errorf("create job: %w", err)
	}
	return j, nil
}

// Get returns the job with id, or ErrNotFound.
func (r *Repo) Get(ctx context.Context, id string) (Job, error) {
	j, err := scan(r.pool.QueryRow(ctx, `SELECT `+columns+` FROM jobs WHERE id = $1`, id))
	if err != nil {
		return Job{}, fmt.Errorf("get job %s: %w", id, err)
	}
	return j, nil
}

// List returns up to limit jobs uploaded by the owner with the given token hash, newest first.
func (r *Repo) List(ctx context.Context, ownerHash []byte, limit int) ([]Job, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT `+columns+` FROM jobs WHERE owner_hash = $1 ORDER BY created_at DESC, id LIMIT $2`, ownerHash, limit)
	if err != nil {
		return nil, fmt.Errorf("list jobs: %w", err)
	}
	defer rows.Close()
	var out []Job
	for rows.Next() {
		j, err := scan(rows)
		if err != nil {
			return nil, fmt.Errorf("scan job: %w", err)
		}
		out = append(out, j)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list jobs: %w", err)
	}
	return out, nil
}

// Unfinished counts PENDING and PROCESSING jobs: all of them, and those created by clientKey.
func (r *Repo) Unfinished(ctx context.Context, clientKey string) (total, forClient int, err error) {
	err = r.pool.QueryRow(ctx,
		`SELECT count(*), count(*) FILTER (WHERE client_key = $1) FROM jobs
		 WHERE status IN ('PENDING', 'PROCESSING')`, clientKey).Scan(&total, &forClient)
	if err != nil {
		return 0, 0, fmt.Errorf("count unfinished jobs: %w", err)
	}
	return total, forClient, nil
}

// MarkProcessing atomically claims a PENDING or PROCESSING job (PROCESSING lets a redelivered
// message reclaim a job whose worker died). Every claim counts as an attempt and resets the
// counters and output keys, so a re-run starts from a clean slate. It returns ErrNotFound for an unknown id and
// ErrNotClaimable when the job is already COMPLETED or FAILED.
func (r *Repo) MarkProcessing(ctx context.Context, id string) (Job, error) {
	j, err := scan(r.pool.QueryRow(ctx,
		`UPDATE jobs SET status = 'PROCESSING', started_at = COALESCE(started_at, now()),
		 attempts = attempts + 1, total_rows = 0, valid_rows = 0, invalid_rows = 0,
		 error_msg = NULL, report_key = NULL, errors_key = NULL, updated_at = now()
		 WHERE id = $1 AND status IN ('PENDING', 'PROCESSING') RETURNING `+columns, id))
	if errors.Is(err, ErrNotFound) {
		if _, getErr := r.Get(ctx, id); getErr != nil {
			return Job{}, getErr
		}
		return Job{}, ErrNotClaimable
	}
	if err != nil {
		return Job{}, fmt.Errorf("mark job %s processing: %w", id, err)
	}
	return j, nil
}

// UpdateProgress records row counts for a PROCESSING job.
func (r *Repo) UpdateProgress(ctx context.Context, id string, total, valid, invalid int64) error {
	return r.exec(ctx, id, "update progress",
		`UPDATE jobs SET total_rows = $2, valid_rows = $3, invalid_rows = $4, updated_at = now()
		 WHERE id = $1 AND status = 'PROCESSING'`, id, total, valid, invalid)
}

// Complete marks a job COMPLETED with its final counts and output keys. An empty key is stored as NULL.
func (r *Repo) Complete(ctx context.Context, id, reportKey, errorsKey string, total, valid, invalid int64) error {
	return r.exec(ctx, id, "complete",
		`UPDATE jobs SET status = 'COMPLETED', report_key = NULLIF($2, ''), errors_key = NULLIF($3, ''),
		 total_rows = $4, valid_rows = $5, invalid_rows = $6, error_msg = NULL, finished_at = now(), updated_at = now()
		 WHERE id = $1 AND status IN ('PENDING', 'PROCESSING')`, id, reportKey, errorsKey, total, valid, invalid)
}

// Fail marks a job FAILED with a message.
func (r *Repo) Fail(ctx context.Context, id, msg string) error {
	return r.exec(ctx, id, "fail",
		`UPDATE jobs SET status = 'FAILED', error_msg = $2, finished_at = now(), updated_at = now()
		 WHERE id = $1 AND status IN ('PENDING', 'PROCESSING')`, id, msg)
}

// FailIfActive marks a PENDING or PROCESSING job FAILED with msg. Jobs already finished (or
// unknown) are left untouched. It reports whether a job was changed.
func (r *Repo) FailIfActive(ctx context.Context, id, msg string) (bool, error) {
	tag, err := r.pool.Exec(ctx,
		`UPDATE jobs SET status = 'FAILED', error_msg = $2, finished_at = now(), updated_at = now()
		 WHERE id = $1 AND status IN ('PENDING', 'PROCESSING')`, id, msg)
	if err != nil {
		return false, fmt.Errorf("fail active job %s: %w", id, err)
	}
	return tag.RowsAffected() > 0, nil
}

// exec runs a single-row update and maps zero affected rows to ErrNotFound or ErrNotClaimable.
func (r *Repo) exec(ctx context.Context, id, op, sql string, args ...any) error {
	tag, err := r.pool.Exec(ctx, sql, args...)
	if err != nil {
		return fmt.Errorf("%s job %s: %w", op, id, err)
	}
	if tag.RowsAffected() == 0 {
		if _, err := r.Get(ctx, id); err != nil {
			return err
		}
		return fmt.Errorf("%s job %s: %w", op, id, ErrNotClaimable)
	}
	return nil
}

// ListExpired returns up to limit finished (COMPLETED or FAILED) jobs whose finish time is
// before cutoff, oldest first. Active jobs are never returned.
func (r *Repo) ListExpired(ctx context.Context, cutoff time.Time, limit int) ([]Job, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT `+columns+` FROM jobs
		 WHERE status IN ('COMPLETED', 'FAILED') AND COALESCE(finished_at, updated_at) < $1
		 ORDER BY COALESCE(finished_at, updated_at), id LIMIT $2`, cutoff, limit)
	if err != nil {
		return nil, fmt.Errorf("list expired jobs: %w", err)
	}
	defer rows.Close()
	var out []Job
	for rows.Next() {
		j, err := scan(rows)
		if err != nil {
			return nil, fmt.Errorf("scan job: %w", err)
		}
		out = append(out, j)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list expired jobs: %w", err)
	}
	return out, nil
}

// DeleteFinished removes a COMPLETED or FAILED job and reports whether a row was deleted.
// A missing or still-active job is left alone, so concurrent sweeps are harmless.
func (r *Repo) DeleteFinished(ctx context.Context, id string) (bool, error) {
	tag, err := r.pool.Exec(ctx, `DELETE FROM jobs WHERE id = $1 AND status IN ('COMPLETED', 'FAILED')`, id)
	if err != nil {
		return false, fmt.Errorf("delete job %s: %w", id, err)
	}
	return tag.RowsAffected() > 0, nil
}
