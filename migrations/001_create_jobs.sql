CREATE TABLE IF NOT EXISTS jobs (
    id            uuid PRIMARY KEY,
    filename      text NOT NULL,
    status        text NOT NULL DEFAULT 'PENDING'
                  CHECK (status IN ('PENDING', 'PROCESSING', 'COMPLETED', 'FAILED')),
    input_key     text NOT NULL,
    report_key    text,
    errors_key    text,
    total_rows    bigint NOT NULL DEFAULT 0,
    valid_rows    bigint NOT NULL DEFAULT 0,
    invalid_rows  bigint NOT NULL DEFAULT 0,
    attempts      integer NOT NULL DEFAULT 0,
    error_msg     text,
    created_at    timestamptz NOT NULL DEFAULT now(),
    updated_at    timestamptz NOT NULL DEFAULT now(),
    started_at    timestamptz,
    finished_at   timestamptz
);

CREATE INDEX IF NOT EXISTS jobs_created_at_idx ON jobs (created_at DESC);
