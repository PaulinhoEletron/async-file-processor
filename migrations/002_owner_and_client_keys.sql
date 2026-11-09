-- owner_hash is the SHA-256 of the per-browser owner token; the job list is filtered by it.
-- client_key is the hex SHA-256 of the normalised client address, used to cap unfinished jobs
-- per client. Neither column ever holds a raw token or address.
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS owner_hash bytea;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS client_key text;

CREATE INDEX IF NOT EXISTS jobs_owner_created_idx ON jobs (owner_hash, created_at DESC);
CREATE INDEX IF NOT EXISTS jobs_unfinished_client_idx ON jobs (client_key)
    WHERE status IN ('PENDING', 'PROCESSING');
