-- name: UpsertSecret :one
INSERT INTO secrets (name, ciphertext, nonce, key_id, repos, pools, refs, allow_pull_requests, description)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
ON CONFLICT (name) DO UPDATE
SET ciphertext = excluded.ciphertext, nonce = excluded.nonce, key_id = excluded.key_id,
    repos = excluded.repos, pools = excluded.pools, refs = excluded.refs,
    allow_pull_requests = excluded.allow_pull_requests, description = excluded.description,
    updated_at = now()
RETURNING *;

-- name: GetSecret :one
SELECT * FROM secrets WHERE name = $1;

-- name: ListSecrets :many
SELECT name, repos, pools, refs, allow_pull_requests, description, created_at, updated_at
FROM secrets ORDER BY name;

-- name: DeleteSecret :execrows
DELETE FROM secrets WHERE name = $1;

-- name: RecordSecretAccess :exec
INSERT INTO secret_accesses (secret_name, job_id, run_id, worker_id, granted, reason)
VALUES ($1, $2, $3, $4, $5, $6);

-- name: ListSecretAccesses :many
SELECT * FROM secret_accesses WHERE secret_name = $1 ORDER BY at DESC, id DESC LIMIT $2;
