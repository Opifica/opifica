-- name: CreateJoinToken :one
INSERT INTO join_tokens (id, token_hash, pool, labels, description, expires_at)
VALUES ($1, $2, $3, $4, $5, $6)
RETURNING *;

-- name: ClaimJoinToken :one
-- Marks a valid token as used. Returns no row if the token is unknown,
-- already used or expired, so each token admits exactly one worker.
UPDATE join_tokens
SET used_at = now()
WHERE token_hash = $1
  AND used_at IS NULL
  AND expires_at > now()
RETURNING *;

-- name: SetJoinTokenWorker :exec
UPDATE join_tokens SET used_by = $2 WHERE id = $1;

-- name: ListJoinTokens :many
SELECT * FROM join_tokens ORDER BY created_at DESC;

-- name: CreateWorker :one
INSERT INTO workers (id, hostname, os, arch, pool, labels, max_concurrent_jobs, version, cert_serial, join_token_id)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
RETURNING *;

-- name: GetWorker :one
SELECT * FROM workers WHERE id = $1;

-- name: ListWorkers :many
SELECT * FROM workers ORDER BY created_at;

-- name: TouchWorker :exec
UPDATE workers SET last_seen_at = now() WHERE id = $1;

-- name: RevokeWorker :one
UPDATE workers SET revoked_at = now() WHERE id = $1 AND revoked_at IS NULL RETURNING *;
