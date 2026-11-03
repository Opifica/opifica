-- name: CreateRepository :one
INSERT INTO repositories (id, name, clone_url, routes, credential_ciphertext, credential_nonce)
VALUES ($1, $2, $3, $4, $5, $6)
RETURNING *;

-- name: UpdateRepository :one
UPDATE repositories
SET clone_url = $2, routes = $3, credential_ciphertext = $4, credential_nonce = $5, updated_at = now()
WHERE name = $1
RETURNING *;

-- name: GetRepositoryByName :one
SELECT * FROM repositories WHERE name = $1;

-- name: GetRepository :one
SELECT * FROM repositories WHERE id = $1;

-- name: ListRepositories :many
SELECT * FROM repositories ORDER BY name;

-- name: DeleteRepository :execrows
DELETE FROM repositories WHERE name = $1;

-- name: CreateTriggerToken :one
INSERT INTO trigger_tokens (id, repository_id, token_hash, description)
VALUES ($1, $2, $3, $4)
RETURNING *;

-- name: UseTriggerToken :one
-- Finds a live token and records its use.
UPDATE trigger_tokens SET last_used_at = now()
WHERE token_hash = $1 AND revoked_at IS NULL
RETURNING *;

-- name: ListTriggerTokens :many
SELECT * FROM trigger_tokens WHERE repository_id = $1 ORDER BY created_at;

-- name: RevokeTriggerToken :execrows
UPDATE trigger_tokens SET revoked_at = now() WHERE id = $1 AND revoked_at IS NULL;

-- name: SetRunRepository :exec
UPDATE runs SET repository_id = $2 WHERE id = $1;
