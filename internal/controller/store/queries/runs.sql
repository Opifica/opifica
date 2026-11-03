-- name: CreateRun :one
INSERT INTO runs (id, repo, clone_url, event_type, ref, commit_sha, branch, base_branch, tag, pool, pipeline_source, repository_id)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
RETURNING *;

-- name: GetRun :one
SELECT * FROM runs WHERE id = $1;

-- name: ListRuns :many
SELECT * FROM runs ORDER BY created_at DESC LIMIT $1;

-- name: CreateJob :exec
INSERT INTO jobs (id, run_id, key, name, pool, os, arch, image, labels, needs, spec, state, queued_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
        CASE WHEN $12::text = 'queued' THEN now() END);

-- name: GetJob :one
SELECT * FROM jobs WHERE id = $1;

-- name: ListRunJobs :many
SELECT * FROM jobs WHERE run_id = $1 ORDER BY created_at, key;

-- name: ClaimJob :one
-- Hands the oldest matching queued job to a worker. SKIP LOCKED makes it
-- safe for several controllers to claim at once: a job goes to one worker.
UPDATE jobs
SET state = 'assigned', worker_id = $1, lease_expires_at = sqlc.arg(lease_expires_at),
    token_hash = sqlc.arg(token_hash), attempt = attempt + 1, started_at = NULL
WHERE id = (
    SELECT q.id FROM jobs q
    JOIN runs r ON r.id = q.run_id
    WHERE q.state = 'queued'
      AND r.cancel_requested_at IS NULL
      AND q.pool = sqlc.arg(pool)
      AND q.os = sqlc.arg(os)
      AND (q.arch = '' OR q.arch = sqlc.arg(arch))
      AND q.image = ANY (sqlc.arg(images)::text[])
      AND q.labels <@ sqlc.arg(worker_labels)::jsonb
    ORDER BY q.queued_at, q.id
    LIMIT 1
    FOR UPDATE OF q SKIP LOCKED
)
  -- Re-checked after the row lock, as a second guard against double claims.
  AND state = 'queued'
RETURNING *;

-- name: UnclaimJob :exec
-- Puts back a job that couldn't be delivered to the worker that claimed it,
-- without counting the attempt.
UPDATE jobs
SET state = 'queued', worker_id = NULL, lease_expires_at = NULL, token_hash = NULL, attempt = attempt - 1
WHERE id = $1 AND worker_id = $2 AND state = 'assigned';

-- name: CountWorkerJobs :one
SELECT count(*) FROM jobs WHERE worker_id = $1 AND state IN ('assigned', 'running');

-- name: RenewLeases :exec
UPDATE jobs SET lease_expires_at = sqlc.arg(lease_expires_at)
WHERE worker_id = sqlc.arg(worker_id) AND id = ANY (sqlc.arg(job_ids)::uuid[])
  AND state IN ('assigned', 'running');

-- name: MarkJobRunning :one
UPDATE jobs SET state = 'running', started_at = coalesce(started_at, now())
WHERE id = $1 AND worker_id = $2 AND state IN ('assigned', 'running')
RETURNING *;

-- name: FinishJob :one
-- Records a final result reported by the worker holding the job.
UPDATE jobs
SET state = sqlc.arg(state), exit_code = sqlc.arg(exit_code), error = sqlc.arg(error),
    finished_at = now(), lease_expires_at = NULL, token_hash = NULL
WHERE id = sqlc.arg(id) AND worker_id = sqlc.arg(worker_id) AND state IN ('assigned', 'running')
RETURNING *;

-- name: RetryJob :one
-- An attempt failed for infrastructure reasons: queue it again if attempts
-- remain and the run isn't being cancelled, otherwise fail it.
UPDATE jobs j
SET state = CASE
        WHEN j.attempt < j.max_attempts AND r.cancel_requested_at IS NULL THEN 'queued'
        WHEN r.cancel_requested_at IS NOT NULL THEN 'cancelled'
        ELSE 'failed' END,
    queued_at = now(), worker_id = NULL, lease_expires_at = NULL, token_hash = NULL,
    error = sqlc.arg(error),
    finished_at = CASE
        WHEN j.attempt < j.max_attempts AND r.cancel_requested_at IS NULL THEN NULL
        ELSE now() END
FROM runs r
WHERE j.id = sqlc.arg(id) AND r.id = j.run_id
  AND (sqlc.narg(worker_id)::uuid IS NULL OR j.worker_id = sqlc.narg(worker_id))
  AND j.state IN ('assigned', 'running')
RETURNING j.*;

-- name: ExpiredLeases :many
SELECT id, worker_id FROM jobs
WHERE state IN ('assigned', 'running') AND lease_expires_at < now()
ORDER BY lease_expires_at
LIMIT 100;

-- name: SkipBlockedJobs :many
-- Pending jobs that need a job which didn't succeed will never run.
UPDATE jobs j
SET state = 'skipped', finished_at = now(), error = 'a job it needs did not succeed'
WHERE j.run_id = $1 AND j.state = 'pending'
  AND EXISTS (
      SELECT 1 FROM jobs d
      WHERE d.run_id = j.run_id AND d.key = ANY (j.needs)
        AND d.state IN ('failed', 'cancelled', 'skipped'))
RETURNING j.id;

-- name: QueueReadyJobs :many
-- Pending jobs whose needs have all succeeded become queued.
UPDATE jobs j
SET state = 'queued', queued_at = now()
WHERE j.run_id = $1 AND j.state = 'pending'
  AND NOT EXISTS (
      SELECT 1 FROM jobs d
      WHERE d.run_id = j.run_id AND d.key = ANY (j.needs) AND d.state <> 'succeeded')
RETURNING j.id;

-- name: CancelWaitingJobs :exec
UPDATE jobs SET state = 'cancelled', finished_at = now(), error = 'run cancelled'
WHERE run_id = $1 AND state IN ('pending', 'queued');

-- name: ActiveRunJobs :many
SELECT id, worker_id FROM jobs WHERE run_id = $1 AND state IN ('assigned', 'running');

-- name: RequestRunCancel :one
UPDATE runs SET cancel_requested_at = now()
WHERE id = $1 AND state = 'running' AND cancel_requested_at IS NULL
RETURNING *;

-- name: FinishRunIfDone :one
-- Closes the run once no job can change state any more.
UPDATE runs r
SET state = CASE
        WHEN EXISTS (SELECT 1 FROM jobs j WHERE j.run_id = r.id AND j.state = 'failed') THEN 'failed'
        WHEN r.cancel_requested_at IS NOT NULL
          OR EXISTS (SELECT 1 FROM jobs j WHERE j.run_id = r.id AND j.state = 'cancelled') THEN 'cancelled'
        ELSE 'succeeded' END,
    finished_at = now()
WHERE r.id = $1 AND r.state = 'running'
  AND NOT EXISTS (
      SELECT 1 FROM jobs j
      WHERE j.run_id = r.id AND j.state IN ('pending', 'queued', 'assigned', 'running'))
RETURNING *;

-- name: JobsToCancel :many
-- Jobs still running in runs being cancelled. Each controller tells the
-- workers it holds sessions for, so cancelling works from any process.
SELECT j.id, j.worker_id FROM jobs j
JOIN runs r ON r.id = j.run_id
WHERE r.cancel_requested_at IS NOT NULL AND j.state IN ('assigned', 'running') AND j.worker_id IS NOT NULL;

-- name: ReleaseUnstartedJobs :many
-- When a worker's session ends, jobs it was given but never reported
-- starting go back to the queue without counting the attempt: the message
-- may never have reached it. Started jobs keep their lease, since the worker
-- may reconnect and carry on.
UPDATE jobs
SET state = 'queued', worker_id = NULL, lease_expires_at = NULL, token_hash = NULL, attempt = attempt - 1
WHERE worker_id = $1 AND state = 'assigned'
RETURNING id;
