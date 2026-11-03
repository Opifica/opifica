-- A run is one pipeline execution for one event, pinned to one commit.
CREATE TABLE runs (
    id                  uuid        PRIMARY KEY,
    repo                text        NOT NULL,
    clone_url           text        NOT NULL DEFAULT '',
    event_type          text        NOT NULL CHECK (event_type IN ('push', 'pull_request', 'tag')),
    ref                 text        NOT NULL DEFAULT '',
    commit_sha          text        NOT NULL,
    branch              text        NOT NULL DEFAULT '',
    base_branch         text        NOT NULL DEFAULT '',
    tag                 text        NOT NULL DEFAULT '',
    -- Decided by the controller's routing policy, never by the pipeline file.
    pool                text        NOT NULL,
    -- The pipeline file exactly as it was at commit_sha, for auditing.
    pipeline_source     text        NOT NULL,
    state               text        NOT NULL DEFAULT 'running'
                                    CHECK (state IN ('running', 'succeeded', 'failed', 'cancelled')),
    cancel_requested_at timestamptz,
    created_at          timestamptz NOT NULL DEFAULT now(),
    finished_at         timestamptz
);

CREATE INDEX runs_created ON runs (created_at DESC);

-- A job is one planned job instance (one matrix combination).
--
-- pending   waiting for the jobs it needs
-- queued    ready; waiting for a matching worker
-- assigned  sent to a worker, which holds a lease
-- running   the worker reported it started
-- succeeded, failed, cancelled, skipped: finished
CREATE TABLE jobs (
    id               uuid        PRIMARY KEY,
    run_id           uuid        NOT NULL REFERENCES runs (id) ON DELETE CASCADE,
    -- Planned ID, unique in the run, e.g. "test[os=linux]".
    key              text        NOT NULL,
    name             text        NOT NULL,
    -- What the job needs from a worker.
    pool             text        NOT NULL,
    os               text        NOT NULL,
    arch             text        NOT NULL DEFAULT '',
    image            text        NOT NULL,
    labels           jsonb       NOT NULL DEFAULT '{}',
    -- Keys of jobs in the same run that must succeed first.
    needs            text[]      NOT NULL DEFAULT '{}',
    -- The opifica.v1.Job sent to the worker, without the per-attempt token.
    spec             bytea       NOT NULL,
    state            text        NOT NULL
                                 CHECK (state IN ('pending', 'queued', 'assigned', 'running',
                                                  'succeeded', 'failed', 'cancelled', 'skipped')),
    attempt          integer     NOT NULL DEFAULT 0,
    max_attempts     integer     NOT NULL DEFAULT 3,
    worker_id        uuid        REFERENCES workers (id),
    lease_expires_at timestamptz,
    -- SHA-256 of the current attempt's job token.
    token_hash       bytea,
    exit_code        integer,
    error            text        NOT NULL DEFAULT '',
    created_at       timestamptz NOT NULL DEFAULT now(),
    queued_at        timestamptz,
    started_at       timestamptz,
    finished_at      timestamptz,
    UNIQUE (run_id, key)
);

CREATE INDEX jobs_queued ON jobs (pool, os, queued_at) WHERE state = 'queued';
CREATE INDEX jobs_leased ON jobs (lease_expires_at) WHERE state IN ('assigned', 'running');
CREATE INDEX jobs_worker ON jobs (worker_id) WHERE state IN ('assigned', 'running');
