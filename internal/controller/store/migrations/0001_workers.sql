-- One-time tokens an admin creates so a new build machine can join.
-- Only a hash of the token is stored; the token itself is shown once.
CREATE TABLE join_tokens (
    id          uuid        PRIMARY KEY,
    token_hash  bytea       NOT NULL UNIQUE,
    -- The pool is decided by the admin who created the token, never by the
    -- worker, so a machine can't place itself in a more trusted pool.
    pool        text        NOT NULL,
    labels      jsonb       NOT NULL DEFAULT '{}',
    description text        NOT NULL DEFAULT '',
    created_at  timestamptz NOT NULL DEFAULT now(),
    expires_at  timestamptz NOT NULL,
    used_at     timestamptz,
    used_by     uuid
);

CREATE TABLE workers (
    id                  uuid        PRIMARY KEY,
    hostname            text        NOT NULL,
    os                  text        NOT NULL,
    arch                text        NOT NULL,
    pool                text        NOT NULL,
    labels              jsonb       NOT NULL DEFAULT '{}',
    max_concurrent_jobs integer     NOT NULL CHECK (max_concurrent_jobs > 0),
    version             text        NOT NULL DEFAULT '',
    -- Serial of the client certificate issued at join, hex-encoded.
    cert_serial         text        NOT NULL,
    join_token_id       uuid        NOT NULL REFERENCES join_tokens (id),
    created_at          timestamptz NOT NULL DEFAULT now(),
    last_seen_at        timestamptz,
    revoked_at          timestamptz
);

ALTER TABLE join_tokens
    ADD CONSTRAINT join_tokens_used_by_fkey FOREIGN KEY (used_by) REFERENCES workers (id);

CREATE INDEX workers_pool ON workers (pool) WHERE revoked_at IS NULL;
