-- Secrets are encrypted with the controller's secrets key (AES-256-GCM).
-- The admin's policy for each secret decides which runs may use it; the
-- pipeline file can only ask.
CREATE TABLE secrets (
    name                text        PRIMARY KEY CHECK (name ~ '^[A-Z_][A-Z0-9_]*$'),
    ciphertext          bytea       NOT NULL,
    nonce               bytea       NOT NULL,
    -- Which secrets key encrypted it, for key rotation.
    key_id              text        NOT NULL,
    -- Repository globs, e.g. {acme/*}. '*' alone matches every repository.
    repos               text[]      NOT NULL CHECK (cardinality(repos) > 0),
    pools               text[]      NOT NULL CHECK (cardinality(pools) > 0),
    -- Ref globs, e.g. {refs/heads/main, refs/tags/v*}. Empty: any ref.
    refs                text[]      NOT NULL DEFAULT '{}',
    -- Pull requests run code that hasn't been reviewed yet: no secrets
    -- unless the admin explicitly allows it.
    allow_pull_requests boolean     NOT NULL DEFAULT false,
    description         text        NOT NULL DEFAULT '',
    created_at          timestamptz NOT NULL DEFAULT now(),
    updated_at          timestamptz NOT NULL DEFAULT now()
);

-- Every secret request, granted or not.
CREATE TABLE secret_accesses (
    id          bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    secret_name text        NOT NULL,
    job_id      uuid,
    run_id      uuid,
    worker_id   uuid,
    granted     boolean     NOT NULL,
    reason      text        NOT NULL DEFAULT '',
    at          timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX secret_accesses_name ON secret_accesses (secret_name, at DESC);
