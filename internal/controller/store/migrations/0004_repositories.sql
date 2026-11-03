-- Repositories Opifica builds: hosted on our own git server or elsewhere
-- (e.g. GitHub, triggered from a workflow).
CREATE TABLE repositories (
    id                    uuid        PRIMARY KEY,
    name                  text        NOT NULL UNIQUE CHECK (name ~ '^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$'),
    -- Where the controller and workers fetch the code.
    clone_url             text        NOT NULL,
    -- Ordered routing rules: [{"event": "push", "ref": "main", "pool": "trusted"}, ...].
    -- The first rule that matches an event decides its pool; no match, no run.
    routes                jsonb       NOT NULL,
    -- Optional read token for cloning, encrypted with the secrets key.
    credential_ciphertext bytea,
    credential_nonce      bytea,
    created_at            timestamptz NOT NULL DEFAULT now(),
    updated_at            timestamptz NOT NULL DEFAULT now()
);

-- Tokens that may trigger runs for one repository. Only hashes are stored.
CREATE TABLE trigger_tokens (
    id            uuid        PRIMARY KEY,
    repository_id uuid        NOT NULL REFERENCES repositories (id) ON DELETE CASCADE,
    token_hash    bytea       NOT NULL UNIQUE,
    description   text        NOT NULL DEFAULT '',
    created_at    timestamptz NOT NULL DEFAULT now(),
    last_used_at  timestamptz,
    revoked_at    timestamptz
);

ALTER TABLE runs ADD COLUMN repository_id uuid REFERENCES repositories (id) ON DELETE SET NULL;
