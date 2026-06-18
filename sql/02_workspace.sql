-- Harmony Retrieval MCP — workspace sessions and handles
-- Idempotent: safe to run multiple times

CREATE TABLE IF NOT EXISTS workspaces (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_active TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS handles (
    id               TEXT PRIMARY KEY,
    type             TEXT NOT NULL,
    status           TEXT NOT NULL DEFAULT 'ready',
    summary          JSONB NOT NULL DEFAULT '{}',
    storage_uri      TEXT,
    workspace_id     UUID REFERENCES workspaces(id) ON DELETE CASCADE,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_accessed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_handles_workspace
    ON handles (workspace_id);
CREATE INDEX IF NOT EXISTS idx_handles_status
    ON handles (status);
CREATE INDEX IF NOT EXISTS idx_handles_type
    ON handles (type);
CREATE INDEX IF NOT EXISTS idx_handles_last_accessed
    ON handles (last_accessed_at);
