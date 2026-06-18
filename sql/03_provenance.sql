-- Harmony Retrieval MCP — data lineage graph
-- Idempotent: safe to run multiple times

CREATE TABLE IF NOT EXISTS provenance_nodes (
    handle_id          TEXT PRIMARY KEY,
    product_short_name TEXT,
    granule_ids        JSONB NOT NULL DEFAULT '[]',
    doi                TEXT,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS provenance_edges (
    id             SERIAL PRIMARY KEY,
    from_handle    TEXT NOT NULL,
    to_handle      TEXT NOT NULL,
    transform_name TEXT NOT NULL,
    parameters     JSONB NOT NULL DEFAULT '{}',
    assumptions    JSONB NOT NULL DEFAULT '[]',
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_prov_nodes_handle
    ON provenance_nodes (handle_id);
CREATE INDEX IF NOT EXISTS idx_prov_edges_from
    ON provenance_edges (from_handle);
CREATE INDEX IF NOT EXISTS idx_prov_edges_to
    ON provenance_edges (to_handle);
