-- Harmony Retrieval MCP — discovery catalog enrichment tables
-- Idempotent: safe to run multiple times

CREATE EXTENSION IF NOT EXISTS postgis;

CREATE TABLE IF NOT EXISTS catalog_concepts (
    id          SERIAL PRIMARY KEY,
    name        TEXT NOT NULL UNIQUE,
    aliases     JSONB NOT NULL DEFAULT '[]',
    disciplines JSONB NOT NULL DEFAULT '[]',
    variables   JSONB NOT NULL DEFAULT '[]',
    products    JSONB NOT NULL DEFAULT '[]',
    keywords    JSONB NOT NULL DEFAULT '[]',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS catalog_variables (
    id                  SERIAL PRIMARY KEY,
    name                TEXT NOT NULL UNIQUE,
    long_name           TEXT,
    units               TEXT,
    valid_range         JSONB,
    description         TEXT,
    quality_flags       JSONB NOT NULL DEFAULT '[]',
    preprocessing_notes JSONB NOT NULL DEFAULT '[]',
    related_variables   JSONB NOT NULL DEFAULT '[]',
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS catalog_products (
    id               SERIAL PRIMARY KEY,
    short_name       TEXT NOT NULL UNIQUE,
    long_name        TEXT,
    version          TEXT,
    mission          TEXT,
    instrument       TEXT,
    concept_ids      JSONB NOT NULL DEFAULT '[]',
    variables        JSONB NOT NULL DEFAULT '[]',
    resolution       TEXT,
    cadence          TEXT,
    coverage         TEXT,
    temporal_extent  TEXT,
    disciplines      JSONB NOT NULL DEFAULT '[]',
    access           JSONB NOT NULL DEFAULT '[]',
    limitations      JSONB NOT NULL DEFAULT '[]',
    citation_doi     TEXT,
    provider         TEXT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_concepts_name
    ON catalog_concepts (name);
CREATE INDEX IF NOT EXISTS idx_products_short_name
    ON catalog_products (short_name);
CREATE INDEX IF NOT EXISTS idx_products_provider
    ON catalog_products (provider);
