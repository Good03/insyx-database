-- ─────────────────────────────────────────
-- Schema: iceberg.scisci
-- Run once via: make shell-trino < conf/trino/schema.sql
-- ─────────────────────────────────────────

CREATE SCHEMA IF NOT EXISTS iceberg.scisci
WITH (location = 's3://iceberg/scisci/');

-- ─────────────────────────────────────────
-- works — one row per publication
-- Partitioned by publication_year for fast
-- year-range queries (core thesis benchmark)
-- ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS iceberg.scisci.works (
    -- Identity
    id                      VARCHAR,
    doi                     VARCHAR,
    -- Content
    title                   VARCHAR,
    abstract                VARCHAR,
    publication_year        INTEGER,
    publication_date        VARCHAR,
    type                    VARCHAR,
    language                VARCHAR,
    -- Metrics
    cited_by_count          INTEGER,
    referenced_works_count  INTEGER,
    -- Classification
    domain                  VARCHAR,
    field                   VARCHAR,
    subfield                VARCHAR,
    primary_topic           VARCHAR,
    topics                  VARCHAR,    -- "Topic1, Topic2, Topic3"
    concepts                VARCHAR,    -- "Concept1, Concept2"
    concepts_full           VARCHAR,    -- full JSON string
    keywords                VARCHAR,    -- "kw1, kw2, kw3"
    keywords_full           VARCHAR,    -- full JSON string
    -- References
    references_full         VARCHAR,    -- full JSON string
    related_full            VARCHAR,
    -- Open access
    is_oa                   BOOLEAN,
    oa_url                  VARCHAR,
    pdf_url                 VARCHAR,
    license                 VARCHAR,
    -- Source / journal
    source_id               VARCHAR,
    source_name             VARCHAR,
    source_type             VARCHAR,
    -- Authors (denormalized flat copy for fast queries)
    num_authors             INTEGER,
    authors                 VARCHAR,    -- "John Smith; Jane Doe"
    author_ids              VARCHAR,    -- "A123; A456"
    full_authors_info       VARCHAR,    -- full JSON string
    -- APC
    apc_currency            VARCHAR,
    apc_value               DOUBLE,
    apc_usd                 DOUBLE,
    -- Timestamps
    created_at              TIMESTAMP(6),
    updated_at              TIMESTAMP(6)
)
WITH (
    format       = 'PARQUET',
    partitioning = ARRAY['publication_year']
);

-- ─────────────────────────────────────────
-- work_authors — one row per author per paper
-- Enables per-author and co-authorship queries
-- Partitioned by publication_year to align
-- with works table for efficient joins
-- ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS iceberg.scisci.work_authors (
    work_id                 VARCHAR,
    author_id               VARCHAR,
    display_name            VARCHAR,
    orcid                   VARCHAR,
    first_institution_id    VARCHAR,
    first_institution_name  VARCHAR,
    country_code            VARCHAR,
    institutions_full       VARCHAR,    -- full JSON string
    institutions_raw        VARCHAR,
    publication_year        INTEGER,
    publication_date        VARCHAR,
    created_at              TIMESTAMP(6),
    updated_at              TIMESTAMP(6)
)
WITH (
    format       = 'PARQUET',
    partitioning = ARRAY['publication_year']
);

-- ─────────────────────────────────────────
-- authors — dimension table, one row per author
-- Deduplicated author profiles
-- Not partitioned (relatively small, ~10M rows)
-- ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS iceberg.scisci.authors (
    author_id               VARCHAR,
    display_name            VARCHAR,
    orcid                   VARCHAR,
    country_code            VARCHAR,
    institutions_full       VARCHAR,
    institutions_raw        VARCHAR,
    works_count             INTEGER,
    cited_by_count          INTEGER,
    created_at              TIMESTAMP(6),
    updated_at              TIMESTAMP(6)
)
WITH (
    format = 'PARQUET'
);