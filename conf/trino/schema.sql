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

-- ─────────────────────────────────────────
-- sources — journals, conferences, repositories
-- Dimension table used by publication/source
-- aggregation queries.
-- ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS iceberg.scisci.sources (
    source_id               VARCHAR,
    display_name            VARCHAR,
    source_type             VARCHAR,
    publisher               VARCHAR,
    issn_l                  VARCHAR,
    country_code            VARCHAR,
    is_oa                   BOOLEAN,
    works_count             INTEGER,
    cited_by_count          INTEGER,
    created_at              TIMESTAMP(6),
    updated_at              TIMESTAMP(6)
)
WITH (
    format = 'PARQUET'
);

-- ─────────────────────────────────────────
-- institutions — universities, companies,
-- hospitals, institutes.
-- Dimension table for affiliation analytics.
-- ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS iceberg.scisci.institutions (
    institution_id          VARCHAR,
    display_name            VARCHAR,
    country_code            VARCHAR,
    institution_type        VARCHAR,
    homepage_url            VARCHAR,
    ror                     VARCHAR,
    works_count             INTEGER,
    cited_by_count          INTEGER,
    created_at              TIMESTAMP(6),
    updated_at              TIMESTAMP(6)
)
WITH (
    format = 'PARQUET'
);

-- ─────────────────────────────────────────
-- work_institutions — publication-affiliation
-- bridge. Partitioned by publication_year so
-- year-filtered institution queries prune files.
-- ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS iceberg.scisci.work_institutions (
    work_id                 VARCHAR,
    author_id               VARCHAR,
    institution_id          VARCHAR,
    institution_name        VARCHAR,
    country_code            VARCHAR,
    author_position         VARCHAR,
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
-- citations — directed citation edges.
-- One row means citing_work_id cites cited_work_id.
-- Partitioned by citing_year for citation-window
-- and forecasting queries.
-- ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS iceberg.scisci.citations (
    citing_work_id          VARCHAR,
    cited_work_id           VARCHAR,
    citing_year             INTEGER,
    cited_year              INTEGER,
    citation_age            INTEGER,
    source_system           VARCHAR,
    created_at              TIMESTAMP(6),
    updated_at              TIMESTAMP(6)
)
WITH (
    format       = 'PARQUET',
    partitioning = ARRAY['citing_year']
);

-- ─────────────────────────────────────────
-- topics — controlled topic/concept dimension.
-- OpenAlex concepts/topics, internal clusters,
-- or model-generated communities can map here.
-- ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS iceberg.scisci.topics (
    topic_id                VARCHAR,
    display_name            VARCHAR,
    domain                  VARCHAR,
    field                   VARCHAR,
    subfield                VARCHAR,
    source_system           VARCHAR,
    created_at              TIMESTAMP(6),
    updated_at              TIMESTAMP(6)
)
WITH (
    format = 'PARQUET'
);

-- ─────────────────────────────────────────
-- work_topics — publication-topic bridge with
-- confidence/score. Partitioned by publication_year
-- for trend and topic-dynamics queries.
-- ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS iceberg.scisci.work_topics (
    work_id                 VARCHAR,
    topic_id                VARCHAR,
    display_name            VARCHAR,
    score                   DOUBLE,
    source_system           VARCHAR,
    publication_year        INTEGER,
    created_at              TIMESTAMP(6),
    updated_at              TIMESTAMP(6)
)
WITH (
    format       = 'PARQUET',
    partitioning = ARRAY['publication_year']
);

-- ─────────────────────────────────────────
-- documents — full-text/document availability
-- and extraction state. Keep text payloads outside
-- the hot metadata table.
-- ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS iceberg.scisci.documents (
    document_id             VARCHAR,
    work_id                 VARCHAR,
    source_system           VARCHAR,
    landing_page_url        VARCHAR,
    pdf_url                 VARCHAR,
    text_object_path        VARCHAR,
    license                 VARCHAR,
    is_publicly_shareable   BOOLEAN,
    extraction_status       VARCHAR,
    publication_year        INTEGER,
    created_at              TIMESTAMP(6),
    updated_at              TIMESTAMP(6)
)
WITH (
    format       = 'PARQUET',
    partitioning = ARRAY['publication_year']
);

-- ─────────────────────────────────────────
-- provenance_events — trace each derived row back
-- to source system, record, license and pipeline.
-- This supports auditability and ERC trust goals.
-- ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS iceberg.scisci.provenance_events (
    event_id                VARCHAR,
    entity_type             VARCHAR,
    entity_id               VARCHAR,
    source_system           VARCHAR,
    source_record_id        VARCHAR,
    source_url              VARCHAR,
    license                 VARCHAR,
    payload_hash            VARCHAR,
    pipeline_version        VARCHAR,
    ingested_at             TIMESTAMP(6),
    ingested_date           DATE
)
WITH (
    format = 'PARQUET'
);
