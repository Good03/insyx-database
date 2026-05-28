# Lakehouse Stack

**Trino + Apache Iceberg + Nessie + PostgreSQL + MinIO**

Thesis: *Lakehouse Architecture for Science of Science: Database Model Design and Experimental Query Performance Evaluation*

## Requirements

- Docker Desktop ≥ 4.x with Compose v2
- 8 GB RAM allocated to Docker (Trino needs 4 GB)

## Quick Start

```bash
# 1. Clone the repo
git clone <repo-url> && cd <repo>

# 2. Create your local secrets file
cp .env.example .env
# Edit .env and fill in your credentials

# 3. Start everything
make up

# 4. Connect to Trino
make shell-trino
```

## Service URLs

| Service       | URL                        | Access         |
|---------------|----------------------------|----------------|
| Trino Web UI  | http://<server-ip>:8080    | Public         |
| MinIO Console | http://<server-ip>:9001    | Public         |
| Nessie UI     | http://localhost:19120     | Localhost only |
| PostgreSQL    | localhost:5432             | Localhost only |

## Common Commands

```bash
make up            # start all services
make down          # stop (keep data)
make reset         # stop + wipe all data
make logs          # tail all logs
make status        # show container health
make shell-trino   # open Trino CLI
make shell-postgres # open psql
```

## First Query

```sql
CREATE SCHEMA iceberg.demo
WITH (location = 's3://iceberg/demo/');

CREATE TABLE iceberg.demo.publications (
    id     BIGINT,
    title  VARCHAR,
    year   INTEGER,
    author VARCHAR
) WITH (format = 'PARQUET', partitioning = ARRAY['year']);

INSERT INTO iceberg.demo.publications VALUES
    (1, 'Science of Science Overview',     2022, 'Fortunato'),
    (2, 'OpenAlex: A fully-open index',    2022, 'Priem'),
    (3, 'Large language models in SciSci', 2025, 'Klarák');

SELECT * FROM iceberg.demo.publications WHERE year = 2022;
```

## Project Layout

```
.
├── docker-compose.yml
├── .env.example          ← commit this
├── .env                  ← DO NOT commit (gitignored)
├── Makefile
└── conf/
    ├── postgres/
    │   └── init.sql      ← creates the nessie DB on first boot
    └── trino/
        └── catalog/
            ├── iceberg.properties
            └── postgresql.properties
```