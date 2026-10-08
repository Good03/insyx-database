# Lakehouse Stack

**Trino + Apache Iceberg + Nessie + PostgreSQL + MinIO**

Thesis: *Lakehouse Architecture for Science of Science: Database Model Design and Experimental Query Performance Evaluation*

## Requirements

- Docker Desktop ≥ 4.x with Compose v2
- 8 GB RAM allocated to Docker (Trino needs 4 GB)

## Quick Start

```bash
# 1. Clone the repo
git clone https://github.com/Good03/insyx-database.git && cd insyx-database

# 2. Create your local secrets file
cp .env.example .env
# Edit .env and fill in your credentials

uv venv .venv

source .venv/bin/activate

uv pip install -r requirements.txt

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
make init-schema   # create Iceberg SciSci tables
make seed          # insert demo SciSci data with batched Trino inserts
make benchmark     # compare Iceberg+Trino with PostgreSQL baseline
```

Python helpers need:

```bash
uv pip install -r requirements.txt
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

## Seed Data

The lakehouse seeder uses `scisci_lakehouse`. It streams rows into PostgreSQL with `COPY`, then loads Iceberg with bulk `INSERT SELECT` queries through Trino:

```bash
py scripts/seed.py --works 10000 --replace-iceberg --optimize
```

For a larger demo:

```bash
py scripts/seed.py --works 100000 --authors 20000 --institutions 5000 --sources 1000 --topics 500 --replace-iceberg --optimize
```

To load the OpenAlex AI subfield export with all work columns:

```bash
py scripts/seed.py --input-json ai_subfield_100k_all_columns.json --replace-iceberg --optimize
```

## PostgreSQL-Only Mode

`scisci_postgres` is an independent relational implementation. It does not use
Trino, Iceberg, MinIO, or the `scisci_lakehouse` staging database. The importer
creates the normalized `scisci` schema, bulk-loads the export with PostgreSQL
`COPY`, and builds indexes after loading:

```bash
make seed-postgres-json
```

For a 1,000-record smoke test:

```bash
make seed-postgres-small
```

The `works` table follows the export shape, including `field_name`, concepts,
keywords, funding, source host/ISSN, and embedding text. The loader also derives
sources, authors, institutions, citations, documents, topics, and work-topic
bridges where the JSON contains enough information.

For a quick parser check without touching PostgreSQL or Iceberg:

```bash
py scripts/seed.py --input-json ai_subfield_100k_all_columns.json --input-limit 1000 --skip-postgres-copy
```

## Application users

Application accounts live in `insyx.public.users`, separate from scientific
authors and the schemas replaced by data seeders. Fresh PostgreSQL volumes create
the `insyx` database and apply `conf/postgres/migrations/001_users.sql` automatically.
The table stores UUID IDs, normalized unique email, display name, optional avatar,
email verification, login timestamps, and a nullable unique Google subject ID.
Multiple users without a Google identity are allowed. No passwords or OAuth tokens
are stored. `updated_at` is maintained by the backend's TypeORM writes.

For an existing volume, initialization scripts do not run again. Create the
application database once if it does not exist, then apply the additive migration:

```bash
docker compose exec postgres sh -c 'createdb -U "$POSTGRES_USER" insyx'
docker compose exec -T postgres sh -c 'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d insyx' < conf/postgres/migrations/001_users.sql
```

Skip `createdb` when `insyx` already exists. The migration can be rerun and does
not delete data. The backend also runs the matching TypeORM migration on startup.
Configure its `DB_NAME=insyx`, `DB_USER` and `DB_PASSWORD` to match this stack, and
point `DB_HOST` at this PostgreSQL instance. If an existing backend uses another
database, apply the SQL there instead and retain that `DB_NAME`.

Google sign-in is prepared at the storage level. Future backend authentication
must verify Google's ID token before storing its stable `sub` in `google_subject`;
email alone must not be used to identify or automatically link a Google account.
See [Google's backend authentication guide](https://developers.google.com/identity/sign-in/web/backend-auth).

## Project Layout

```
.
├── docker-compose.yml
├── .env.example          ← commit this
├── .env                  ← DO NOT commit (gitignored)
├── Makefile
├── scripts/
│   ├── seed.py
│   └── benchmark.py
└── conf/
    ├── postgres/
    │   └── init.sql      ← creates the nessie DB on first boot
    └── trino/
        └── catalog/
            ├── iceberg.properties
            └── postgresql.properties
```
