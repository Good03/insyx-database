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

## Institution locations and work affiliations

`work_institutions` connects works to institutions through author affiliations,
allowing a work to belong to several institutions. Institution work/citation
counts count each work once per institution, even when several authors share it.
`institutions.latitude` and `institutions.longitude` are double precision.

Flattened exports lack institution coordinates. Supply a metadata JSON array
using the exact institution IDs present in `full_authors_info`:

```json
[
  {"institution_id": "I1", "display_name": "Institution 1", "latitude": 48.15, "longitude": 17.1},
  {"institution_id": "I2", "display_name": "Institution 2", "latitude": 50.08, "longitude": 14.43}
]
```

These are illustrative locations. Use actual institution coordinates for real
data. The metadata loader also accepts `id` and nested
`geo: {"latitude": ..., "longitude": ...}`. Coordinates must be supplied as a
pair within latitude −90…90 and longitude −180…180; invalid values abort staging.
Only institutions affiliated with imported works are loaded. Unknown coordinates
remain NULL and are excluded from the backend map. Synthetic seeders generate
demo coordinates, which do not represent real institutions.

```bash
python scripts/seed.py --input-json works.json --institutions-json institutions.json
# Or PostgreSQL-only:
python scripts/seed_postgres.py --input-json works.json --institutions-json institutions.json
```

For existing Iceberg tables, run `make init-schema` before updating the backend.
The schema script and staged lakehouse loader add missing coordinate columns
without deleting existing rows. PostgreSQL-only imports also add missing columns.
These upgrades leave existing locations NULL; use verified metadata when loading
works, or update institution rows by ID to backfill existing data. Import commands
append data unless replacement is explicitly requested; avoid importing the same
works twice. Application user tables are separate from these scientific tables.

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
`authors` and the schemas replaced by data seeders. Fresh PostgreSQL volumes
create `insyx` and apply both account migrations. The schema matches the backend's
current authentication model: UUID ID, normalized unique email, nullable name and
affiliation, password hash, nullable unique Google ID (`sub`), password-reset hash
and expiry, optional avatar, email verification, login and creation/update timestamps.
OAuth access/refresh tokens and plaintext passwords are not stored.

For an existing volume, initialization scripts do not run again. Create `insyx`
once if it does not exist, then apply **both** additive migrations in order:

```bash
docker compose exec postgres sh -c 'createdb -U "$POSTGRES_USER" insyx'
docker compose exec -T postgres sh -c 'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d insyx' < conf/postgres/migrations/001_users.sql
docker compose exec -T postgres sh -c 'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d insyx' < conf/postgres/migrations/002_users_auth_compat.sql
```

Skip `createdb` when `insyx` already exists. The scripts can be rerun. The second
migration upgrades both the earlier feature schema and the pulled backend main
schema, preserving account IDs and credentials. It renames the old snake_case
columns to the current backend's column names and adds missing columns. Normalized
email collisions or ambiguous old/new column pairs abort the transaction instead
of merging accounts or losing data. It does not automatically downgrade.
`updatedAt` is maintained by TypeORM writes. Back up accounts before upgrading.

The backend runs matching TypeORM migrations on startup, with schema synchronization
disabled. Its default deployment uses a separate PostgreSQL on port 5433. These SQL
scripts do not copy accounts between databases. To use this stack's PostgreSQL for
accounts, configure backend `DB_NAME=insyx` and explicitly point `DB_HOST`, `DB_PORT`,
`DB_USER`, and `DB_PASSWORD` at this instance. For an existing backend database,
apply the scripts there instead and retain its connection settings.

The pulled backend main already implements Google sign-in. Keep the unique
`googleId` from the server-verified Google token's `sub` claim as the provider
identity. It is distinct from the UUID used by application APIs. See
[Google's backend authentication guide](https://developers.google.com/identity/sign-in/web/backend-auth).

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
