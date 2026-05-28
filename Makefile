.PHONY: up down reset logs status shell-trino shell-postgres init-schema seed seed-small seed-100k seed-stage-only load-stage optimize counts benchmark benchmark-fast demo

PY ?= py -3.12
WORKS ?= 100000
AUTHORS ?= 20000
INSTITUTIONS ?= 5000
SOURCES ?= 1000
TOPICS ?= 500
BENCH_RUNS ?= 5
BENCH_WARMUP ?= 1

# Start all services
up:
	docker compose up -d

# Stop all services (keep data)
down:
	docker compose down

# Stop and wipe all data volumes (full reset)
reset:
	docker compose down -v

# Tail logs for all services (Ctrl+C to stop)
logs:
	docker compose logs -f

# Show status of all containers
status:
	docker compose ps

# Open Trino CLI
shell-trino:
	docker exec -it trino trino

# Open PostgreSQL shell
shell-postgres:
	docker exec -it postgres psql -U $${POSTGRES_USER} -d $${POSTGRES_DB}

# Create Iceberg schema and tables (run once after first 'make up')
init-schema:
	docker exec -i trino trino < conf/trino/schema.sql

# Generate configurable SciSci data with PostgreSQL staging + Iceberg bulk load
seed:
	$(PY) scripts/seed.py --works $(WORKS) --authors $(AUTHORS) --institutions $(INSTITUTIONS) --sources $(SOURCES) --topics $(TOPICS) --replace-iceberg --optimize

# Small quick demo dataset
seed-small:
	$(PY) scripts/seed.py --works 10000 --authors 2000 --institutions 500 --sources 100 --topics 100 --replace-iceberg --optimize

# Default supervisor-scale local demo dataset
seed-100k:
	$(PY) scripts/seed.py --works 100000 --authors 20000 --institutions 5000 --sources 1000 --topics 500 --replace-iceberg --optimize

# Generate and copy to PostgreSQL staging only, no Iceberg load
seed-stage-only:
	$(PY) scripts/seed.py --works $(WORKS) --authors $(AUTHORS) --institutions $(INSTITUTIONS) --sources $(SOURCES) --topics $(TOPICS) --skip-iceberg-load

# Load already prepared PostgreSQL stage tables into Iceberg
load-stage:
	$(PY) scripts/seed.py --load-existing-stage --replace-iceberg --optimize

# Compact Iceberg small files after appends or experiments
optimize:
	docker exec trino trino --execute "ALTER TABLE iceberg.scisci.works EXECUTE optimize; ALTER TABLE iceberg.scisci.work_authors EXECUTE optimize; ALTER TABLE iceberg.scisci.work_institutions EXECUTE optimize; ALTER TABLE iceberg.scisci.work_topics EXECUTE optimize; ALTER TABLE iceberg.scisci.citations EXECUTE optimize;"

# Show main table counts from Iceberg
counts:
	docker exec trino trino --execute "SELECT 'works' AS table_name, COUNT(*) AS rows FROM iceberg.scisci.works UNION ALL SELECT 'authors', COUNT(*) FROM iceberg.scisci.authors UNION ALL SELECT 'institutions', COUNT(*) FROM iceberg.scisci.institutions UNION ALL SELECT 'work_authors', COUNT(*) FROM iceberg.scisci.work_authors UNION ALL SELECT 'citations', COUNT(*) FROM iceberg.scisci.citations UNION ALL SELECT 'work_topics', COUNT(*) FROM iceberg.scisci.work_topics;"

# Run same analytical queries on Iceberg+Trino and PostgreSQL baseline
benchmark:
	$(PY) scripts/benchmark.py --runs $(BENCH_RUNS) --warmup $(BENCH_WARMUP)

# One-pass benchmark smoke test
benchmark-fast:
	$(PY) scripts/benchmark.py --runs 1 --warmup 0

# End-to-end local demo: stack, schema, 100k data, counts, benchmark
demo: up init-schema seed-100k counts benchmark-fast
