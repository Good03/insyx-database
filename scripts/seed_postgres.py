"""Load the OpenAlex export directly into PostgreSQL without Trino or Iceberg."""

from __future__ import annotations

import argparse
import shutil
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import psycopg

from seed_stage import COLUMNS, TABLE_ORDER, generate_json_files, pg_type


POSTGRES_SCHEMA = "scisci"

INDEX_STATEMENTS = [
    "CREATE INDEX IF NOT EXISTS works_publication_year_idx ON scisci.works(publication_year)",
    "CREATE INDEX IF NOT EXISTS works_source_id_idx ON scisci.works(source_id)",
    "CREATE INDEX IF NOT EXISTS works_cited_by_count_idx ON scisci.works(cited_by_count DESC)",
    "CREATE INDEX IF NOT EXISTS work_authors_author_year_idx ON scisci.work_authors(author_id, publication_year)",
    "CREATE INDEX IF NOT EXISTS work_authors_work_id_idx ON scisci.work_authors(work_id)",
    "CREATE INDEX IF NOT EXISTS work_institutions_institution_year_idx ON scisci.work_institutions(institution_id, publication_year)",
    "CREATE INDEX IF NOT EXISTS work_institutions_work_id_idx ON scisci.work_institutions(work_id)",
    "CREATE INDEX IF NOT EXISTS citations_citing_year_idx ON scisci.citations(citing_year)",
    "CREATE INDEX IF NOT EXISTS citations_cited_work_id_idx ON scisci.citations(cited_work_id)",
    "CREATE INDEX IF NOT EXISTS work_topics_topic_year_idx ON scisci.work_topics(topic_id, publication_year)",
    "CREATE INDEX IF NOT EXISTS documents_work_id_idx ON scisci.documents(work_id)",
    "CREATE INDEX IF NOT EXISTS provenance_entity_idx ON scisci.provenance_events(entity_type, entity_id)",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Direct OpenAlex JSON import into PostgreSQL SciSci tables.")
    parser.add_argument("--input-json", type=Path, required=True)
    parser.add_argument("--input-limit", type=int, default=0)
    parser.add_argument("--replace", action="store_true", help="Replace the existing scisci schema before importing.")
    parser.add_argument("--stage-dir", default="staging")
    parser.add_argument("--keep-stage-files", action="store_true")
    parser.add_argument("--id-prefix", default=datetime.now().strftime("postgres-%Y%m%d%H%M%S"))
    parser.add_argument("--pg-host", default="127.0.0.1")
    parser.add_argument("--pg-port", type=int, default=5432)
    parser.add_argument("--pg-db", default="scisci_postgres")
    parser.add_argument("--pg-user", default="admin")
    parser.add_argument("--pg-password", default="password123")
    return parser.parse_args()


def connect(args: argparse.Namespace):
    return psycopg.connect(
        host=args.pg_host,
        port=args.pg_port,
        dbname=args.pg_db,
        user=args.pg_user,
        password=args.pg_password,
        connect_timeout=10,
    )


def prepare_schema(args: argparse.Namespace) -> None:
    with connect(args) as conn:
        with conn.cursor() as cur:
            if args.replace:
                cur.execute("DROP SCHEMA IF EXISTS scisci CASCADE")
            cur.execute("CREATE SCHEMA IF NOT EXISTS scisci")
            for table in TABLE_ORDER:
                columns = ", ".join(f"{column} {pg_type(column)}" for column in COLUMNS[table])
                cur.execute(f"CREATE TABLE IF NOT EXISTS {POSTGRES_SCHEMA}.{table} ({columns})")
        conn.commit()


def copy_files(args: argparse.Namespace, run_dir: Path) -> None:
    with connect(args) as conn:
        with conn.cursor() as cur:
            for table in TABLE_ORDER:
                columns = ", ".join(COLUMNS[table])
                path = run_dir / f"{table}.tsv"
                started = time.perf_counter()
                with cur.copy(
                    f"COPY {POSTGRES_SCHEMA}.{table} ({columns}) "
                    "FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t', NULL '\\N')"
                ) as copy:
                    with path.open("r", encoding="utf-8") as handle:
                        while chunk := handle.read(1024 * 1024):
                            copy.write(chunk)
                print(f"copied {table} in {time.perf_counter() - started:.2f}s")

            for statement in INDEX_STATEMENTS:
                cur.execute(statement)
            for table in TABLE_ORDER:
                cur.execute(f"ANALYZE {POSTGRES_SCHEMA}.{table}")
        conn.commit()


def main() -> int:
    started = time.perf_counter()
    args = parse_args()
    if not args.input_json.exists():
        raise FileNotFoundError(args.input_json)

    run_dir = Path(args.stage_dir) / args.id_prefix
    run_dir.mkdir(parents=True, exist_ok=True)
    try:
        print(f"staging {args.input_json} for direct PostgreSQL import into {args.pg_db}")
        counts = generate_json_files(args, run_dir)
        prepare_schema(args)
        copy_files(args, run_dir)
    finally:
        if not args.keep_stage_files:
            shutil.rmtree(run_dir, ignore_errors=True)

    print(f"loaded PostgreSQL-only SciSci schema in {time.perf_counter() - started:.2f}s")
    for table in TABLE_ORDER:
        print(f"  {table}: {counts[table]:,}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
