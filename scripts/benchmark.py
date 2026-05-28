"""
Benchmark Iceberg+Trino against plain PostgreSQL for SciSci queries.

Usage:
    pip install trino psycopg[binary]
    python scripts/benchmark.py --runs 5

The script copies the current Iceberg demo data into PostgreSQL benchmark
tables, adds PostgreSQL indexes, runs the same analytical queries on both
engines, and writes a CSV report under benchmarks/.
"""

from __future__ import annotations

import argparse
import csv
import os
import statistics
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def import_clients():
    try:
        import psycopg
        import trino
    except ImportError as exc:
        missing = exc.name or "dependency"
        print(f"Missing Python package: {missing}", file=sys.stderr)
        print("Install dependencies: pip install trino psycopg[binary]", file=sys.stderr)
        raise SystemExit(2) from exc
    return psycopg, trino


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument("--warmup", type=int, default=1)
    parser.add_argument("--year-from", type=int, default=2020)
    parser.add_argument("--year-to", type=int, default=2024)
    parser.add_argument("--limit", type=int, default=0, help="Optional row limit for PostgreSQL copy.")
    parser.add_argument("--no-prepare-postgres", action="store_true")
    parser.add_argument("--out-dir", default="benchmarks")
    return parser.parse_args()


def trino_connection(trino_mod: Any):
    return trino_mod.dbapi.connect(
        host=os.getenv("TRINO_HOST", "localhost"),
        port=int(os.getenv("TRINO_PORT", "8080")),
        user=os.getenv("TRINO_USER", "benchmark"),
        catalog="iceberg",
        schema="scisci",
    )


def postgres_connection(psycopg_mod: Any):
    return psycopg_mod.connect(
        host=os.getenv("PGHOST", "localhost"),
        port=int(os.getenv("PGPORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "appdb"),
        user=os.getenv("POSTGRES_USER", "admin"),
        password=os.getenv("POSTGRES_PASSWORD", "password123"),
    )


def fetch_trino(cur: Any, sql: str) -> list[tuple[Any, ...]]:
    cur.execute(sql)
    return [tuple(row) for row in cur.fetchall()]


def prepare_postgres(pg_conn: Any, trino_cur: Any, limit: int) -> None:
    suffix = f" LIMIT {limit}" if limit > 0 else ""

    works = fetch_trino(
        trino_cur,
        """
        SELECT id, doi, title, publication_year, publication_date, type, language,
               cited_by_count, referenced_works_count, domain, field, subfield,
               primary_topic, is_oa, source_id, source_name, source_type, num_authors
        FROM iceberg.scisci.works
        """
        + suffix,
    )
    work_authors = fetch_trino(
        trino_cur,
        """
        SELECT work_id, author_id, display_name, orcid, first_institution_id,
               first_institution_name, country_code, publication_year, publication_date
        FROM iceberg.scisci.work_authors
        """
        + suffix,
    )
    authors = fetch_trino(
        trino_cur,
        """
        SELECT author_id, display_name, orcid, country_code, works_count, cited_by_count
        FROM iceberg.scisci.authors
        """
        + suffix,
    )
    work_institutions = fetch_trino(
        trino_cur,
        """
        SELECT work_id, author_id, institution_id, institution_name, country_code,
               author_position, publication_year, publication_date
        FROM iceberg.scisci.work_institutions
        """
        + suffix,
    )
    citations = fetch_trino(
        trino_cur,
        """
        SELECT citing_work_id, cited_work_id, citing_year, cited_year, citation_age, source_system
        FROM iceberg.scisci.citations
        """
        + suffix,
    )
    work_topics = fetch_trino(
        trino_cur,
        """
        SELECT work_id, topic_id, display_name, score, source_system, publication_year
        FROM iceberg.scisci.work_topics
        """
        + suffix,
    )

    with pg_conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS scisci_bench")
        cur.execute("DROP TABLE IF EXISTS scisci_bench.work_topics")
        cur.execute("DROP TABLE IF EXISTS scisci_bench.citations")
        cur.execute("DROP TABLE IF EXISTS scisci_bench.work_institutions")
        cur.execute("DROP TABLE IF EXISTS scisci_bench.work_authors")
        cur.execute("DROP TABLE IF EXISTS scisci_bench.authors")
        cur.execute("DROP TABLE IF EXISTS scisci_bench.works")

        cur.execute(
            """
            CREATE TABLE scisci_bench.works (
                id TEXT,
                doi TEXT,
                title TEXT,
                publication_year INTEGER,
                publication_date TEXT,
                type TEXT,
                language TEXT,
                cited_by_count INTEGER,
                referenced_works_count INTEGER,
                domain TEXT,
                field TEXT,
                subfield TEXT,
                primary_topic TEXT,
                is_oa BOOLEAN,
                source_id TEXT,
                source_name TEXT,
                source_type TEXT,
                num_authors INTEGER
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE scisci_bench.work_authors (
                work_id TEXT,
                author_id TEXT,
                display_name TEXT,
                orcid TEXT,
                first_institution_id TEXT,
                first_institution_name TEXT,
                country_code TEXT,
                publication_year INTEGER,
                publication_date TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE scisci_bench.authors (
                author_id TEXT,
                display_name TEXT,
                orcid TEXT,
                country_code TEXT,
                works_count INTEGER,
                cited_by_count INTEGER
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE scisci_bench.work_institutions (
                work_id TEXT,
                author_id TEXT,
                institution_id TEXT,
                institution_name TEXT,
                country_code TEXT,
                author_position TEXT,
                publication_year INTEGER,
                publication_date TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE scisci_bench.citations (
                citing_work_id TEXT,
                cited_work_id TEXT,
                citing_year INTEGER,
                cited_year INTEGER,
                citation_age INTEGER,
                source_system TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE scisci_bench.work_topics (
                work_id TEXT,
                topic_id TEXT,
                display_name TEXT,
                score DOUBLE PRECISION,
                source_system TEXT,
                publication_year INTEGER
            )
            """
        )

        if works:
            cur.executemany(
                """
                INSERT INTO scisci_bench.works VALUES
                (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                works,
            )
        if work_authors:
            cur.executemany(
                """
                INSERT INTO scisci_bench.work_authors VALUES
                (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                work_authors,
            )
        if authors:
            cur.executemany(
                """
                INSERT INTO scisci_bench.authors VALUES
                (%s, %s, %s, %s, %s, %s)
                """,
                authors,
            )
        if work_institutions:
            cur.executemany(
                """
                INSERT INTO scisci_bench.work_institutions VALUES
                (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                work_institutions,
            )
        if citations:
            cur.executemany(
                """
                INSERT INTO scisci_bench.citations VALUES
                (%s, %s, %s, %s, %s, %s)
                """,
                citations,
            )
        if work_topics:
            cur.executemany(
                """
                INSERT INTO scisci_bench.work_topics VALUES
                (%s, %s, %s, %s, %s, %s)
                """,
                work_topics,
            )

        cur.execute("CREATE INDEX bench_works_year_idx ON scisci_bench.works(publication_year)")
        cur.execute("CREATE INDEX bench_works_source_idx ON scisci_bench.works(source_name)")
        cur.execute("CREATE INDEX bench_works_id_idx ON scisci_bench.works(id)")
        cur.execute("CREATE INDEX bench_work_authors_year_author_idx ON scisci_bench.work_authors(publication_year, author_id)")
        cur.execute("CREATE INDEX bench_work_authors_work_idx ON scisci_bench.work_authors(work_id)")
        cur.execute("CREATE INDEX bench_authors_id_idx ON scisci_bench.authors(author_id)")
        cur.execute("CREATE INDEX bench_work_inst_year_inst_idx ON scisci_bench.work_institutions(publication_year, institution_id)")
        cur.execute("CREATE INDEX bench_citations_year_idx ON scisci_bench.citations(citing_year)")
        cur.execute("CREATE INDEX bench_citations_cited_idx ON scisci_bench.citations(cited_work_id)")
        cur.execute("CREATE INDEX bench_work_topics_year_topic_idx ON scisci_bench.work_topics(publication_year, topic_id)")
        cur.execute("ANALYZE scisci_bench.works")
        cur.execute("ANALYZE scisci_bench.work_authors")
        cur.execute("ANALYZE scisci_bench.authors")
        cur.execute("ANALYZE scisci_bench.work_institutions")
        cur.execute("ANALYZE scisci_bench.citations")
        cur.execute("ANALYZE scisci_bench.work_topics")

    pg_conn.commit()
    print(
        "Prepared PostgreSQL baseline: "
        f"{len(works)} works, {len(work_authors)} work_authors, {len(authors)} authors, "
        f"{len(work_institutions)} work_institutions, {len(citations)} citations, {len(work_topics)} work_topics"
    )


def query_set(year_from: int, year_to: int) -> list[dict[str, str]]:
    trino_prefix = "iceberg.scisci"
    pg_prefix = "scisci_bench"
    return [
        {
            "name": "yearly_publications",
            "trino": f"""
                SELECT publication_year, count(*) AS papers, sum(cited_by_count) AS citations
                FROM {trino_prefix}.works
                WHERE publication_year BETWEEN {year_from} AND {year_to}
                GROUP BY publication_year
                ORDER BY publication_year
            """,
            "postgres": f"""
                SELECT publication_year, count(*) AS papers, sum(cited_by_count) AS citations
                FROM {pg_prefix}.works
                WHERE publication_year BETWEEN {year_from} AND {year_to}
                GROUP BY publication_year
                ORDER BY publication_year
            """,
        },
        {
            "name": "top_cited_works",
            "trino": f"""
                SELECT id, title, cited_by_count
                FROM {trino_prefix}.works
                WHERE publication_year BETWEEN {year_from} AND {year_to}
                ORDER BY cited_by_count DESC
                LIMIT 20
            """,
            "postgres": f"""
                SELECT id, title, cited_by_count
                FROM {pg_prefix}.works
                WHERE publication_year BETWEEN {year_from} AND {year_to}
                ORDER BY cited_by_count DESC
                LIMIT 20
            """,
        },
        {
            "name": "source_aggregation",
            "trino": f"""
                SELECT source_name, count(*) AS papers, sum(cited_by_count) AS citations
                FROM {trino_prefix}.works
                WHERE publication_year BETWEEN {year_from} AND {year_to}
                GROUP BY source_name
                ORDER BY papers DESC, citations DESC
                LIMIT 20
            """,
            "postgres": f"""
                SELECT source_name, count(*) AS papers, sum(cited_by_count) AS citations
                FROM {pg_prefix}.works
                WHERE publication_year BETWEEN {year_from} AND {year_to}
                GROUP BY source_name
                ORDER BY papers DESC, citations DESC
                LIMIT 20
            """,
        },
        {
            "name": "author_productivity",
            "trino": f"""
                SELECT author_id, max(display_name) AS display_name, count(DISTINCT work_id) AS papers
                FROM {trino_prefix}.work_authors
                WHERE publication_year BETWEEN {year_from} AND {year_to}
                GROUP BY author_id
                ORDER BY papers DESC
                LIMIT 20
            """,
            "postgres": f"""
                SELECT author_id, max(display_name) AS display_name, count(DISTINCT work_id) AS papers
                FROM {pg_prefix}.work_authors
                WHERE publication_year BETWEEN {year_from} AND {year_to}
                GROUP BY author_id
                ORDER BY papers DESC
                LIMIT 20
            """,
        },
        {
            "name": "author_citation_join",
            "trino": f"""
                SELECT a.author_id, a.display_name, count(w.id) AS papers, sum(w.cited_by_count) AS citations
                FROM {trino_prefix}.works w
                JOIN {trino_prefix}.work_authors wa ON wa.work_id = w.id
                JOIN {trino_prefix}.authors a ON a.author_id = wa.author_id
                WHERE w.publication_year BETWEEN {year_from} AND {year_to}
                GROUP BY a.author_id, a.display_name
                ORDER BY citations DESC
                LIMIT 20
            """,
            "postgres": f"""
                SELECT a.author_id, a.display_name, count(w.id) AS papers, sum(w.cited_by_count) AS citations
                FROM {pg_prefix}.works w
                JOIN {pg_prefix}.work_authors wa ON wa.work_id = w.id
                JOIN {pg_prefix}.authors a ON a.author_id = wa.author_id
                WHERE w.publication_year BETWEEN {year_from} AND {year_to}
                GROUP BY a.author_id, a.display_name
                ORDER BY citations DESC
                LIMIT 20
            """,
        },
        {
            "name": "institution_productivity",
            "trino": f"""
                SELECT institution_id, max(institution_name) AS institution_name, count(DISTINCT work_id) AS papers
                FROM {trino_prefix}.work_institutions
                WHERE publication_year BETWEEN {year_from} AND {year_to}
                GROUP BY institution_id
                ORDER BY papers DESC
                LIMIT 20
            """,
            "postgres": f"""
                SELECT institution_id, max(institution_name) AS institution_name, count(DISTINCT work_id) AS papers
                FROM {pg_prefix}.work_institutions
                WHERE publication_year BETWEEN {year_from} AND {year_to}
                GROUP BY institution_id
                ORDER BY papers DESC
                LIMIT 20
            """,
        },
        {
            "name": "citation_age_distribution",
            "trino": f"""
                SELECT citation_age, count(*) AS citations
                FROM {trino_prefix}.citations
                WHERE citing_year BETWEEN {year_from} AND {year_to}
                GROUP BY citation_age
                ORDER BY citation_age
                LIMIT 20
            """,
            "postgres": f"""
                SELECT citation_age, count(*) AS citations
                FROM {pg_prefix}.citations
                WHERE citing_year BETWEEN {year_from} AND {year_to}
                GROUP BY citation_age
                ORDER BY citation_age
                LIMIT 20
            """,
        },
        {
            "name": "topic_growth",
            "trino": f"""
                SELECT display_name, count(DISTINCT work_id) AS papers, avg(score) AS avg_score
                FROM {trino_prefix}.work_topics
                WHERE publication_year BETWEEN {year_from} AND {year_to}
                GROUP BY display_name
                ORDER BY papers DESC
                LIMIT 20
            """,
            "postgres": f"""
                SELECT display_name, count(DISTINCT work_id) AS papers, avg(score) AS avg_score
                FROM {pg_prefix}.work_topics
                WHERE publication_year BETWEEN {year_from} AND {year_to}
                GROUP BY display_name
                ORDER BY papers DESC
                LIMIT 20
            """,
        },
    ]


def time_query(cur: Any, sql: str) -> tuple[float, int]:
    started = time.perf_counter()
    cur.execute(sql)
    rows = cur.fetchall()
    elapsed_ms = (time.perf_counter() - started) * 1000
    return elapsed_ms, len(rows)


def run_benchmark(trino_conn: Any, pg_conn: Any, args: argparse.Namespace) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    queries = query_set(args.year_from, args.year_to)

    trino_cur = trino_conn.cursor()
    pg_cur = pg_conn.cursor()
    try:
        for query in queries:
            for engine, cur, sql_key in (
                ("iceberg_trino", trino_cur, "trino"),
                ("postgresql", pg_cur, "postgres"),
            ):
                for i in range(args.warmup):
                    time_query(cur, query[sql_key])
                    print(f"Warmup {i + 1}: {engine} {query['name']}")

                for run in range(1, args.runs + 1):
                    elapsed_ms, row_count = time_query(cur, query[sql_key])
                    results.append(
                        {
                            "timestamp": datetime.now(timezone.utc).isoformat(),
                            "engine": engine,
                            "query": query["name"],
                            "run": run,
                            "elapsed_ms": round(elapsed_ms, 3),
                            "row_count": row_count,
                            "year_from": args.year_from,
                            "year_to": args.year_to,
                        }
                    )
                    print(f"{engine:14s} {query['name']:22s} run={run} {elapsed_ms:.2f} ms rows={row_count}")
    finally:
        getattr(trino_cur, "close", lambda: None)()
        getattr(pg_cur, "close", lambda: None)()

    return results


def write_results(results: list[dict[str, Any]], out_dir: str) -> Path:
    target_dir = Path(out_dir)
    target_dir.mkdir(parents=True, exist_ok=True)
    path = target_dir / f"results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(results[0].keys()))
        writer.writeheader()
        writer.writerows(results)
    return path


def print_summary(results: list[dict[str, Any]]) -> None:
    print("\nSummary")
    grouped: dict[tuple[str, str], list[float]] = {}
    for row in results:
        grouped.setdefault((row["engine"], row["query"]), []).append(float(row["elapsed_ms"]))

    for (engine, query), values in sorted(grouped.items()):
        avg = statistics.mean(values)
        med = statistics.median(values)
        best = min(values)
        print(f"{engine:14s} {query:22s} avg={avg:8.2f} ms median={med:8.2f} ms best={best:8.2f} ms")


def main() -> int:
    args = parse_args()
    psycopg_mod, trino_mod = import_clients()

    trino_conn = trino_connection(trino_mod)
    pg_conn = postgres_connection(psycopg_mod)
    try:
        trino_cur = trino_conn.cursor()
        try:
            if not args.no_prepare_postgres:
                prepare_postgres(pg_conn, trino_cur, args.limit)
        finally:
            getattr(trino_cur, "close", lambda: None)()

        results = run_benchmark(trino_conn, pg_conn, args)
        if not results:
            print("No benchmark results produced.", file=sys.stderr)
            return 1
        path = write_results(results, args.out_dir)
        print_summary(results)
        print(f"\nWrote CSV: {path}")
        return 0
    finally:
        trino_conn.close()
        pg_conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
