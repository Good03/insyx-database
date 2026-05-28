from __future__ import annotations

import argparse
import csv
import random
import shutil
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import psycopg
import trino


DOMAINS = ["Computer Science", "Physics", "Biology", "Medicine", "Engineering"]
FIELDS = ["Machine Learning", "Quantum Computing", "Genomics", "Epidemiology", "Robotics"]
SUBFIELDS = ["Deep Learning", "NLP", "Computer Vision", "Bioinformatics", "Signal Processing"]
TYPES = ["journal-article", "conference-paper", "review", "preprint"]
LANGUAGES = ["en", "de", "fr", "es", "zh"]
SOURCE_TYPES = ["journal", "conference", "repository"]
INSTITUTION_TYPES = ["education", "healthcare", "company", "facility", "government"]
COUNTRIES = ["SK", "CZ", "HU", "AT", "DE", "US", "GB", "BR"]

TABLE_ORDER = [
    "sources",
    "topics",
    "authors",
    "institutions",
    "works",
    "work_authors",
    "work_institutions",
    "work_topics",
    "citations",
    "documents",
    "provenance_events",
]

COLUMNS: dict[str, list[str]] = {
    "works": ["id", "doi", "title", "abstract", "publication_year", "publication_date", "type", "language", "cited_by_count", "referenced_works_count", "domain", "field", "subfield", "primary_topic", "is_oa", "source_id", "source_name", "source_type", "num_authors", "authors", "author_ids", "apc_usd"],
    "work_authors": ["work_id", "author_id", "display_name", "orcid", "first_institution_id", "first_institution_name", "country_code", "publication_year", "publication_date"],
    "authors": ["author_id", "display_name", "orcid", "country_code", "institutions_full", "works_count", "cited_by_count"],
    "sources": ["source_id", "display_name", "source_type", "publisher", "issn_l", "country_code", "is_oa", "works_count", "cited_by_count"],
    "institutions": ["institution_id", "display_name", "country_code", "institution_type", "homepage_url", "ror", "works_count", "cited_by_count"],
    "work_institutions": ["work_id", "author_id", "institution_id", "institution_name", "country_code", "author_position", "publication_year", "publication_date"],
    "citations": ["citing_work_id", "cited_work_id", "citing_year", "cited_year", "citation_age", "source_system"],
    "topics": ["topic_id", "display_name", "domain", "field", "subfield", "source_system"],
    "work_topics": ["work_id", "topic_id", "display_name", "score", "source_system", "publication_year"],
    "documents": ["document_id", "work_id", "source_system", "landing_page_url", "pdf_url", "text_object_path", "license", "is_publicly_shareable", "extraction_status", "publication_year"],
    "provenance_events": ["event_id", "entity_type", "entity_id", "source_system", "source_record_id", "source_url", "license", "payload_hash", "pipeline_version", "ingested_at", "ingested_date"],
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fast staged seed: TSV files -> PostgreSQL COPY -> Trino INSERT SELECT -> Iceberg.")
    parser.add_argument("--works", type=int, default=100_000)
    parser.add_argument("--authors", type=int, default=20_000)
    parser.add_argument("--institutions", type=int, default=5_000)
    parser.add_argument("--sources", type=int, default=1_000)
    parser.add_argument("--topics", type=int, default=500)
    parser.add_argument("--max-authors-per-work", type=int, default=8)
    parser.add_argument("--max-topics-per-work", type=int, default=3)
    parser.add_argument("--max-citations-per-work", type=int, default=5)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--id-prefix", default=datetime.now().strftime("W%Y%m%d%H%M%S"))
    parser.add_argument("--stage-dir", default="staging")
    parser.add_argument("--keep-stage-files", action="store_true")
    parser.add_argument("--replace-iceberg", action="store_true")
    parser.add_argument("--skip-iceberg-load", action="store_true")
    parser.add_argument("--load-existing-stage", action="store_true", help="Skip generation/COPY and load current scisci_stage tables into Iceberg.")
    parser.add_argument("--optimize", action="store_true")
    parser.add_argument("--pg-host", default="127.0.0.1")
    parser.add_argument("--pg-port", type=int, default=5432)
    parser.add_argument("--pg-db", default="appdb")
    parser.add_argument("--pg-user", default="admin")
    parser.add_argument("--pg-password", default="password123")
    parser.add_argument("--trino-host", default="localhost")
    parser.add_argument("--trino-port", type=int, default=8080)
    return parser.parse_args()


def pg_connect(args: argparse.Namespace):
    return psycopg.connect(
        host=args.pg_host,
        port=args.pg_port,
        dbname=args.pg_db,
        user=args.pg_user,
        password=args.pg_password,
        connect_timeout=10,
    )


def trino_connect(args: argparse.Namespace):
    return trino.dbapi.connect(host=args.trino_host, port=args.trino_port, user="seed_stage")


def rand_date() -> date:
    start = date(2000, 1, 1)
    end = date(2024, 12, 31)
    return start + timedelta(days=random.randint(0, (end - start).days))


def work_id(prefix: str, index: int) -> str:
    return f"{prefix}{index:010d}"


def clean_value(value: Any) -> Any:
    if value is None:
        return r"\N"
    if isinstance(value, bool):
        return "true" if value else "false"
    return value


def write_row(writer: csv.writer, row: tuple[Any, ...]) -> None:
    writer.writerow([clean_value(value) for value in row])


def prepare_stage(args: argparse.Namespace) -> None:
    ddl = """
    DROP SCHEMA IF EXISTS scisci_stage CASCADE;
    CREATE SCHEMA scisci_stage;
    CREATE TABLE scisci_stage.works (id text, doi text, title text, abstract text, publication_year integer, publication_date text, type text, language text, cited_by_count integer, referenced_works_count integer, domain text, field text, subfield text, primary_topic text, is_oa boolean, source_id text, source_name text, source_type text, num_authors integer, authors text, author_ids text, apc_usd double precision);
    CREATE TABLE scisci_stage.work_authors (work_id text, author_id text, display_name text, orcid text, first_institution_id text, first_institution_name text, country_code text, publication_year integer, publication_date text);
    CREATE TABLE scisci_stage.authors (author_id text, display_name text, orcid text, country_code text, institutions_full text, works_count integer, cited_by_count integer);
    CREATE TABLE scisci_stage.sources (source_id text, display_name text, source_type text, publisher text, issn_l text, country_code text, is_oa boolean, works_count integer, cited_by_count integer);
    CREATE TABLE scisci_stage.institutions (institution_id text, display_name text, country_code text, institution_type text, homepage_url text, ror text, works_count integer, cited_by_count integer);
    CREATE TABLE scisci_stage.work_institutions (work_id text, author_id text, institution_id text, institution_name text, country_code text, author_position text, publication_year integer, publication_date text);
    CREATE TABLE scisci_stage.citations (citing_work_id text, cited_work_id text, citing_year integer, cited_year integer, citation_age integer, source_system text);
    CREATE TABLE scisci_stage.topics (topic_id text, display_name text, domain text, field text, subfield text, source_system text);
    CREATE TABLE scisci_stage.work_topics (work_id text, topic_id text, display_name text, score double precision, source_system text, publication_year integer);
    CREATE TABLE scisci_stage.documents (document_id text, work_id text, source_system text, landing_page_url text, pdf_url text, text_object_path text, license text, is_publicly_shareable boolean, extraction_status text, publication_year integer);
    CREATE TABLE scisci_stage.provenance_events (event_id text, entity_type text, entity_id text, source_system text, source_record_id text, source_url text, license text, payload_hash text, pipeline_version text, ingested_at timestamp, ingested_date date);
    """
    with pg_connect(args) as conn:
        with conn.cursor() as cur:
            cur.execute(ddl)
        conn.commit()


def make_pools(args: argparse.Namespace):
    institutions = [
        {
            "institution_id": f"I{index + 1:08d}",
            "display_name": f"Institution {index + 1}",
            "country_code": random.choice(COUNTRIES),
            "institution_type": random.choice(INSTITUTION_TYPES),
            "homepage_url": f"https://institution-{index + 1}.example.org",
            "ror": f"https://ror.org/{index + 1:07d}",
        }
        for index in range(args.institutions)
    ]
    sources = [
        {
            "source_id": f"S{index + 1:08d}",
            "display_name": f"Journal {index + 1}",
            "source_type": random.choice(SOURCE_TYPES),
            "publisher": f"Publisher {random.randint(1, max(10, args.sources // 10))}",
            "issn_l": f"{random.randint(1000, 9999)}-{random.randint(1000, 9999)}",
            "country_code": random.choice(COUNTRIES),
            "is_oa": random.choice([True, False]),
        }
        for index in range(args.sources)
    ]
    topics = [
        {
            "topic_id": f"T{index + 1:08d}",
            "display_name": f"{random.choice(FIELDS)} {random.choice(SUBFIELDS)} {index + 1}",
            "domain": random.choice(DOMAINS),
            "field": random.choice(FIELDS),
            "subfield": random.choice(SUBFIELDS),
            "source_system": "synthetic",
        }
        for index in range(args.topics)
    ]
    authors = []
    for index in range(args.authors):
        institution = random.choice(institutions)
        authors.append(
            {
                "author_id": f"A{index + 1:010d}",
                "display_name": f"Author {index + 1}",
                "orcid": f"0000-{random.randint(1000,9999)}-{random.randint(1000,9999)}-{random.randint(1000,9999)}",
                "country_code": random.choice(COUNTRIES),
                "institution_id": institution["institution_id"],
                "institution_name": institution["display_name"],
                "institution_country": institution["country_code"],
            }
        )
    return institutions, sources, topics, authors


def open_writers(run_dir: Path):
    handles = {}
    writers = {}
    for table in TABLE_ORDER:
        handle = (run_dir / f"{table}.tsv").open("w", newline="", encoding="utf-8")
        handles[table] = handle
        writers[table] = csv.writer(handle, delimiter="\t", lineterminator="\n")
    return handles, writers


def generate_files(args: argparse.Namespace, run_dir: Path) -> dict[str, int]:
    random.seed(args.seed)
    institutions, sources, topics, authors = make_pools(args)
    handles, writers = open_writers(run_dir)
    counts = {table: 0 for table in TABLE_ORDER}
    now = datetime.now()

    author_works = {author["author_id"]: 0 for author in authors}
    author_citations = {author["author_id"]: 0 for author in authors}
    institution_works = {institution["institution_id"]: 0 for institution in institutions}
    institution_citations = {institution["institution_id"]: 0 for institution in institutions}
    source_works = {source["source_id"]: 0 for source in sources}
    source_citations = {source["source_id"]: 0 for source in sources}
    previous_years: list[int] = []

    try:
        for index in range(1, args.works + 1):
            wid = work_id(args.id_prefix, index)
            pub_date = rand_date()
            pub_year = pub_date.year
            source = random.choice(sources)
            chosen_topics = random.sample(topics, random.randint(1, min(args.max_topics_per_work, len(topics))))
            primary_topic = chosen_topics[0]
            chosen_authors = random.sample(authors, random.randint(1, min(args.max_authors_per_work, len(authors))))
            cited_by_count = random.randint(0, 500)
            is_oa = random.choice([True, False])
            license_name = "cc-by" if is_oa else None
            author_names = "; ".join(author["display_name"] for author in chosen_authors)
            author_ids = "; ".join(author["author_id"] for author in chosen_authors)

            write_row(
                writers["works"],
                (
                    wid,
                    f"10.{1000 + index % 9000}/synthetic-{index}",
                    f"{primary_topic['field']} study {index}",
                    f"Synthetic abstract for work {index} in {primary_topic['domain']}.",
                    pub_year,
                    str(pub_date),
                    random.choice(TYPES),
                    random.choice(LANGUAGES),
                    cited_by_count,
                    random.randint(5, 60),
                    primary_topic["domain"],
                    primary_topic["field"],
                    primary_topic["subfield"],
                    primary_topic["display_name"],
                    is_oa,
                    source["source_id"],
                    source["display_name"],
                    source["source_type"],
                    len(chosen_authors),
                    author_names,
                    author_ids,
                    round(random.uniform(0, 3000), 2),
                ),
            )
            counts["works"] += 1
            source_works[source["source_id"]] += 1
            source_citations[source["source_id"]] += cited_by_count

            seen_institutions = set()
            for position, author in enumerate(chosen_authors):
                author_works[author["author_id"]] += 1
                author_citations[author["author_id"]] += cited_by_count
                seen_institutions.add(author["institution_id"])
                write_row(writers["work_authors"], (wid, author["author_id"], author["display_name"], author["orcid"], author["institution_id"], author["institution_name"], author["country_code"], pub_year, str(pub_date)))
                counts["work_authors"] += 1
                write_row(writers["work_institutions"], (wid, author["author_id"], author["institution_id"], author["institution_name"], author["institution_country"], "first" if position == 0 else "coauthor", pub_year, str(pub_date)))
                counts["work_institutions"] += 1

            for institution_id in seen_institutions:
                institution_works[institution_id] += 1
                institution_citations[institution_id] += cited_by_count

            for topic in chosen_topics:
                write_row(writers["work_topics"], (wid, topic["topic_id"], topic["display_name"], round(random.uniform(0.5, 1.0), 4), topic["source_system"], pub_year))
                counts["work_topics"] += 1

            if previous_years:
                for _ in range(random.randint(0, args.max_citations_per_work)):
                    cited_index = random.randint(1, len(previous_years))
                    cited_year = previous_years[cited_index - 1]
                    write_row(writers["citations"], (wid, work_id(args.id_prefix, cited_index), pub_year, cited_year, pub_year - cited_year, "synthetic"))
                    counts["citations"] += 1
            previous_years.append(pub_year)

            landing_page_url = f"https://openalex.org/{wid}"
            write_row(writers["documents"], (f"D{args.id_prefix[1:]}{index:010d}", wid, "synthetic", landing_page_url, None, None, license_name, is_oa, "metadata_only", pub_year))
            counts["documents"] += 1
            write_row(writers["provenance_events"], (f"P{args.id_prefix[1:]}{index:010d}", "work", wid, "synthetic", wid, landing_page_url, license_name, f"synthetic-{wid}", "seed-stage-v2", now, now.date()))
            counts["provenance_events"] += 1

            if index % 10_000 == 0:
                print(f"generated {index:,}/{args.works:,} works")

        for source in sources:
            write_row(writers["sources"], (source["source_id"], source["display_name"], source["source_type"], source["publisher"], source["issn_l"], source["country_code"], source["is_oa"], source_works[source["source_id"]], source_citations[source["source_id"]]))
            counts["sources"] += 1
        for topic in topics:
            write_row(writers["topics"], (topic["topic_id"], topic["display_name"], topic["domain"], topic["field"], topic["subfield"], topic["source_system"]))
            counts["topics"] += 1
        for author in authors:
            write_row(writers["authors"], (author["author_id"], author["display_name"], author["orcid"], author["country_code"], author["institution_name"], author_works[author["author_id"]], author_citations[author["author_id"]]))
            counts["authors"] += 1
        for institution in institutions:
            write_row(writers["institutions"], (institution["institution_id"], institution["display_name"], institution["country_code"], institution["institution_type"], institution["homepage_url"], institution["ror"], institution_works[institution["institution_id"]], institution_citations[institution["institution_id"]]))
            counts["institutions"] += 1
    finally:
        for handle in handles.values():
            handle.close()

    return counts


def copy_files_to_postgres(args: argparse.Namespace, run_dir: Path) -> None:
    with pg_connect(args) as conn:
        with conn.cursor() as cur:
            for table in TABLE_ORDER:
                columns = ", ".join(COLUMNS[table])
                path = run_dir / f"{table}.tsv"
                started = time.perf_counter()
                with cur.copy(f"COPY scisci_stage.{table} ({columns}) FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t', NULL '\\N')") as copy:
                    with path.open("r", encoding="utf-8") as handle:
                        while chunk := handle.read(1024 * 1024):
                            copy.write(chunk)
                print(f"copied {table} to PostgreSQL in {time.perf_counter() - started:.2f}s")
        conn.commit()


def split_sql_script(text: str) -> list[str]:
    statements = []
    current = []
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("--"):
            continue
        current.append(line)
        if stripped.endswith(";"):
            statements.append("\n".join(current).rstrip(";"))
            current = []
    if current:
        statements.append("\n".join(current))
    return statements


def reset_iceberg(cur: Any) -> None:
    for table in reversed(TABLE_ORDER):
        cur.execute(f"DROP TABLE IF EXISTS iceberg.scisci.{table}")
    schema_path = Path(__file__).resolve().parents[1] / "conf" / "trino" / "schema.sql"
    for statement in split_sql_script(schema_path.read_text(encoding="utf-8")):
        cur.execute(statement)


def load_iceberg(args: argparse.Namespace) -> None:
    conn = trino_connect(args)
    cur = conn.cursor()
    try:
        if args.replace_iceberg:
            print("resetting Iceberg tables")
            reset_iceberg(cur)

        for table in TABLE_ORDER:
            columns = ", ".join(COLUMNS[table])
            started = time.perf_counter()
            cur.execute(f"INSERT INTO iceberg.scisci.{table} ({columns}) SELECT {columns} FROM postgresql.scisci_stage.{table}")
            print(f"loaded Iceberg table {table} in {time.perf_counter() - started:.2f}s")

        if args.optimize:
            for table in ["works", "work_authors", "work_institutions", "work_topics", "citations"]:
                started = time.perf_counter()
                cur.execute(f"ALTER TABLE iceberg.scisci.{table} EXECUTE optimize")
                print(f"optimized {table} in {time.perf_counter() - started:.2f}s")
    finally:
        getattr(cur, "close", lambda: None)()
        conn.close()


def main() -> int:
    total_started = time.perf_counter()
    args = parse_args()
    run_dir = Path(args.stage_dir) / args.id_prefix
    run_dir.mkdir(parents=True, exist_ok=True)

    print(f"staging {args.works:,} works with id prefix {args.id_prefix}")
    if args.load_existing_stage:
        started = time.perf_counter()
        load_iceberg(args)
        print(f"loaded existing PostgreSQL stage into Iceberg in {time.perf_counter() - started:.2f}s")
        print(f"done in {time.perf_counter() - total_started:.2f}s")
        return 0

    prepare_stage(args)

    started = time.perf_counter()
    counts = generate_files(args, run_dir)
    print(f"generated TSV files in {time.perf_counter() - started:.2f}s")
    for table in TABLE_ORDER:
        print(f"  {table}: {counts[table]:,}")

    started = time.perf_counter()
    copy_files_to_postgres(args, run_dir)
    print(f"copied all staging files to PostgreSQL in {time.perf_counter() - started:.2f}s")

    if not args.skip_iceberg_load:
        started = time.perf_counter()
        load_iceberg(args)
        print(f"loaded Iceberg in {time.perf_counter() - started:.2f}s")

    if not args.keep_stage_files:
        shutil.rmtree(run_dir, ignore_errors=True)

    print(f"done in {time.perf_counter() - total_started:.2f}s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
