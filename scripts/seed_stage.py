from __future__ import annotations

import argparse
import csv
import hashlib
import json
import random
import re
import shutil
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable

import psycopg


DOMAINS = ["Computer Science", "Physics", "Biology", "Medicine", "Engineering"]
FIELDS = ["Machine Learning", "Quantum Computing", "Genomics", "Epidemiology", "Robotics"]
SUBFIELDS = ["Deep Learning", "NLP", "Computer Vision", "Bioinformatics", "Signal Processing"]
TYPES = ["journal-article", "conference-paper", "review", "preprint"]
LANGUAGES = ["en", "de", "fr", "es", "zh"]
SOURCE_TYPES = ["journal", "conference", "repository"]
INSTITUTION_TYPES = ["education", "healthcare", "company", "facility", "government"]
COUNTRIES = ["SK", "CZ", "HU", "AT", "DE", "US", "GB", "BR"]

OPENALEX_SOURCE_SYSTEM = "openalex_export"
MEMORY_ADDRESS_PATTERN = re.compile(r"^<memory at 0x[0-9a-fA-F]+>$")
OPTIMIZE_PARTITIONS_PER_QUERY = 50

OPTIMIZE_PARTITION_COLUMNS = {
    "works": "publication_year",
    "work_authors": "publication_year",
    "work_institutions": "publication_year",
    "work_topics": "publication_year",
    "citations": "citing_year",
}

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

WORK_COLUMNS = [
    "id",
    "doi",
    "title",
    "publication_year",
    "publication_date",
    "type",
    "language",
    "cited_by_count",
    "referenced_works_count",
    "domain",
    "field_name",
    "subfield",
    "primary_topic",
    "topics",
    "concepts",
    "concepts_full",
    "keywords",
    "keywords_full",
    "references_full",
    "related_full",
    "license",
    "pdf_url",
    "abstract",
    "is_oa",
    "oa_url",
    "source_id",
    "source_name",
    "source_type",
    "authors",
    "author_ids",
    "num_authors",
    "full_authors_info",
    "apc_currency",
    "apc_value",
    "apc_usd",
    "funders",
    "grants",
    "created_at",
    "updated_at",
    "source_host_name",
    "source_issn",
    "cited_by_count_int",
    "embedding",
]

COLUMNS: dict[str, list[str]] = {
    "works": WORK_COLUMNS,
    "work_authors": [
        "work_id",
        "author_id",
        "display_name",
        "orcid",
        "first_institution_id",
        "first_institution_name",
        "country_code",
        "institutions_full",
        "institutions_raw",
        "publication_year",
        "publication_date",
        "created_at",
        "updated_at",
    ],
    "authors": [
        "author_id",
        "display_name",
        "orcid",
        "country_code",
        "institutions_full",
        "institutions_raw",
        "works_count",
        "cited_by_count",
        "created_at",
        "updated_at",
    ],
    "sources": [
        "source_id",
        "display_name",
        "source_type",
        "publisher",
        "issn_l",
        "host_name",
        "country_code",
        "is_oa",
        "works_count",
        "cited_by_count",
        "created_at",
        "updated_at",
    ],
    "institutions": [
        "institution_id",
        "display_name",
        "country_code",
        "institution_type",
        "homepage_url",
        "ror",
        "works_count",
        "cited_by_count",
        "created_at",
        "updated_at",
    ],
    "work_institutions": [
        "work_id",
        "author_id",
        "institution_id",
        "institution_name",
        "country_code",
        "author_position",
        "publication_year",
        "publication_date",
        "created_at",
        "updated_at",
    ],
    "citations": [
        "citing_work_id",
        "cited_work_id",
        "citing_year",
        "cited_year",
        "citation_age",
        "source_system",
        "created_at",
        "updated_at",
    ],
    "topics": [
        "topic_id",
        "display_name",
        "domain",
        "field_name",
        "subfield",
        "source_system",
        "created_at",
        "updated_at",
    ],
    "work_topics": [
        "work_id",
        "topic_id",
        "display_name",
        "score",
        "source_system",
        "publication_year",
        "created_at",
        "updated_at",
    ],
    "documents": [
        "document_id",
        "work_id",
        "source_system",
        "landing_page_url",
        "pdf_url",
        "text_object_path",
        "license",
        "is_publicly_shareable",
        "extraction_status",
        "publication_year",
        "created_at",
        "updated_at",
    ],
    "provenance_events": [
        "event_id",
        "entity_type",
        "entity_id",
        "source_system",
        "source_record_id",
        "source_url",
        "license",
        "payload_hash",
        "pipeline_version",
        "ingested_at",
        "ingested_date",
    ],
}

INTEGER_COLUMNS = {
    "publication_year",
    "cited_by_count",
    "referenced_works_count",
    "num_authors",
    "cited_by_count_int",
    "works_count",
    "citing_year",
    "cited_year",
    "citation_age",
}
DOUBLE_COLUMNS = {"apc_value", "apc_usd", "score"}
BOOLEAN_COLUMNS = {"is_oa", "is_publicly_shareable"}
TIMESTAMP_COLUMNS = {"created_at", "updated_at", "ingested_at"}
DATE_COLUMNS = {"ingested_date"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Staged seed: TSV files -> PostgreSQL COPY -> Trino INSERT SELECT -> Iceberg.")
    parser.add_argument("--input-json", type=Path, help="OpenAlex all-columns JSON export to load instead of synthetic data.")
    parser.add_argument("--input-limit", type=int, default=0, help="Optional maximum number of JSON records to import.")
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
    parser.add_argument("--skip-postgres-copy", action="store_true", help="Only generate TSV files; do not create/copy PostgreSQL staging tables.")
    parser.add_argument("--skip-iceberg-load", action="store_true")
    parser.add_argument("--load-existing-stage", action="store_true", help="Skip generation/COPY and load current scisci_stage tables into Iceberg.")
    parser.add_argument("--optimize", action="store_true")
    parser.add_argument("--optimize-only", action="store_true", help="Compact existing Iceberg tables without loading staging data.")
    parser.add_argument("--pg-host", default="127.0.0.1")
    parser.add_argument("--pg-port", type=int, default=5432)
    parser.add_argument("--pg-db", default="scisci_lakehouse")
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
    try:
        import trino
    except ImportError as exc:
        raise SystemExit("Install the lakehouse dependencies with: pip install -r requirements.txt") from exc
    return trino.dbapi.connect(host=args.trino_host, port=args.trino_port, user="seed_stage")


def rand_date() -> date:
    start = date(2000, 1, 1)
    end = date(2024, 12, 31)
    return start + timedelta(days=random.randint(0, (end - start).days))


def work_id(prefix: str, index: int) -> str:
    return f"{prefix}{index:010d}"


def stable_id(prefix: str, value: str) -> str:
    digest = hashlib.sha1(value.encode("utf-8")).hexdigest()[:16].upper()
    return f"{prefix}{digest}"


def pg_type(column: str) -> str:
    if column in INTEGER_COLUMNS:
        return "integer"
    if column in DOUBLE_COLUMNS:
        return "double precision"
    if column in BOOLEAN_COLUMNS:
        return "boolean"
    if column in TIMESTAMP_COLUMNS:
        return "timestamp"
    if column in DATE_COLUMNS:
        return "date"
    return "text"


def clean_value(value: Any) -> Any:
    if value is None:
        return r"\N"
    if isinstance(value, bool):
        return "true" if value else "false"
    return value


def write_row(writer: csv.writer, row: tuple[Any, ...]) -> None:
    writer.writerow([clean_value(value) for value in row])


def row_for(table: str, values: dict[str, Any]) -> tuple[Any, ...]:
    return tuple(values.get(column) for column in COLUMNS[table])


def text_value(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, (list, dict)):
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    return str(value)


def embedding_value(value: Any) -> str | None:
    """Return a serializable embedding, excluding Python memory-address placeholders."""
    text = text_value(value)
    if text and MEMORY_ADDRESS_PATTERN.fullmatch(text.strip()):
        return None
    return text


def has_value(value: Any) -> bool:
    return value is not None and not (isinstance(value, str) and value.strip() == "")


def to_int(value: Any) -> int | None:
    if not has_value(value):
        return None
    if isinstance(value, bool):
        return int(value)
    try:
        return int(value)
    except (TypeError, ValueError):
        try:
            return int(float(str(value)))
        except (TypeError, ValueError):
            return None


def to_float(value: Any) -> float | None:
    if not has_value(value):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def to_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"true", "t", "yes", "y", "1"}:
        return True
    if text in {"false", "f", "no", "n", "0"}:
        return False
    return None


def first_present(*values: Any) -> Any:
    for value in values:
        if has_value(value):
            return value
    return None


def update_if_present(target: dict[str, Any], key: str, value: Any) -> None:
    if has_value(value) and not has_value(target.get(key)):
        target[key] = value


def split_comma_list(value: Any) -> list[str]:
    text = text_value(value)
    if not text:
        return []
    return [part.strip() for part in text.split(",") if part.strip()]


def split_semicolon_list(value: Any) -> list[str]:
    text = text_value(value)
    if not text:
        return []
    return [part.strip() for part in text.split(";") if part.strip()]


def split_identifier_list(value: Any) -> list[str]:
    text = text_value(value)
    if not text:
        return []
    return [part.strip() for part in re.split(r"[;,]", text) if part.strip()]


def split_work_ids(value: Any) -> list[str]:
    return [part for part in split_identifier_list(value) if part.startswith("W")]


def parse_concepts_full(value: Any) -> list[dict[str, Any]]:
    concepts = []
    for raw in split_semicolon_list(value):
        parts = [part.strip() for part in raw.split(",")]
        if len(parts) < 4:
            continue
        name = ", ".join(parts[:-3]).strip()
        concept_id = parts[-3] or stable_id("C", name)
        level = to_int(parts[-2])
        score = to_float(parts[-1])
        if name:
            concepts.append(
                {
                    "topic_id": concept_id,
                    "display_name": name,
                    "level": level,
                    "score": score,
                    "source_system": "openalex_concept",
                }
            )
    return concepts


def parse_full_authors_info(value: Any) -> list[dict[str, Any]]:
    authors = []
    for raw in split_semicolon_list(value):
        parts = [part.strip() or None for part in raw.split(",")]
        if not parts:
            continue
        author_id = parts[0] if len(parts) > 0 else None
        orcid = parts[1] if len(parts) > 1 else None
        display_name = parts[2] if len(parts) > 2 else None
        raw_affiliation = parts[3] if len(parts) > 3 else None
        institution_id = parts[4] if len(parts) > 4 else None
        institution_name = parts[5] if len(parts) > 5 else None
        institution_type = parts[6] if len(parts) > 6 else None
        country_code = parts[7] if len(parts) > 7 else None
        if not author_id and not display_name:
            continue
        if not author_id and display_name:
            author_id = stable_id("A", display_name)
        institutions_full = raw if institution_id or institution_name else None
        authors.append(
            {
                "author_id": author_id,
                "display_name": display_name,
                "orcid": orcid,
                "first_institution_id": institution_id,
                "first_institution_name": institution_name,
                "institution_type": institution_type,
                "country_code": country_code,
                "institutions_full": institutions_full,
                "institutions_raw": raw_affiliation or raw,
            }
        )
    return authors


def parse_authors(record: dict[str, Any]) -> list[dict[str, Any]]:
    parsed = parse_full_authors_info(record.get("full_authors_info"))
    if parsed:
        return parsed

    names = split_semicolon_list(record.get("authors"))
    ids = split_identifier_list(record.get("author_ids"))
    authors = []
    for index in range(max(len(names), len(ids))):
        display_name = names[index] if index < len(names) else None
        author_id = ids[index] if index < len(ids) else None
        if not author_id and display_name:
            author_id = stable_id("A", display_name)
        if not author_id and not display_name:
            continue
        authors.append(
            {
                "author_id": author_id,
                "display_name": display_name,
                "orcid": None,
                "first_institution_id": None,
                "first_institution_name": None,
                "institution_type": None,
                "country_code": None,
                "institutions_full": None,
                "institutions_raw": None,
            }
        )
    return authors


def iter_json_data_records(path: Path) -> Iterable[dict[str, Any]]:
    decoder = json.JSONDecoder()
    buffer = ""
    eof = False
    in_array = False
    chunk_size = 1024 * 1024

    with path.open("r", encoding="utf-8") as handle:
        while True:
            if not eof and (not in_array or len(buffer) < chunk_size):
                chunk = handle.read(chunk_size)
                if chunk:
                    buffer += chunk
                else:
                    eof = True

            if not in_array:
                stripped = buffer.lstrip()
                if stripped.startswith("["):
                    buffer = stripped[1:]
                    in_array = True
                else:
                    key_index = buffer.find('"data"')
                    if key_index == -1:
                        if eof:
                            raise ValueError(f"No top-level data array found in {path}")
                        buffer = buffer[-64:]
                        continue
                    bracket_index = buffer.find("[", key_index)
                    if bracket_index == -1:
                        if eof:
                            raise ValueError(f"No data array opening bracket found in {path}")
                        continue
                    buffer = buffer[bracket_index + 1 :]
                    in_array = True

            while True:
                buffer = buffer.lstrip()
                if buffer.startswith("]"):
                    return
                if buffer.startswith(","):
                    buffer = buffer[1:]
                    continue
                if not buffer:
                    break
                try:
                    record, consumed = decoder.raw_decode(buffer)
                except json.JSONDecodeError:
                    if eof:
                        raise
                    break
                if not isinstance(record, dict):
                    raise ValueError(f"Expected object records in {path}")
                yield record
                buffer = buffer[consumed:]

            if eof:
                tail = buffer.strip()
                if not tail or tail.startswith("]"):
                    return
                raise ValueError(f"Unexpected trailing JSON while reading {path}")


def prepare_stage(args: argparse.Namespace) -> None:
    statements = ["DROP SCHEMA IF EXISTS scisci_stage CASCADE", "CREATE SCHEMA scisci_stage"]
    for table in TABLE_ORDER:
        column_defs = ", ".join(f"{column} {pg_type(column)}" for column in COLUMNS[table])
        statements.append(f"CREATE TABLE scisci_stage.{table} ({column_defs})")
    ddl = ";\n".join(statements) + ";"

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
            "host_name": f"source-{index + 1}.example.org",
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
            "field_name": random.choice(FIELDS),
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
                "institution_type": institution["institution_type"],
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
            topics_text = ", ".join(topic["display_name"] for topic in chosen_topics)
            full_authors_info = "; ".join(
                (
                    f"{author['author_id']}, {author['orcid']}, {author['display_name']}, , "
                    f"{author['institution_id']}, {author['institution_name']}, "
                    f"{author['institution_type']}, {author['institution_country']}"
                )
                for author in chosen_authors
            )

            work_values = {
                "id": wid,
                "doi": f"10.{1000 + index % 9000}/synthetic-{index}",
                "title": f"{primary_topic['field_name']} study {index}",
                "publication_year": pub_year,
                "publication_date": str(pub_date),
                "type": random.choice(TYPES),
                "language": random.choice(LANGUAGES),
                "cited_by_count": cited_by_count,
                "referenced_works_count": random.randint(5, 60),
                "domain": primary_topic["domain"],
                "field_name": primary_topic["field_name"],
                "subfield": primary_topic["subfield"],
                "primary_topic": primary_topic["display_name"],
                "topics": topics_text,
                "concepts": None,
                "concepts_full": None,
                "keywords": None,
                "keywords_full": None,
                "references_full": None,
                "related_full": None,
                "license": license_name,
                "pdf_url": None,
                "abstract": f"Synthetic abstract for work {index} in {primary_topic['domain']}.",
                "is_oa": is_oa,
                "oa_url": f"https://openalex.org/{wid}" if is_oa else None,
                "source_id": source["source_id"],
                "source_name": source["display_name"],
                "source_type": source["source_type"],
                "authors": author_names,
                "author_ids": author_ids,
                "num_authors": len(chosen_authors),
                "full_authors_info": full_authors_info,
                "apc_currency": "USD",
                "apc_value": None,
                "apc_usd": round(random.uniform(0, 3000), 2),
                "funders": None,
                "grants": None,
                "created_at": now,
                "updated_at": now,
                "source_host_name": source["host_name"],
                "source_issn": source["issn_l"],
                "cited_by_count_int": cited_by_count,
                "embedding": None,
            }
            source_works[source["source_id"]] += 1
            source_citations[source["source_id"]] += cited_by_count

            seen_institutions = set()
            for position, author in enumerate(chosen_authors):
                author_works[author["author_id"]] += 1
                author_citations[author["author_id"]] += cited_by_count
                seen_institutions.add(author["institution_id"])
                write_row(
                    writers["work_authors"],
                    row_for(
                        "work_authors",
                        {
                            "work_id": wid,
                            "author_id": author["author_id"],
                            "display_name": author["display_name"],
                            "orcid": author["orcid"],
                            "first_institution_id": author["institution_id"],
                            "first_institution_name": author["institution_name"],
                            "country_code": author["country_code"],
                            "institutions_full": author["institution_name"],
                            "institutions_raw": author["institution_name"],
                            "publication_year": pub_year,
                            "publication_date": str(pub_date),
                            "created_at": now,
                            "updated_at": now,
                        },
                    ),
                )
                counts["work_authors"] += 1
                write_row(
                    writers["work_institutions"],
                    row_for(
                        "work_institutions",
                        {
                            "work_id": wid,
                            "author_id": author["author_id"],
                            "institution_id": author["institution_id"],
                            "institution_name": author["institution_name"],
                            "country_code": author["institution_country"],
                            "author_position": "first" if position == 0 else "coauthor",
                            "publication_year": pub_year,
                            "publication_date": str(pub_date),
                            "created_at": now,
                            "updated_at": now,
                        },
                    ),
                )
                counts["work_institutions"] += 1

            for institution_id in seen_institutions:
                institution_works[institution_id] += 1
                institution_citations[institution_id] += cited_by_count

            for topic in chosen_topics:
                write_row(
                    writers["work_topics"],
                    row_for(
                        "work_topics",
                        {
                            "work_id": wid,
                            "topic_id": topic["topic_id"],
                            "display_name": topic["display_name"],
                            "score": round(random.uniform(0.5, 1.0), 4),
                            "source_system": topic["source_system"],
                            "publication_year": pub_year,
                            "created_at": now,
                            "updated_at": now,
                        },
                    ),
                )
                counts["work_topics"] += 1

            cited_ids: list[str] = []
            if previous_years:
                for _ in range(random.randint(0, args.max_citations_per_work)):
                    cited_index = random.randint(1, len(previous_years))
                    cited_year = previous_years[cited_index - 1]
                    cited_work_id = work_id(args.id_prefix, cited_index)
                    cited_ids.append(cited_work_id)
                    write_row(
                        writers["citations"],
                        row_for(
                            "citations",
                            {
                                "citing_work_id": wid,
                                "cited_work_id": cited_work_id,
                                "citing_year": pub_year,
                                "cited_year": cited_year,
                                "citation_age": pub_year - cited_year,
                                "source_system": "synthetic",
                                "created_at": now,
                                "updated_at": now,
                            },
                        ),
                    )
                    counts["citations"] += 1
            if cited_ids:
                work_values["references_full"] = ", ".join(cited_ids) + ","
                work_values["referenced_works_count"] = len(cited_ids)
            write_row(writers["works"], row_for("works", work_values))
            counts["works"] += 1
            previous_years.append(pub_year)

            landing_page_url = f"https://openalex.org/{wid}"
            write_row(
                writers["documents"],
                row_for(
                    "documents",
                    {
                        "document_id": stable_id("D", wid),
                        "work_id": wid,
                        "source_system": "synthetic",
                        "landing_page_url": landing_page_url,
                        "pdf_url": None,
                        "text_object_path": None,
                        "license": license_name,
                        "is_publicly_shareable": is_oa,
                        "extraction_status": "metadata_only",
                        "publication_year": pub_year,
                        "created_at": now,
                        "updated_at": now,
                    },
                ),
            )
            counts["documents"] += 1
            write_row(
                writers["provenance_events"],
                row_for(
                    "provenance_events",
                    {
                        "event_id": stable_id("P", wid),
                        "entity_type": "work",
                        "entity_id": wid,
                        "source_system": "synthetic",
                        "source_record_id": wid,
                        "source_url": landing_page_url,
                        "license": license_name,
                        "payload_hash": stable_id("H", wid),
                        "pipeline_version": "seed-stage-v3",
                        "ingested_at": now,
                        "ingested_date": now.date(),
                    },
                ),
            )
            counts["provenance_events"] += 1

            if index % 10_000 == 0:
                print(f"generated {index:,}/{args.works:,} works")

        for source in sources:
            write_row(
                writers["sources"],
                row_for(
                    "sources",
                    {
                        "source_id": source["source_id"],
                        "display_name": source["display_name"],
                        "source_type": source["source_type"],
                        "publisher": source["publisher"],
                        "issn_l": source["issn_l"],
                        "host_name": source["host_name"],
                        "country_code": source["country_code"],
                        "is_oa": source["is_oa"],
                        "works_count": source_works[source["source_id"]],
                        "cited_by_count": source_citations[source["source_id"]],
                        "created_at": now,
                        "updated_at": now,
                    },
                ),
            )
            counts["sources"] += 1
        for topic in topics:
            write_row(
                writers["topics"],
                row_for(
                    "topics",
                    {
                        "topic_id": topic["topic_id"],
                        "display_name": topic["display_name"],
                        "domain": topic["domain"],
                        "field_name": topic["field_name"],
                        "subfield": topic["subfield"],
                        "source_system": topic["source_system"],
                        "created_at": now,
                        "updated_at": now,
                    },
                ),
            )
            counts["topics"] += 1
        for author in authors:
            write_row(
                writers["authors"],
                row_for(
                    "authors",
                    {
                        "author_id": author["author_id"],
                        "display_name": author["display_name"],
                        "orcid": author["orcid"],
                        "country_code": author["country_code"],
                        "institutions_full": author["institution_name"],
                        "institutions_raw": author["institution_name"],
                        "works_count": author_works[author["author_id"]],
                        "cited_by_count": author_citations[author["author_id"]],
                        "created_at": now,
                        "updated_at": now,
                    },
                ),
            )
            counts["authors"] += 1
        for institution in institutions:
            write_row(
                writers["institutions"],
                row_for(
                    "institutions",
                    {
                        "institution_id": institution["institution_id"],
                        "display_name": institution["display_name"],
                        "country_code": institution["country_code"],
                        "institution_type": institution["institution_type"],
                        "homepage_url": institution["homepage_url"],
                        "ror": institution["ror"],
                        "works_count": institution_works[institution["institution_id"]],
                        "cited_by_count": institution_citations[institution["institution_id"]],
                        "created_at": now,
                        "updated_at": now,
                    },
                ),
            )
            counts["institutions"] += 1
    finally:
        for handle in handles.values():
            handle.close()

    return counts


def generate_json_files(args: argparse.Namespace, run_dir: Path) -> dict[str, int]:
    if args.input_json is None:
        raise ValueError("--input-json is required for JSON import")
    input_path = args.input_json
    if not input_path.exists():
        raise FileNotFoundError(input_path)

    handles, writers = open_writers(run_dir)
    counts = {table: 0 for table in TABLE_ORDER}
    now = datetime.now()
    source_stats: dict[str, dict[str, Any]] = {}
    topic_stats: dict[str, dict[str, Any]] = {}
    author_stats: dict[str, dict[str, Any]] = {}
    institution_stats: dict[str, dict[str, Any]] = {}
    discarded_embedding_count = 0

    try:
        for record in iter_json_data_records(input_path):
            if args.input_limit > 0 and counts["works"] >= args.input_limit:
                break

            cited_by_count = to_int(record.get("cited_by_count")) or 0
            cited_by_count_int = to_int(record.get("cited_by_count_int"))
            if cited_by_count_int is None:
                cited_by_count_int = cited_by_count
            pub_year = to_int(record.get("publication_year"))
            wid = text_value(record.get("id"))
            if not wid:
                continue

            authors = parse_authors(record)
            authors_text = text_value(record.get("authors"))
            author_ids_text = text_value(record.get("author_ids"))
            if not authors_text and authors:
                authors_text = "; ".join(author["display_name"] for author in authors if author.get("display_name"))
            if not author_ids_text and authors:
                author_ids_text = ", ".join(author["author_id"] for author in authors if author.get("author_id"))

            source_id = text_value(record.get("source_id"))
            source_name = text_value(record.get("source_name"))
            if not source_id and source_name:
                source_id = stable_id("S", source_name)

            embedding = embedding_value(record.get("embedding"))
            if record.get("embedding") is not None and embedding is None:
                discarded_embedding_count += 1

            work_values = {
                "id": wid,
                "doi": text_value(record.get("doi")),
                "title": text_value(record.get("title")),
                "publication_year": pub_year,
                "publication_date": text_value(record.get("publication_date")),
                "type": text_value(record.get("type")),
                "language": text_value(record.get("language")),
                "cited_by_count": cited_by_count,
                "referenced_works_count": to_int(record.get("referenced_works_count")),
                "domain": text_value(record.get("domain")),
                "field_name": text_value(first_present(record.get("field_name"), record.get("field"))),
                "subfield": text_value(record.get("subfield")),
                "primary_topic": text_value(record.get("primary_topic")),
                "topics": text_value(record.get("topics")),
                "concepts": text_value(record.get("concepts")),
                "concepts_full": text_value(record.get("concepts_full")),
                "keywords": text_value(record.get("keywords")),
                "keywords_full": text_value(record.get("keywords_full")),
                "references_full": text_value(record.get("references_full")),
                "related_full": text_value(record.get("related_full")),
                "license": text_value(record.get("license")),
                "pdf_url": text_value(record.get("pdf_url")),
                "abstract": text_value(record.get("abstract")),
                "is_oa": to_bool(record.get("is_oa")),
                "oa_url": text_value(record.get("oa_url")),
                "source_id": source_id,
                "source_name": source_name,
                "source_type": text_value(record.get("source_type")),
                "authors": authors_text,
                "author_ids": author_ids_text,
                "num_authors": to_int(record.get("num_authors")),
                "full_authors_info": text_value(record.get("full_authors_info")),
                "apc_currency": text_value(record.get("apc_currency")),
                "apc_value": to_float(record.get("apc_value")),
                "apc_usd": to_float(record.get("apc_usd")),
                "funders": text_value(record.get("funders")),
                "grants": text_value(record.get("grants")),
                "created_at": text_value(record.get("created_at")),
                "updated_at": text_value(record.get("updated_at")),
                "source_host_name": text_value(record.get("source_host_name")),
                "source_issn": text_value(record.get("source_issn")),
                "cited_by_count_int": cited_by_count_int,
                "embedding": embedding,
            }
            write_row(writers["works"], row_for("works", work_values))
            counts["works"] += 1

            if source_id or source_name:
                source_key = source_id or stable_id("S", source_name or "unknown")
                source = source_stats.setdefault(
                    source_key,
                    {
                        "source_id": source_key,
                        "display_name": source_name,
                        "source_type": text_value(record.get("source_type")),
                        "publisher": None,
                        "issn_l": text_value(record.get("source_issn")),
                        "host_name": text_value(record.get("source_host_name")),
                        "country_code": None,
                        "is_oa": False,
                        "works_count": 0,
                        "cited_by_count": 0,
                        "created_at": now,
                        "updated_at": now,
                    },
                )
                update_if_present(source, "display_name", source_name)
                update_if_present(source, "source_type", text_value(record.get("source_type")))
                update_if_present(source, "issn_l", text_value(record.get("source_issn")))
                update_if_present(source, "host_name", text_value(record.get("source_host_name")))
                source["works_count"] += 1
                source["cited_by_count"] += cited_by_count
                source["is_oa"] = bool(source["is_oa"] or work_values["is_oa"])

            for position, author in enumerate(authors):
                author_id = author.get("author_id")
                display_name = author.get("display_name")
                if not author_id and display_name:
                    author_id = stable_id("A", display_name)
                if not author_id:
                    continue

                first_institution_id = author.get("first_institution_id")
                first_institution_name = author.get("first_institution_name")
                if not first_institution_id and first_institution_name:
                    first_institution_id = stable_id("I", first_institution_name)

                write_row(
                    writers["work_authors"],
                    row_for(
                        "work_authors",
                        {
                            "work_id": wid,
                            "author_id": author_id,
                            "display_name": display_name,
                            "orcid": author.get("orcid"),
                            "first_institution_id": first_institution_id,
                            "first_institution_name": first_institution_name,
                            "country_code": author.get("country_code"),
                            "institutions_full": author.get("institutions_full"),
                            "institutions_raw": author.get("institutions_raw"),
                            "publication_year": pub_year,
                            "publication_date": work_values["publication_date"],
                            "created_at": now,
                            "updated_at": now,
                        },
                    ),
                )
                counts["work_authors"] += 1

                author_row = author_stats.setdefault(
                    author_id,
                    {
                        "author_id": author_id,
                        "display_name": display_name,
                        "orcid": author.get("orcid"),
                        "country_code": author.get("country_code"),
                        "institutions_full": author.get("institutions_full"),
                        "institutions_raw": author.get("institutions_raw"),
                        "works_count": 0,
                        "cited_by_count": 0,
                        "created_at": now,
                        "updated_at": now,
                    },
                )
                update_if_present(author_row, "display_name", display_name)
                update_if_present(author_row, "orcid", author.get("orcid"))
                update_if_present(author_row, "country_code", author.get("country_code"))
                update_if_present(author_row, "institutions_full", author.get("institutions_full"))
                update_if_present(author_row, "institutions_raw", author.get("institutions_raw"))
                author_row["works_count"] += 1
                author_row["cited_by_count"] += cited_by_count

                if first_institution_id or first_institution_name:
                    institution_id = first_institution_id or stable_id("I", first_institution_name or "unknown")
                    write_row(
                        writers["work_institutions"],
                        row_for(
                            "work_institutions",
                            {
                                "work_id": wid,
                                "author_id": author_id,
                                "institution_id": institution_id,
                                "institution_name": first_institution_name,
                                "country_code": author.get("country_code"),
                                "author_position": "first" if position == 0 else "coauthor",
                                "publication_year": pub_year,
                                "publication_date": work_values["publication_date"],
                                "created_at": now,
                                "updated_at": now,
                            },
                        ),
                    )
                    counts["work_institutions"] += 1

                    institution_row = institution_stats.setdefault(
                        institution_id,
                        {
                            "institution_id": institution_id,
                            "display_name": first_institution_name,
                            "country_code": author.get("country_code"),
                            "institution_type": author.get("institution_type"),
                            "homepage_url": None,
                            "ror": None,
                            "works_count": 0,
                            "cited_by_count": 0,
                            "created_at": now,
                            "updated_at": now,
                        },
                    )
                    update_if_present(institution_row, "display_name", first_institution_name)
                    update_if_present(institution_row, "country_code", author.get("country_code"))
                    update_if_present(institution_row, "institution_type", author.get("institution_type"))
                    institution_row["works_count"] += 1
                    institution_row["cited_by_count"] += cited_by_count

            topic_names = split_comma_list(work_values["topics"])
            primary_topic = work_values["primary_topic"]
            if primary_topic and primary_topic not in topic_names:
                topic_names.insert(0, primary_topic)
            for topic_name in topic_names:
                topic_id = stable_id("T", topic_name)
                topic_key = f"openalex_topic:{topic_id}"
                topic_stats.setdefault(
                    topic_key,
                    {
                        "topic_id": topic_id,
                        "display_name": topic_name,
                        "domain": work_values["domain"],
                        "field_name": work_values["field_name"],
                        "subfield": work_values["subfield"],
                        "source_system": "openalex_topic",
                        "created_at": now,
                        "updated_at": now,
                    },
                )
                write_row(
                    writers["work_topics"],
                    row_for(
                        "work_topics",
                        {
                            "work_id": wid,
                            "topic_id": topic_id,
                            "display_name": topic_name,
                            "score": 1.0 if topic_name == primary_topic else None,
                            "source_system": "openalex_topic",
                            "publication_year": pub_year,
                            "created_at": now,
                            "updated_at": now,
                        },
                    ),
                )
                counts["work_topics"] += 1

            for concept in parse_concepts_full(work_values["concepts_full"]):
                topic_key = f"{concept['source_system']}:{concept['topic_id']}"
                topic_stats.setdefault(
                    topic_key,
                    {
                        "topic_id": concept["topic_id"],
                        "display_name": concept["display_name"],
                        "domain": work_values["domain"],
                        "field_name": work_values["field_name"],
                        "subfield": work_values["subfield"],
                        "source_system": concept["source_system"],
                        "created_at": now,
                        "updated_at": now,
                    },
                )
                write_row(
                    writers["work_topics"],
                    row_for(
                        "work_topics",
                        {
                            "work_id": wid,
                            "topic_id": concept["topic_id"],
                            "display_name": concept["display_name"],
                            "score": concept["score"],
                            "source_system": concept["source_system"],
                            "publication_year": pub_year,
                            "created_at": now,
                            "updated_at": now,
                        },
                    ),
                )
                counts["work_topics"] += 1

            for cited_id in split_work_ids(work_values["references_full"]):
                write_row(
                    writers["citations"],
                    row_for(
                        "citations",
                        {
                            "citing_work_id": wid,
                            "cited_work_id": cited_id,
                            "citing_year": pub_year,
                            "cited_year": None,
                            "citation_age": None,
                            "source_system": OPENALEX_SOURCE_SYSTEM,
                            "created_at": now,
                            "updated_at": now,
                        },
                    ),
                )
                counts["citations"] += 1

            landing_page_url = f"https://openalex.org/{wid}"
            write_row(
                writers["documents"],
                row_for(
                    "documents",
                    {
                        "document_id": stable_id("D", wid),
                        "work_id": wid,
                        "source_system": OPENALEX_SOURCE_SYSTEM,
                        "landing_page_url": landing_page_url,
                        "pdf_url": work_values["pdf_url"],
                        "text_object_path": None,
                        "license": work_values["license"],
                        "is_publicly_shareable": work_values["is_oa"],
                        "extraction_status": "pdf_available" if work_values["pdf_url"] else "metadata_only",
                        "publication_year": pub_year,
                        "created_at": now,
                        "updated_at": now,
                    },
                ),
            )
            counts["documents"] += 1

            payload = json.dumps(record, ensure_ascii=False, sort_keys=True)
            write_row(
                writers["provenance_events"],
                row_for(
                    "provenance_events",
                    {
                        "event_id": stable_id("P", wid),
                        "entity_type": "work",
                        "entity_id": wid,
                        "source_system": OPENALEX_SOURCE_SYSTEM,
                        "source_record_id": wid,
                        "source_url": landing_page_url,
                        "license": work_values["license"],
                        "payload_hash": hashlib.sha1(payload.encode("utf-8")).hexdigest(),
                        "pipeline_version": "seed-stage-openalex-json-v1",
                        "ingested_at": now,
                        "ingested_date": now.date(),
                    },
                ),
            )
            counts["provenance_events"] += 1

            if counts["works"] % 10_000 == 0:
                print(f"generated {counts['works']:,} works from {input_path}")

        for source in source_stats.values():
            write_row(writers["sources"], row_for("sources", source))
            counts["sources"] += 1
        for topic in topic_stats.values():
            write_row(writers["topics"], row_for("topics", topic))
            counts["topics"] += 1
        for author in author_stats.values():
            write_row(writers["authors"], row_for("authors", author))
            counts["authors"] += 1
        for institution in institution_stats.values():
            write_row(writers["institutions"], row_for("institutions", institution))
            counts["institutions"] += 1
    finally:
        for handle in handles.values():
            handle.close()

    if discarded_embedding_count:
        print(
            f"discarded {discarded_embedding_count:,} non-serializable embedding placeholder(s) "
            f"from {input_path}"
        )

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


def optimize_table(cur: Any, table: str, partition_column: str) -> None:
    cur.execute(
        f"SELECT DISTINCT {partition_column} "
        f"FROM iceberg.scisci.{table} "
        f"WHERE {partition_column} IS NOT NULL "
        f"ORDER BY {partition_column}"
    )
    partitions = [int(row[0]) for row in cur.fetchall()]
    if not partitions:
        print(f"optimized {table}: no partitions")
        return

    started = time.perf_counter()
    for offset in range(0, len(partitions), OPTIMIZE_PARTITIONS_PER_QUERY):
        batch = partitions[offset : offset + OPTIMIZE_PARTITIONS_PER_QUERY]
        partition_list = ", ".join(str(value) for value in batch)
        cur.execute(
            f"ALTER TABLE iceberg.scisci.{table} EXECUTE optimize "
            f"WHERE {partition_column} IN ({partition_list})"
        )
        print(f"optimized {table}: {min(offset + len(batch), len(partitions))}/{len(partitions)} partitions")

    print(f"optimized {table} in {time.perf_counter() - started:.2f}s")


def optimize_iceberg(args: argparse.Namespace) -> None:
    conn = trino_connect(args)
    cur = conn.cursor()
    try:
        for table, partition_column in OPTIMIZE_PARTITION_COLUMNS.items():
            optimize_table(cur, table, partition_column)
    finally:
        getattr(cur, "close", lambda: None)()
        conn.close()


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
            optimize_iceberg(args)
    finally:
        getattr(cur, "close", lambda: None)()
        conn.close()


def main() -> int:
    total_started = time.perf_counter()
    args = parse_args()
    if args.optimize_only and (args.input_json or args.load_existing_stage or args.replace_iceberg):
        raise SystemExit("--optimize-only cannot be combined with load options")

    if args.optimize_only:
        started = time.perf_counter()
        optimize_iceberg(args)
        print(f"optimized existing Iceberg tables in {time.perf_counter() - started:.2f}s")
        return 0

    run_dir = Path(args.stage_dir) / args.id_prefix
    run_dir.mkdir(parents=True, exist_ok=True)

    if args.load_existing_stage:
        started = time.perf_counter()
        load_iceberg(args)
        print(f"loaded existing PostgreSQL stage into Iceberg in {time.perf_counter() - started:.2f}s")
        print(f"done in {time.perf_counter() - total_started:.2f}s")
        return 0

    if args.input_json:
        source_label = str(args.input_json)
        if args.input_limit > 0:
            source_label += f" (limit {args.input_limit:,})"
        print(f"staging OpenAlex JSON export {source_label}")
    else:
        print(f"staging {args.works:,} synthetic works with id prefix {args.id_prefix}")

    if not args.skip_postgres_copy:
        prepare_stage(args)

    started = time.perf_counter()
    counts = generate_json_files(args, run_dir) if args.input_json else generate_files(args, run_dir)
    print(f"generated TSV files in {time.perf_counter() - started:.2f}s")
    for table in TABLE_ORDER:
        print(f"  {table}: {counts[table]:,}")

    if args.skip_postgres_copy:
        print("skipped PostgreSQL COPY and Iceberg load")
    else:
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
