from __future__ import annotations

import argparse
import random
import time
from collections import defaultdict
from datetime import date, timedelta
from typing import Any, Iterable

import trino
from faker import Faker


DOMAINS = ["Computer Science", "Physics", "Biology", "Medicine", "Engineering"]
FIELDS = ["Machine Learning", "Quantum Computing", "Genomics", "Epidemiology", "Robotics"]
SUBFIELDS = ["Deep Learning", "NLP", "Computer Vision", "Bioinformatics", "Signal Processing"]
TYPES = ["journal-article", "conference-paper", "review", "preprint"]
LANGUAGES = ["en", "de", "fr", "es", "zh"]
SOURCE_TYPES = ["journal", "conference", "repository"]
INSTITUTION_TYPES = ["education", "healthcare", "company", "facility", "government"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fast demo seed for Iceberg SciSci tables.")
    parser.add_argument("--works", type=int, default=500)
    parser.add_argument("--authors", type=int, default=200)
    parser.add_argument("--institutions", type=int, default=100)
    parser.add_argument("--sources", type=int, default=50)
    parser.add_argument("--topics", type=int, default=40)
    parser.add_argument("--batch-size", type=int, default=250)
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=8080)
    return parser.parse_args()


def connect(args: argparse.Namespace):
    return trino.dbapi.connect(
        host=args.host,
        port=args.port,
        user="seed",
        catalog="iceberg",
        schema="scisci",
    )


def rand_date(start_year: int = 2000, end_year: int = 2024) -> date:
    start = date(start_year, 1, 1)
    end = date(end_year, 12, 31)
    return start + timedelta(days=random.randint(0, (end - start).days))


def sql_literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    text = str(value).replace("'", "''")
    return f"'{text}'"


def chunks(values: list[tuple[Any, ...]], size: int) -> Iterable[list[tuple[Any, ...]]]:
    for index in range(0, len(values), size):
        yield values[index : index + size]


def insert_rows(
    cursor: Any,
    table: str,
    columns: list[str],
    rows: list[tuple[Any, ...]],
    batch_size: int,
) -> None:
    if not rows:
        print(f"{table}: 0 rows")
        return

    columns_sql = ", ".join(columns)
    inserted = 0
    started = time.perf_counter()
    for batch in chunks(rows, batch_size):
        values_sql = ",\n".join(
            "(" + ", ".join(sql_literal(value) for value in row) + ")" for row in batch
        )
        cursor.execute(f"INSERT INTO iceberg.scisci.{table} ({columns_sql}) VALUES {values_sql}")
        inserted += len(batch)
        print(f"{table}: {inserted}/{len(rows)} rows")

    elapsed = time.perf_counter() - started
    print(f"{table}: inserted {len(rows)} rows in {elapsed:.2f}s")


def build_data(args: argparse.Namespace) -> dict[str, list[tuple[Any, ...]]]:
    fake = Faker()

    institutions = [
        {
            "institution_id": f"I{idx + 1:07d}",
            "display_name": fake.company(),
            "country_code": fake.country_code(),
            "institution_type": random.choice(INSTITUTION_TYPES),
            "homepage_url": fake.url(),
            "ror": f"https://ror.org/{fake.lexify('???????')}",
        }
        for idx in range(args.institutions)
    ]

    sources = [
        {
            "source_id": f"S{idx + 1:07d}",
            "display_name": f"{fake.company()} Journal",
            "source_type": random.choice(SOURCE_TYPES),
            "publisher": fake.company(),
            "issn_l": f"{random.randint(1000, 9999)}-{random.randint(1000, 9999)}",
            "country_code": fake.country_code(),
            "is_oa": random.choice([True, False]),
        }
        for idx in range(args.sources)
    ]

    topics = [
        {
            "topic_id": f"T{idx + 1:07d}",
            "display_name": f"{random.choice(FIELDS)} {random.choice(SUBFIELDS)}",
            "domain": random.choice(DOMAINS),
            "field": random.choice(FIELDS),
            "subfield": random.choice(SUBFIELDS),
            "source_system": "mock",
        }
        for idx in range(args.topics)
    ]

    authors = []
    for idx in range(args.authors):
        institution = random.choice(institutions)
        authors.append(
            {
                "author_id": f"A{idx + 1:09d}",
                "display_name": fake.name(),
                "orcid": f"0000-{random.randint(1000,9999)}-{random.randint(1000,9999)}-{random.randint(1000,9999)}",
                "country_code": fake.country_code(),
                "institution_id": institution["institution_id"],
                "institution_name": institution["display_name"],
                "institution_country": institution["country_code"],
            }
        )

    works = []
    for idx in range(args.works):
        pub_date = rand_date()
        paper_authors = random.sample(authors, random.randint(1, min(8, len(authors))))
        source = random.choice(sources)
        selected_topics = random.sample(topics, random.randint(1, min(3, len(topics))))
        primary_topic = selected_topics[0]

        works.append(
            {
                "id": f"W{idx + 1:010d}",
                "doi": f"10.{random.randint(1000,9999)}/{fake.lexify('??????')}",
                "title": fake.sentence(nb_words=8).rstrip("."),
                "abstract": fake.paragraph(nb_sentences=4),
                "publication_year": pub_date.year,
                "publication_date": str(pub_date),
                "type": random.choice(TYPES),
                "language": random.choice(LANGUAGES),
                "cited_by_count": random.randint(0, 500),
                "referenced_works_count": random.randint(5, 60),
                "domain": primary_topic["domain"],
                "field": primary_topic["field"],
                "subfield": primary_topic["subfield"],
                "primary_topic": primary_topic["display_name"],
                "is_oa": random.choice([True, False]),
                "source_id": source["source_id"],
                "source_name": source["display_name"],
                "source_type": source["source_type"],
                "num_authors": len(paper_authors),
                "authors": paper_authors,
                "topics": selected_topics,
                "apc_usd": round(random.uniform(0, 3000), 2),
            }
        )

    author_citations: defaultdict[str, int] = defaultdict(int)
    author_works_count: defaultdict[str, int] = defaultdict(int)
    institution_citations: defaultdict[str, int] = defaultdict(int)
    institution_works_count: defaultdict[str, int] = defaultdict(int)
    source_citations: defaultdict[str, int] = defaultdict(int)
    source_works_count: defaultdict[str, int] = defaultdict(int)

    works_rows = []
    work_author_rows = []
    work_institution_rows = []
    work_topic_rows = []
    document_rows = []
    provenance_rows = []
    citation_rows = []

    for idx, work in enumerate(works, start=1):
        author_names = "; ".join(author["display_name"] for author in work["authors"])
        author_ids = "; ".join(author["author_id"] for author in work["authors"])
        works_rows.append(
            (
                work["id"],
                work["doi"],
                work["title"],
                work["abstract"],
                work["publication_year"],
                work["publication_date"],
                work["type"],
                work["language"],
                work["cited_by_count"],
                work["referenced_works_count"],
                work["domain"],
                work["field"],
                work["subfield"],
                work["primary_topic"],
                work["is_oa"],
                work["source_id"],
                work["source_name"],
                work["source_type"],
                work["num_authors"],
                author_names,
                author_ids,
                work["apc_usd"],
            )
        )

        source_works_count[work["source_id"]] += 1
        source_citations[work["source_id"]] += work["cited_by_count"]
        seen_institutions = set()

        for pos, author in enumerate(work["authors"]):
            author_citations[author["author_id"]] += work["cited_by_count"]
            author_works_count[author["author_id"]] += 1
            seen_institutions.add(author["institution_id"])
            work_author_rows.append(
                (
                    work["id"],
                    author["author_id"],
                    author["display_name"],
                    author["orcid"],
                    author["institution_id"],
                    author["institution_name"],
                    author["country_code"],
                    work["publication_year"],
                    work["publication_date"],
                )
            )
            work_institution_rows.append(
                (
                    work["id"],
                    author["author_id"],
                    author["institution_id"],
                    author["institution_name"],
                    author["institution_country"],
                    "first" if pos == 0 else "coauthor",
                    work["publication_year"],
                    work["publication_date"],
                )
            )

        for institution_id in seen_institutions:
            institution_works_count[institution_id] += 1
            institution_citations[institution_id] += work["cited_by_count"]

        for topic in work["topics"]:
            work_topic_rows.append(
                (
                    work["id"],
                    topic["topic_id"],
                    topic["display_name"],
                    round(random.uniform(0.5, 1.0), 4),
                    topic["source_system"],
                    work["publication_year"],
                )
            )

        license_name = "cc-by" if work["is_oa"] else None
        landing_page_url = f"https://openalex.org/{work['id']}"
        document_rows.append(
            (
                f"D{idx:010d}",
                work["id"],
                "mock",
                landing_page_url,
                None,
                None,
                license_name,
                work["is_oa"],
                "metadata_only",
                work["publication_year"],
            )
        )
        provenance_rows.append(
            (
                f"P{idx:010d}",
                "work",
                work["id"],
                "mock",
                work["id"],
                landing_page_url,
                license_name,
                f"mock-{work['id']}",
                "demo-v1",
            )
        )

        candidates = [other for other in works[: idx - 1] if other["publication_year"] <= work["publication_year"]]
        for cited in random.sample(candidates, min(random.randint(0, 5), len(candidates))):
            citation_rows.append(
                (
                    work["id"],
                    cited["id"],
                    work["publication_year"],
                    cited["publication_year"],
                    work["publication_year"] - cited["publication_year"],
                    "mock",
                )
            )

    author_rows = [
        (
            author["author_id"],
            author["display_name"],
            author["orcid"],
            author["country_code"],
            author["institution_name"],
            author_works_count[author["author_id"]],
            author_citations[author["author_id"]],
        )
        for author in authors
    ]
    institution_rows = [
        (
            institution["institution_id"],
            institution["display_name"],
            institution["country_code"],
            institution["institution_type"],
            institution["homepage_url"],
            institution["ror"],
            institution_works_count[institution["institution_id"]],
            institution_citations[institution["institution_id"]],
        )
        for institution in institutions
    ]
    source_rows = [
        (
            source["source_id"],
            source["display_name"],
            source["source_type"],
            source["publisher"],
            source["issn_l"],
            source["country_code"],
            source["is_oa"],
            source_works_count[source["source_id"]],
            source_citations[source["source_id"]],
        )
        for source in sources
    ]
    topic_rows = [
        (
            topic["topic_id"],
            topic["display_name"],
            topic["domain"],
            topic["field"],
            topic["subfield"],
            topic["source_system"],
        )
        for topic in topics
    ]

    return {
        "works": works_rows,
        "sources": source_rows,
        "topics": topic_rows,
        "work_authors": work_author_rows,
        "work_institutions": work_institution_rows,
        "work_topics": work_topic_rows,
        "authors": author_rows,
        "institutions": institution_rows,
        "citations": citation_rows,
        "documents": document_rows,
        "provenance_events": provenance_rows,
    }


def main() -> int:
    args = parse_args()
    print(
        f"Generating demo data: works={args.works}, authors={args.authors}, "
        f"institutions={args.institutions}, sources={args.sources}, topics={args.topics}"
    )
    data = build_data(args)
    conn = connect(args)
    cur = conn.cursor()
    try:
        insert_rows(
            cur,
            "works",
            [
                "id",
                "doi",
                "title",
                "abstract",
                "publication_year",
                "publication_date",
                "type",
                "language",
                "cited_by_count",
                "referenced_works_count",
                "domain",
                "field",
                "subfield",
                "primary_topic",
                "is_oa",
                "source_id",
                "source_name",
                "source_type",
                "num_authors",
                "authors",
                "author_ids",
                "apc_usd",
            ],
            data["works"],
            args.batch_size,
        )
        insert_rows(
            cur,
            "sources",
            [
                "source_id",
                "display_name",
                "source_type",
                "publisher",
                "issn_l",
                "country_code",
                "is_oa",
                "works_count",
                "cited_by_count",
            ],
            data["sources"],
            args.batch_size,
        )
        insert_rows(
            cur,
            "topics",
            ["topic_id", "display_name", "domain", "field", "subfield", "source_system"],
            data["topics"],
            args.batch_size,
        )
        insert_rows(
            cur,
            "work_authors",
            [
                "work_id",
                "author_id",
                "display_name",
                "orcid",
                "first_institution_id",
                "first_institution_name",
                "country_code",
                "publication_year",
                "publication_date",
            ],
            data["work_authors"],
            args.batch_size,
        )
        insert_rows(
            cur,
            "work_institutions",
            [
                "work_id",
                "author_id",
                "institution_id",
                "institution_name",
                "country_code",
                "author_position",
                "publication_year",
                "publication_date",
            ],
            data["work_institutions"],
            args.batch_size,
        )
        insert_rows(
            cur,
            "work_topics",
            ["work_id", "topic_id", "display_name", "score", "source_system", "publication_year"],
            data["work_topics"],
            args.batch_size,
        )
        insert_rows(
            cur,
            "authors",
            [
                "author_id",
                "display_name",
                "orcid",
                "country_code",
                "institutions_full",
                "works_count",
                "cited_by_count",
            ],
            data["authors"],
            args.batch_size,
        )
        insert_rows(
            cur,
            "institutions",
            [
                "institution_id",
                "display_name",
                "country_code",
                "institution_type",
                "homepage_url",
                "ror",
                "works_count",
                "cited_by_count",
            ],
            data["institutions"],
            args.batch_size,
        )
        insert_rows(
            cur,
            "citations",
            ["citing_work_id", "cited_work_id", "citing_year", "cited_year", "citation_age", "source_system"],
            data["citations"],
            args.batch_size,
        )
        insert_rows(
            cur,
            "documents",
            [
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
            ],
            data["documents"],
            args.batch_size,
        )
        insert_rows(
            cur,
            "provenance_events",
            [
                "event_id",
                "entity_type",
                "entity_id",
                "source_system",
                "source_record_id",
                "source_url",
                "license",
                "payload_hash",
                "pipeline_version",
            ],
            data["provenance_events"],
            args.batch_size,
        )
    finally:
        getattr(cur, "close", lambda: None)()
        conn.close()

    print("\nDone. Example:")
    print("  SELECT COUNT(*) FROM iceberg.scisci.works;")
    print("  SELECT * FROM iceberg.scisci.works ORDER BY cited_by_count DESC LIMIT 5;")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
