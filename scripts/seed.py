"""
seed.py - inserts mock SciSci data into Iceberg via Trino
Usage:
    pip install trino faker
    python scripts/seed.py
"""

import trino
import random
from datetime import date, timedelta
from faker import Faker

fake = Faker()

conn = trino.dbapi.connect(
    host="localhost",
    port=8080,
    user="seed",
    catalog="iceberg",
    schema="scisci",
)
cur = conn.cursor()

DOMAINS   = ["Computer Science", "Physics", "Biology", "Medicine", "Engineering"]
FIELDS    = ["Machine Learning", "Quantum Computing", "Genomics", "Epidemiology", "Robotics"]
SUBFIELDS = ["Deep Learning", "NLP", "Computer Vision", "Bioinformatics", "Signal Processing"]
TYPES     = ["journal-article", "conference-paper", "review", "preprint"]
LANGUAGES = ["en", "de", "fr", "es", "zh"]

def rand_date(start_year=2000, end_year=2024):
    start = date(start_year, 1, 1)
    end   = date(end_year, 12, 31)
    return start + timedelta(days=random.randint(0, (end - start).days))

def esc(s):
    return s.replace("'", "''") if s else ""

# ── pre-generate author pool ──────────────────────────────────
# Real OpenAlex data has many papers per author — simulate that
print("Generating author pool...")
AUTHOR_POOL_SIZE = 200
author_pool = []
for _ in range(AUTHOR_POOL_SIZE):
    author_pool.append({
        "author_id":    f"A{random.randint(1000000, 9999999)}",
        "display_name": fake.name(),
        "orcid":        f"0000-{random.randint(1000,9999)}-{random.randint(1000,9999)}-{random.randint(1000,9999)}",
        "country_code": fake.country_code(),
        "institution":  fake.company(),
        "inst_id":      f"I{random.randint(10000, 99999)}",
    })

# ── seed works ────────────────────────────────────────────────
print("Seeding works...")
WORKS_COUNT = 500
work_rows   = []

for i in range(WORKS_COUNT):
    pid      = f"W{random.randint(1000000000, 9999999999)}"
    pub_date = rand_date()
    pub_year = pub_date.year

    # pick 1-8 authors from the pool
    n_authors    = random.randint(1, 8)
    paper_authors = random.sample(author_pool, min(n_authors, len(author_pool)))
    authors_str   = "; ".join(a["display_name"] for a in paper_authors)
    author_ids_str= "; ".join(a["author_id"]    for a in paper_authors)

    work_rows.append({
        "id":          pid,
        "doi":         f"10.{random.randint(1000,9999)}/{fake.lexify('??????')}",
        "title":       esc(fake.sentence(nb_words=8).rstrip(".")),
        "abstract":    esc(fake.paragraph(nb_sentences=4)),
        "pub_year":    pub_year,
        "pub_date":    str(pub_date),
        "type":        random.choice(TYPES),
        "language":    random.choice(LANGUAGES),
        "cited":       random.randint(0, 500),
        "ref_count":   random.randint(5, 60),
        "domain":      random.choice(DOMAINS),
        "field":       random.choice(FIELDS),
        "subfield":    random.choice(SUBFIELDS),
        "topic":       random.choice(FIELDS),
        "is_oa":       random.choice([True, False]),
        "source_name": esc(fake.company() + " Journal"),
        "source_type": random.choice(["journal", "conference", "repository"]),
        "num_authors": len(paper_authors),
        "authors_str": esc(authors_str),
        "author_ids":  esc(author_ids_str),
        "apc_usd":     round(random.uniform(0, 3000), 2),
        "authors":     paper_authors,  # keep for work_authors seeding
    })

    if (i + 1) % 50 == 0:
        print(f"  {i + 1}/{WORKS_COUNT} works prepared")

for i, w in enumerate(work_rows):
    cur.execute("""
        INSERT INTO iceberg.scisci.works (
            id, doi, title, abstract,
            publication_year, publication_date, type, language,
            cited_by_count, referenced_works_count,
            domain, field, subfield, primary_topic,
            is_oa, source_name, source_type,
            num_authors, authors, author_ids, apc_usd
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, [
        w["id"], w["doi"], w["title"], w["abstract"],
        w["pub_year"], w["pub_date"], w["type"], w["language"],
        w["cited"], w["ref_count"],
        w["domain"], w["field"], w["subfield"], w["topic"],
        w["is_oa"], w["source_name"], w["source_type"],
        w["num_authors"], w["authors_str"], w["author_ids"], w["apc_usd"],
    ])
    if (i + 1) % 50 == 0:
        print(f"  {i + 1}/{WORKS_COUNT} works inserted")

# ── seed work_authors ─────────────────────────────────────────
print("Seeding work_authors...")
for w in work_rows:
    for a in w["authors"]:
        cur.execute("""
            INSERT INTO iceberg.scisci.work_authors (
                work_id, author_id, display_name, orcid,
                first_institution_id, first_institution_name,
                country_code, publication_year, publication_date
            ) VALUES (?,?,?,?,?,?,?,?,?)
        """, [
            w["id"], a["author_id"], a["display_name"], a["orcid"],
            a["inst_id"], esc(a["institution"]),
            a["country_code"], w["pub_year"], w["pub_date"],
        ])

print(f"  work_authors inserted")

# ── seed authors (deduplicated from pool) ─────────────────────
print("Seeding authors...")

# track citations per author by summing from works they appear in
author_citations: dict[str, int] = {a["author_id"]: 0 for a in author_pool}
author_works_count: dict[str, int] = {a["author_id"]: 0 for a in author_pool}

for w in work_rows:
    for a in w["authors"]:
        author_citations[a["author_id"]]   += w["cited"]
        author_works_count[a["author_id"]] += 1

for a in author_pool:
    cur.execute("""
        INSERT INTO iceberg.scisci.authors (
            author_id, display_name, orcid,
            country_code, institutions_full,
            works_count, cited_by_count
        ) VALUES (?,?,?,?,?,?,?)
    """, [
        a["author_id"],
        a["display_name"],
        a["orcid"],
        a["country_code"],
        esc(a["institution"]),
        author_works_count[a["author_id"]],
        author_citations[a["author_id"]],
    ])

print(f"  {AUTHOR_POOL_SIZE} authors inserted")

# ── summary ───────────────────────────────────────────────────
print("\nDone! Verify with:")
print("  make shell-trino")
print("  SELECT COUNT(*) FROM iceberg.scisci.works;")
print("  SELECT COUNT(*) FROM iceberg.scisci.work_authors;")
print("  SELECT COUNT(*) FROM iceberg.scisci.authors;")