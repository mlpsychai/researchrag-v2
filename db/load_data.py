"""
Load seed_papers.json and expanded_corpus.json into the corpus schema.
"""
import json
import re
import logging
from pathlib import Path

from .connection import get_connection

logger = logging.getLogger(__name__)

ARTICLES_DIR = Path(__file__).parent.parent / "articles"


def normalize_title(title: str) -> str:
    """Match the normalization in models/paper.py."""
    t = title.lower()
    t = re.sub(r"[^\w\s]", "", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t


def normalize_name(name: str) -> str:
    return re.sub(r"\s+", " ", name.strip().lower())


def _upsert_venue(cur, venue_name: str):
    """Insert or get venue_id. Returns None if empty."""
    if not venue_name or not venue_name.strip():
        return None
    norm = venue_name.strip().lower()
    cur.execute("""
        INSERT INTO venues (name, name_normalized)
        VALUES (%s, %s)
        ON CONFLICT (name_normalized) DO UPDATE SET name = EXCLUDED.name
        RETURNING venue_id
    """, (venue_name.strip(), norm))
    return cur.fetchone()[0]


def _upsert_author(cur, name: str, affiliation=None):
    """Insert or get author_id."""
    norm = normalize_name(name)
    cur.execute("""
        INSERT INTO authors (name, name_normalized, affiliation)
        VALUES (%s, %s, %s)
        ON CONFLICT (name_normalized, COALESCE(affiliation, ''))
        DO UPDATE SET name = EXCLUDED.name
        RETURNING author_id
    """, (name.strip(), norm, affiliation))
    return cur.fetchone()[0]


def _upsert_keyword(cur, keyword: str):
    """Insert or get keyword_id."""
    norm = keyword.strip().lower()
    cur.execute("""
        INSERT INTO keywords (keyword, keyword_normalized)
        VALUES (%s, %s)
        ON CONFLICT (keyword_normalized) DO NOTHING
    """, (keyword.strip(), norm))
    cur.execute(
        "SELECT keyword_id FROM keywords WHERE keyword_normalized = %s",
        (norm,)
    )
    return cur.fetchone()[0]


def _insert_paper(cur, paper_data: dict, is_seed: bool):
    """
    Insert a paper. Returns paper_id on success, None if duplicate.
    Dedup: DOI first, title_normalized fallback.
    """
    title = paper_data.get("title", "")
    title_norm = normalize_title(title)
    doi = paper_data.get("doi")

    year = paper_data.get("year")
    if isinstance(year, str):
        try:
            year = int(year)
        except (ValueError, TypeError):
            year = None

    venue_id = _upsert_venue(cur, paper_data.get("venue"))
    citations = paper_data.get("citations") or paper_data.get("num_citations") or 0
    url = paper_data.get("url") or paper_data.get("eprint_url") or None
    source = paper_data.get("source_api") or paper_data.get("source")
    found_via = paper_data.get("found_via_keywords")

    # Try insert — ON CONFLICT handles dedup
    if doi:
        # Has DOI: try DOI-based dedup
        cur.execute(
            "SELECT paper_id FROM papers WHERE doi = %s",
            (doi.lower().strip(),)
        )
        if cur.fetchone():
            logger.debug(f"Duplicate DOI: {doi}")
            return None

    # Check title dedup
    cur.execute(
        "SELECT paper_id FROM papers WHERE title_normalized = %s",
        (title_norm,)
    )
    existing = cur.fetchone()
    if existing:
        logger.debug(f"Duplicate title: {title[:60]}...")
        return None

    cur.execute("""
        INSERT INTO papers (
            title, title_normalized, abstract, year, venue_id,
            doi, url, citations_count, source_api, is_seed, found_via_keywords
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING paper_id
    """, (
        title, title_norm, paper_data.get("abstract"), year, venue_id,
        doi.lower().strip() if doi else None, url, citations,
        source, is_seed, found_via
    ))
    return cur.fetchone()[0]


def load_seed_papers():
    """Load articles/seed_papers.json into the corpus."""
    path = ARTICLES_DIR / "seed_papers.json"
    papers = json.loads(path.read_text())
    logger.info(f"Loading {len(papers)} seed papers...")

    loaded = 0
    with get_connection(schema="corpus") as conn:
        with conn.cursor() as cur:
            for p in papers:
                paper_id = _insert_paper(cur, p, is_seed=True)
                if paper_id is None:
                    continue
                loaded += 1

                for pos, author_name in enumerate(p.get("authors", [])):
                    aid = _upsert_author(cur, author_name)
                    cur.execute("""
                        INSERT INTO paper_authors (paper_id, author_id, author_position)
                        VALUES (%s, %s, %s) ON CONFLICT DO NOTHING
                    """, (paper_id, aid, pos))

    logger.info(f"Loaded {loaded}/{len(papers)} seed papers")
    return loaded


def load_expanded_corpus():
    """Load articles/expanded_corpus.json into the corpus."""
    path = ARTICLES_DIR / "expanded_corpus.json"
    data = json.loads(path.read_text())
    papers = data.get("papers", [])
    logger.info(f"Loading {len(papers)} expanded corpus papers...")

    loaded = 0
    with get_connection(schema="corpus") as conn:
        with conn.cursor() as cur:
            for p in papers:
                paper_id = _insert_paper(cur, p, is_seed=False)
                if paper_id is None:
                    continue
                loaded += 1

                for pos, author_name in enumerate(p.get("authors", [])):
                    aid = _upsert_author(cur, author_name)
                    cur.execute("""
                        INSERT INTO paper_authors (paper_id, author_id, author_position)
                        VALUES (%s, %s, %s) ON CONFLICT DO NOTHING
                    """, (paper_id, aid, pos))

    logger.info(f"Loaded {loaded}/{len(papers)} expanded corpus papers")
    return loaded


def load_keywords():
    """Load extracted_keywords.json and link keywords to seed papers."""
    path = ARTICLES_DIR / "extracted_keywords.json"
    data = json.loads(path.read_text())

    with get_connection(schema="corpus") as conn:
        with conn.cursor() as cur:
            for paper_entry in data.get("papers", []):
                title_norm = normalize_title(paper_entry["title"])
                cur.execute(
                    "SELECT paper_id FROM papers WHERE title_normalized = %s",
                    (title_norm,)
                )
                row = cur.fetchone()
                if not row:
                    logger.warning(
                        f"Keyword paper not found: {paper_entry['title'][:50]}"
                    )
                    continue
                paper_id = row[0]

                for kw in paper_entry.get("keywords", []):
                    kid = _upsert_keyword(cur, kw)
                    cur.execute("""
                        INSERT INTO paper_keywords (paper_id, keyword_id)
                        VALUES (%s, %s) ON CONFLICT DO NOTHING
                    """, (paper_id, kid))

    logger.info("Keywords loaded and linked to seed papers")


def load_all():
    """Run the full data load pipeline."""
    seed_count = load_seed_papers()
    expanded_count = load_expanded_corpus()
    load_keywords()
    total = seed_count + expanded_count
    logger.info(f"Data load complete: {seed_count} seeds + {expanded_count} expanded = {total} total")
    return total
