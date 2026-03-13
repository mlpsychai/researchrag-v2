#!/usr/bin/env python3
"""
Load book PDFs into a specified schema.
Extracts text, chunks, inserts paper + chunks, generates embeddings.
"""
import sys
import os
import re
import logging

sys.path.insert(0, os.path.dirname(__file__))
from dotenv import load_dotenv
load_dotenv()

import fitz  # PyMuPDF
from sentence_transformers import SentenceTransformer
from db.connection import get_connection

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

CHUNK_SIZE = 6000
CHUNK_OVERLAP = 600
EMBED_MODEL = "all-MiniLM-L6-v2"
BATCH_SIZE = 32


def extract_full_text(filepath):
    doc = fitz.open(filepath)
    text = ""
    for page in doc:
        text += page.get_text() + "\n"
    doc.close()
    return text


def clean_text(text):
    text = text.replace("\x00", "")
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r"\n\s*\d+\s*\n", "\n", text)
    text = re.sub(r"(\w)-\s*\n\s*(\w)", r"\1\2", text)
    return text.strip()


def chunk_text(text):
    paragraphs = [p.strip() for p in re.split(r"\n\s*\n", text) if p.strip()]
    chunks = []
    current_chunk = []
    current_len = 0

    for para in paragraphs:
        para_len = len(para)

        if para_len > CHUNK_SIZE:
            if current_chunk:
                chunks.append("\n\n".join(current_chunk))
                current_chunk = []
                current_len = 0
            sentences = re.split(r"(?<=[.!?])\s+", para)
            sent_chunk = []
            sent_len = 0
            for sent in sentences:
                if sent_len + len(sent) > CHUNK_SIZE and sent_chunk:
                    chunks.append(" ".join(sent_chunk))
                    kept = []
                    kept_len = 0
                    for s in reversed(sent_chunk):
                        if kept_len + len(s) > CHUNK_OVERLAP:
                            break
                        kept.insert(0, s)
                        kept_len += len(s)
                    sent_chunk = kept
                    sent_len = sum(len(s) for s in sent_chunk)
                sent_chunk.append(sent)
                sent_len += len(sent)
            if sent_chunk:
                chunks.append(" ".join(sent_chunk))
            continue

        if current_len + para_len > CHUNK_SIZE and current_chunk:
            chunks.append("\n\n".join(current_chunk))
            overlap_paras = []
            overlap_len = 0
            for p in reversed(current_chunk):
                if overlap_len + len(p) > CHUNK_OVERLAP:
                    break
                overlap_paras.insert(0, p)
                overlap_len += len(p)
            current_chunk = overlap_paras
            current_len = overlap_len

        current_chunk.append(para)
        current_len += para_len

    if current_chunk:
        chunks.append("\n\n".join(current_chunk))

    return chunks


def normalize_title(title):
    t = title.lower()
    t = re.sub(r"[^\w\s]", "", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t


def load_book(schema, filepath, title, authors, year=None):
    """Load a single book PDF into the given schema."""
    print(f"\n{'=' * 60}")
    print(f"Loading: {title}")
    print(f"Schema:  {schema}")
    print(f"File:    {os.path.basename(filepath)}")

    # Extract and chunk
    raw_text = extract_full_text(filepath)
    text = clean_text(raw_text)
    if len(text) < 200:
        print(f"  WARNING: Very little text extracted ({len(text)} chars)")
        return None

    chunks = chunk_text(text)
    print(f"  Extracted {len(text):,} chars -> {len(chunks)} chunks")

    title_norm = normalize_title(title)

    # Insert into DB
    with get_connection(schema=schema) as conn:
        with conn.cursor() as cur:
            # Check for duplicate
            cur.execute("SELECT paper_id FROM papers WHERE title_normalized = %s", (title_norm,))
            existing = cur.fetchone()
            if existing:
                print(f"  Already loaded (paper_id={existing[0]}), skipping.")
                return existing[0]

            # Insert paper
            cur.execute("""
                INSERT INTO papers (title, title_normalized, year, is_seed, source_api)
                VALUES (%s, %s, %s, TRUE, 'local_pdf')
                RETURNING paper_id
            """, (title, title_norm, year))
            paper_id = cur.fetchone()[0]
            print(f"  Inserted paper_id={paper_id}")

            # Insert authors
            for pos, author_name in enumerate(authors):
                name_norm = re.sub(r"\s+", " ", author_name.strip().lower())
                cur.execute("""
                    INSERT INTO authors (name, name_normalized)
                    VALUES (%s, %s)
                    ON CONFLICT (name_normalized, COALESCE(affiliation, ''))
                    DO UPDATE SET name = EXCLUDED.name
                    RETURNING author_id
                """, (author_name.strip(), name_norm))
                author_id = cur.fetchone()[0]
                cur.execute("""
                    INSERT INTO paper_authors (paper_id, author_id, author_position)
                    VALUES (%s, %s, %s) ON CONFLICT DO NOTHING
                """, (paper_id, author_id, pos))

            # Insert chunks
            for idx, chunk_content in enumerate(chunks):
                cur.execute("""
                    INSERT INTO chunks (paper_id, content, chunk_index, source_type)
                    VALUES (%s, %s, %s, 'fulltext')
                """, (paper_id, chunk_content, idx))

            print(f"  Inserted {len(chunks)} chunks")

    return paper_id


def embed_schema(schema):
    """Generate embeddings for all chunks missing them in a schema."""
    print(f"\nGenerating embeddings for schema '{schema}'...")
    model = SentenceTransformer(EMBED_MODEL)

    with get_connection(schema=schema) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT chunk_id, content FROM chunks WHERE embedding IS NULL ORDER BY chunk_id;")
            rows = cur.fetchall()

            if not rows:
                print("  All chunks already embedded.")
                return

            print(f"  Embedding {len(rows)} chunks...")
            for i in range(0, len(rows), BATCH_SIZE):
                batch = rows[i:i + BATCH_SIZE]
                ids = [r[0] for r in batch]
                texts = [r[1] for r in batch]
                embeddings = model.encode(texts, show_progress_bar=False)
                for chunk_id, emb in zip(ids, embeddings):
                    cur.execute("UPDATE chunks SET embedding = %s WHERE chunk_id = %s;", (emb.tolist(), chunk_id))
                print(f"    Embedded {min(i + BATCH_SIZE, len(rows))}/{len(rows)}")

            # Also embed the papers (title + first chunk as proxy)
            cur.execute("""
                SELECT p.paper_id, p.title FROM papers p
                WHERE p.embedding IS NULL ORDER BY p.paper_id;
            """)
            paper_rows = cur.fetchall()
            if paper_rows:
                texts = [r[1] for r in paper_rows]
                embeddings = model.encode(texts, show_progress_bar=False)
                for (paper_id, _), emb in zip(paper_rows, embeddings):
                    cur.execute("UPDATE papers SET embedding = %s WHERE paper_id = %s;", (emb.tolist(), paper_id))
                print(f"  Embedded {len(paper_rows)} papers")

    print(f"  Done.")


# ── Book definitions ──────────────────────────────────────────────

PSYCH_LIB = r"C:\Users\sm4663\tablet_files\psych_library"

MMPI3_BOOKS = [
    {
        "file": os.path.join(PSYCH_LIB, "Interpreting the MMPI-3 -- Yossef S. Ben-Porath, Martin Sellbom -- ( WeLib.org ).pdf"),
        "title": "Interpreting the MMPI-3",
        "authors": ["Yossef S. Ben-Porath", "Martin Sellbom"],
        "year": 2021,
    },
]

ANNA_FREUD_BOOKS = [
    {
        "file": os.path.join(PSYCH_LIB, "Anna Freud, Melanie Klein, And The Psychoanalysis Of -- Holder, Alex(Author) -- ( WeLib.org ).pdf"),
        "title": "Anna Freud, Melanie Klein, and the Psychoanalysis of Children and Adolescents",
        "authors": ["Alex Holder"],
        "year": 2005,
    },
    {
        "file": os.path.join(PSYCH_LIB, "The Writings of Anna Freud- Problems of psychoanalytic -- Dorothy Burlingham; Anna Freud -- ( WeLib.org ).pdf"),
        "title": "The Writings of Anna Freud: Problems of Psychoanalytic Training, Diagnosis, and the Technique of Therapy",
        "authors": ["Anna Freud", "Dorothy Burlingham"],
        "year": 1974,
    },
]


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Load books into a schema")
    parser.add_argument("--schema", required=True, help="Target schema (mmpi3, anna_freud, etc.)")
    parser.add_argument("--embed", action="store_true", default=True, help="Generate embeddings after loading")
    args = parser.parse_args()

    schema = args.schema.lower()

    if schema == "mmpi3":
        books = MMPI3_BOOKS
    elif schema == "anna_freud":
        books = ANNA_FREUD_BOOKS
    else:
        print(f"No book definitions for schema '{schema}'. Add them to load_books.py.")
        sys.exit(1)

    for book in books:
        load_book(schema, book["file"], book["title"], book["authors"], book.get("year"))

    if args.embed:
        embed_schema(schema)

    # Verify
    with get_connection(schema=schema) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM papers;")
            p = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM chunks;")
            c = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM chunks WHERE embedding IS NOT NULL;")
            ce = cur.fetchone()[0]
    print(f"\n{'=' * 60}")
    print(f"Schema '{schema}' summary:")
    print(f"  Papers: {p}")
    print(f"  Chunks: {c} ({ce} with embeddings)")
