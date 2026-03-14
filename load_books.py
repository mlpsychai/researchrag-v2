#!/usr/bin/env python3
"""
Load book PDFs and EPUBs into a specified schema.
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
from ebooklib import epub
from bs4 import BeautifulSoup
from sentence_transformers import SentenceTransformer
from db.connection import get_connection

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

CHUNK_SIZE = 6000
CHUNK_OVERLAP = 600
EMBED_MODEL = "all-MiniLM-L6-v2"
BATCH_SIZE = 32


def extract_full_text(filepath):
    if filepath.lower().endswith('.epub'):
        return extract_epub_text(filepath)
    if filepath.lower().endswith('.txt'):
        with open(filepath, "r", encoding="utf-8") as f:
            return f.read()
    doc = fitz.open(filepath)
    text = ""
    for page in doc:
        text += page.get_text() + "\n"
    doc.close()
    return text


def extract_epub_text(filepath):
    book = epub.read_epub(filepath, options={"ignore_ncx": True})
    text = ""
    for item in book.get_items_of_type(9):  # ITEM_DOCUMENT
        soup = BeautifulSoup(item.get_content(), "html.parser")
        text += soup.get_text() + "\n"
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

LIB = r"Z:\researchrag\library"

ANNA_FREUD_BOOKS = [
    {
        "file": os.path.join(LIB, "anna_freud", "f1ac3b726da0ef97574e738b89ff9c6d_The Writings of Anna Freud Volume 1- Introduction to -- Anna Freud -- ( WeLib.org ).pdf"),
        "title": "The Writings of Anna Freud, Volume 1: Introduction to Psychoanalysis",
        "authors": ["Anna Freud"],
        "year": 1974,
    },
    {
        "file": os.path.join(LIB, "anna_freud", "c15ef7eef2766332ac40770ea4c91f28_The Writings of Anna Freud (Writings of Anna Freud, V. 3)- -- Written in collaboration with Dorothy Burlingham -- ( WeLib.org ).pdf"),
        "title": "The Writings of Anna Freud, Volume 3: Infants Without Families and Reports on the Hampstead Nurseries",
        "authors": ["Anna Freud", "Dorothy Burlingham"],
        "year": 1973,
    },
    {
        "file": os.path.join(LIB, "anna_freud", "fba1758b4d76ce7763f0aa6bba3d7cd7_The writings of Anna Freud - vol. 6. - Normality and -- Freud, Anna, 1895-1982 -- ( WeLib.org ).pdf"),
        "title": "The Writings of Anna Freud, Volume 6: Normality and Pathology in Childhood",
        "authors": ["Anna Freud"],
        "year": 1965,
    },
    {
        "file": os.path.join(LIB, "anna_freud", "2e65e72875c85b3ce927cf3ccfb45b26_The Writings of Anna Freud, Vol. 8- Psychoanalytic -- Freud, Anna, 1895-1982 -- ( WeLib.org ).pdf"),
        "title": "The Writings of Anna Freud, Volume 8: Psychoanalytic Psychology of Normal Development",
        "authors": ["Anna Freud"],
        "year": 1981,
    },
    {
        "file": os.path.join(LIB, "anna_freud", "e230a029853abb8fd43ddd2c4494174f_The ego and the mechanisms of defense -- Anna Freud -- ( WeLib.org ).pdf"),
        "title": "The Ego and the Mechanisms of Defense",
        "authors": ["Anna Freud"],
        "year": 1936,
    },
    {
        "file": os.path.join(LIB, "anna_freud", "55a97383497853ddc71ad41542cfff97_Children in the hospital -- Thesi Bergmann; Anna Freud -- ( WeLib.org ).pdf"),
        "title": "Children in the Hospital",
        "authors": ["Thesi Bergmann", "Anna Freud"],
        "year": 1965,
    },
    {
        "file": os.path.join(LIB, "anna_freud", "6a567097d0ff912899d915c16a2b45f7_The psychoanalytic study of the child - volume 2 -- Anna Freud -- ( WeLib.org ).pdf"),
        "title": "The Psychoanalytic Study of the Child, Volume 2",
        "authors": ["Anna Freud"],
        "year": 1946,
    },
    {
        "file": os.path.join(LIB, "anna_freud", "e367915c371ad61ee862d4db5bb94d4a_The Psychoanalytic Study of the Child- Volume 35 (The -- Ruth S. Eissler; Albert J. Solnit; Anna Freud; Marianne -- ( WeLib.org ).pdf"),
        "title": "The Psychoanalytic Study of the Child, Volume 35",
        "authors": ["Ruth S. Eissler", "Albert J. Solnit", "Anna Freud", "Marianne Kris"],
        "year": 1980,
    },
    {
        "file": os.path.join(LIB, "anna_freud", "0423752f0109b996e4ac44ed3668de03_The Writings of Anna Freud- Problems of psychoanalytic -- Dorothy Burlingham; Anna Freud -- ( WeLib.org ).pdf"),
        "title": "The Writings of Anna Freud: Problems of Psychoanalytic Training, Diagnosis, and the Technique of Therapy",
        "authors": ["Anna Freud", "Dorothy Burlingham"],
        "year": 1974,
    },
]

PCOS_BOOKS = [
    {
        "file": os.path.join(LIB, "pcos", "3657baf03e934a10480cee4e0a52be49_Polycystic Ovary Syndrome - A Guide to Clinical Management -- Adam H Balen; Gerard S Conway; Roy Homburg; Richard S Legro -- ( WeLib.org ).pdf"),
        "title": "Polycystic Ovary Syndrome: A Guide to Clinical Management",
        "authors": ["Adam H. Balen", "Gerard S. Conway", "Roy Homburg", "Richard S. Legro"],
        "year": 2005,
    },
    {
        "file": os.path.join(LIB, "pcos", "Polycystic Ovary Syndrome-Basic Science to Clinical Advances -- Rehana Rehman, Aisha Sheikh -- ( WeLib.org ).pdf"),
        "title": "Polycystic Ovary Syndrome: Basic Science to Clinical Advances",
        "authors": ["Rehana Rehman", "Aisha Sheikh"],
        "year": 2022,
    },
    {
        "file": os.path.join(LIB, "pcos", "Recommendations from the 2023 International Evidence-based -- Recommendations from the 2023 International Evidence-based -- ( WeLib.org ).pdf"),
        "title": "Recommendations from the 2023 International Evidence-based Guideline for the Assessment and Management of PCOS",
        "authors": ["Helena J. Teede", "Marie L. Misso", "Michael F. Costello"],
        "year": 2023,
    },
    {
        "file": os.path.join(LIB, "pcos", "85338.pdf"),
        "title": "PCOS: Science and Clinical Practice",
        "authors": [],
    },
    {
        "file": os.path.join(LIB, "pcos", "Assessment_of_Serum_Elements_C.pdf"),
        "title": "Assessment of Serum Elements in PCOS",
        "authors": [],
    },
    {
        "file": os.path.join(LIB, "pcos", "dgad762.pdf"),
        "title": "PCOS and Cardiovascular Disease",
        "authors": [],
    },
    {
        "file": os.path.join(LIB, "pcos", "fendo-13-1051111.pdf"),
        "title": "Frontiers in Endocrinology: PCOS Research",
        "authors": [],
    },
    {
        "file": os.path.join(LIB, "pcos", "fendo-16-1551958.pdf"),
        "title": "Frontiers in Endocrinology: PCOS Advances",
        "authors": [],
    },
    {
        "file": os.path.join(LIB, "pcos", "fnut-12-1628853.pdf"),
        "title": "Frontiers in Nutrition: PCOS and Nutrition",
        "authors": [],
    },
    {
        "file": os.path.join(LIB, "pcos", "s41467-024-49749-1.pdf"),
        "title": "Nature Communications: PCOS Genetics",
        "authors": [],
    },
    {
        "file": os.path.join(LIB, "pcos", "lvae125_supplementary_data.pdf"),
        "title": "PCOS Supplementary Data",
        "authors": [],
    },
    {
        "file": os.path.join(LIB, "pcos", "e36966f265c984b680412fc101de29e9_Polycystic Ovary Syndrome-Current and Emerging Concepts, 2e -- Lubna Pal; David B. Seifer -- ( WeLib.org ).epub"),
        "title": "Polycystic Ovary Syndrome: Current and Emerging Concepts",
        "authors": ["Lubna Pal", "David B. Seifer"],
        "year": 2014,
    },
]

PAI_BOOKS = [
    {
        "file": os.path.join(LIB, "personality_assessment_inventory", "An Interpretive Guide to the Personality Assessment -- Leslie Charles Morey -- ( WeLib.org ).pdf"),
        "title": "An Interpretive Guide to the Personality Assessment Inventory",
        "authors": ["Leslie Charles Morey"],
        "year": 2003,
    },
    {
        "file": os.path.join(LIB, "personality_assessment_inventory", "f80fe47c3689ed1b92f5752b2d3c2795_Clinical Applications of the Personality Assessment -- Mark A. Blais; Matthew Ryan Baity; Christopher J. Hopwood -- ( WeLib.org ).epub"),
        "title": "Clinical Applications of the Personality Assessment Inventory",
        "authors": ["Mark A. Blais", "Matthew Ryan Baity", "Christopher J. Hopwood"],
        "year": 2010,
    },
    {
        "file": r"C:\Users\sm4663\skeleton-assess\PAI docs\PAI MANUAL\manualcase.txt",
        "title": "PAI Profile Interpretation: Configural Strategies",
        "authors": ["Leslie Charles Morey"],
        "year": 1996,
    },
]

SCHEMA_BOOKS = {
    "anna_freud": ANNA_FREUD_BOOKS,
    "pcos": PCOS_BOOKS,
    "personality_assessment_inventory": PAI_BOOKS,
}


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Load books into a schema")
    parser.add_argument("--schema", required=True, help="Target schema (anna_freud, pcos, personality_assessment_inventory)")
    parser.add_argument("--embed", action="store_true", default=True, help="Generate embeddings after loading")
    args = parser.parse_args()

    schema = args.schema.lower()

    if schema not in SCHEMA_BOOKS:
        print(f"No book definitions for schema '{schema}'.")
        print(f"Available: {', '.join(SCHEMA_BOOKS.keys())}")
        sys.exit(1)

    books = SCHEMA_BOOKS[schema]

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

