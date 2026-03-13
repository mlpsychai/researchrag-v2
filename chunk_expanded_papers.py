#!/usr/bin/env python3
"""
Chunk the downloaded expanded PDFs and insert into the chunks table.
Matches PDFs to papers in the DB by title similarity.
"""
import sys
import os
import re
import logging

sys.path.insert(0, os.path.dirname(__file__))
from dotenv import load_dotenv
load_dotenv()

import json
import fitz  # PyMuPDF
from db.connection import get_connection

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

PDF_DIR = os.path.join(os.path.dirname(__file__), "articles", "pdfs", "expanded")
CORPUS_FILE = os.path.join(os.path.dirname(__file__), "articles", "expanded_corpus.json")

CHUNK_SIZE = 6000
CHUNK_OVERLAP = 600


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


def split_into_paragraphs(text):
    paragraphs = re.split(r"\n\s*\n", text)
    return [p.strip() for p in paragraphs if p.strip()]


def chunk_text(text, chunk_size=CHUNK_SIZE, overlap=CHUNK_OVERLAP):
    paragraphs = split_into_paragraphs(text)
    chunks = []
    current_chunk = []
    current_len = 0

    for para in paragraphs:
        para_len = len(para)

        if para_len > chunk_size:
            if current_chunk:
                chunks.append("\n\n".join(current_chunk))
                current_chunk = []
                current_len = 0

            sentences = re.split(r"(?<=[.!?])\s+", para)
            sent_chunk = []
            sent_len = 0
            for sent in sentences:
                if sent_len + len(sent) > chunk_size and sent_chunk:
                    chunks.append(" ".join(sent_chunk))
                    overlap_text = " ".join(sent_chunk)
                    if len(overlap_text) > overlap:
                        kept = []
                        kept_len = 0
                        for s in reversed(sent_chunk):
                            if kept_len + len(s) > overlap:
                                break
                            kept.insert(0, s)
                            kept_len += len(s)
                        sent_chunk = kept
                        sent_len = sum(len(s) for s in sent_chunk)
                    else:
                        sent_len = len(overlap_text)
                sent_chunk.append(sent)
                sent_len += len(sent)
            if sent_chunk:
                chunks.append(" ".join(sent_chunk))
            continue

        if current_len + para_len > chunk_size and current_chunk:
            chunks.append("\n\n".join(current_chunk))
            overlap_paras = []
            overlap_len = 0
            for p in reversed(current_chunk):
                if overlap_len + len(p) > overlap:
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
    return re.sub(r"[^a-z0-9\s]", "", title.lower()).strip()


def main():
    # Load corpus JSON to map filenames -> titles
    with open(CORPUS_FILE) as f:
        data = json.load(f)
    corpus_papers = data["papers"]

    # Build title -> corpus entry lookup
    corpus_by_title = {}
    for p in corpus_papers:
        norm = normalize_title(p["title"])
        corpus_by_title[norm] = p

    # Get DB papers (expanded only) for matching
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT paper_id, title FROM papers WHERE is_seed = FALSE;")
            db_papers = cur.fetchall()

    db_by_title = {}
    for paper_id, title in db_papers:
        norm = normalize_title(title)
        db_by_title[norm] = paper_id

    # Process each PDF
    pdf_files = sorted([f for f in os.listdir(PDF_DIR) if f.endswith(".pdf")])
    print(f"Processing {len(pdf_files)} PDFs...")

    total_chunks = 0
    matched = 0
    unmatched = 0

    with get_connection() as conn:
        with conn.cursor() as cur:
            for pdf_file in pdf_files:
                filepath = os.path.join(PDF_DIR, pdf_file)

                # Extract title from filename: "Title (year).pdf"
                name_part = pdf_file.rsplit(" (", 1)[0] if " (" in pdf_file else pdf_file.replace(".pdf", "")
                norm_name = normalize_title(name_part)

                # Try to find matching paper in DB
                paper_id = None

                # Direct title match
                if norm_name in db_by_title:
                    paper_id = db_by_title[norm_name]
                else:
                    # Fuzzy: check if DB title starts with the filename title (truncated filenames)
                    for db_norm, pid in db_by_title.items():
                        if db_norm.startswith(norm_name) or norm_name.startswith(db_norm):
                            paper_id = pid
                            break

                if paper_id is None:
                    unmatched += 1
                    logger.warning(f"No DB match for: {pdf_file}")
                    continue

                # Check if chunks already exist for this paper
                cur.execute("SELECT COUNT(*) FROM chunks WHERE paper_id = %s;", (paper_id,))
                if cur.fetchone()[0] > 0:
                    continue

                # Extract and chunk
                try:
                    raw_text = extract_full_text(filepath)
                except Exception as e:
                    logger.warning(f"Failed to read PDF: {pdf_file} ({e})")
                    continue
                text = clean_text(raw_text)

                if len(text) < 200:
                    logger.warning(f"Too little text from: {pdf_file}")
                    continue

                chunks = chunk_text(text)

                for idx, chunk_content in enumerate(chunks):
                    cur.execute(
                        """INSERT INTO chunks (paper_id, content, chunk_index, source_type)
                           VALUES (%s, %s, %s, 'fulltext')""",
                        (paper_id, chunk_content, idx),
                    )

                total_chunks += len(chunks)
                matched += 1
                print(f"  [{matched}] Paper {paper_id}: {len(text):,} chars -> {len(chunks)} chunks  ({pdf_file[:60]})")

    print(f"\nDone: {matched} papers chunked, {unmatched} unmatched, {total_chunks} total chunks inserted")


if __name__ == "__main__":
    main()
