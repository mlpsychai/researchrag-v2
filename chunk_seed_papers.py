#!/usr/bin/env python3
"""
Chunk the 14 seed PDFs and insert into the chunks table.

Strategy:
  - Extract full text from each PDF via PyMuPDF
  - Split on paragraph boundaries, targeting ~6000 chars (~1500 tokens) per chunk
  - ~600 char overlap between consecutive chunks
  - Special handling for paper #4 (UNESCO book chapter, pages 74-82)
"""
import sys
import os
import re
import logging

sys.path.insert(0, os.path.dirname(__file__))
from dotenv import load_dotenv
load_dotenv()

import fitz  # PyMuPDF
from db.connection import get_connection

logging.basicConfig(level=logging.INFO, format="%(levelname)s [%(name)s] %(message)s")
logger = logging.getLogger(__name__)

PDF_DIR = os.path.join(os.path.dirname(__file__), "articles", "pdfs")

# Target chunk size in characters (~1500 tokens ≈ 6000 chars)
CHUNK_SIZE = 6000
CHUNK_OVERLAP = 600  # ~10% overlap

# Map paper_id (from DB) -> PDF filename, with special page ranges
PAPER_PDF_MAP = {
    1: "01_One year in the classroom with ChatGPT empirical insights and transformative imp.pdf",
    2: "02_AI Tools in Society Impacts on Cognitive Offloading and the Future of Critical T.pdf",
    3: "03_Integrating artificial intelligence into science lessons teachers experiences an.pdf",
    4: ("395236eng.pdf", 74, 83),  # UNESCO book, chapter pages 74-82
    5: "The wicked problem of AI and assessment.pdf",
    6: "1-s2.0-S2666920X23000036-main.pdf",
    7: "Brit J Educational Tech - 2024 - Fan - Beware of metacognitive laziness  Effects of generative artificial intelligence on.pdf",
    8: "1-s2.0-S0747563225003413-main.pdf",
    9: "bastani-et-al-2025-generative-ai-without-guardrails-can-harm-learning-evidence-from-high-school-mathematics.pdf",
    10: "Talk is cheap  why structural assessment changes are needed for a time of GenAI.pdf",
    11: "11_ChatGPT Bullshit spewer or the end of traditional assessments in higher educatio.pdf",
    12: "RAND_RRA956-21.pdf",
    13: "13_A Framework for Generative AI-Driven Assessment in Higher Education.pdf",
    14: "Where s the line  It s an absurd line   towards a framework for acceptable uses of AI in assessment.pdf",
}


def extract_full_text(filepath, start_page=0, end_page=None):
    """Extract all text from a PDF (or a page range)."""
    doc = fitz.open(filepath)
    if end_page is None:
        end_page = len(doc)
    text = ""
    for i in range(start_page, min(end_page, len(doc))):
        text += doc[i].get_text() + "\n"
    doc.close()
    return text


def clean_text(text):
    """Clean extracted PDF text."""
    # Remove null bytes
    text = text.replace("\x00", "")
    # Collapse multiple blank lines
    text = re.sub(r"\n{3,}", "\n\n", text)
    # Remove page headers/footers (lines that are just numbers)
    text = re.sub(r"\n\s*\d+\s*\n", "\n", text)
    # Fix hyphenated line breaks (word- \nword -> word-word)
    text = re.sub(r"(\w)-\s*\n\s*(\w)", r"\1\2", text)
    return text.strip()


def split_into_paragraphs(text):
    """Split text into paragraphs on double newlines."""
    paragraphs = re.split(r"\n\s*\n", text)
    return [p.strip() for p in paragraphs if p.strip()]


def chunk_text(text, chunk_size=CHUNK_SIZE, overlap=CHUNK_OVERLAP):
    """
    Split text into chunks at paragraph boundaries.
    Each chunk targets ~chunk_size chars with ~overlap chars of overlap.
    """
    paragraphs = split_into_paragraphs(text)
    chunks = []
    current_chunk = []
    current_len = 0

    for para in paragraphs:
        para_len = len(para)

        # If a single paragraph exceeds chunk_size, split it by sentences
        if para_len > chunk_size:
            # Flush current chunk first
            if current_chunk:
                chunks.append("\n\n".join(current_chunk))
                current_chunk = []
                current_len = 0

            # Split long paragraph by sentences
            sentences = re.split(r"(?<=[.!?])\s+", para)
            sent_chunk = []
            sent_len = 0
            for sent in sentences:
                if sent_len + len(sent) > chunk_size and sent_chunk:
                    chunks.append(" ".join(sent_chunk))
                    # Keep overlap worth of sentences
                    overlap_text = " ".join(sent_chunk)
                    if len(overlap_text) > overlap:
                        # Find sentences that fit in overlap
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

        # Would adding this paragraph exceed the target?
        if current_len + para_len > chunk_size and current_chunk:
            chunks.append("\n\n".join(current_chunk))

            # Build overlap from end of current chunk
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

    # Final chunk
    if current_chunk:
        chunks.append("\n\n".join(current_chunk))

    return chunks


def main():
    with get_connection() as conn:
        with conn.cursor() as cur:
            # Check if chunks already exist
            cur.execute("SELECT COUNT(*) FROM chunks;")
            existing = cur.fetchone()[0]
            if existing > 0:
                print(f"WARNING: {existing} chunks already exist in the table.")
                resp = input("Delete existing chunks and re-chunk? [y/N]: ").strip().lower()
                if resp != "y":
                    print("Aborted.")
                    return
                cur.execute("DELETE FROM chunks;")
                print(f"Deleted {existing} existing chunks.")

            total_chunks = 0

            for paper_id, pdf_info in sorted(PAPER_PDF_MAP.items()):
                # Handle special page range for paper #4
                if isinstance(pdf_info, tuple):
                    filename, start_page, end_page = pdf_info
                else:
                    filename = pdf_info
                    start_page, end_page = 0, None

                filepath = os.path.join(PDF_DIR, filename)
                if not os.path.exists(filepath):
                    logger.warning(f"Paper {paper_id}: PDF not found: {filename}")
                    continue

                # Extract and clean text
                raw_text = extract_full_text(filepath, start_page, end_page)
                text = clean_text(raw_text)

                if not text:
                    logger.warning(f"Paper {paper_id}: No text extracted")
                    continue

                # Chunk
                chunks = chunk_text(text)

                # Insert chunks
                for idx, chunk_content in enumerate(chunks):
                    cur.execute(
                        """INSERT INTO chunks (paper_id, content, chunk_index, source_type)
                           VALUES (%s, %s, %s, 'fulltext')""",
                        (paper_id, chunk_content, idx),
                    )

                total_chunks += len(chunks)
                print(f"  Paper {paper_id:2d}: {len(text):7,} chars -> {len(chunks):3d} chunks  ({filename[:60]})")

            print(f"\nTotal: {total_chunks} chunks inserted into DB")

    print("Done.")


if __name__ == "__main__":
    main()
