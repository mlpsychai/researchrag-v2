#!/usr/bin/env python3
"""
Extract keywords from the 14 seed PDFs.
Strategy:
  1. Parse first 2 pages for explicit "Keywords:" section
  2. Check PDF metadata for keywords
  3. Fall back to Claude API to extract keywords from abstract/first page
"""
import sys
import os
import re
import json
import logging

sys.path.insert(0, ".")
from dotenv import load_dotenv
load_dotenv()

import fitz  # PyMuPDF
import anthropic

import config

logging.basicConfig(level=logging.INFO, format="%(levelname)s [%(name)s] %(message)s")
logger = logging.getLogger(__name__)

PDF_DIR = os.path.join(os.path.dirname(__file__), "articles", "pdfs")

# Map filenames to paper numbers and titles
PAPER_MAP = [
    (1, "01_One year in the classroom with ChatGPT empirical insights and transformative imp.pdf",
     "One year in the classroom with ChatGPT: empirical insights and transformative impacts"),
    (2, "02_AI Tools in Society Impacts on Cognitive Offloading and the Future of Critical T.pdf",
     "AI Tools in Society: Impacts on Cognitive Offloading and the Future of Critical Thinking"),
    (3, "03_Integrating artificial intelligence into science lessons teachers experiences an.pdf",
     "Integrating artificial intelligence into science lessons: teachers' experiences and views"),
    (4, "395236eng.pdf",
     "The end of assessment as we know it: GenAI, inequality and the future of knowing"),
    (5, "The wicked problem of AI and assessment.pdf",
     "The wicked problem of AI and assessment"),
    (6, "1-s2.0-S2666920X23000036-main.pdf",
     "AI Literacy in Early Childhood Education: The Challenges and Opportunities"),
    (7, "Brit J Educational Tech - 2024 - Fan - Beware of metacognitive laziness  Effects of generative artificial intelligence on.pdf",
     "Beware of metacognitive laziness: Effects of generative AI on learning motivation, processes, and performance"),
    (8, "1-s2.0-S0747563225003413-main.pdf",
     "Negative perceptions of outsourcing to artificial intelligence"),
    (9, "bastani-et-al-2025-generative-ai-without-guardrails-can-harm-learning-evidence-from-high-school-mathematics.pdf",
     "Generative AI without guardrails can harm learning: Evidence from high school mathematics"),
    (10, "Talk is cheap  why structural assessment changes are needed for a time of GenAI.pdf",
     "Talk is cheap: why structural assessment changes are needed for a time of GenAI"),
    (11, "11_ChatGPT Bullshit spewer or the end of traditional assessments in higher educatio.pdf",
     "ChatGPT: Bullshit spewer or the end of traditional assessments in higher education?"),
    (12, "RAND_RRA956-21.pdf",
     "Using Artificial Intelligence Tools in K-12 Classrooms"),
    (13, "13_A Framework for Generative AI-Driven Assessment in Higher Education.pdf",
     "A Framework for Generative AI-Driven Assessment in Higher Education"),
    (14, "Where s the line  It s an absurd line   towards a framework for acceptable uses of AI in assessment.pdf",
     "'Where's the line? It's an absurd line': towards a framework for acceptable uses of AI in assessment"),
]


def extract_text_from_pdf(filepath, max_pages=3):
    """Extract text from first N pages of a PDF."""
    try:
        doc = fitz.open(filepath)
        text = ""
        for i in range(min(max_pages, len(doc))):
            text += doc[i].get_text() + "\n"
        doc.close()
        return text
    except Exception as e:
        logger.error(f"Failed to extract text from {filepath}: {e}")
        return ""


def extract_pdf_metadata_keywords(filepath):
    """Check PDF metadata for keywords."""
    try:
        doc = fitz.open(filepath)
        meta = doc.metadata
        doc.close()
        if meta and meta.get("keywords"):
            raw = meta["keywords"]
            # Split on comma, semicolon, or newline
            keywords = [k.strip() for k in re.split(r"[;,\n]", raw) if k.strip()]
            return keywords
    except Exception:
        pass
    return []


def parse_keywords_from_text(text):
    """Look for explicit 'Keywords:' section in the text."""
    # Common patterns: "Keywords:", "KEYWORDS:", "Key words:", "Index Terms:"
    patterns = [
        r"(?i)keywords?\s*[:：]\s*(.+?)(?:\n\n|\n[A-Z1-9]|\n\s*\n|introduction|1\.\s)",
        r"(?i)key\s*words?\s*[:：]\s*(.+?)(?:\n\n|\n[A-Z1-9]|\n\s*\n|introduction|1\.\s)",
        r"(?i)index\s+terms?\s*[:：]\s*(.+?)(?:\n\n|\n[A-Z1-9]|\n\s*\n|introduction|1\.\s)",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.DOTALL)
        if match:
            raw = match.group(1).strip()
            # Clean up: remove newlines within the keywords block
            raw = re.sub(r"\s+", " ", raw)
            # Split on comma, semicolon, or bullet
            keywords = [k.strip().rstrip(".") for k in re.split(r"[;,•·|]", raw) if k.strip()]
            # Filter out likely non-keywords (too long or too short)
            keywords = [k for k in keywords if 2 < len(k) < 80]
            if keywords:
                return keywords
    return []


def extract_keywords_with_claude(text, title, client):
    """Use Claude to extract keywords from the paper text."""
    # Truncate text to ~3000 chars to save tokens
    truncated = text[:4000]

    response = client.messages.create(
        model="claude-sonnet-4-5-20250929",
        max_tokens=300,
        messages=[{
            "role": "user",
            "content": f"""Extract the key academic keywords/keyphrases from this paper. Return ONLY a JSON array of strings, nothing else.

Title: {title}

Text from first pages:
{truncated}

Return 5-10 keywords as a JSON array like: ["keyword1", "keyword2", ...]"""
        }]
    )

    try:
        raw = response.content[0].text.strip()
        # Handle case where Claude wraps in markdown code block
        if raw.startswith("```"):
            raw = re.sub(r"```\w*\n?", "", raw).strip()
        keywords = json.loads(raw)
        if isinstance(keywords, list):
            return [str(k).strip() for k in keywords if k]
    except (json.JSONDecodeError, IndexError) as e:
        logger.warning(f"Failed to parse Claude response: {e}")
        logger.warning(f"Raw response: {raw[:200]}")
    return []


def main():
    client = anthropic.Anthropic(api_key=config.ANTHROPIC_API_KEY)
    all_results = []
    all_keywords = set()

    for num, filename, title in PAPER_MAP:
        filepath = os.path.join(PDF_DIR, filename)
        print(f"\n{'=' * 70}")
        print(f"[{num}/14] {title[:65]}")

        if not os.path.exists(filepath):
            print(f"  FILE NOT FOUND: {filename}")
            all_results.append({
                "paper_num": num,
                "title": title,
                "filename": filename,
                "keywords": [],
                "extraction_method": "not_found",
            })
            continue

        # Extract text — special case for UNESCO book (paper #4, chapter starts at page 74)
        if num == 4:
            text = extract_text_from_pdf(filepath, max_pages=80)
            # Extract just the Perkins & Roe chapter (pages 74-82 approx)
            doc = fitz.open(filepath)
            chapter_text = ""
            for pg in range(74, min(83, len(doc))):
                chapter_text += doc[pg].get_text() + "\n"
            doc.close()
            text = chapter_text
        else:
            text = extract_text_from_pdf(filepath)
        if not text.strip():
            print(f"  WARNING: No text extracted from PDF")

        # Layer 1: PDF metadata
        keywords = extract_pdf_metadata_keywords(filepath)
        method = "pdf_metadata"

        # Layer 2: Parse explicit keywords section
        if not keywords:
            keywords = parse_keywords_from_text(text)
            method = "text_parsing"

        # Layer 3: Claude fallback
        if not keywords:
            print(f"  No keywords found in text, using Claude...")
            keywords = extract_keywords_with_claude(text, title, client)
            method = "claude_extraction"

        # Normalize: lowercase, deduplicate
        seen = set()
        normalized = []
        for k in keywords:
            k_lower = k.lower().strip()
            if k_lower not in seen and len(k_lower) > 2:
                seen.add(k_lower)
                normalized.append(k)
        keywords = normalized

        print(f"  Method: {method}")
        print(f"  Keywords ({len(keywords)}): {', '.join(keywords)}")

        all_keywords.update(k.lower() for k in keywords)

        all_results.append({
            "paper_num": num,
            "title": title,
            "filename": filename,
            "keywords": keywords,
            "extraction_method": method,
        })

    # Summary
    print(f"\n{'=' * 70}")
    print("KEYWORD EXTRACTION SUMMARY")
    print("-" * 70)

    for r in all_results:
        status = f"{len(r['keywords'])} keywords ({r['extraction_method']})"
        print(f"  [{r['paper_num']:2d}] {status:40s} {r['title'][:40]}")

    # Deduplicated keyword list across all papers
    sorted_keywords = sorted(all_keywords)
    print(f"\n  Total unique keywords across all papers: {len(sorted_keywords)}")
    print(f"\n  All keywords:")
    for k in sorted_keywords:
        print(f"    - {k}")

    # Save results
    output = {
        "papers": all_results,
        "all_unique_keywords": sorted_keywords,
        "total_papers": len(all_results),
        "total_unique_keywords": len(sorted_keywords),
    }
    output_path = os.path.join(os.path.dirname(__file__), "articles", "extracted_keywords.json")
    with open(output_path, "w") as f:
        json.dump(output, f, indent=2)
    print(f"\n  Results saved to: {output_path}")


if __name__ == "__main__":
    main()
