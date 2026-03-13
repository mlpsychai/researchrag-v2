#!/usr/bin/env python3
"""
Download PDFs for expanded corpus papers using eprint_url from scholarly.
Saves to articles/pdfs/expanded/
"""
import sys
import os
import json
import time
import re
import logging

sys.path.insert(0, os.path.dirname(__file__))

import requests

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

PDF_DIR = os.path.join(os.path.dirname(__file__), "articles", "pdfs", "expanded")
CORPUS_FILE = os.path.join(os.path.dirname(__file__), "articles", "expanded_corpus.json")

# Reasonable headers to avoid being blocked
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/pdf,*/*",
}

TIMEOUT = 30
DELAY = 2  # seconds between downloads


def sanitize_filename(title, max_len=80):
    """Create a safe filename from a paper title."""
    name = re.sub(r'[<>:"/\\|?*]', '', title)
    name = re.sub(r'\s+', ' ', name).strip()
    if len(name) > max_len:
        name = name[:max_len].rsplit(' ', 1)[0]
    return name


def download_pdf(url, filepath):
    """Download a PDF from url to filepath. Returns True on success."""
    try:
        resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT, allow_redirects=True)
        content_type = resp.headers.get("Content-Type", "")

        # Check if we actually got a PDF
        if resp.status_code == 200:
            if "pdf" in content_type or resp.content[:5] == b"%PDF-":
                with open(filepath, "wb") as f:
                    f.write(resp.content)
                return True
            else:
                # Some URLs redirect to HTML landing pages
                logger.debug(f"  Not a PDF (content-type: {content_type})")
                return False
        else:
            logger.debug(f"  HTTP {resp.status_code}")
            return False
    except Exception as e:
        logger.debug(f"  Error: {e}")
        return False


def main():
    os.makedirs(PDF_DIR, exist_ok=True)

    with open(CORPUS_FILE) as f:
        data = json.load(f)
    papers = data["papers"]

    # Filter to post-2010 with eprint_url
    targets = [
        p for p in papers
        if p.get("eprint_url")
        and p.get("year")
        and int(p["year"]) >= 2010
    ]

    print(f"Attempting to download {len(targets)} PDFs...")

    success = 0
    failed = 0
    skipped = 0

    for i, paper in enumerate(targets):
        title = paper["title"]
        url = paper["eprint_url"]
        year = paper["year"]
        safe_name = sanitize_filename(title)
        filename = f"{safe_name} ({year}).pdf"
        filepath = os.path.join(PDF_DIR, filename)

        # Skip if already downloaded
        if os.path.exists(filepath) and os.path.getsize(filepath) > 1000:
            skipped += 1
            continue

        ok = download_pdf(url, filepath)
        if ok:
            size_kb = os.path.getsize(filepath) / 1024
            print(f"  [{i+1}/{len(targets)}] OK ({size_kb:.0f} KB) {title[:60]}")
            success += 1
        else:
            failed += 1
            # Clean up empty/invalid file
            if os.path.exists(filepath):
                os.remove(filepath)

        time.sleep(DELAY)

    print(f"\nDone: {success} downloaded, {failed} failed, {skipped} already had")
    print(f"PDFs saved to: {PDF_DIR}")


if __name__ == "__main__":
    main()
