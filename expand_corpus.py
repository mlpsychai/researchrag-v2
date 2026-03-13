#!/usr/bin/env python3
"""
Expand the corpus by searching Google Scholar with the 76 extracted keywords.
5 results per keyword, deduplicated against seed papers and across queries.
"""
import sys
import os
import json
import time
import re
import logging

sys.path.insert(0, ".")
from scholarly import scholarly

logging.basicConfig(level=logging.INFO, format="%(levelname)s [%(name)s] %(message)s")
logger = logging.getLogger(__name__)

KEYWORDS_FILE = os.path.join(os.path.dirname(__file__), "articles", "extracted_keywords.json")
SEED_FILE = os.path.join(os.path.dirname(__file__), "articles", "seed_papers.json")
OUTPUT_FILE = os.path.join(os.path.dirname(__file__), "articles", "expanded_corpus.json")

MAX_RESULTS_PER_KEYWORD = 5
DELAY_BETWEEN_QUERIES = 5  # seconds


def normalize_title(title):
    t = title.lower()
    t = re.sub(r"[^\w\s]", "", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t


def load_seed_titles():
    """Load seed paper titles for dedup."""
    titles = set()
    if os.path.exists(SEED_FILE):
        with open(SEED_FILE) as f:
            seeds = json.load(f)
        for s in seeds:
            titles.add(normalize_title(s.get("title", "")))
    return titles


def parse_pub(pub):
    """Convert a scholarly publication to a clean dict."""
    bib = pub.get("bib", {})
    return {
        "title": bib.get("title", ""),
        "authors": bib.get("author", []),
        "year": bib.get("pub_year"),
        "venue": bib.get("venue", ""),
        "abstract": bib.get("abstract", ""),
        "num_citations": pub.get("num_citations", 0),
        "url": pub.get("pub_url") or pub.get("eprint_url") or "",
        "eprint_url": pub.get("eprint_url") or "",
        "source": "google_scholar",
    }


def main():
    # Load keywords
    with open(KEYWORDS_FILE) as f:
        data = json.load(f)
    keywords = data["all_unique_keywords"]
    print(f"Loaded {len(keywords)} keywords")

    # Load seed titles for dedup
    seed_titles = load_seed_titles()
    print(f"Loaded {len(seed_titles)} seed paper titles for dedup")

    # Track all found papers by normalized title
    seen_titles = set(seed_titles)
    all_papers = []
    keyword_results = {}  # keyword -> list of paper indices
    errors = []

    for i, keyword in enumerate(keywords):
        print(f"\n[{i+1}/{len(keywords)}] Searching: '{keyword}'")
        keyword_papers = []

        try:
            results = scholarly.search_pubs(keyword)
            count = 0
            for _ in range(MAX_RESULTS_PER_KEYWORD * 2):  # fetch extra in case of dupes
                if count >= MAX_RESULTS_PER_KEYWORD:
                    break
                try:
                    pub = next(results)
                    paper = parse_pub(pub)
                    norm = normalize_title(paper["title"])

                    if not norm or len(norm) < 10:
                        continue

                    if norm in seen_titles:
                        # Already have this paper — just note it was found again
                        continue

                    seen_titles.add(norm)
                    paper["found_via_keywords"] = [keyword]
                    paper["paper_index"] = len(all_papers)
                    all_papers.append(paper)
                    keyword_papers.append(len(all_papers) - 1)
                    count += 1

                    print(f"  + {paper['title'][:65]} ({paper['year']}, cited: {paper['num_citations']})")

                except StopIteration:
                    break

            keyword_results[keyword] = keyword_papers
            if count == 0:
                print(f"  (no new papers)")

        except Exception as e:
            error_msg = str(e)
            logger.error(f"Error searching '{keyword}': {error_msg}")
            errors.append({"keyword": keyword, "error": error_msg})

            # If we get blocked, wait longer
            if "429" in error_msg or "blocked" in error_msg.lower() or "captcha" in error_msg.lower():
                print(f"  Rate limited — waiting 30s...")
                time.sleep(30)

        # Delay between queries
        if i < len(keywords) - 1:
            time.sleep(DELAY_BETWEEN_QUERIES)

    # Tag papers found via multiple keywords
    keyword_to_papers = {}
    for paper in all_papers:
        norm = normalize_title(paper["title"])
        keyword_to_papers[norm] = paper

    # Second pass: check if any papers were found via multiple keywords
    # (already handled by dedup — papers only appear once)

    # Summary
    print(f"\n{'=' * 70}")
    print("EXPANSION SUMMARY")
    print("-" * 70)
    print(f"  Keywords searched:     {len(keywords)}")
    print(f"  New papers found:      {len(all_papers)}")
    print(f"  Seed papers (deduped): {len(seed_titles)}")
    print(f"  Errors:                {len(errors)}")

    if errors:
        print(f"\n  Errors:")
        for e in errors:
            print(f"    - {e['keyword']}: {e['error'][:60]}")

    # Top papers by citation count
    top_cited = sorted(all_papers, key=lambda p: p.get("num_citations", 0), reverse=True)[:20]
    print(f"\n  Top 20 by citations:")
    for j, p in enumerate(top_cited):
        print(f"    {j+1:2d}. [{p['num_citations']:5d} cites] {p['title'][:55]} ({p['year']})")

    # Save
    output = {
        "total_new_papers": len(all_papers),
        "keywords_searched": len(keywords),
        "papers": all_papers,
        "keyword_results": keyword_results,
        "errors": errors,
    }
    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f, indent=2)
    print(f"\n  Results saved to: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
