#!/usr/bin/env python3
"""
Quick test of scholarly (Google Scholar) with a few keywords from our seed papers.
"""
import sys
import time
from scholarly import scholarly

# Test with 5 representative keywords
TEST_KEYWORDS = [
    "generative AI assessment higher education",
    "metacognitive laziness artificial intelligence",
    "cognitive offloading AI tools",
    "AI literacy early childhood education",
    "academic integrity generative AI",
]

MAX_RESULTS_PER_QUERY = 3


def main():
    for i, query in enumerate(TEST_KEYWORDS):
        print(f"\n{'=' * 70}")
        print(f"Query {i+1}: '{query}'")
        print("-" * 70)

        try:
            results = scholarly.search_pubs(query)
            for j in range(MAX_RESULTS_PER_QUERY):
                try:
                    pub = next(results)
                    bib = pub.get("bib", {})
                    title = bib.get("title", "N/A")
                    author = ", ".join(bib.get("author", [])[:3])
                    year = bib.get("pub_year", "N/A")
                    venue = bib.get("venue", "N/A")
                    abstract = bib.get("abstract", "")[:100]
                    cited = pub.get("num_citations", 0)
                    url = pub.get("pub_url") or pub.get("eprint_url") or "N/A"

                    print(f"\n  [{j+1}] {title[:70]}")
                    print(f"      Authors: {author}")
                    print(f"      Year: {year}  Cited by: {cited}  Venue: {venue[:50]}")
                    if abstract:
                        print(f"      Abstract: {abstract}...")
                    print(f"      URL: {url[:80]}")
                except StopIteration:
                    print(f"  (only {j} results)")
                    break
        except Exception as e:
            print(f"  ERROR: {e}")

        # Delay between queries to avoid being blocked
        if i < len(TEST_KEYWORDS) - 1:
            print(f"\n  Waiting 5s before next query...")
            time.sleep(5)

    print(f"\n{'=' * 70}")
    print("Done.")


if __name__ == "__main__":
    main()
