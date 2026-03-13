#!/usr/bin/env python3
"""
RESEARCH-RAG Phase 1 Demo
Verifies connectivity to all 4 academic APIs and unified search.

Run:  python demo.py
      python demo.py --query "cognitive load theory"
      python demo.py --source semantic_scholar
"""
import sys
import argparse
import logging

# Ensure project root is on path
sys.path.insert(0, ".")

from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s [%(name)s] %(message)s",
)

DEFAULT_QUERY = "cognitive behavioral therapy"
SEP = "-" * 70


def test_semantic_scholar(query: str) -> bool:
    print(f"\n{'=' * 70}")
    print("SEMANTIC SCHOLAR")
    print(SEP)
    from api_clients.semantic_scholar import SemanticScholarClient
    client = SemanticScholarClient()
    papers = client.search(query, max_results=3)
    if not papers:
        print("  No results returned")
        return False
    for p in papers:
        print(f"  [{p.source_rank}] {p.title[:70]}")
        print(f"       Year: {p.year}  DOI: {p.doi or 'N/A'}  Citations: {p.citations_count}")
    return True


def test_openalex(query: str) -> bool:
    print(f"\n{'=' * 70}")
    print("OPENALEX")
    print(SEP)
    from api_clients.openalex import OpenAlexClient
    client = OpenAlexClient()
    papers = client.search(query, max_results=3)
    if not papers:
        print("  No results returned")
        return False
    for p in papers:
        print(f"  [{p.source_rank}] {p.title[:70]}")
        print(f"       Year: {p.year}  DOI: {p.doi or 'N/A'}  Citations: {p.citations_count}")
    return True


def test_pubmed(query: str) -> bool:
    print(f"\n{'=' * 70}")
    print("PUBMED")
    print(SEP)
    from api_clients.pubmed import PubMedClient
    client = PubMedClient()
    papers = client.search(query, max_results=3)
    if not papers:
        print("  No results (may be normal for non-biomedical queries)")
        return True
    for p in papers:
        print(f"  [{p.source_rank}] {p.title[:70]}")
        print(f"       Year: {p.year}  DOI: {p.doi or 'N/A'}  PMID: {p.pubmed_id}")
    return True


def test_arxiv(query: str) -> bool:
    print(f"\n{'=' * 70}")
    print("ARXIV")
    print(SEP)
    from api_clients.arxiv_client import ArXivClient
    client = ArXivClient()
    papers = client.search(query, max_results=3)
    if not papers:
        print("  No results returned")
        return False
    for p in papers:
        print(f"  [{p.source_rank}] {p.title[:70]}")
        print(f"       Year: {p.year}  arXiv: {p.arxiv_id}  DOI: {p.doi or 'N/A'}")
    return True


def test_unified(query: str) -> bool:
    print(f"\n{'=' * 70}")
    print("UNIFIED SEARCH (all 4 sources, deduplicated)")
    print(SEP)
    from search.unified_search import UnifiedSearch
    searcher = UnifiedSearch()
    results = searcher.search(query, max_results_per_source=5)
    if not results:
        print("  No results returned")
        return False
    for i, r in enumerate(results[:8]):
        p = r.paper
        sources_str = ", ".join(r.found_in)
        print(f"  [{i + 1}] score={r.relevance_score:.2f} sources=[{sources_str}]")
        print(f"       {p.title[:65]}")
        print(f"       Year: {p.year}  DOI: {p.doi or 'N/A'}")
    print(f"\n  Total unique papers: {len(results)}")
    return True


def main():
    parser = argparse.ArgumentParser(description="Test RESEARCH-RAG API connections")
    parser.add_argument("--query", "-q", default=DEFAULT_QUERY)
    parser.add_argument(
        "--source", "-s",
        choices=["semantic_scholar", "openalex", "pubmed", "arxiv", "unified"],
        default=None,
    )
    args = parser.parse_args()

    print(f"\nRESEARCH-RAG Phase 1 API Test")
    print(f"Query: '{args.query}'")

    tests = {
        "semantic_scholar": test_semantic_scholar,
        "openalex": test_openalex,
        "pubmed": test_pubmed,
        "arxiv": test_arxiv,
        "unified": test_unified,
    }

    run_tests = {args.source: tests[args.source]} if args.source else tests

    results = {}
    for name, fn in run_tests.items():
        try:
            results[name] = fn(args.query)
        except Exception as e:
            print(f"\n  ERROR in {name}: {e}")
            results[name] = False

    print(f"\n{'=' * 70}")
    print("RESULTS SUMMARY")
    print(SEP)
    for name, passed in results.items():
        status = "PASS" if passed else "FAIL"
        print(f"  {status:4s}  {name}")

    failed = [n for n, p in results.items() if not p]
    if failed:
        print(f"\nFailed: {', '.join(failed)}")
        sys.exit(1)
    else:
        print("\nAll tests passed.")


if __name__ == "__main__":
    main()
