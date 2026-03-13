#!/usr/bin/env python3
"""
One-off script to fetch metadata and download PDFs for the 14 seed papers.
Uses DOI lookups via Semantic Scholar and OpenAlex, with title search fallback.
"""
import sys
import os
import time
import logging
import requests

sys.path.insert(0, ".")
from dotenv import load_dotenv
load_dotenv()

from api_clients.semantic_scholar import SemanticScholarClient
from api_clients.openalex import OpenAlexClient

logging.basicConfig(level=logging.INFO, format="%(levelname)s [%(name)s] %(message)s")
logger = logging.getLogger(__name__)

# 14 seed papers: (image_file, doi_or_none, title, fallback_search_query)
SEED_PAPERS = [
    ("2026-02-09 13.35.06.jpg", "10.3389/feduc.2025.1574477",
     "One year in the classroom with ChatGPT: empirical insights and transformative impacts", None),
    ("2026-02-11 07.54.03.jpg", "10.3390/soc15010006",
     "AI Tools in Society: Impacts on Cognitive Offloading and the Future of Critical Thinking", None),
    ("2026-02-12 22.13.45.jpg", "10.1186/s40594-023-00454-3",
     "Integrating artificial intelligence into science lessons: teachers' experiences and views", None),
    ("2026-02-14 08.07.07.jpg", None,
     "The end of assessment as we know it: GenAI, inequality and the future of knowing",
     "The end of assessment GenAI inequality future of knowing Perkins Roe"),
    ("2026-02-15 08.45.12.jpg", "10.1080/02602938.2025.2553340",
     "The wicked problem of AI and assessment", None),
    ("2026-02-15 09.00.27.jpg", "10.1016/j.caeai.2023.100124",
     "Artificial Intelligence (AI) Literacy in Early Childhood Education: The Challenges and Opportunities", None),
    ("2026-02-17 12.14.08.jpg", "10.1111/bjet.13544",
     "Beware of metacognitive laziness: Effects of generative artificial intelligence on learning motivation, processes, and performance", None),
    ("2026-02-18 07.10.28.jpg", "10.1016/j.chb.2025.108894",
     "Negative perceptions of outsourcing to artificial intelligence", None),
    ("2026-02-18 08.37.34.jpg", "10.1073/pnas.2422633122",
     "Generative AI without guardrails can harm learning: Evidence from high school mathematics", None),
    ("2026-02-19 08.06.07.jpg", "10.1080/02602938.2025.2503964",
     "Talk is cheap: why structural assessment changes are needed for a time of GenAI", None),
    ("2026-02-20 08.46.37.jpg", "10.37074/jalt.2023.6.1.9",
     "ChatGPT: Bullshit spewer or the end of traditional assessments in higher education?", None),
    ("2026-02-21 09.15.53.jpg", None,
     "Using Artificial Intelligence Tools in K-12 Classrooms",
     "Using Artificial Intelligence Tools K-12 Classrooms RAND Diliberti"),
    ("2026-02-21 12.36.28.jpg", "10.3390/info16060472",
     "A Framework for Generative AI-Driven Assessment in Higher Education", None),
    ("2026-02-22 08.31.54.jpg", "10.1080/02602938.2025.2456207",
     "Where's the line? It's an absurd line: towards a framework for acceptable uses of AI in assessment", None),
]

PDF_DIR = os.path.join(os.path.dirname(__file__), "articles", "pdfs")


def fetch_paper_metadata(doi, title, fallback_query, s2_client, oa_client):
    """Try to get full metadata via DOI lookup, fall back to title search."""
    from models.paper import normalize_title

    # Try OpenAlex by DOI first (faster, no rate limit issues)
    if doi:
        logger.info(f"Looking up DOI via OpenAlex: {doi}")
        try:
            paper = oa_client.get_paper(f"https://doi.org/{doi}")
            if paper:
                return paper
        except Exception as e:
            logger.warning(f"OpenAlex DOI lookup failed: {e}")

    # Try Semantic Scholar by DOI
    if doi:
        logger.info(f"Looking up DOI via Semantic Scholar: {doi}")
        try:
            paper = s2_client.get_paper(f"DOI:{doi}")
            if paper:
                return paper
        except Exception as e:
            logger.warning(f"Semantic Scholar DOI lookup failed: {e}")

    # Fallback: search by title — try OpenAlex first (more generous rate limits)
    query = fallback_query or title
    logger.info(f"DOI lookup failed, searching by title: {title[:60]}...")

    try:
        results = oa_client.search(query, max_results=3)
        if results:
            norm_title = normalize_title(title)
            for r in results:
                if normalize_title(r.title) == norm_title:
                    return r
            return results[0]
    except Exception as e:
        logger.warning(f"OpenAlex title search failed: {e}")

    # Last resort: Semantic Scholar search
    try:
        time.sleep(3)  # extra delay before S2 search
        results = s2_client.search(query, max_results=3)
        if results:
            norm_title = normalize_title(title)
            for r in results:
                if normalize_title(r.title) == norm_title:
                    return r
            return results[0]
    except Exception as e:
        logger.warning(f"Semantic Scholar title search failed: {e}")

    return None


def try_download_pdf(paper, doi, index):
    """Try to download a PDF for the paper."""
    pdf_url = None

    # Check if paper has an open access PDF URL
    if paper and paper.url and paper.url.endswith(".pdf"):
        pdf_url = paper.url
    elif paper and paper.raw:
        # Semantic Scholar: openAccessPdf
        oa_pdf = (paper.raw.get("openAccessPdf") or {}).get("url")
        if oa_pdf:
            pdf_url = oa_pdf
        # OpenAlex: best_oa_location
        best_oa = paper.raw.get("best_oa_location") or {}
        if not pdf_url:
            pdf_url = best_oa.get("pdf_url")

    # Try Unpaywall via DOI as another source
    if not pdf_url and doi:
        try:
            resp = requests.get(
                f"https://api.unpaywall.org/v2/{doi}",
                params={"email": "research@example.com"},
                timeout=10,
            )
            if resp.status_code == 200:
                data = resp.json()
                best = data.get("best_oa_location") or {}
                pdf_url = best.get("url_for_pdf") or best.get("url_for_landing_page")
        except Exception as e:
            logger.warning(f"Unpaywall lookup failed: {e}")

    if not pdf_url:
        return None

    # Download
    safe_title = "".join(c if c.isalnum() or c in " -_" else "" for c in (paper.title if paper else f"paper_{index}"))
    safe_title = safe_title[:80].strip()
    filename = f"{index:02d}_{safe_title}.pdf"
    filepath = os.path.join(PDF_DIR, filename)

    logger.info(f"Downloading PDF: {pdf_url[:80]}...")
    try:
        resp = requests.get(pdf_url, timeout=30, headers={"User-Agent": "researchrag/0.1"})
        if resp.status_code == 200 and len(resp.content) > 1000:
            # Basic check it's actually a PDF
            if resp.content[:5] == b"%PDF-" or b"%PDF-" in resp.content[:100]:
                with open(filepath, "wb") as f:
                    f.write(resp.content)
                return filepath
            else:
                logger.warning(f"Downloaded content is not a PDF")
                return None
        else:
            logger.warning(f"PDF download failed: status={resp.status_code} size={len(resp.content)}")
            return None
    except Exception as e:
        logger.warning(f"PDF download error: {e}")
        return None


def main():
    os.makedirs(PDF_DIR, exist_ok=True)

    s2 = SemanticScholarClient()
    oa = OpenAlexClient()

    results = []
    for i, (img, doi, title, fallback) in enumerate(SEED_PAPERS, 1):
        print(f"\n{'=' * 70}")
        print(f"[{i}/14] {title[:65]}")
        print(f"  DOI: {doi or 'N/A'}")
        print(f"  Image: {img}")

        paper = fetch_paper_metadata(doi, title, fallback, s2, oa)

        if paper:
            print(f"  FOUND: {paper.title[:65]}")
            print(f"  Source: {paper.source_api}  Year: {paper.year}  Citations: {paper.citations_count}")
            print(f"  Authors: {', '.join(a.name for a in paper.authors[:3])}")
            if paper.venue:
                print(f"  Venue: {paper.venue}")

            pdf_path = try_download_pdf(paper, doi, i)
            if pdf_path:
                print(f"  PDF: DOWNLOADED -> {os.path.basename(pdf_path)}")
            else:
                print(f"  PDF: Not available for download")

            results.append({
                "index": i,
                "title": paper.title,
                "doi": paper.doi or doi,
                "year": paper.year,
                "authors": [a.name for a in paper.authors],
                "venue": paper.venue,
                "citations": paper.citations_count,
                "abstract": paper.abstract[:200] + "..." if paper.abstract else None,
                "url": paper.url,
                "pdf_downloaded": pdf_path is not None,
                "source_api": paper.source_api,
            })
        else:
            print(f"  NOT FOUND in any API")
            results.append({
                "index": i,
                "title": title,
                "doi": doi,
                "found": False,
            })

        # Delay between papers to respect rate limits (S2 is strict at 1 req/sec)
        time.sleep(2)

    # Summary
    print(f"\n{'=' * 70}")
    print("SUMMARY")
    print("-" * 70)
    found = [r for r in results if r.get("source_api")]
    pdfs = [r for r in results if r.get("pdf_downloaded")]
    print(f"  Papers found:      {len(found)}/14")
    print(f"  PDFs downloaded:   {len(pdfs)}/14")
    print(f"  PDF directory:     {PDF_DIR}")

    not_found = [r for r in results if not r.get("source_api")]
    if not_found:
        print(f"\n  Not found:")
        for r in not_found:
            print(f"    - {r['title'][:60]}")

    no_pdf = [r for r in found if not r.get("pdf_downloaded")]
    if no_pdf:
        print(f"\n  Found but no PDF available:")
        for r in no_pdf:
            print(f"    - {r['title'][:60]}")
            if r.get("url"):
                print(f"      URL: {r['url']}")

    # Save results to JSON
    import json
    results_path = os.path.join(os.path.dirname(__file__), "articles", "seed_papers.json")
    with open(results_path, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\n  Results saved to: {results_path}")


if __name__ == "__main__":
    main()
