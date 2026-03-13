"""
OpenAlex API client.
Free, no key required. Polite pool activated by including mailto param.
"""
import logging
from typing import List, Optional, Dict, Any

from models.paper import Paper, Author
from api_clients.base_client import BaseAcademicClient
import config

logger = logging.getLogger(__name__)


class OpenAlexClient(BaseAcademicClient):
    SOURCE_NAME = "openalex"

    def __init__(self):
        super().__init__()
        self.MIN_INTERVAL = 1.0 / config.RATE_LIMITS["openalex"]
        self.base_url = config.OPENALEX_BASE_URL
        self._mailto = config.OPENALEX_EMAIL or ""

    def _add_mailto(self, params: Dict) -> Dict:
        if self._mailto:
            params["mailto"] = self._mailto
        return params

    def search(self, query: str, max_results: int = 10) -> List[Paper]:
        url = f"{self.base_url}/works"
        params = self._add_mailto({
            "search": query,
            "per-page": min(max_results, 200),
            "select": (
                "id,doi,title,display_name,abstract_inverted_index,"
                "authorships,publication_year,primary_location,best_oa_location,"
                "cited_by_count,referenced_works_count,ids,open_access"
            ),
        })

        logger.info(f"OpenAlex search: '{query}' (max {max_results})")
        data = self._get_with_retry(url, params)

        papers = []
        for i, raw in enumerate(data.get("results", [])):
            try:
                paper = self._normalize(raw)
                paper.source_rank = i
                papers.append(paper)
            except Exception as e:
                logger.warning(f"OpenAlex: failed to normalize result: {e}")

        logger.info(f"OpenAlex: returned {len(papers)} papers")
        return papers

    def get_paper(self, work_id: str) -> Optional[Paper]:
        url = f"{self.base_url}/works/{work_id}"
        data = self._get_with_retry(url, self._add_mailto({}))
        if not data:
            return None
        try:
            return self._normalize(data)
        except Exception as e:
            logger.warning(f"OpenAlex get_paper failed: {e}")
            return None

    def _normalize(self, raw: Dict[str, Any]) -> Paper:
        doi_raw = raw.get("doi") or ""
        doi = doi_raw.replace("https://doi.org/", "").strip() or None

        ids = raw.get("ids") or {}
        arxiv_url = ids.get("arxiv") or ""
        arxiv_id = arxiv_url.split("arxiv.org/abs/")[-1] if arxiv_url else None
        pubmed_id = ids.get("pmid") or ids.get("pubmed")
        if pubmed_id:
            pubmed_id = str(pubmed_id).replace("https://pubmed.ncbi.nlm.nih.gov/", "")

        abstract = _reconstruct_abstract(raw.get("abstract_inverted_index"))

        authors = []
        for a in (raw.get("authorships") or []):
            auth_info = a.get("author") or {}
            institutions = a.get("institutions") or []
            affil = institutions[0].get("display_name") if institutions else None
            authors.append(Author(
                name=auth_info.get("display_name", ""),
                author_id=auth_info.get("id"),
                affiliation=affil,
            ))

        primary = raw.get("primary_location") or {}
        source = primary.get("source") or {}
        venue = source.get("display_name")

        best_oa = raw.get("best_oa_location") or {}
        url = best_oa.get("landing_page_url") or (
            f"https://doi.org/{doi}" if doi else None
        )

        oa_id = raw.get("id", "").replace("https://openalex.org/", "")

        return Paper(
            paper_id=f"oa:{oa_id}",
            title=raw.get("display_name") or raw.get("title") or "",
            source_api=self.SOURCE_NAME,
            abstract=abstract,
            authors=authors,
            year=raw.get("publication_year"),
            venue=venue,
            doi=doi,
            arxiv_id=arxiv_id,
            pubmed_id=str(pubmed_id) if pubmed_id else None,
            openalex_id=oa_id,
            citations_count=raw.get("cited_by_count"),
            references_count=raw.get("referenced_works_count"),
            url=url,
            raw=raw,
        )


def _reconstruct_abstract(inverted_index: Optional[Dict]) -> Optional[str]:
    """OpenAlex stores abstracts as inverted indexes. Reconstruct the original text."""
    if not inverted_index:
        return None
    word_positions = []
    for word, positions in inverted_index.items():
        for pos in positions:
            word_positions.append((pos, word))
    word_positions.sort(key=lambda x: x[0])
    return " ".join(w for _, w in word_positions)
