"""
Semantic Scholar Graph API v1 client.
Free tier: 1 req/sec unauthenticated, 10/sec with API key.
"""
import logging
from typing import List, Optional, Dict, Any

from models.paper import Paper, Author
from api_clients.base_client import BaseAcademicClient
import config

logger = logging.getLogger(__name__)

PAPER_FIELDS = (
    "paperId,title,abstract,year,authors,venue,publicationVenue,"
    "externalIds,citationCount,referenceCount,url,openAccessPdf"
)


class SemanticScholarClient(BaseAcademicClient):
    SOURCE_NAME = "semantic_scholar"

    def __init__(self):
        super().__init__(api_key=config.SEMANTIC_SCHOLAR_API_KEY or None)
        self.MIN_INTERVAL = 1.0 / config.RATE_LIMITS["semantic_scholar"]
        self.base_url = config.SEMANTIC_SCHOLAR_BASE_URL

    def _configure_auth(self, api_key: str) -> None:
        self.session.headers["x-api-key"] = api_key

    def search(self, query: str, max_results: int = 10) -> List[Paper]:
        url = f"{self.base_url}/paper/search"
        params = {
            "query": query,
            "limit": min(max_results, 100),
            "fields": PAPER_FIELDS,
        }

        logger.info(f"SemanticScholar search: '{query}' (max {max_results})")
        data = self._get_with_retry(url, params)

        papers = []
        for i, raw in enumerate(data.get("data", [])):
            try:
                paper = self._normalize(raw)
                paper.source_rank = i
                papers.append(paper)
            except Exception as e:
                logger.warning(f"SemanticScholar: failed to normalize paper: {e}")

        logger.info(f"SemanticScholar: returned {len(papers)} papers")
        return papers

    def get_paper(self, paper_id: str) -> Optional[Paper]:
        url = f"{self.base_url}/paper/{paper_id}"
        params = {"fields": PAPER_FIELDS}
        data = self._get_with_retry(url, params)
        if not data:
            return None
        try:
            return self._normalize(data)
        except Exception as e:
            logger.warning(f"SemanticScholar get_paper failed: {e}")
            return None

    def _normalize(self, raw: Dict[str, Any]) -> Paper:
        ext = raw.get("externalIds") or {}
        doi = ext.get("DOI") or ext.get("doi")
        arxiv_id = ext.get("ArXiv")
        pubmed_id = ext.get("PubMed")

        authors = [
            Author(name=a.get("name", ""), author_id=a.get("authorId"))
            for a in (raw.get("authors") or [])
        ]

        oa = raw.get("openAccessPdf") or {}
        url = oa.get("url") or raw.get("url")

        pub_venue = raw.get("publicationVenue") or {}
        venue = pub_venue.get("name") or raw.get("venue")

        return Paper(
            paper_id=f"s2:{raw['paperId']}",
            title=raw.get("title") or "",
            source_api=self.SOURCE_NAME,
            abstract=raw.get("abstract"),
            authors=authors,
            year=raw.get("year"),
            venue=venue,
            doi=doi,
            arxiv_id=arxiv_id,
            pubmed_id=pubmed_id,
            semantic_scholar_id=raw.get("paperId"),
            citations_count=raw.get("citationCount"),
            references_count=raw.get("referenceCount"),
            url=url,
            raw=raw,
        )
