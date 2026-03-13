"""
Unified search interface that queries all 4 academic APIs,
deduplicates results, and returns a ranked merged list.
"""
import logging
from typing import List, Dict, Optional
from dataclasses import dataclass, field

from models.paper import Paper, normalize_title
from api_clients import (
    SemanticScholarClient,
    OpenAlexClient,
    PubMedClient,
    ArXivClient,
)

logger = logging.getLogger(__name__)

SOURCE_WEIGHTS: Dict[str, float] = {
    "semantic_scholar": 1.0,
    "openalex": 1.0,
    "pubmed": 1.0,
    "arxiv": 1.0,
}


@dataclass
class SearchResult:
    paper: Paper
    found_in: List[str] = field(default_factory=list)
    relevance_score: float = 0.0


class UnifiedSearch:
    def __init__(self):
        self.clients = {
            "semantic_scholar": SemanticScholarClient(),
            "openalex": OpenAlexClient(),
            "pubmed": PubMedClient(),
            "arxiv": ArXivClient(),
        }

    def search(
        self,
        query: str,
        max_results_per_source: int = 10,
        sources: Optional[List[str]] = None,
    ) -> List[SearchResult]:
        active_sources = sources or list(self.clients.keys())
        all_papers: List[Paper] = []

        for source_name in active_sources:
            client = self.clients.get(source_name)
            if not client:
                logger.warning(f"Unknown source: {source_name}")
                continue
            try:
                papers = client.search(query, max_results=max_results_per_source)
                all_papers.extend(papers)
                logger.info(f"{source_name}: {len(papers)} results")
            except Exception as e:
                logger.error(f"{source_name} search failed: {e}")

        results = self._deduplicate_and_rank(all_papers)
        logger.info(
            f"Unified search: {len(all_papers)} raw -> {len(results)} deduplicated"
        )
        return results

    def _deduplicate_and_rank(self, papers: List[Paper]) -> List[SearchResult]:
        seen: Dict[str, SearchResult] = {}

        for paper in papers:
            key = _dedup_key(paper)
            weight = SOURCE_WEIGHTS.get(paper.source_api, 1.0)
            rank_score = weight / (paper.source_rank + 1)

            if key in seen:
                existing = seen[key]
                existing.relevance_score += rank_score
                if paper.source_api not in existing.found_in:
                    existing.found_in.append(paper.source_api)
                existing.paper = _pick_richer(existing.paper, paper)
            else:
                seen[key] = SearchResult(
                    paper=paper,
                    found_in=[paper.source_api],
                    relevance_score=rank_score,
                )

        return sorted(seen.values(), key=lambda r: r.relevance_score, reverse=True)


def _dedup_key(paper: Paper) -> str:
    if paper.doi:
        return f"doi:{paper.doi.lower().strip()}"
    return f"title:{normalize_title(paper.title)}"


def _pick_richer(a: Paper, b: Paper) -> Paper:
    a_score = (1 if a.abstract else 0) + (a.citations_count or 0) / 10000
    b_score = (1 if b.abstract else 0) + (b.citations_count or 0) / 10000
    return a if a_score >= b_score else b
