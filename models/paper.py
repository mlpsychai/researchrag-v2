"""
Canonical Paper data model used across all API clients.
"""
import re
from dataclasses import dataclass, field, asdict
from typing import List, Optional, Dict, Any
from datetime import datetime


@dataclass
class Author:
    name: str
    author_id: Optional[str] = None
    affiliation: Optional[str] = None


@dataclass
class Paper:
    """
    Normalized representation of an academic paper.

    paper_id:   source-prefixed unique ID, e.g. "s2:abc123", "oa:W123",
                "pm:12345678", "arxiv:2301.00001"
    source_api: which API returned this result
    """
    paper_id: str
    title: str
    source_api: str

    # Core bibliographic metadata
    abstract: Optional[str] = None
    authors: List[Author] = field(default_factory=list)
    year: Optional[int] = None
    venue: Optional[str] = None
    volume: Optional[str] = None
    issue: Optional[str] = None
    pages: Optional[str] = None

    # Identifiers
    doi: Optional[str] = None
    arxiv_id: Optional[str] = None
    pubmed_id: Optional[str] = None
    semantic_scholar_id: Optional[str] = None
    openalex_id: Optional[str] = None

    # Impact
    citations_count: Optional[int] = None
    references_count: Optional[int] = None
    url: Optional[str] = None

    # Search result metadata
    source_rank: int = 0
    fetched_at: str = field(
        default_factory=lambda: datetime.utcnow().isoformat()
    )

    # Raw source data
    raw: Optional[Dict[str, Any]] = field(default=None, repr=False)

    def __hash__(self):
        if self.doi:
            return hash(("doi", self.doi.lower().strip()))
        return hash(("title", normalize_title(self.title)))

    def __eq__(self, other):
        if not isinstance(other, Paper):
            return False
        if self.doi and other.doi:
            return self.doi.lower().strip() == other.doi.lower().strip()
        return normalize_title(self.title) == normalize_title(other.title)

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d.pop("raw", None)
        return d

    @property
    def best_identifier(self) -> str:
        if self.doi:
            return f"doi:{self.doi}"
        if self.arxiv_id:
            return f"arxiv:{self.arxiv_id}"
        if self.pubmed_id:
            return f"pmid:{self.pubmed_id}"
        return self.paper_id


def normalize_title(title: str) -> str:
    t = title.lower()
    t = re.sub(r"[^\w\s]", "", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t
