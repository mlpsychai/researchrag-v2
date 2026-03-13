"""
arXiv API client.
Returns Atom XML feed. No API key required. Max 3 req/sec.
"""
import logging
import re
from typing import List, Optional, Dict, Any
from xml.etree import ElementTree as ET

from models.paper import Paper, Author
from api_clients.base_client import BaseAcademicClient
import config

logger = logging.getLogger(__name__)

NS = {
    "atom": "http://www.w3.org/2005/Atom",
    "arxiv": "http://arxiv.org/schemas/atom",
}


class ArXivClient(BaseAcademicClient):
    SOURCE_NAME = "arxiv"

    def __init__(self):
        super().__init__()
        self.MIN_INTERVAL = 1.0 / config.RATE_LIMITS["arxiv"]
        self.base_url = config.ARXIV_BASE_URL

    def search(self, query: str, max_results: int = 10) -> List[Paper]:
        arxiv_query = f"ti:{query} OR abs:{query}"
        url = f"{self.base_url}/query"
        params = {
            "search_query": arxiv_query,
            "start": 0,
            "max_results": min(max_results, 100),
            "sortBy": "relevance",
            "sortOrder": "descending",
        }

        logger.info(f"arXiv search: '{query}' (max {max_results})")
        self._throttle()
        response = self.session.get(url, params=params, timeout=15)
        response.raise_for_status()

        papers = self._parse_feed(response.text)
        logger.info(f"arXiv: returned {len(papers)} papers")
        return papers

    def get_paper(self, arxiv_id: str) -> Optional[Paper]:
        url = f"{self.base_url}/query"
        params = {"id_list": arxiv_id, "max_results": 1}

        self._throttle()
        response = self.session.get(url, params=params, timeout=15)
        response.raise_for_status()

        papers = self._parse_feed(response.text)
        return papers[0] if papers else None

    def _parse_feed(self, xml_text: str) -> List[Paper]:
        root = ET.fromstring(xml_text)
        papers = []
        for i, entry in enumerate(root.findall("atom:entry", NS)):
            try:
                paper = self._normalize_entry(entry)
                paper.source_rank = i
                papers.append(paper)
            except Exception as e:
                logger.warning(f"arXiv: failed to parse entry: {e}")
        return papers

    def _normalize_entry(self, entry: ET.Element) -> Paper:
        id_url = entry.findtext("atom:id", default="", namespaces=NS)
        arxiv_id = _extract_arxiv_id(id_url)

        title = (entry.findtext("atom:title", default="", namespaces=NS) or "").strip()
        # arXiv titles often have newlines in them
        title = re.sub(r"\s+", " ", title)

        abstract = (entry.findtext("atom:summary", default="", namespaces=NS) or "").strip()

        published = entry.findtext("atom:published", default="", namespaces=NS)
        year = None
        if published:
            try:
                year = int(published[:4])
            except ValueError:
                pass

        authors = []
        for author_el in entry.findall("atom:author", NS):
            name = author_el.findtext("atom:name", default="", namespaces=NS)
            if name:
                authors.append(Author(name=name.strip()))

        doi_el = entry.find("arxiv:doi", NS)
        doi = doi_el.text.strip() if doi_el is not None and doi_el.text else None

        journal_ref_el = entry.find("arxiv:journal_ref", NS)
        venue = journal_ref_el.text.strip() if journal_ref_el is not None and journal_ref_el.text else None

        primary_cat = entry.find("arxiv:primary_category", NS)
        category = primary_cat.get("term") if primary_cat is not None else None

        url = f"https://arxiv.org/abs/{arxiv_id}" if arxiv_id else id_url

        return Paper(
            paper_id=f"arxiv:{arxiv_id}",
            title=title,
            source_api=self.SOURCE_NAME,
            abstract=abstract,
            authors=authors,
            year=year,
            venue=venue or category,
            doi=doi,
            arxiv_id=arxiv_id,
            url=url,
            raw={"id": id_url, "title": title},
        )

    def _normalize(self, raw: Dict[str, Any]) -> Paper:
        raise NotImplementedError("Use _normalize_entry for arXiv XML entries")


def _extract_arxiv_id(id_url: str) -> str:
    match = re.search(r"abs/(.+?)(?:v\d+)?$", id_url)
    if match:
        return match.group(1)
    return id_url
