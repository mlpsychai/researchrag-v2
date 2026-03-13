"""
PubMed/NCBI Entrez API client.
Uses Bio.Entrez from biopython for XML parsing.
Requires NCBI_EMAIL in config. Optional NCBI_API_KEY raises limit from 3 to 10 req/sec.
"""
import logging
from typing import List, Optional, Dict, Any

from Bio import Entrez, Medline

from models.paper import Paper, Author
from api_clients.base_client import BaseAcademicClient
import config

logger = logging.getLogger(__name__)


class PubMedClient(BaseAcademicClient):
    SOURCE_NAME = "pubmed"

    def __init__(self):
        super().__init__(api_key=config.NCBI_API_KEY or None)
        self.MIN_INTERVAL = 1.0 / config.RATE_LIMITS["pubmed"]
        Entrez.email = config.NCBI_EMAIL
        if config.NCBI_API_KEY:
            Entrez.api_key = config.NCBI_API_KEY

    def search(self, query: str, max_results: int = 10) -> List[Paper]:
        logger.info(f"PubMed search: '{query}' (max {max_results})")

        # Step 1: Get PMIDs
        self._throttle()
        try:
            handle = Entrez.esearch(db="pubmed", term=query, retmax=max_results, sort="relevance")
            search_results = Entrez.read(handle)
            handle.close()
        except Exception as e:
            logger.error(f"PubMed esearch failed: {e}")
            return []

        pmids = search_results.get("IdList", [])
        if not pmids:
            logger.info("PubMed: no results found")
            return []

        # Step 2: Fetch full records
        self._throttle()
        try:
            handle = Entrez.efetch(
                db="pubmed", id=",".join(pmids),
                rettype="medline", retmode="text",
            )
            records = list(Medline.parse(handle))
            handle.close()
        except Exception as e:
            logger.error(f"PubMed efetch failed: {e}")
            return []

        papers = []
        for i, record in enumerate(records):
            try:
                paper = self._normalize(record)
                paper.source_rank = i
                papers.append(paper)
            except Exception as e:
                logger.warning(f"PubMed: failed to normalize record: {e}")

        logger.info(f"PubMed: returned {len(papers)} papers")
        return papers

    def get_paper(self, pubmed_id: str) -> Optional[Paper]:
        self._throttle()
        try:
            handle = Entrez.efetch(
                db="pubmed", id=pubmed_id,
                rettype="medline", retmode="text",
            )
            records = list(Medline.parse(handle))
            handle.close()
        except Exception as e:
            logger.error(f"PubMed get_paper failed: {e}")
            return None

        if not records:
            return None
        try:
            return self._normalize(records[0])
        except Exception as e:
            logger.warning(f"PubMed normalize failed: {e}")
            return None

    def _normalize(self, record: Dict[str, Any]) -> Paper:
        pmid = record.get("PMID", "")

        doi = None
        for aid in record.get("AID", []):
            if aid.endswith("[doi]"):
                doi = aid.replace("[doi]", "").strip()
                break

        # Prefer full author names (FAU) over abbreviated (AU)
        full_authors = record.get("FAU", [])
        short_authors = record.get("AU", [])
        author_names = full_authors if full_authors else short_authors
        authors = [Author(name=a) for a in author_names]

        dp = record.get("DP", "")
        year = None
        if dp:
            try:
                year = int(dp.split()[0])
            except (ValueError, IndexError):
                pass

        url = f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/" if pmid else None

        return Paper(
            paper_id=f"pm:{pmid}",
            title=record.get("TI", ""),
            source_api=self.SOURCE_NAME,
            abstract=record.get("AB"),
            authors=authors,
            year=year,
            venue=record.get("JT"),
            volume=record.get("VI"),
            issue=record.get("IP"),
            pages=record.get("PG"),
            doi=doi,
            pubmed_id=pmid,
            url=url,
            raw=dict(record),
        )
