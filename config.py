"""
Centralized configuration for researchrag.
"""
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / ".env")

# Anthropic
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")

# NCBI / PubMed
NCBI_API_KEY = os.getenv("NCBI_API_KEY", "")
NCBI_EMAIL = os.getenv("NCBI_EMAIL", "research@example.com")

# Semantic Scholar
SEMANTIC_SCHOLAR_API_KEY = os.getenv("SEMANTIC_SCHOLAR_API_KEY", "")

# OpenAlex
OPENALEX_EMAIL = os.getenv("OPENALEX_EMAIL", "")

# Neon Postgres
DATABASE_URL = os.getenv("DATABASE_URL")

# Rate limits (requests per second)
RATE_LIMITS = {
    "semantic_scholar": 1.0 if not SEMANTIC_SCHOLAR_API_KEY else 10.0,
    "openalex": 10.0,
    "pubmed": 3.0 if not NCBI_API_KEY else 10.0,
    "arxiv": 3.0,
}

DEFAULT_MAX_RESULTS = 10

# API base URLs
SEMANTIC_SCHOLAR_BASE_URL = "https://api.semanticscholar.org/graph/v1"
OPENALEX_BASE_URL = "https://api.openalex.org"
ARXIV_BASE_URL = "http://export.arxiv.org/api"
