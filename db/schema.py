"""
Schema creation and management for researchrag.
"""
import re
import logging
from .connection import get_connection

logger = logging.getLogger(__name__)

# --- Topic schema template (reusable for any research topic) ---

TOPIC_SCHEMA_TEMPLATE = """
CREATE EXTENSION IF NOT EXISTS vector SCHEMA public;

CREATE SCHEMA IF NOT EXISTS {schema_name};
SET search_path TO {schema_name}, public;

-- venues
CREATE TABLE IF NOT EXISTS venues (
    venue_id        SERIAL PRIMARY KEY,
    name            TEXT NOT NULL,
    name_normalized TEXT NOT NULL,
    UNIQUE(name_normalized)
);

-- papers
CREATE TABLE IF NOT EXISTS papers (
    paper_id            SERIAL PRIMARY KEY,
    title               TEXT NOT NULL,
    title_normalized    TEXT NOT NULL,
    abstract            TEXT,
    year                INTEGER,
    venue_id            INTEGER REFERENCES venues(venue_id),
    volume              TEXT,
    issue               TEXT,
    pages               TEXT,

    -- External identifiers
    doi                 TEXT,
    arxiv_id            TEXT,
    pubmed_id           TEXT,
    semantic_scholar_id TEXT,
    openalex_id         TEXT,
    url                 TEXT,

    -- Impact
    citations_count     INTEGER DEFAULT 0,
    references_count    INTEGER DEFAULT 0,

    -- Source tracking
    source_api          TEXT,
    is_seed             BOOLEAN DEFAULT FALSE,
    found_via_keywords  TEXT[],

    -- Embedding (384-dim for all-MiniLM-L6-v2)
    embedding           vector(384),

    -- Summaries and citations
    summary             TEXT,
    summary_generated_at TIMESTAMPTZ,
    apa_citation        TEXT,

    -- Timestamps
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_papers_doi
    ON papers(doi) WHERE doi IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_papers_title_norm
    ON papers(title_normalized);
CREATE INDEX IF NOT EXISTS idx_papers_year ON papers(year);
CREATE INDEX IF NOT EXISTS idx_papers_seed ON papers(is_seed) WHERE is_seed = TRUE;

-- authors
CREATE TABLE IF NOT EXISTS authors (
    author_id       SERIAL PRIMARY KEY,
    name            TEXT NOT NULL,
    name_normalized TEXT NOT NULL,
    affiliation     TEXT,
    external_id     TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_authors_dedup
    ON authors(name_normalized, COALESCE(affiliation, ''));
CREATE INDEX IF NOT EXISTS idx_authors_norm ON authors(name_normalized);

-- paper_authors (junction)
CREATE TABLE IF NOT EXISTS paper_authors (
    paper_id        INTEGER REFERENCES papers(paper_id) ON DELETE CASCADE,
    author_id       INTEGER REFERENCES authors(author_id) ON DELETE CASCADE,
    author_position INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (paper_id, author_id)
);

-- keywords
CREATE TABLE IF NOT EXISTS keywords (
    keyword_id          SERIAL PRIMARY KEY,
    keyword             TEXT NOT NULL,
    keyword_normalized  TEXT NOT NULL,
    UNIQUE(keyword_normalized)
);

-- paper_keywords (junction)
CREATE TABLE IF NOT EXISTS paper_keywords (
    paper_id    INTEGER REFERENCES papers(paper_id) ON DELETE CASCADE,
    keyword_id  INTEGER REFERENCES keywords(keyword_id) ON DELETE CASCADE,
    PRIMARY KEY (paper_id, keyword_id)
);

-- chunks (for full-text RAG)
CREATE TABLE IF NOT EXISTS chunks (
    chunk_id    SERIAL PRIMARY KEY,
    paper_id    INTEGER REFERENCES papers(paper_id) ON DELETE CASCADE,
    content     TEXT NOT NULL,
    chunk_index INTEGER NOT NULL DEFAULT 0,
    source_type TEXT NOT NULL DEFAULT 'abstract',  -- 'abstract', 'fulltext'
    embedding   vector(384),
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_chunks_paper ON chunks(paper_id);
"""

# Original corpus schema SQL (kept for backward compatibility)
CORPUS_SCHEMA_SQL = TOPIC_SCHEMA_TEMPLATE.format(schema_name="corpus")

# --- Per-user schema template ---

USER_SCHEMA_TEMPLATE = """
CREATE SCHEMA IF NOT EXISTS user_{username};
SET search_path TO user_{username};

CREATE TABLE IF NOT EXISTS saved_queries (
    query_id    SERIAL PRIMARY KEY,
    query_text  TEXT NOT NULL,
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS annotations (
    annotation_id   SERIAL PRIMARY KEY,
    paper_id        INTEGER NOT NULL,
    note            TEXT,
    tags            TEXT[],
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS collections (
    collection_id   SERIAL PRIMARY KEY,
    name            TEXT NOT NULL,
    description     TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS collection_papers (
    collection_id   INTEGER REFERENCES collections(collection_id) ON DELETE CASCADE,
    paper_id        INTEGER NOT NULL,
    added_at        TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (collection_id, paper_id)
);
"""


def init_db():
    """Create the shared corpus schema and tables."""
    with get_connection(schema="public") as conn:
        with conn.cursor() as cur:
            cur.execute(CORPUS_SCHEMA_SQL)
    logger.info("Corpus schema initialized")


def create_topic_schema(schema_name: str):
    """Create a new topic schema with the full table structure.

    schema_name is the raw Postgres schema name (e.g. 'personality_assessment_inventory').
    Sanitized to lowercase alphanumeric + underscores.
    """
    safe = re.sub(r'[^\w]', '_', schema_name.lower()).strip('_')
    sql = TOPIC_SCHEMA_TEMPLATE.format(schema_name=safe)
    with get_connection(schema="public") as conn:
        with conn.cursor() as cur:
            # Create schema
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {safe};")
            # Include corpus in search path so the vector type is accessible
            # (pgvector extension is installed in the corpus schema)
            cur.execute(f"SET search_path TO {safe}, corpus, public;")
            # Create all tables (skip the schema/extension preamble lines)
            table_sql = sql.split("-- venues", 1)[-1]
            cur.execute("-- venues" + table_sql)
    logger.info(f"Topic schema '{safe}' initialized")
    return safe


def create_user_schema(username: str):
    """Create a per-user schema. Username sanitized to alphanumeric + underscores."""
    safe = re.sub(r'[^\w]', '_', username.lower())
    sql = USER_SCHEMA_TEMPLATE.replace("{username}", safe)
    with get_connection(schema="public") as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
    logger.info(f"User schema 'user_{safe}' initialized")
