# Architecture

## Overview

ResearchRAG is a semantic search + RAG synthesis tool for academic research. Users select a research topic, then can:
- **Ask** questions and get Claude-synthesized answers with citations
- **Search** for relevant paper excerpts via vector similarity
- **Browse** and filter the paper corpus

## Stack

| Layer | Technology |
|-------|-----------|
| Frontend | Gradio (hosted on HF Spaces) |
| LLM | Claude Haiku 4.5 (Anthropic API) |
| Embeddings | all-MiniLM-L6-v2 (sentence-transformers, 384-dim) |
| Database | Neon Postgres + pgvector |
| Deployment | Hugging Face Spaces |

## Multi-Topic Schema Design

Each research topic has its own Postgres schema with identical table structures. This provides complete data isolation without separate databases.

```
Neon Postgres
├── public          (pgvector extension — DO NOT USE for data)
├── corpus          (GenAI in Education & Assessment — 264 papers)
├── mmpi3           (MMPI-3 — loaded from book PDFs)
├── anna_freud      (Anna Freud — loaded from book PDFs)
├── pcos            (PCOS — empty, ready for data)
└── personality_assessment_inventory (PAI — empty, ready for data)
```

The pgvector extension is installed in the `corpus` schema (not `public` — Neon restriction). All connections include `corpus` in the search_path so the `vector` type is accessible.

### Schema Tables

Each topic schema contains:
- `papers` — paper metadata, embeddings, summaries, APA citations
- `authors` — author names and affiliations
- `paper_authors` — junction table (paper ↔ author)
- `chunks` — text chunks with vector embeddings (for RAG retrieval)
- `venues` — publication venues
- `keywords` / `paper_keywords` — keyword tagging

Template defined in `db/schema.py` → `TOPIC_SCHEMA_TEMPLATE`.

## File Map

### App (runs on HF Spaces)

| File | Purpose |
|------|---------|
| `app.py` | Gradio UI — topic selector, Ask/Search/Browse tabs |
| `rag_pipeline.py` | Embed question → retrieve chunks → synthesize with Claude |
| `config.py` | Reads env vars (API keys, DB URL) |
| `db/connection.py` | Postgres connection with schema-aware search_path |
| `db/schema.py` | Schema templates and creation functions |
| `requirements.txt` | Python dependencies (NOT gradio — HF manages that) |

### Data Pipeline (run locally, not on HF)

| File | Purpose |
|------|---------|
| `extract_keywords.py` | Extract keywords from seed PDFs |
| `expand_corpus.py` | Search Google Scholar to find related papers |
| `fetch_seed_papers.py` | Look up seed paper DOIs and metadata |
| `download_expanded_pdfs.py` | Download PDFs for expanded papers |
| `chunk_seed_papers.py` | Chunk seed PDFs into DB |
| `chunk_expanded_papers.py` | Chunk expanded PDFs into DB |
| `generate_embeddings.py` | Generate vector embeddings for chunks |
| `load_books.py` | Load book PDFs into any schema (mmpi3, anna_freud) |
| `enrich_metadata.py` | Enrich paper metadata from APIs |
| `setup_db.py` | Initialize database schemas |

### Support Modules

| Directory | Purpose |
|-----------|---------|
| `api_clients/` | API clients for Semantic Scholar, OpenAlex, PubMed, arXiv |
| `models/` | Paper/Author dataclasses, dedup logic |
| `search/` | Unified multi-API search |

## RAG Pipeline Flow

```
User question
    ↓
Embed with all-MiniLM-L6-v2
    ↓
pgvector cosine similarity search (selected schema)
    ↓
Top-K chunks retrieved
    ↓
Build context + reference list
    ↓
Claude Haiku synthesizes answer with [1], [2] citations
    ↓
Display answer + sources in Gradio
```

## Topic Selector

`app.py` dynamically discovers available schemas by querying `information_schema.tables` for schemas that have a `papers` table. Display names are mapped in `SCHEMA_DISPLAY_NAMES`. Switching topics updates:
- Header stats (paper count, chunk count)
- Keyword filter dropdown (Browse tab)
- All queries route to the selected schema
