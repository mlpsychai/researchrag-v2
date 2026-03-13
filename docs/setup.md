# Local Development Setup

## Prerequisites

- Python 3.12 (not 3.13)
- Git with credential manager
- Access to Neon Postgres database

## Environment Variables

Create a `.env` file in the project root:

```
ANTHROPIC_API_KEY=sk-ant-...
DATABASE_URL=postgresql://user:pass@host/dbname?sslmode=require
NCBI_API_KEY=           # optional, for PubMed
NCBI_EMAIL=             # optional
SEMANTIC_SCHOLAR_API_KEY=  # optional
OPENALEX_EMAIL=         # optional
```

## Install Dependencies

```bash
pip install -r requirements.txt
```

Note: `gradio` is NOT in requirements.txt (HF Spaces manages it). For local testing:
```bash
pip install gradio==5.12.0
```

## Windows-Specific Notes

- Use the full Python path: `/c/Users/sm4663/AppData/Local/Programs/Python/Python312/python`
- `python3` doesn't work on this machine — use the full path
- Always set `PYTHONIOENCODING=utf-8` to avoid cp1252 encoding errors
- Use `taskkill //F //PID <pid>` to kill processes (not pkill)
- Do NOT install venvs on network drives (Z:) — extremely slow

## Database

The database is Neon Postgres with pgvector. Connection is managed by `db/connection.py`.

**Important:** The pgvector extension is in the `corpus` schema (not `public`). The connection manager includes `corpus` in the search_path automatically.

### Initialize a new topic schema

```python
from db.schema import create_topic_schema
create_topic_schema("my_new_topic")
```

### Load books into a schema

```bash
python load_books.py --schema mmpi3
```

Add book definitions to `MMPI3_BOOKS`, `ANNA_FREUD_BOOKS`, etc. in `load_books.py`.

## Running Locally

```bash
PYTHONIOENCODING=utf-8 python app.py
```

Opens at http://localhost:7860

## Project Locations

| Path | Purpose |
|------|---------|
| `C:\Users\sm4663\researchrag-v2` | **Active repo** — push from here |
| `C:\Users\sm4663\researchrag` | Old repo (has both remotes but dirty git history) |
| `Z:\researchrag` | Network drive clone (GitHub only, do NOT use for deployment) |
| `C:\Users\sm4663\tablet_files\psych_library` | Book PDFs from Samsung Tab S9 Ultra |
