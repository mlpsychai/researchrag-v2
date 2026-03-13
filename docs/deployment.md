# Deployment Guide

## Remotes

| Remote | URL | Purpose |
|--------|-----|---------|
| `origin` | `https://github.com/mlpsychai/researchrag.git` | Source code backup |
| `hf` | `https://huggingface.co/spaces/smapoles/researchrag_v2` | Live deployment |

## How to Deploy

From `C:\Users\sm4663\researchrag-v2`:

```bash
git add -A
git commit -m "description of changes"
git push origin main
git push hf main
```

That's it. No special branches, no force push, no workarounds.

## HF Space Secrets

These must be set in the HF Space settings (Settings > Variables and secrets):

| Secret | Description |
|--------|-------------|
| `ANTHROPIC_API_KEY` | Claude API key for RAG synthesis |
| `DATABASE_URL` | Neon Postgres connection string |
| `USERS` | Comma-separated `user:pass` pairs for auth |

Secrets are NOT in the git repo. They persist across deploys.

## HF Space Configuration

Controlled by `README.md` frontmatter:

```yaml
sdk: gradio
sdk_version: "5.12.0"    # Do NOT add gradio to requirements.txt
python_version: "3.12"    # 3.13 breaks gradio (removed audioop module)
app_file: app.py
```

**Important:**
- Gradio is managed by HF via `sdk_version` — do NOT put it in `requirements.txt`
- Python must be 3.12 (not 3.13) because `audioop` was removed in 3.13 and Gradio's pydub needs it
- Theme goes in `gr.Blocks(theme=...)`, NOT in `launch(theme=...)`

## HF Credentials

HF access token is stored in the Windows Credential Manager:
- Host: `huggingface.co`
- Username: `smapoles`
- Git authenticates automatically

## Troubleshooting

### "type vector does not exist"
The pgvector extension lives in the `corpus` schema. `db/connection.py` sets `search_path TO {schema}, corpus, public` to make the vector type accessible from all schemas.

### "gr.update() not found"
`gr.update()` was removed in Gradio 5. Use `gr.Dropdown(choices=..., value=...)` or `gr.Markdown(value=..., visible=...)` instead.

### HF rejects push (large files)
This should not happen with this clean repo. If it does, check that `.gitignore` excludes `articles/`, `guide_images/`, and any binary files. Never commit PDFs or images to this repo.

### App shows "Runtime error"
Check container logs on HF: click the logs icon (terminal) near the "Running" status badge, then select the "Container" tab.
