#!/usr/bin/env python3
"""
Generate embeddings for chunks and papers missing them.
Uses sentence-transformers all-MiniLM-L6-v2 (384-dim).
"""
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))
from dotenv import load_dotenv
load_dotenv()

import logging
from sentence_transformers import SentenceTransformer
from db.connection import get_connection

logging.basicConfig(level=logging.INFO, format="%(levelname)s [%(name)s] %(message)s")
logger = logging.getLogger(__name__)

MODEL_NAME = "all-MiniLM-L6-v2"
BATCH_SIZE = 32


def embed_chunks(model):
    """Generate embeddings for all chunks missing them."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT chunk_id, content FROM chunks WHERE embedding IS NULL ORDER BY chunk_id;"
            )
            rows = cur.fetchall()

            if not rows:
                print("All chunks already have embeddings.")
                return

            print(f"Generating embeddings for {len(rows)} chunks...")

            for i in range(0, len(rows), BATCH_SIZE):
                batch = rows[i : i + BATCH_SIZE]
                ids = [r[0] for r in batch]
                texts = [r[1] for r in batch]

                embeddings = model.encode(texts, show_progress_bar=False)

                for chunk_id, emb in zip(ids, embeddings):
                    cur.execute(
                        "UPDATE chunks SET embedding = %s WHERE chunk_id = %s;",
                        (emb.tolist(), chunk_id),
                    )

                print(f"  Embedded chunks {i + 1}-{min(i + BATCH_SIZE, len(rows))} / {len(rows)}")

    print(f"Done: {len(rows)} chunk embeddings generated.")


def embed_papers(model):
    """Generate embeddings for papers missing them (using title + abstract)."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT paper_id, title, abstract FROM papers
                   WHERE embedding IS NULL ORDER BY paper_id;"""
            )
            rows = cur.fetchall()

            if not rows:
                print("All papers already have embeddings.")
                return

            print(f"Generating embeddings for {len(rows)} papers...")

            texts = []
            for paper_id, title, abstract in rows:
                # Combine title + abstract for a richer embedding
                text = title or ""
                if abstract:
                    text += " " + abstract
                texts.append(text.strip())

            embeddings = model.encode(texts, show_progress_bar=False)

            for (paper_id, _, _), emb in zip(rows, embeddings):
                cur.execute(
                    "UPDATE papers SET embedding = %s WHERE paper_id = %s;",
                    (emb.tolist(), paper_id),
                )
                print(f"  Embedded paper {paper_id}")

    print(f"Done: {len(rows)} paper embeddings generated.")


def main():
    print(f"Loading model: {MODEL_NAME}")
    model = SentenceTransformer(MODEL_NAME)
    print(f"Model loaded (dim={model.get_sentence_embedding_dimension()})")

    embed_papers(model)
    embed_chunks(model)

    # Verify
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM papers WHERE embedding IS NOT NULL;")
            p = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM papers;")
            pt = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM chunks WHERE embedding IS NOT NULL;")
            c = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM chunks;")
            ct = cur.fetchone()[0]
    print(f"\nVerification:")
    print(f"  Papers with embeddings: {p}/{pt}")
    print(f"  Chunks with embeddings: {c}/{ct}")


if __name__ == "__main__":
    main()
