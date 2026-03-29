#!/usr/bin/env python3
"""
RAG pipeline for researchrag.
Embeds a user question, retrieves relevant chunks via pgvector,
and synthesizes an answer with Claude.
"""
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))
from dotenv import load_dotenv
load_dotenv()

from sentence_transformers import SentenceTransformer
import anthropic
import config
from db.connection import get_connection

MODEL_NAME = "all-MiniLM-L6-v2"
TOP_K = 8
CLAUDE_MODELS = {
    "Haiku 4.5": "claude-haiku-4-5-20251001",
    "Sonnet 4.6": "claude-sonnet-4-6-20250514",
    "Opus 4.6": "claude-opus-4-6-20250529",
    "Opus 4.6 (1M)": "claude-opus-4-6-20250529",
}
CLAUDE_MODEL = "claude-haiku-4-5-20251001"

_embed_model = None


def get_embed_model():
    global _embed_model
    if _embed_model is None:
        _embed_model = SentenceTransformer(MODEL_NAME)
    return _embed_model


def retrieve_chunks(question, top_k=TOP_K, schema="corpus"):
    """Embed the question and retrieve the most relevant chunks from pgvector."""
    model = get_embed_model()
    q_embedding = model.encode(question).tolist()

    with get_connection(schema=schema) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT c.chunk_id, c.paper_id, c.content, c.chunk_index,
                       p.title, p.year, p.doi,
                       c.embedding <=> %s::vector AS distance
                FROM chunks c
                JOIN papers p ON c.paper_id = p.paper_id
                WHERE c.embedding IS NOT NULL
                ORDER BY c.embedding <=> %s::vector
                LIMIT %s;
                """,
                (q_embedding, q_embedding, top_k),
            )
            rows = cur.fetchall()

    results = []
    for row in rows:
        results.append({
            "chunk_id": row[0],
            "paper_id": row[1],
            "content": row[2],
            "chunk_index": row[3],
            "title": row[4],
            "year": row[5],
            "doi": row[6],
            "distance": row[7],
        })
    return results


def build_context(chunks):
    """Format retrieved chunks into context for Claude."""
    context_parts = []
    seen_papers = {}
    for i, chunk in enumerate(chunks):
        paper_key = chunk["paper_id"]
        if paper_key not in seen_papers:
            seen_papers[paper_key] = len(seen_papers) + 1
        ref_num = seen_papers[paper_key]

        cite = f"[{ref_num}] {chunk['title']}"
        if chunk["year"]:
            cite += f" ({chunk['year']})"

        context_parts.append(
            f"--- Source {ref_num}, chunk {chunk['chunk_index']} ---\n"
            f"{chunk['content']}\n"
        )

    # Build reference list
    refs = []
    for paper_key, ref_num in seen_papers.items():
        chunk = next(c for c in chunks if c["paper_id"] == paper_key)
        ref = f"[{ref_num}] {chunk['title']}"
        if chunk["year"]:
            ref += f" ({chunk['year']})"
        if chunk["doi"]:
            ref += f" doi:{chunk['doi']}"
        refs.append(ref)

    return "\n\n".join(context_parts), "\n".join(refs)


SCHEMA_PROMPTS = {
    "corpus": (
        "You are a research assistant helping academics explore a corpus of papers "
        "on generative AI in education and assessment."
    ),
    "mmpi3": (
        "You are a research assistant helping psychologists and clinicians explore texts "
        "on the MMPI-3 (Minnesota Multiphasic Personality Inventory-3) personality assessment."
    ),
    "anna_freud": (
        "You are a research assistant helping scholars explore the writings of and about "
        "Anna Freud, including child psychoanalysis, ego psychology, and defense mechanisms."
    ),
    "pcos": (
        "You are a research assistant helping researchers explore literature "
        "on Polycystic Ovary Syndrome (PCOS)."
    ),
    "personality_assessment_inventory": (
        "You are a research assistant helping clinicians explore literature "
        "on the Personality Assessment Inventory (PAI)."
    ),
}


def query(question, top_k=TOP_K, schema="corpus", model_name=None):
    """Full RAG pipeline: retrieve chunks, synthesize answer with Claude."""
    chunks = retrieve_chunks(question, top_k, schema=schema)

    if not chunks:
        return "No relevant content found in the corpus.", [], ""

    context_text, references = build_context(chunks)

    topic_prompt = SCHEMA_PROMPTS.get(schema, "You are a research assistant.")
    system_prompt = (
        f"{topic_prompt} Answer the user's question "
        "based on the provided source excerpts. Cite sources using bracket notation "
        "like [1], [2]. If the sources don't contain enough information to fully "
        "answer, say so. Be specific and use evidence from the texts."
    )

    model_id = CLAUDE_MODELS.get(model_name, CLAUDE_MODEL)

    client = anthropic.Anthropic(api_key=config.ANTHROPIC_API_KEY)
    response = client.messages.create(
        model=model_id,
        max_tokens=2000,
        system=system_prompt,
        messages=[{
            "role": "user",
            "content": (
                f"Question: {question}\n\n"
                f"Source excerpts:\n{context_text}\n\n"
                f"References:\n{references}\n\n"
                f"Please answer the question based on these sources, citing them with [1], [2], etc."
            ),
        }],
    )

    answer = response.content[0].text
    return answer, chunks, references


def get_papers_list(schema="corpus"):
    """Return list of (paper_id, title) for a schema."""
    with get_connection(schema=schema) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT paper_id, title FROM papers ORDER BY title;")
            return cur.fetchall()


def synthesize(paper_ids, prompt, schema="corpus", model_name=None):
    """Retrieve all chunks for selected papers and synthesize with Claude."""
    if not paper_ids or not prompt.strip():
        return ""

    with get_connection(schema=schema) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT c.content, p.title, p.paper_id
                FROM chunks c
                JOIN papers p ON c.paper_id = p.paper_id
                WHERE c.paper_id = ANY(%s)
                ORDER BY p.paper_id, c.chunk_index;
                """,
                (paper_ids,),
            )
            rows = cur.fetchall()

    if not rows:
        return "No content found for the selected documents."

    # Build context grouped by paper
    seen = {}
    context_parts = []
    for content, title, pid in rows:
        if pid not in seen:
            seen[pid] = len(seen) + 1
            context_parts.append(f"\n=== [{seen[pid]}] {title} ===\n")
        context_parts.append(content)

    refs = "\n".join(f"[{num}] {next(t for _, t, p in rows if p == pid)}"
                     for pid, num in seen.items())
    context_text = "\n\n".join(context_parts)

    topic_prompt = SCHEMA_PROMPTS.get(schema, "You are a research assistant.")
    system_prompt = (
        f"{topic_prompt} Synthesize a response based on the provided documents. "
        "Cite sources using bracket notation like [1], [2]. "
        "Be thorough and use specific evidence from the texts."
    )

    model_id = CLAUDE_MODELS.get(model_name, CLAUDE_MODEL)

    client = anthropic.Anthropic(api_key=config.ANTHROPIC_API_KEY)
    response = client.messages.create(
        model=model_id,
        max_tokens=2000,
        system=system_prompt,
        messages=[{
            "role": "user",
            "content": (
                f"{prompt}\n\n"
                f"Documents:\n{context_text}\n\n"
                f"References:\n{refs}"
            ),
        }],
    )

    return response.content[0].text


def main():
    """Interactive CLI for testing the RAG pipeline."""
    print("ResearchRAG — ask questions about the corpus")
    print("Type 'quit' to exit\n")

    # Pre-load the embedding model
    print("Loading embedding model...")
    get_embed_model()
    print("Ready.\n")

    while True:
        question = input("Question: ").strip()
        if not question or question.lower() in ("quit", "exit", "q"):
            break

        print("\nSearching...")
        answer, chunks, refs = query(question)

        print(f"\n{'=' * 70}")
        print(answer)
        print(f"\n{'─' * 70}")
        print("Sources:")
        print(refs)
        print(f"{'=' * 70}\n")


if __name__ == "__main__":
    main()
