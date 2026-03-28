#!/usr/bin/env python3
"""
Gradio web UI for ResearchRAG.
Semantic search + RAG synthesis over multiple research topic schemas.
"""
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))
from dotenv import load_dotenv
load_dotenv()

import gradio as gr
from rag_pipeline import query, retrieve_chunks, get_embed_model
from db.connection import get_connection


# ── Schema / topic registry ──────────────────────────────────────

def get_available_schemas():
    """Discover schemas that have the papers table (i.e. topic schemas)."""
    with get_connection(schema="public") as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT table_schema FROM information_schema.tables
                WHERE table_name = 'papers'
                  AND table_schema NOT IN ('information_schema', 'pg_catalog', 'pg_toast', 'public')
                ORDER BY table_schema;
            """)
            return [row[0] for row in cur.fetchall()]


SCHEMA_DISPLAY_NAMES = {
    "corpus": "GenAI in Education & Assessment",
    "mmpi3": "MMPI-3",
    "anna_freud": "Anna Freud",
    "pcos": "PCOS",
    "personality_assessment_inventory": "Personality Assessment Inventory",
    "psychological_assessment": "Psychological Assessment",
}


def schema_display(schema):
    return SCHEMA_DISPLAY_NAMES.get(schema, schema)


def display_to_schema(display_name):
    for k, v in SCHEMA_DISPLAY_NAMES.items():
        if v == display_name:
            return k
    return display_name


# ── Stats ─────────────────────────────────────────────────────────

def get_corpus_stats(schema="corpus"):
    """Get basic stats for a schema."""
    with get_connection(schema=schema) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM papers;")
            total_papers = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM papers WHERE is_seed = TRUE;")
            seed_papers = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM chunks;")
            total_chunks = cur.fetchone()[0]
            cur.execute("SELECT COUNT(DISTINCT paper_id) FROM chunks;")
            fulltext_papers = cur.fetchone()[0]
    return total_papers, seed_papers, total_chunks, fulltext_papers


# ── RAG Q&A ───────────────────────────────────────────────────────

def rag_query(question, top_k, topic_name):
    """Run the RAG pipeline and format output for Gradio."""
    if not question.strip():
        return "", ""

    schema = display_to_schema(topic_name)
    top_k = int(top_k)
    answer, chunks, refs = query(question, top_k=top_k, schema=schema)

    # Format source details
    sources_md = ""
    if chunks:
        seen = {}
        for c in chunks:
            pid = c["paper_id"]
            if pid not in seen:
                seen[pid] = c
        sources_md = "### Sources\n\n"
        for i, (pid, c) in enumerate(seen.items(), 1):
            title = c["title"]
            year = c["year"] or "n.d."
            doi = c["doi"]
            line = f"**[{i}]** {title} ({year})"
            if doi:
                line += f"  \ndoi: [{doi}](https://doi.org/{doi})"
            sources_md += line + "\n\n"

    return answer, sources_md


# ── Semantic Search ───────────────────────────────────────────────

def search_papers(search_query, top_k, topic_name):
    """Semantic search over chunks — returns matching excerpts."""
    if not search_query.strip():
        return ""

    schema = display_to_schema(topic_name)
    top_k = int(top_k)
    chunks = retrieve_chunks(search_query, top_k=top_k, schema=schema)

    if not chunks:
        return "No results found."

    results_md = ""
    for i, c in enumerate(chunks, 1):
        title = c["title"]
        year = c["year"] or "n.d."
        doi = c["doi"]
        distance = c["distance"]
        similarity = max(0, 1 - distance)
        excerpt = c["content"][:500] + "..." if len(c["content"]) > 500 else c["content"]

        results_md += f"### {i}. {title} ({year})\n"
        results_md += f"**Relevance:** {similarity:.1%}"
        if doi:
            results_md += f" | **DOI:** [{doi}](https://doi.org/{doi})"
        results_md += f"\n\n> {excerpt}\n\n---\n\n"

    return results_md


# ── Browse ────────────────────────────────────────────────────────

def get_all_keywords(schema="corpus"):
    """Get all unique found_via_keywords for the filter dropdown."""
    with get_connection(schema=schema) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT DISTINCT unnest(found_via_keywords) AS kw
                   FROM papers WHERE found_via_keywords IS NOT NULL
                   ORDER BY kw;"""
            )
            return [row[0] for row in cur.fetchall()]


def browse_papers(sort_by, paper_type, year_min, year_max, keyword, topic_name):
    """Return a markdown table of papers with sorting, filtering, and keyword grouping."""
    schema = display_to_schema(topic_name)

    # Build query
    conditions = []
    params = []

    # Filter: paper type
    if paper_type == "Seed only":
        conditions.append("is_seed = TRUE")
    elif paper_type == "Expanded only":
        conditions.append("is_seed = FALSE")

    # Filter: year range (include papers with NULL year)
    conditions.append("(year IS NULL OR (year >= %s AND year <= %s))")
    params.append(int(year_min))
    params.append(int(year_max))

    # Filter: keyword
    if keyword and keyword != "All":
        conditions.append("%s = ANY(found_via_keywords)")
        params.append(keyword)

    where = "WHERE " + " AND ".join(conditions) if conditions else ""

    # Sort
    sort_map = {
        "Year (newest)": "year DESC NULLS LAST",
        "Year (oldest)": "year ASC NULLS LAST",
        "Citations (most)": "citations_count DESC NULLS LAST",
        "Citations (least)": "citations_count ASC NULLS LAST",
        "Title A-Z": "title ASC",
        "Title Z-A": "title DESC",
        "Seed first": "is_seed DESC, title ASC",
        "Expanded first": "is_seed ASC, title ASC",
    }
    order = sort_map.get(sort_by, "year DESC NULLS LAST")

    query_sql = f"""
        SELECT title, year, doi, is_seed, citations_count
        FROM papers {where}
        ORDER BY {order};
    """

    with get_connection(schema=schema) as conn:
        with conn.cursor() as cur:
            cur.execute(query_sql, params)
            rows = cur.fetchall()

    if not rows:
        return f"No papers found matching filters. ({len(rows)} results)"

    md = f"**{len(rows)} papers found**\n\n"
    md += "| # | Title | Year | Type | Citations | DOI |\n"
    md += "|---|-------|------|------|-----------|-----|\n"
    for i, (title, year, doi, is_seed, cites) in enumerate(rows, 1):
        short_title = title[:80] + "..." if len(title) > 80 else title
        type_flag = "Seed" if is_seed else "Expanded"
        cites_str = str(cites) if cites else ""
        doi_str = f"[link](https://doi.org/{doi})" if doi else ""
        md += f"| {i} | {short_title} | {year or ''} | {type_flag} | {cites_str} | {doi_str} |\n"

    return md


# ── Topic change handler ─────────────────────────────────────────

def on_topic_change(topic_name):
    """Update header stats and keyword filter when topic changes."""
    schema = display_to_schema(topic_name)
    total_papers, seed_papers, total_chunks, fulltext_papers = get_corpus_stats(schema)

    header = (
        f"# ResearchRAG — {topic_name}\n"
        f"**{total_papers} papers** | "
        f"{fulltext_papers} full-text | "
        f"{total_chunks} searchable chunks"
    )

    keywords = get_all_keywords(schema)
    keyword_choices = ["All"] + keywords

    return header, gr.Dropdown(choices=keyword_choices, value="All")


# ── Build UI ──────────────────────────────────────────────────────

# Pre-load embedding model at startup
print("Loading embedding model...")
get_embed_model()
print("Ready.")

# Discover available schemas
available_schemas = get_available_schemas()
topic_choices = [schema_display(s) for s in available_schemas]
default_topic = topic_choices[0] if topic_choices else "corpus"

# Initial stats
default_schema = display_to_schema(default_topic)
total_papers, seed_papers, total_chunks, fulltext_papers = get_corpus_stats(default_schema)

with gr.Blocks(title="ResearchRAG", theme=gr.themes.Soft()) as app:

    # Topic selector
    with gr.Row():
        topic_dropdown = gr.Dropdown(
            choices=topic_choices,
            value=default_topic,
            label="Research Topic",
            scale=2,
        )

    # Dynamic header
    header_md = gr.Markdown(
        f"# ResearchRAG — {default_topic}\n"
        f"**{total_papers} papers** | "
        f"{fulltext_papers} full-text | "
        f"{total_chunks} searchable chunks"
    )

    with gr.Tabs():
        # Tab 1: RAG Q&A
        with gr.Tab("Ask a Question"):
            gr.Markdown("Ask a research question and get a synthesized answer with citations.")
            with gr.Row():
                with gr.Column(scale=4):
                    question_input = gr.Textbox(
                        label="Your question",
                        placeholder="e.g., What are the validity scales in the MMPI-3?",
                        lines=2,
                    )
                with gr.Column(scale=1):
                    top_k_slider = gr.Slider(
                        minimum=3, maximum=20, value=8, step=1,
                        label="Sources to retrieve",
                    )
            ask_btn = gr.Button("Ask", variant="primary")
            answer_output = gr.Markdown(label="Answer")
            sources_output = gr.Markdown(label="Sources")

            status_text = gr.Markdown(visible=False)

            def rag_with_status(question, top_k, topic_name):
                yield gr.Markdown(value="*Searching and synthesizing — this may take a moment...*", visible=True), "", ""
                answer, sources = rag_query(question, top_k, topic_name)
                yield gr.Markdown(value="", visible=False), answer, sources

            ask_btn.click(
                fn=rag_with_status,
                inputs=[question_input, top_k_slider, topic_dropdown],
                outputs=[status_text, answer_output, sources_output],
            )
            question_input.submit(
                fn=rag_with_status,
                inputs=[question_input, top_k_slider, topic_dropdown],
                outputs=[status_text, answer_output, sources_output],
            )

        # Tab 2: Semantic Search
        with gr.Tab("Search"):
            gr.Markdown("Semantic search across chunks — find the most relevant excerpts.")
            with gr.Row():
                with gr.Column(scale=4):
                    search_input = gr.Textbox(
                        label="Search query",
                        placeholder="e.g., defense mechanisms, ego psychology",
                        lines=1,
                    )
                with gr.Column(scale=1):
                    search_k_slider = gr.Slider(
                        minimum=3, maximum=20, value=8, step=1,
                        label="Results",
                    )
            search_btn = gr.Button("Search", variant="primary")
            search_output = gr.Markdown(label="Results")

            search_status = gr.Markdown(visible=False)

            def search_with_status(search_query, top_k, topic_name):
                yield gr.Markdown(value="*Searching...*", visible=True), ""
                results = search_papers(search_query, top_k, topic_name)
                yield gr.Markdown(value="", visible=False), results

            search_btn.click(
                fn=search_with_status,
                inputs=[search_input, search_k_slider, topic_dropdown],
                outputs=[search_status, search_output],
            )
            search_input.submit(
                fn=search_with_status,
                inputs=[search_input, search_k_slider, topic_dropdown],
                outputs=[search_status, search_output],
            )

        # Tab 3: Browse Corpus
        with gr.Tab("Browse Corpus"):
            gr.Markdown("Browse and filter papers in the corpus.")

            all_keywords = get_all_keywords(default_schema)

            with gr.Row():
                sort_dropdown = gr.Dropdown(
                    choices=["Title A-Z", "Title Z-A", "Year (newest)", "Year (oldest)", "Citations (most)", "Citations (least)", "Seed first", "Expanded first"],
                    value="Title A-Z",
                    label="Sort by",
                )
                type_filter = gr.Dropdown(
                    choices=["All", "Seed only", "Expanded only"],
                    value="All",
                    label="Paper type",
                )
                keyword_filter = gr.Dropdown(
                    choices=["All"] + all_keywords,
                    value="All",
                    label="Keyword",
                )
            with gr.Row():
                year_min_slider = gr.Slider(
                    minimum=1900, maximum=2026, value=1900, step=1,
                    label="Year from",
                )
                year_max_slider = gr.Slider(
                    minimum=1900, maximum=2026, value=2026, step=1,
                    label="Year to",
                )

            browse_btn = gr.Button("Apply Filters", variant="primary")
            browse_output = gr.Markdown()

            browse_inputs = [sort_dropdown, type_filter, year_min_slider, year_max_slider, keyword_filter, topic_dropdown]

            browse_btn.click(
                fn=browse_papers,
                inputs=browse_inputs,
                outputs=[browse_output],
            )

    # Wire topic change to update header + keyword filter
    topic_dropdown.change(
        fn=on_topic_change,
        inputs=[topic_dropdown],
        outputs=[header_md, keyword_filter],
    )

if __name__ == "__main__":
    app.launch(server_name="0.0.0.0", server_port=7860)
