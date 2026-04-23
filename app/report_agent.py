import logging
from pathlib import Path

import anthropic
import psycopg

from settings import Settings

logger = logging.getLogger(__name__)

# Stable system prompt — cached on every call so repeated /report hits are cheap
_SYSTEM_PROMPT = """\
You are a data analyst writing a concise business report from PostgreSQL query results.
Produce a clean, professional markdown document with these sections:

1. **Executive Summary** — 2-3 sentences on overall data health and headline numbers.
2. **Key Metrics** — daily order trends, top customers by lifetime spend, top SKUs by revenue and units sold.
3. **Data Quality Findings** — one subsection per DQ view; call out counts and specific IDs where relevant.
4. **Notable Observations** — patterns, anomalies, or recommendations worth actioning.

Rules:
- Be specific with numbers; never say "some" when you have a count.
- Use markdown tables for tabular data.
- Flag data quality issues with a ⚠️ prefix.
- Keep the whole report under 600 words.\
"""

_VIEWS = {
    "Daily Metrics": "SELECT * FROM vw_daily_metrics",
    "Top 10 Customers by Lifetime Spend": "SELECT * FROM vw_top_customers",
    "Top 10 SKUs by Revenue and Units Sold": "SELECT * FROM vw_top_skus",
    "DQ — Duplicate Customers": "SELECT * FROM vw_dq_duplicate_customers",
    "DQ — Orphaned Orders (missing customer)": "SELECT * FROM vw_dq_orphaned_orders",
    "DQ — Invalid Order Items (qty/price ≤ 0)": "SELECT * FROM vw_dq_invalid_order_items",
    "DQ — Orders with Unknown Status": "SELECT * FROM vw_dq_invalid_order_status",
}


def _rows_to_markdown(description, rows: list) -> str:
    if not rows:
        return "_No rows returned._\n"
    cols = [d.name for d in description]
    header = " | ".join(cols)
    sep = " | ".join("---" for _ in cols)
    body = "\n".join(
        " | ".join("NULL" if v is None else str(v) for v in row) for row in rows
    )
    return f"{header}\n{sep}\n{body}\n"


def _query_all_views(settings: Settings) -> str:
    sections: list[str] = []
    with psycopg.connect(
        host=settings.db_host,
        port=settings.db_port,
        dbname=settings.db_name,
        user=settings.db_user,
        password=settings.db_password,
    ) as conn:
        with conn.cursor() as cur:
            for title, sql in _VIEWS.items():
                try:
                    cur.execute(sql)
                    rows = cur.fetchall()
                    table = _rows_to_markdown(cur.description, rows)
                except Exception as exc:
                    table = f"_Query failed: {exc}_\n"
                sections.append(f"#### {title}\n\n{table}")

    return "\n---\n\n".join(sections)


def generate_report(settings: Settings) -> Path:
    logger.info("querying views for report")
    data_block = _query_all_views(settings)

    logger.info("sending data to Claude for narrative generation")
    client = anthropic.Anthropic()

    response = client.messages.create(
        model="claude-opus-4-7",
        max_tokens=2048,
        system=[
            {
                "type": "text",
                "text": _SYSTEM_PROMPT,
                # Cache the system prompt — stable across every report call
                "cache_control": {"type": "ephemeral"},
            }
        ],
        messages=[
            {
                "role": "user",
                "content": (
                    "Here are the query results from our database views. "
                    "Write the report now.\n\n"
                    f"{data_block}"
                ),
            }
        ],
    )

    report_text = next(b.text for b in response.content if b.type == "text")
    logger.info(
        "report generated — cache_read=%d tokens",
        response.usage.cache_read_input_tokens,
    )

    report_path = Path(__file__).parent / "REPORT.md"
    report_path.write_text(report_text, encoding="utf-8")
    logger.info("report written to %s", report_path)

    return report_path
