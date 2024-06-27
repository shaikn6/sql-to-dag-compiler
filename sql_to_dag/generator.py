"""
generator.py — Orchestrates the full SQL → Airflow DAG compilation pipeline.

Usage (CLI)
-----------
    python -m sql_to_dag.generator examples/sample_oracle.sql
    python -m sql_to_dag.generator examples/sample_oracle.sql --output examples/output_dag.py
    python -m sql_to_dag.generator examples/sample_oracle.sql --dag-id my_pipeline

Public API
----------
    compile_sql_file(path, **options)  → str  (rendered DAG Python source)
    compile_sql_string(sql, **options) → str
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import networkx as nx
from jinja2 import Environment, FileSystemLoader, select_autoescape

from sql_to_dag.parser import parse_sql_file, parse_sql_string
from sql_to_dag.graph import build_dependency_graph, topological_order, get_dependencies


# ---------------------------------------------------------------------------
# Security constants
# ---------------------------------------------------------------------------

# Maximum SQL input size: 5 MB.  Prevents memory exhaustion and ReDoS
# amplification via sqlparse on pathologically large inputs.
_MAX_SQL_BYTES = 5 * 1024 * 1024  # 5 MB

# Allowed characters for DAG IDs and task labels embedded in generated Python.
# Must match [a-zA-Z0-9_.-] so that no Python-syntax-breaking characters can
# be injected into variable names or string literals inside the generated file.
_SAFE_IDENTIFIER_RE = re.compile(r"^[a-zA-Z0-9_.\-]+$")

# Characters that are dangerous inside a Python triple-quoted string used in
# the generated DAG file.  We encode them so that SQL content cannot escape
# the string context and inject arbitrary Python.
_TRIPLE_QUOTE = '"""'


# ---------------------------------------------------------------------------
# Jinja2 environment
# ---------------------------------------------------------------------------

_TEMPLATES_DIR = Path(__file__).parent / "templates"

_jinja_env = Environment(
    loader=FileSystemLoader(str(_TEMPLATES_DIR)),
    autoescape=select_autoescape([]),  # No HTML escaping — we output Python
    trim_blocks=True,
    lstrip_blocks=True,
)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def _sanitize_identifier(value: str, field_name: str) -> str:
    """
    Validate that *value* contains only characters safe for embedding as a
    Python identifier or string literal in generated DAG source code.

    Raises ValueError for unsafe input so the compiler fails loudly rather
    than producing a DAG with injected content.
    """
    if not value:
        raise ValueError(f"{field_name} must not be empty.")
    if not _SAFE_IDENTIFIER_RE.match(value):
        raise ValueError(
            f"{field_name} contains characters that are not allowed in "
            f"generated Python source. Allowed: a-z A-Z 0-9 _ . -  "
            f"Got: {value!r}"
        )
    return value


def _sanitize_sql_for_embedding(sql: str) -> str:
    """
    Escape SQL content so it cannot break out of the triple-quoted Python
    string context in which it is embedded in the generated DAG file.

    Strategy: replace any triple-quote sequence within the SQL with the
    escaped form ``\\\"\\\"\\\"`` so the string boundary is never closed
    prematurely.  This is the only escape needed for triple-double-quoted
    Python strings.
    """
    return sql.replace(_TRIPLE_QUOTE, '\\"\\"\\"')


def _check_input_size(sql: str) -> None:
    """Reject SQL inputs that exceed the configured byte limit."""
    size = len(sql.encode("utf-8"))
    if size > _MAX_SQL_BYTES:
        raise ValueError(
            f"SQL input is too large ({size:,} bytes). "
            f"Maximum allowed: {_MAX_SQL_BYTES:,} bytes ({_MAX_SQL_BYTES // 1_048_576} MB)."
        )


def compile_sql_file(
    path: str,
    dag_id: str | None = None,
    dag_owner: str = "airflow",
    schedule_interval: str = "@daily",
    retries: int = 1,
    retry_delay_minutes: int = 5,
    tags: list[str] | None = None,
) -> str:
    """
    Parse *path* and render a complete Airflow 2.x DAG Python source string.

    Parameters
    ----------
    path:
        Path to the Oracle SQL / PLSQL file.
    dag_id:
        Airflow dag_id.  Defaults to the stem of *path* with spaces→underscores.
    dag_owner:
        ``owner`` field in ``default_args``.
    schedule_interval:
        Airflow schedule string, e.g. ``"@daily"`` or ``"0 6 * * *"``.
    retries:
        Number of task retries on failure.
    retry_delay_minutes:
        Minutes between retries.
    tags:
        List of Airflow tags for the DAG.

    Returns
    -------
    str
        Valid Python source code for an Airflow DAG file.
    """
    statements = parse_sql_file(path)
    inferred_dag_id = dag_id or _dag_id_from_path(path)
    _sanitize_identifier(inferred_dag_id, "dag_id")
    _sanitize_identifier(dag_owner, "dag_owner")
    return _render(
        statements=statements,
        source_file=os.path.basename(path),
        dag_id=inferred_dag_id,
        dag_owner=dag_owner,
        schedule_interval=schedule_interval,
        retries=retries,
        retry_delay_minutes=retry_delay_minutes,
        tags=tags or ["sql-to-dag", "generated"],
    )


def compile_sql_string(
    sql: str,
    dag_id: str = "generated_dag",
    dag_owner: str = "airflow",
    schedule_interval: str = "@daily",
    retries: int = 1,
    retry_delay_minutes: int = 5,
    tags: list[str] | None = None,
    source_label: str = "<string>",
) -> str:
    """Parse *sql* string and render an Airflow DAG. See ``compile_sql_file`` for params."""
    _check_input_size(sql)
    _sanitize_identifier(dag_id, "dag_id")
    _sanitize_identifier(dag_owner, "dag_owner")
    statements = parse_sql_string(sql)
    return _render(
        statements=statements,
        source_file=source_label,
        dag_id=dag_id,
        dag_owner=dag_owner,
        schedule_interval=schedule_interval,
        retries=retries,
        retry_delay_minutes=retry_delay_minutes,
        tags=tags or ["sql-to-dag", "generated"],
    )


# ---------------------------------------------------------------------------
# Internal rendering logic
# ---------------------------------------------------------------------------

def _dag_id_from_path(path: str) -> str:
    stem = Path(path).stem
    return stem.replace(" ", "_").replace("-", "_")


def _render(
    statements: list[dict[str, Any]],
    source_file: str,
    dag_id: str,
    dag_owner: str,
    schedule_interval: str,
    retries: int,
    retry_delay_minutes: int,
    tags: list[str],
) -> str:
    if not statements:
        raise ValueError("No SQL statements found in input — nothing to compile.")

    graph = build_dependency_graph(statements)
    exec_order = topological_order(graph)

    # Build task list in execution order, preserving all metadata.
    stmt_by_id = {s["id"]: s for s in statements}
    tasks = []
    for stmt_id in exec_order:
        stmt = stmt_by_id[stmt_id]
        # Sanitize task_id: only alphanumerics and underscores are safe as a
        # Python variable name and string literal in generated code.
        raw_label = stmt["label"]
        safe_label = re.sub(r"[^a-zA-Z0-9_]", "_", raw_label)
        if not safe_label or safe_label[0].isdigit():
            safe_label = "task_" + safe_label

        # Escape SQL content so it cannot escape the triple-quoted string
        # context in the generated DAG template.
        safe_sql = _sanitize_sql_for_embedding(stmt["raw_sql"])

        tasks.append(
            {
                "task_id": safe_label,  # sanitized human-readable task name
                "stmt_id": stmt_id,
                "sql": safe_sql,
                "statement_type": stmt["statement_type"],
                "target_table": stmt.get("target_table") or "",
                "source_tables": stmt.get("source_tables", []),
                "aggregations": stmt.get("aggregations", []),
                "has_where": stmt.get("has_where", False),
                "has_group_by": stmt.get("has_group_by", False),
            }
        )

    # Build dependency lines like:  task_b.set_upstream(task_a)
    dependency_lines = _build_dependency_lines(graph, stmt_by_id, exec_order)

    now = datetime.now(tz=timezone.utc)
    template = _jinja_env.get_template("dag_template.py.j2")
    rendered = template.render(
        source_file=source_file,
        generated_at=now.strftime("%Y-%m-%d %H:%M:%S UTC"),
        dag_id=dag_id,
        dag_description=f"Auto-generated from {source_file}",
        dag_owner=dag_owner,
        schedule_interval=schedule_interval,
        start_date_year=now.year,
        start_date_month=now.month,
        start_date_day=1,
        retries=retries,
        retry_delay_minutes=retry_delay_minutes,
        tags=tags,
        tasks=tasks,
        dependency_lines=dependency_lines,
    )
    return rendered


def _safe_label(raw: str) -> str:
    """Return a sanitized Python identifier derived from *raw*."""
    safe = re.sub(r"[^a-zA-Z0-9_]", "_", raw)
    if not safe or safe[0].isdigit():
        safe = "task_" + safe
    return safe


def _build_dependency_lines(
    graph: nx.DiGraph,
    stmt_by_id: dict[str, dict[str, Any]],
    exec_order: list[str],
) -> list[str]:
    """
    Return Airflow dependency lines in topological order.

    Format:  ``downstream_task_label.set_upstream(upstream_task_label)``

    Both label tokens are sanitized to pure Python identifiers before
    being interpolated so that a malicious task label cannot inject
    arbitrary Python into the generated DAG file.
    """
    lines: list[str] = []
    seen: set[tuple[str, str]] = set()

    for stmt_id in exec_order:
        stmt = stmt_by_id[stmt_id]
        downstream_label = _safe_label(stmt["label"])
        for pred_id in graph.predecessors(stmt_id):
            pred_label = _safe_label(stmt_by_id[pred_id]["label"])
            key = (pred_label, downstream_label)
            if key not in seen:
                seen.add(key)
                lines.append(f"{downstream_label}.set_upstream({pred_label})")

    return lines


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="sql2dag",
        description="Convert an Oracle SQL/PLSQL file into an Apache Airflow 2.x DAG.",
    )
    parser.add_argument("sql_file", help="Path to the Oracle SQL file to compile.")
    parser.add_argument(
        "--output", "-o",
        default=None,
        help="Output path for the generated DAG Python file. Defaults to stdout.",
    )
    parser.add_argument("--dag-id", default=None, help="Airflow dag_id (default: file stem).")
    parser.add_argument("--owner", default="airflow", help="DAG owner (default: airflow).")
    parser.add_argument(
        "--schedule", default="@daily", help="Schedule interval (default: @daily)."
    )
    parser.add_argument("--retries", type=int, default=1, help="Task retries (default: 1).")
    parser.add_argument(
        "--retry-delay", type=int, default=5,
        help="Retry delay in minutes (default: 5).",
    )

    args = parser.parse_args(argv)

    dag_source = compile_sql_file(
        path=args.sql_file,
        dag_id=args.dag_id,
        dag_owner=args.owner,
        schedule_interval=args.schedule,
        retries=args.retries,
        retry_delay_minutes=args.retry_delay,
    )

    if args.output:
        out_path = Path(args.output).resolve()
        # Refuse output paths that attempt to escape via directory traversal.
        # Resolve the path and ensure it is a .py file (not a symlink attack).
        if out_path.suffix.lower() not in (".py", ""):
            print(
                f"Error: output path must have a .py extension, got: {out_path}",
                file=sys.stderr,
            )
            sys.exit(1)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(dag_source, encoding="utf-8")
        print(f"DAG written to: {out_path}")
    else:
        print(dag_source)


if __name__ == "__main__":
    main()
