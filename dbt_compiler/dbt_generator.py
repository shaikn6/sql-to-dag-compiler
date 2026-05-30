"""
dbt_generator.py — Parse SQL CTEs and SELECT statements and emit dbt model artefacts.

Outputs
-------
- models/staging/<model>.sql       — individual dbt SQL files (SELECT only)
- models/marts/<model>.sql         — aggregated / mart-layer models
- models/staging/schema.yml        — column descriptions per model
- models/marts/schema.yml          — column descriptions per mart model
- sources.yml                      — raw source table declarations

Grain detection
---------------
If a model contains GROUP BY or aggregate functions it is classified as
row-level-aggregated → materialized: table.
Otherwise → materialized: view.

Supports: aliases, subqueries, UNION ALL, window functions, CTEs.
"""

from __future__ import annotations

import re
import textwrap
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import sqlparse
from sqlparse import tokens as T
from sqlparse.sql import Identifier, IdentifierList


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class DbtColumn:
    name: str
    description: str = ""
    data_type: str = ""


@dataclass
class DbtModel:
    name: str
    layer: str          # "staging" | "marts"
    raw_sql: str        # cleaned SELECT body (no CTE preamble)
    materialized: str   # "view" | "table"
    columns: list[DbtColumn] = field(default_factory=list)
    source_refs: list[str] = field(default_factory=list)   # source() refs
    model_refs: list[str] = field(default_factory=list)    # ref() refs
    description: str = ""

    @property
    def dbt_sql(self) -> str:
        """Return the dbt SQL with {{ ref() }} and {{ source() }} substitutions."""
        sql = self.raw_sql
        for ref in self.model_refs:
            pattern = re.compile(
                r"(?<!['\"])\b" + re.escape(ref) + r"\b(?!['\"])",
                re.IGNORECASE,
            )
            sql = pattern.sub(f"{{{{ ref('{ref}') }}}}", sql)
        for src in self.source_refs:
            # sources appear as schema.table — replace the table part with source()
            parts = src.split(".")
            if len(parts) == 2:
                schema, tbl = parts
                pattern = re.compile(
                    r"(?<!['\"])\b" + re.escape(src) + r"\b(?!['\"])",
                    re.IGNORECASE,
                )
                sql = pattern.sub(f"{{{{ source('{schema}', '{tbl}') }}}}", sql)
        return sql


@dataclass
class DbtSource:
    schema: str
    tables: list[str] = field(default_factory=list)


@dataclass
class DbtCompileResult:
    models: list[DbtModel] = field(default_factory=list)
    sources: list[DbtSource] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Regex helpers
# ---------------------------------------------------------------------------

_STRIP_SINGLE_LINE_COMMENT = re.compile(r"--[^\n]*", re.MULTILINE)
_STRIP_BLOCK_COMMENT = re.compile(r"/\*.*?\*/", re.DOTALL)

_CTE_BLOCK_RE = re.compile(
    r"^\s*WITH\s+(.+?)\s*(?=SELECT\s)",
    re.IGNORECASE | re.DOTALL,
)
_CTE_DEF_RE = re.compile(
    r"(\w+)\s+AS\s*\((.+?)\)(?=\s*,|\s*SELECT|\s*\)|\s*$)",
    re.IGNORECASE | re.DOTALL,
)

_UNION_ALL_RE = re.compile(r"\bUNION\s+ALL\b", re.IGNORECASE)
_WINDOW_FN_RE = re.compile(r"\bOVER\s*\(", re.IGNORECASE)
_AGG_FN_RE = re.compile(
    r"\b(SUM|COUNT|AVG|MIN|MAX|STDDEV|VARIANCE|MEDIAN|LISTAGG)\s*\(",
    re.IGNORECASE,
)
_GROUP_BY_RE = re.compile(r"\bGROUP\s+BY\b", re.IGNORECASE)
_FROM_JOIN_RE = re.compile(
    r"(?:FROM|JOIN)\s+([\w]+(?:\.[\w]+)+|[\w]+)(?:\s+(?:AS\s+)?[\w]+)?",
    re.IGNORECASE,
)
_CTAS_RE = re.compile(
    r"CREATE\s+(?:OR\s+REPLACE\s+)?TABLE\s+(\S+)\s+AS\s+(.*)",
    re.IGNORECASE | re.DOTALL,
)
_INSERT_SELECT_RE = re.compile(
    r"INSERT\s+INTO\s+(\S+)\s+(?:\(.*?\))?\s*(SELECT\s+.*)",
    re.IGNORECASE | re.DOTALL,
)

_ALIAS_RE = re.compile(
    r"(?:FROM|JOIN)\s+\S+\s+(?:AS\s+)?(\w+)\b",
    re.IGNORECASE,
)

_SELECT_COLS_RE = re.compile(
    r"SELECT\s+(.*?)\s+FROM\b",
    re.IGNORECASE | re.DOTALL,
)

_CAST_RE = re.compile(r"CAST\s*\(.+?\s+AS\s+(\w+)\)", re.IGNORECASE)
_COLNAME_RE = re.compile(r"(?:AS\s+)?(\w+)\s*$", re.IGNORECASE)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def compile_sql_to_dbt(sql: str) -> DbtCompileResult:
    """
    Parse *sql* (may contain multiple statements) and return a DbtCompileResult
    containing model and source metadata.
    """
    cleaned = _strip_comments(sql)
    raw_statements = sqlparse.split(cleaned)

    result = DbtCompileResult()
    known_model_names: set[str] = set()

    for raw in raw_statements:
        stripped = raw.strip()
        if not stripped:
            continue
        _process_statement(stripped, result, known_model_names)

    # Deduplicate sources
    result.sources = _merge_sources(result.sources)
    return result


def write_dbt_project(result: DbtCompileResult, output_dir: str) -> dict[str, str]:
    """
    Write dbt artefacts to *output_dir* and return a dict of
    ``{relative_path: file_content}`` for all written files.
    """
    base = Path(output_dir)
    written: dict[str, str] = {}

    staging_models = [m for m in result.models if m.layer == "staging"]
    mart_models = [m for m in result.models if m.layer == "marts"]

    # Model SQL files
    for model in staging_models:
        path = base / "models" / "staging" / f"{model.name}.sql"
        path.parent.mkdir(parents=True, exist_ok=True)
        content = _render_model_sql(model)
        path.write_text(content, encoding="utf-8")
        written[f"models/staging/{model.name}.sql"] = content

    for model in mart_models:
        path = base / "models" / "marts" / f"{model.name}.sql"
        path.parent.mkdir(parents=True, exist_ok=True)
        content = _render_model_sql(model)
        path.write_text(content, encoding="utf-8")
        written[f"models/marts/{model.name}.sql"] = content

    # schema.yml per layer
    if staging_models:
        schema_path = base / "models" / "staging" / "schema.yml"
        content = _render_schema_yml(staging_models)
        schema_path.write_text(content, encoding="utf-8")
        written["models/staging/schema.yml"] = content

    if mart_models:
        schema_path = base / "models" / "marts" / "schema.yml"
        content = _render_schema_yml(mart_models)
        schema_path.write_text(content, encoding="utf-8")
        written["models/marts/schema.yml"] = content

    # sources.yml
    if result.sources:
        sources_path = base / "sources.yml"
        content = _render_sources_yml(result.sources)
        sources_path.write_text(content, encoding="utf-8")
        written["sources.yml"] = content

    return written


# ---------------------------------------------------------------------------
# Statement processing
# ---------------------------------------------------------------------------

def _process_statement(
    sql: str,
    result: DbtCompileResult,
    known_model_names: set[str],
) -> None:
    """Extract CTEs + final SELECT from *sql* and populate *result*."""
    upper = sql.upper().strip()

    # Extract target table name + select body
    target_table: str | None = None
    select_body: str = sql

    m_ctas = _CTAS_RE.match(sql.strip())
    m_ins = _INSERT_SELECT_RE.match(sql.strip())

    if m_ctas:
        target_table = m_ctas.group(1).strip().lower()
        select_body = m_ctas.group(2).strip()
    elif m_ins:
        target_table = m_ins.group(1).strip().lower()
        select_body = m_ins.group(2).strip()
    else:
        # Plain SELECT — derive name from first CTE or fallback
        select_body = sql

    # Parse out CTEs
    ctes = _extract_ctes(select_body)
    final_select = _extract_final_select(select_body)

    # Register each CTE as its own dbt model
    for cte_name, cte_sql in ctes.items():
        model = _build_model_from_sql(
            name=cte_name,
            sql=cte_sql,
            known_model_names=known_model_names,
            is_cte=True,
        )
        result.models.append(model)
        known_model_names.add(cte_name)
        # Collect sources from CTE
        _collect_sources(cte_sql, known_model_names, result)

    # Final SELECT / target table as a model
    if final_select and target_table:
        short_name = target_table.split(".")[-1]
        model = _build_model_from_sql(
            name=short_name,
            sql=final_select,
            known_model_names=known_model_names,
            is_cte=False,
            full_table_name=target_table,
        )
        result.models.append(model)
        known_model_names.add(short_name)
        _collect_sources(final_select, known_model_names, result)


def _build_model_from_sql(
    name: str,
    sql: str,
    known_model_names: set[str],
    is_cte: bool,
    full_table_name: str | None = None,
) -> DbtModel:
    is_aggregated = bool(_GROUP_BY_RE.search(sql)) or bool(_AGG_FN_RE.search(sql))
    has_window = bool(_WINDOW_FN_RE.search(sql))
    is_union = bool(_UNION_ALL_RE.search(sql))

    materialized = "table" if (is_aggregated or has_window) else "view"

    # Classify layer: mart if aggregated + not CTE, else staging
    if full_table_name:
        schema = full_table_name.split(".")[0] if "." in full_table_name else "staging"
        layer = "marts" if schema in ("mart", "marts", "dw", "warehouse") else "staging"
    else:
        layer = "staging"

    columns = _extract_column_metadata(sql)

    # Detect refs to known models
    model_refs = [m for m in known_model_names if _references_name(sql, m)]

    # Detect external source refs (schema.table patterns not in known models)
    sources_raw = _extract_table_refs(sql)
    source_refs = [
        s for s in sources_raw
        if "." in s and s.split(".")[-1] not in known_model_names
    ]

    description = _auto_description(name, is_aggregated, is_union, has_window)

    return DbtModel(
        name=name,
        layer=layer,
        raw_sql=sql,
        materialized=materialized,
        columns=columns,
        source_refs=source_refs,
        model_refs=model_refs,
        description=description,
    )


# ---------------------------------------------------------------------------
# Column extraction
# ---------------------------------------------------------------------------

def _extract_column_metadata(sql: str) -> list[DbtColumn]:
    """Best-effort extraction of projected column names from the outermost SELECT."""
    m = _SELECT_COLS_RE.search(sql)
    if not m:
        return []
    col_clause = m.group(1).strip()
    if col_clause.upper() == "*":
        return [DbtColumn(name="*", description="All columns from upstream")]

    columns: list[DbtColumn] = []
    for raw_col in _split_top_level_commas(col_clause):
        raw_col = raw_col.strip()
        if not raw_col:
            continue
        col_name, data_type = _parse_column_expression(raw_col)
        if col_name:
            columns.append(DbtColumn(
                name=col_name.lower(),
                description=_col_description(col_name, raw_col),
                data_type=data_type,
            ))
    return columns


def _parse_column_expression(expr: str) -> tuple[str, str]:
    """Return (column_name, data_type) from a SELECT expression fragment."""
    expr = expr.strip().rstrip(",")
    data_type = ""

    # CAST(... AS type) — extract type
    cm = _CAST_RE.search(expr)
    if cm:
        data_type = cm.group(1).upper()

    # AS alias — rightmost word after AS
    as_match = re.search(r"\bAS\s+(\w+)\s*$", expr, re.IGNORECASE)
    if as_match:
        return as_match.group(1), data_type

    # No AS — take rightmost identifier token
    parts = re.split(r"[\s,.()+*/\-]", expr)
    parts = [p for p in parts if p and re.match(r"^\w+$", p)]
    if parts:
        return parts[-1], data_type

    return "", data_type


def _col_description(col_name: str, expr: str) -> str:
    if _AGG_FN_RE.search(expr):
        fn_match = _AGG_FN_RE.search(expr)
        fn = fn_match.group(1).upper() if fn_match else "Aggregated"
        return f"{fn} of {col_name}"
    if _WINDOW_FN_RE.search(expr):
        return f"Window function result: {col_name}"
    if _CAST_RE.search(expr):
        return f"Cast column: {col_name}"
    return f"Column: {col_name}"


# ---------------------------------------------------------------------------
# CTE parsing
# ---------------------------------------------------------------------------

def _extract_ctes(sql: str) -> dict[str, str]:
    """Return ordered dict of {cte_name: cte_sql} from a WITH ... SELECT block."""
    stripped = sql.strip()
    if not re.match(r"^\s*WITH\b", stripped, re.IGNORECASE):
        return {}

    # Find position of final SELECT after all CTE definitions
    # Strategy: find the WITH block, split CTE defs by matching parens
    after_with = re.sub(r"^\s*WITH\s+", "", stripped, flags=re.IGNORECASE)
    ctes: dict[str, str] = {}

    pos = 0
    text = after_with
    while pos < len(text):
        # Find CTE name
        m_name = re.match(r"\s*(\w+)\s+AS\s*\(", text[pos:], re.IGNORECASE)
        if not m_name:
            break
        name = m_name.group(1)
        open_paren_pos = pos + m_name.end() - 1  # position of '('
        # Match balanced parens
        body, end_pos = _extract_balanced(text, open_paren_pos)
        ctes[name] = body
        pos = end_pos
        # Skip comma or hit SELECT
        remainder = text[pos:].lstrip()
        if remainder.upper().startswith("SELECT"):
            break
        if remainder.startswith(","):
            pos += text[pos:].index(",") + 1

    return ctes


def _extract_final_select(sql: str) -> str:
    """Return the final SELECT statement after any WITH CTE block."""
    stripped = sql.strip()
    if not re.match(r"^\s*WITH\b", stripped, re.IGNORECASE):
        return stripped  # No WITH — the whole thing is the SELECT

    # Walk through CTE defs and find where the final SELECT starts
    after_with = re.sub(r"^\s*WITH\s+", "", stripped, flags=re.IGNORECASE)
    pos = 0
    text = after_with
    while pos < len(text):
        m_name = re.match(r"\s*(\w+)\s+AS\s*\(", text[pos:], re.IGNORECASE)
        if not m_name:
            break
        open_paren_pos = pos + m_name.end() - 1
        _, end_pos = _extract_balanced(text, open_paren_pos)
        pos = end_pos
        remainder = text[pos:].lstrip()
        if remainder.upper().startswith("SELECT"):
            return remainder
        if remainder.startswith(","):
            idx = text.index(",", pos)
            pos = idx + 1
    return ""


def _extract_balanced(text: str, open_pos: int) -> tuple[str, int]:
    """Return (inner_content, position_after_closing_paren) for balanced parens."""
    depth = 0
    start = open_pos
    i = open_pos
    while i < len(text):
        if text[i] == "(":
            depth += 1
        elif text[i] == ")":
            depth -= 1
            if depth == 0:
                return text[start + 1:i], i + 1
        i += 1
    return text[start + 1:], len(text)


# ---------------------------------------------------------------------------
# Source collection
# ---------------------------------------------------------------------------

def _collect_sources(
    sql: str,
    known_model_names: set[str],
    result: DbtCompileResult,
) -> None:
    refs = _extract_table_refs(sql)
    for ref in refs:
        if "." in ref:
            schema, table = ref.split(".", 1)
            if table not in known_model_names:
                # Add to sources list
                existing = next((s for s in result.sources if s.schema == schema), None)
                if existing:
                    if table not in existing.tables:
                        existing.tables.append(table)
                else:
                    result.sources.append(DbtSource(schema=schema, tables=[table]))


def _extract_table_refs(sql: str) -> list[str]:
    matches = _FROM_JOIN_RE.findall(sql)
    refs = []
    seen: set[str] = set()
    for raw in matches:
        name = raw.strip().lower().rstrip(");,")
        if name and name not in seen and name.lower() != "dual":
            seen.add(name)
            refs.append(name)
    return refs


def _references_name(sql: str, name: str) -> bool:
    pattern = re.compile(r"\b" + re.escape(name) + r"\b", re.IGNORECASE)
    return bool(pattern.search(sql))


# ---------------------------------------------------------------------------
# Rendering helpers
# ---------------------------------------------------------------------------

def _render_model_sql(model: DbtModel) -> str:
    config = (
        f"{{% set config(materialized='{model.materialized}') %}}\n\n"
        if False  # we use the config block form below
        else f"{{{{ config(materialized='{model.materialized}') }}}}\n\n"
    )
    return config + model.dbt_sql.strip() + "\n"


def _render_schema_yml(models: list[DbtModel]) -> str:
    lines = ["version: 2", "", "models:"]
    for model in models:
        lines.append(f"  - name: {model.name}")
        if model.description:
            lines.append(f"    description: \"{model.description}\"")
        lines.append(f"    config:")
        lines.append(f"      materialized: {model.materialized}")
        if model.columns:
            lines.append("    columns:")
            for col in model.columns:
                if col.name == "*":
                    continue
                lines.append(f"      - name: {col.name}")
                if col.description:
                    lines.append(f"        description: \"{col.description}\"")
                if col.data_type:
                    lines.append(f"        data_type: \"{col.data_type}\"")
    return "\n".join(lines) + "\n"


def _render_sources_yml(sources: list[DbtSource]) -> str:
    lines = ["version: 2", "", "sources:"]
    for src in sources:
        lines.append(f"  - name: {src.schema}")
        lines.append(f"    schema: {src.schema}")
        lines.append("    tables:")
        for tbl in sorted(src.tables):
            lines.append(f"      - name: {tbl}")
    return "\n".join(lines) + "\n"


def _auto_description(
    name: str,
    is_aggregated: bool,
    is_union: bool,
    has_window: bool,
) -> str:
    parts = [f"dbt model: {name}."]
    if is_aggregated:
        parts.append("Aggregated grain.")
    if is_union:
        parts.append("UNION ALL of multiple sub-selects.")
    if has_window:
        parts.append("Contains window functions.")
    return " ".join(parts)


# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------

def _strip_comments(sql: str) -> str:
    no_block = _STRIP_BLOCK_COMMENT.sub(" ", sql)
    return _STRIP_SINGLE_LINE_COMMENT.sub(" ", no_block)


def _split_top_level_commas(text: str) -> list[str]:
    """Split *text* by commas that are NOT inside parentheses."""
    parts: list[str] = []
    depth = 0
    current: list[str] = []
    for ch in text:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        if ch == "," and depth == 0:
            parts.append("".join(current).strip())
            current = []
        else:
            current.append(ch)
    if current:
        parts.append("".join(current).strip())
    return parts


def _merge_sources(sources: list[DbtSource]) -> list[DbtSource]:
    merged: dict[str, DbtSource] = {}
    for src in sources:
        if src.schema in merged:
            for tbl in src.tables:
                if tbl not in merged[src.schema].tables:
                    merged[src.schema].tables.append(tbl)
        else:
            merged[src.schema] = DbtSource(schema=src.schema, tables=list(src.tables))
    return list(merged.values())
