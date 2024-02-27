"""
column_tracer.py — Trace an individual column from source table through all CTEs
to the final output, recording the transformation applied at each step.

Public API
----------
trace_column(sql, column_name) → ColumnLineage
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

import sqlparse


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class TransformStep:
    """A single hop in a column's lineage path."""
    model_name: str          # CTE name or target table name
    input_col: str           # column name coming INTO this step
    output_col: str          # column name leaving this step (alias)
    transformation: str      # human-readable description of what was applied
    sql_snippet: str         # the relevant expression fragment


@dataclass
class ColumnLineage:
    """Full lineage path for a single column from source to final output."""
    column_name: str
    source_table: str | None
    final_output: str | None
    path: list[TransformStep] = field(default_factory=list)
    found: bool = False

    @property
    def depth(self) -> int:
        return len(self.path)


# ---------------------------------------------------------------------------
# Regex helpers (mirrors some from dbt_generator but scoped here)
# ---------------------------------------------------------------------------

_STRIP_SINGLE = re.compile(r"--[^\n]*", re.MULTILINE)
_STRIP_BLOCK = re.compile(r"/\*.*?\*/", re.DOTALL)
_WITH_RE = re.compile(r"^\s*WITH\b", re.IGNORECASE)
_CTAS_RE = re.compile(
    r"CREATE\s+(?:OR\s+REPLACE\s+)?TABLE\s+(\S+)\s+AS\s+",
    re.IGNORECASE,
)
_INSERT_RE = re.compile(r"INSERT\s+INTO\s+(\S+)", re.IGNORECASE)
_SELECT_COLS_RE = re.compile(r"SELECT\s+(.*?)\s+FROM\b", re.IGNORECASE | re.DOTALL)
_AGG_RE = re.compile(
    r"\b(SUM|COUNT|AVG|MIN|MAX|STDDEV|VARIANCE|MEDIAN|LISTAGG)\s*\(",
    re.IGNORECASE,
)
_CAST_RE = re.compile(r"CAST\s*\(.+?\s+AS\s+(\w+)\)", re.IGNORECASE)
_WINDOW_RE = re.compile(r"\bOVER\s*\(", re.IGNORECASE)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def trace_column(sql: str, column_name: str) -> ColumnLineage:
    """
    Trace *column_name* through all CTEs and statements in *sql*.

    Returns a ColumnLineage with the full transformation path.
    """
    cleaned = _strip_comments(sql)
    statements = sqlparse.split(cleaned)
    lineage = ColumnLineage(column_name=column_name, source_table=None, final_output=None)

    for raw in statements:
        stripped = raw.strip()
        if not stripped:
            continue
        _trace_statement(stripped, column_name, lineage)

    return lineage


# ---------------------------------------------------------------------------
# Statement-level tracing
# ---------------------------------------------------------------------------

def _trace_statement(sql: str, column_name: str, lineage: ColumnLineage) -> None:
    """Walk CTEs and final SELECT inside *sql*, building lineage.path."""
    # Determine target table name
    target_table: str | None = None
    m_ctas = _CTAS_RE.search(sql)
    m_ins = _INSERT_RE.search(sql)
    if m_ctas:
        target_table = m_ctas.group(1).strip().lower().split(".")[-1]
    elif m_ins:
        target_table = m_ins.group(1).strip().lower().split(".")[-1]

    # Parse CTEs
    ctes = _parse_ctes(sql)

    # Determine what the "final" SELECT is
    final_select = _extract_final_select(sql)
    final_name = target_table or "final_output"

    # Walk CTEs in definition order
    current_col = column_name
    for cte_name, cte_sql in ctes.items():
        step = _find_column_in_select(cte_sql, current_col, cte_name)
        if step:
            lineage.path.append(step)
            lineage.found = True
            current_col = step.output_col  # follow alias

    # Check final SELECT
    if final_select:
        step = _find_column_in_select(final_select, current_col, final_name)
        if step:
            lineage.path.append(step)
            lineage.found = True
            lineage.final_output = final_name

    # If nothing found yet, check if column is in any CTE's FROM source
    if not lineage.found and lineage.path:
        lineage.found = True

    # Try to determine source table (first occurrence in a FROM clause)
    if lineage.path and lineage.source_table is None:
        lineage.source_table = _find_source_table(sql, column_name)


def _find_column_in_select(sql: str, column_name: str, model_name: str) -> TransformStep | None:
    """Look for *column_name* (or an alias of it) in the SELECT clause of *sql*."""
    m = _SELECT_COLS_RE.search(sql)
    if not m:
        return None

    col_clause = m.group(1).strip()
    parts = _split_top_level_commas(col_clause)

    for expr in parts:
        expr = expr.strip()
        input_col, output_col, transformation, snippet = _classify_expression(expr, column_name)
        if input_col:
            return TransformStep(
                model_name=model_name,
                input_col=input_col,
                output_col=output_col,
                transformation=transformation,
                sql_snippet=snippet,
            )
    return None


def _classify_expression(
    expr: str, target_col: str
) -> tuple[str, str, str, str]:
    """
    Return (input_col, output_col, transformation, snippet) if *target_col* appears in *expr*.
    All empty strings if not found.
    """
    # Detect alias
    alias_match = re.search(r"\bAS\s+(\w+)\s*$", expr, re.IGNORECASE)
    alias = alias_match.group(1) if alias_match else None

    # Does target_col appear in this expression?
    pattern = re.compile(r"(?<!['\"])\b" + re.escape(target_col) + r"\b(?!['\"])", re.IGNORECASE)
    if not pattern.search(expr):
        # Maybe the alias IS the target column
        if alias and alias.lower() == target_col.lower():
            # expression itself contains the column before alias
            bare = re.sub(r"\bAS\s+\w+\s*$", "", expr, flags=re.IGNORECASE).strip()
            transformation = _describe_transformation(bare)
            return bare, alias, transformation, expr

        return "", "", "", ""

    output_col = alias if alias else target_col
    transformation = _describe_transformation(expr)
    return target_col, output_col, transformation, expr


def _describe_transformation(expr: str) -> str:
    """Produce a human-readable transformation label for the expression."""
    expr_u = expr.upper()
    if _AGG_RE.search(expr):
        fn = _AGG_RE.search(expr).group(1).upper()
        return f"aggregate:{fn}"
    if _WINDOW_RE.search(expr):
        return "window_function"
    if _CAST_RE.search(expr):
        cast_type = _CAST_RE.search(expr).group(1).upper()
        return f"cast:{cast_type}"
    if re.search(r"\bAS\s+\w+\s*$", expr, re.IGNORECASE):
        return "rename"
    if re.search(r"[+\-*/]", expr):
        return "arithmetic"
    if re.search(r"\bCOALESCE\b|\bNVL\b|\bNULLIF\b", expr, re.IGNORECASE):
        return "null_handling"
    if re.search(r"\bCASE\b", expr, re.IGNORECASE):
        return "case_expression"
    return "passthrough"


def _find_source_table(sql: str, column_name: str) -> str | None:
    """Heuristic: the first table in FROM that is NOT a CTE name."""
    from_re = re.compile(
        r"FROM\s+([\w]+(?:\.[\w]+)?)\b",
        re.IGNORECASE,
    )
    ctes = set(_parse_ctes(sql).keys())
    for m in from_re.finditer(sql):
        tbl = m.group(1).strip().lower()
        if tbl not in ctes:
            return tbl
    return None


# ---------------------------------------------------------------------------
# CTE parsing (lightweight, duplicated from dbt_generator to keep module independent)
# ---------------------------------------------------------------------------

def _parse_ctes(sql: str) -> dict[str, str]:
    stripped = sql.strip()
    if not _WITH_RE.match(stripped):
        return {}
    after_with = re.sub(r"^\s*WITH\s+", "", stripped, flags=re.IGNORECASE)
    ctes: dict[str, str] = {}
    pos = 0
    text = after_with
    while pos < len(text):
        m_name = re.match(r"\s*(\w+)\s+AS\s*\(", text[pos:], re.IGNORECASE)
        if not m_name:
            break
        name = m_name.group(1)
        open_paren_pos = pos + m_name.end() - 1
        body, end_pos = _extract_balanced(text, open_paren_pos)
        ctes[name] = body
        pos = end_pos
        remainder = text[pos:].lstrip()
        if remainder.upper().startswith("SELECT"):
            break
        if remainder.startswith(","):
            pos += text[pos:].index(",") + 1
    return ctes


def _extract_final_select(sql: str) -> str:
    stripped = sql.strip()
    if not _WITH_RE.match(stripped):
        m_ctas = _CTAS_RE.search(stripped)
        if m_ctas:
            return stripped[m_ctas.end():].strip()
        m_ins = _INSERT_RE.search(stripped)
        if m_ins:
            sel_start = re.search(r"\bSELECT\b", stripped[m_ins.end():], re.IGNORECASE)
            if sel_start:
                return stripped[m_ins.end() + sel_start.start():]
        return stripped
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
    depth = 0
    i = open_pos
    while i < len(text):
        if text[i] == "(":
            depth += 1
        elif text[i] == ")":
            depth -= 1
            if depth == 0:
                return text[open_pos + 1:i], i + 1
        i += 1
    return text[open_pos + 1:], len(text)


def _split_top_level_commas(text: str) -> list[str]:
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


def _strip_comments(sql: str) -> str:
    no_block = _STRIP_BLOCK.sub(" ", sql)
    return _STRIP_SINGLE.sub(" ", no_block)
