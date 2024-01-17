"""
parser.py — Parses Oracle SQL/PLSQL into structured statement metadata.

Each statement is represented as a dict:
    {
        "id":             str,           # e.g. "stmt_0"
        "raw_sql":        str,           # original SQL text
        "statement_type": str,           # "CTAS", "INSERT_SELECT", "INSERT_VALUES", "UNKNOWN"
        "target_table":   str | None,    # fully-qualified target table
        "source_tables":  list[str],     # all tables read by this statement
        "has_where":      bool,
        "has_group_by":   bool,
        "aggregations":   list[str],     # detected aggregate function names
        "label":          str,           # short human-readable task label
    }
"""

from __future__ import annotations

import re
import textwrap
from typing import Any

import sqlparse
from sqlparse.sql import IdentifierList, Identifier, Where
from sqlparse.tokens import Keyword, DML, DDL, Punctuation


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def parse_sql_file(path: str) -> list[dict[str, Any]]:
    """Read *path* and return a list of statement metadata dicts."""
    with open(path, "r", encoding="utf-8") as fh:
        sql_text = fh.read()
    return parse_sql_string(sql_text)


def parse_sql_string(sql_text: str) -> list[dict[str, Any]]:
    """Parse *sql_text* and return a list of statement metadata dicts."""
    # Strip Oracle-style block comments and line comments, but keep the SQL.
    cleaned = _strip_plsql_block_delimiters(sql_text)

    raw_statements = sqlparse.split(cleaned)
    results: list[dict[str, Any]] = []

    stmt_index = 0
    for raw in raw_statements:
        stripped = raw.strip()
        if not stripped:
            continue
        metadata = _parse_single_statement(stripped, stmt_index)
        results.append(metadata)
        stmt_index += 1

    return results


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

_AGGREGATE_FUNCTIONS = {
    "SUM", "COUNT", "AVG", "MIN", "MAX",
    "STDDEV", "VARIANCE", "MEDIAN", "LISTAGG",
}

# Matches Oracle PLSQL block delimiters that sqlparse doesn't fully handle.
_PLSQL_BLOCK_RE = re.compile(
    r"\bBEGIN\b.*?\bEND\b\s*;",
    re.IGNORECASE | re.DOTALL,
)

_CREATE_TABLE_AS_RE = re.compile(
    r"CREATE\s+TABLE\s+(\S+)\s+AS\s+",
    re.IGNORECASE,
)

_INSERT_INTO_RE = re.compile(
    r"INSERT\s+INTO\s+(\S+)",
    re.IGNORECASE,
)

_FROM_JOIN_RE = re.compile(
    r"(?:FROM|JOIN)\s+([\w]+(?:\.[\w]+)+|[\w]+)",
    re.IGNORECASE,
)

_SINGLE_LINE_COMMENT_RE = re.compile(r"--[^\n]*", re.MULTILINE)
_BLOCK_COMMENT_RE = re.compile(r"/\*.*?\*/", re.DOTALL)

_AGG_RE = re.compile(
    r"\b(" + "|".join(_AGGREGATE_FUNCTIONS) + r")\s*\(",
    re.IGNORECASE,
)

_GROUP_BY_RE = re.compile(r"\bGROUP\s+BY\b", re.IGNORECASE)
_WHERE_RE = re.compile(r"\bWHERE\b", re.IGNORECASE)


def _strip_plsql_block_delimiters(sql_text: str) -> str:
    """Remove PL/SQL anonymous block wrappers (BEGIN…END;) that wrap pure SQL."""
    return _PLSQL_BLOCK_RE.sub("", sql_text)


def _normalise_table_name(name: str) -> str:
    """Lower-case and strip trailing punctuation from a table reference."""
    return name.strip().lower().rstrip(");,")


def _strip_comments(sql: str) -> str:
    """Remove single-line (--) and block (/* */) SQL comments."""
    no_block = _BLOCK_COMMENT_RE.sub(" ", sql)
    no_single = _SINGLE_LINE_COMMENT_RE.sub(" ", no_block)
    return no_single


def _detect_statement_type(upper_sql: str) -> str:
    if re.search(r"CREATE\s+TABLE\s+\S+\s+AS\s+SELECT", upper_sql):
        return "CTAS"
    if re.search(r"INSERT\s+INTO\s+\S+\s+SELECT", upper_sql):
        return "INSERT_SELECT"
    if re.search(r"INSERT\s+INTO\s+\S+\s+VALUES", upper_sql):
        return "INSERT_VALUES"
    return "UNKNOWN"


def _extract_target_table(sql: str, stmt_type: str) -> str | None:
    """Return the fully-qualified target table name, or None."""
    clean = _strip_comments(sql)
    if stmt_type == "CTAS":
        m = _CREATE_TABLE_AS_RE.search(clean)
    elif stmt_type in ("INSERT_SELECT", "INSERT_VALUES"):
        m = _INSERT_INTO_RE.search(clean)
    else:
        return None

    if m:
        return _normalise_table_name(m.group(1))
    return None


def _extract_source_tables(sql: str, target_table: str | None) -> list[str]:
    """Return all tables referenced in FROM / JOIN clauses (excluding the target)."""
    clean = _strip_comments(sql)
    matches = _FROM_JOIN_RE.findall(clean)
    tables = []
    seen: set[str] = set()
    for raw in matches:
        name = _normalise_table_name(raw)
        if name and name != target_table and name not in seen:
            # Skip obvious Oracle pseudo-tables / dual
            if name.lower() == "dual":
                continue
            seen.add(name)
            tables.append(name)
    return tables


def _extract_aggregations(sql: str) -> list[str]:
    clean = _strip_comments(sql)
    found = _AGG_RE.findall(clean)
    return list({f.upper() for f in found})


def _make_task_label(stmt_type: str, target_table: str | None, index: int) -> str:
    if target_table:
        short = target_table.split(".")[-1]  # strip schema prefix
        verb = {
            "CTAS": "create",
            "INSERT_SELECT": "insert",
            "INSERT_VALUES": "insert",
        }.get(stmt_type, "run")
        return f"{verb}_{short}"
    return f"stmt_{index}"


def _parse_single_statement(sql: str, index: int) -> dict[str, Any]:
    clean_sql = _strip_comments(sql)
    upper_sql = clean_sql.upper()

    stmt_type = _detect_statement_type(upper_sql)
    target_table = _extract_target_table(sql, stmt_type)
    source_tables = _extract_source_tables(sql, target_table)
    aggregations = _extract_aggregations(sql)
    has_where = bool(_WHERE_RE.search(sql))
    has_group_by = bool(_GROUP_BY_RE.search(sql))
    label = _make_task_label(stmt_type, target_table, index)

    return {
        "id": f"stmt_{index}",
        "raw_sql": sql,
        "statement_type": stmt_type,
        "target_table": target_table,
        "source_tables": source_tables,
        "has_where": has_where,
        "has_group_by": has_group_by,
        "aggregations": aggregations,
        "label": label,
    }
