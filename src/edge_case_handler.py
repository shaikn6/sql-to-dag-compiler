"""
edge_case_handler.py — Detect and preprocess complex SQL patterns that V1 may miss.

Patterns handled:
    - CTEs (multi-CTE WITH clauses)
    - Nested subqueries (3+ levels deep)
    - MERGE statements (Oracle / SQL Server)
    - Dynamic SQL (EXECUTE IMMEDIATE, sp_executesql)
    - Stored procedures with OUT parameters
    - Recursive CTEs (WITH RECURSIVE or Oracle-style CONNECT BY)
    - Window functions (OVER PARTITION BY / OVER ORDER BY)
    - PIVOT / UNPIVOT
    - Lateral joins (LATERAL, CROSS APPLY, OUTER APPLY)

Public API
----------
    EdgeCaseHandler.detect_patterns(sql)     → list[SQLPattern]
    EdgeCaseHandler.preprocess(sql)          → tuple[str, list[Warning]]
    EdgeCaseHandler.extract_cte_dependencies(sql) → list[str]
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------

class PatternType(str, Enum):
    CTE = "CTE"
    RECURSIVE_CTE = "RECURSIVE_CTE"
    NESTED_SUBQUERY = "NESTED_SUBQUERY"
    MERGE = "MERGE"
    DYNAMIC_SQL = "DYNAMIC_SQL"
    OUT_PARAMETER = "OUT_PARAMETER"
    WINDOW_FUNCTION = "WINDOW_FUNCTION"
    PIVOT = "PIVOT"
    UNPIVOT = "UNPIVOT"
    LATERAL_JOIN = "LATERAL_JOIN"


@dataclass
class SQLPattern:
    """A detected complex SQL pattern."""
    pattern_type: PatternType
    location: int          # approximate character offset in the SQL string
    complexity_score: int  # 1 = low, 2 = medium, 3 = high


@dataclass
class Warning:
    """A preprocessing warning for a detected pattern."""
    code: str        # e.g. "W001"
    message: str
    line: int        # 1-based line number (0 if unknown)


# ---------------------------------------------------------------------------
# Regex catalogue
# ---------------------------------------------------------------------------

# WITH … AS (  — detects the presence of any CTE block
_CTE_RE = re.compile(
    r"\bWITH\s+\w+\s+AS\s*\(",
    re.IGNORECASE | re.DOTALL,
)

# WITH RECURSIVE … (standard SQL / PostgreSQL)
_RECURSIVE_CTE_RE = re.compile(
    r"\bWITH\s+RECURSIVE\b",
    re.IGNORECASE,
)

# Oracle hierarchical: CONNECT BY (used as a proxy for recursive traversal)
_CONNECT_BY_RE = re.compile(
    r"\bCONNECT\s+BY\b",
    re.IGNORECASE,
)

# MERGE INTO … USING …
_MERGE_RE = re.compile(
    r"\bMERGE\s+INTO\b",
    re.IGNORECASE,
)

# EXECUTE IMMEDIATE (Oracle dynamic SQL)
_EXEC_IMMEDIATE_RE = re.compile(
    r"\bEXECUTE\s+IMMEDIATE\b",
    re.IGNORECASE,
)

# sp_executesql (SQL Server dynamic SQL)
_SP_EXECUTESQL_RE = re.compile(
    r"\bsp_executesql\b",
    re.IGNORECASE,
)

# EXEC ( or EXECUTE ( with a string arg — simplified heuristic
_EXEC_STRING_RE = re.compile(
    r"\bEXEC(?:UTE)?\s*\(\s*['\"]",
    re.IGNORECASE,
)

# OUT or OUTPUT parameter keyword in procedure signatures
_OUT_PARAM_RE = re.compile(
    r"\bOUT(?:PUT)?\b\s+\w",
    re.IGNORECASE,
)

# Window function: OVER (  or OVER(
_WINDOW_FUNC_RE = re.compile(
    r"\bOVER\s*\(",
    re.IGNORECASE,
)

# PIVOT (  or UNPIVOT (
_PIVOT_RE = re.compile(
    r"\bPIVOT\s*\(",
    re.IGNORECASE,
)
_UNPIVOT_RE = re.compile(
    r"\bUNPIVOT\s*\(",
    re.IGNORECASE,
)

# LATERAL join (standard SQL)
_LATERAL_RE = re.compile(
    r"\bLATERAL\s*\(",
    re.IGNORECASE,
)

# CROSS APPLY / OUTER APPLY (SQL Server)
_APPLY_RE = re.compile(
    r"\b(?:CROSS|OUTER)\s+APPLY\b",
    re.IGNORECASE,
)

# CTE name extractor: captures the alias in  <name> AS (
_CTE_NAME_RE = re.compile(
    r",?\s*(\w+)\s+AS\s*\(",
    re.IGNORECASE,
)

# Opening WITH keyword (to anchor the multi-CTE scan)
_WITH_KEYWORD_RE = re.compile(
    r"\bWITH\b",
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

class EdgeCaseHandler:
    """Detect and preprocess complex SQL edge cases."""

    # ------------------------------------------------------------------
    # Pattern detection
    # ------------------------------------------------------------------

    def detect_patterns(self, sql: str) -> list[SQLPattern]:
        """
        Scan *sql* and return a list of detected :class:`SQLPattern` objects.

        Each pattern carries:
        - ``pattern_type`` — the kind of edge case
        - ``location``     — approximate character offset in the string
        - ``complexity_score`` — 1 (low), 2 (medium), or 3 (high)
        """
        patterns: list[SQLPattern] = []

        # Recursive CTE (check before plain CTE — it's more specific)
        for m in _RECURSIVE_CTE_RE.finditer(sql):
            patterns.append(SQLPattern(PatternType.RECURSIVE_CTE, m.start(), 3))

        # Oracle CONNECT BY (recursive hierarchical)
        for m in _CONNECT_BY_RE.finditer(sql):
            patterns.append(SQLPattern(PatternType.RECURSIVE_CTE, m.start(), 3))

        # Plain CTEs (only if no recursive CTE already matched at this position)
        recursive_positions = {p.location for p in patterns if p.pattern_type == PatternType.RECURSIVE_CTE}
        for m in _CTE_RE.finditer(sql):
            # Skip if WITH RECURSIVE is overlapping this position
            if not any(abs(m.start() - rp) < 20 for rp in recursive_positions):
                patterns.append(SQLPattern(PatternType.CTE, m.start(), 1))

        # Nested subqueries — count nesting depth
        max_depth = self._max_subquery_depth(sql)
        if max_depth >= 3:
            patterns.append(SQLPattern(PatternType.NESTED_SUBQUERY, 0, min(max_depth, 3)))

        # MERGE
        for m in _MERGE_RE.finditer(sql):
            patterns.append(SQLPattern(PatternType.MERGE, m.start(), 2))

        # Dynamic SQL
        for pattern_re in (_EXEC_IMMEDIATE_RE, _SP_EXECUTESQL_RE, _EXEC_STRING_RE):
            for m in pattern_re.finditer(sql):
                patterns.append(SQLPattern(PatternType.DYNAMIC_SQL, m.start(), 3))

        # OUT parameters
        for m in _OUT_PARAM_RE.finditer(sql):
            patterns.append(SQLPattern(PatternType.OUT_PARAMETER, m.start(), 1))

        # Window functions
        for m in _WINDOW_FUNC_RE.finditer(sql):
            patterns.append(SQLPattern(PatternType.WINDOW_FUNCTION, m.start(), 1))

        # PIVOT / UNPIVOT
        for m in _PIVOT_RE.finditer(sql):
            patterns.append(SQLPattern(PatternType.PIVOT, m.start(), 2))
        for m in _UNPIVOT_RE.finditer(sql):
            patterns.append(SQLPattern(PatternType.UNPIVOT, m.start(), 2))

        # Lateral joins
        for pattern_re in (_LATERAL_RE, _APPLY_RE):
            for m in pattern_re.finditer(sql):
                patterns.append(SQLPattern(PatternType.LATERAL_JOIN, m.start(), 2))

        # Sort by location for deterministic ordering
        patterns.sort(key=lambda p: (p.location, p.pattern_type.value))
        return patterns

    # ------------------------------------------------------------------
    # Preprocessing
    # ------------------------------------------------------------------

    def preprocess(self, sql: str) -> tuple[str, list[Warning]]:
        """
        Normalize *sql* before main parsing.

        Steps performed:
        1. Strip single-line (``--``) and block (``/* */``) comments.
        2. Collapse multiple whitespace to a single space.
        3. Strip Jinja2 ``{{ ... }}`` and ``{% ... %}`` expressions
           (safe pass-through for dbt-adjacent SQL).
        4. Emit warnings for patterns that require manual review.

        Returns
        -------
        tuple[str, list[Warning]]
            ``(cleaned_sql, warnings)``
        """
        warnings: list[Warning] = []
        patterns = self.detect_patterns(sql)

        # Emit warnings for each detected pattern
        _warning_meta: dict[PatternType, tuple[str, str]] = {
            PatternType.RECURSIVE_CTE: (
                "W001",
                "Recursive CTE detected — ensure the anchor/recursive parts are correctly identified.",
            ),
            PatternType.CTE: (
                "W002",
                "CTE block detected — extract_cte_dependencies() for full dependency order.",
            ),
            PatternType.NESTED_SUBQUERY: (
                "W003",
                "Deeply nested subquery (3+ levels) detected — lineage may be incomplete.",
            ),
            PatternType.MERGE: (
                "W004",
                "MERGE statement detected — both source and target tables are referenced; verify lineage.",
            ),
            PatternType.DYNAMIC_SQL: (
                "W005",
                "Dynamic SQL detected — static analysis cannot resolve runtime-constructed queries.",
            ),
            PatternType.OUT_PARAMETER: (
                "W006",
                "OUT/OUTPUT parameter detected — result set may flow through an output variable.",
            ),
            PatternType.WINDOW_FUNCTION: (
                "W007",
                "Window function (OVER) detected — no special handling required; noting for lineage.",
            ),
            PatternType.PIVOT: (
                "W008",
                "PIVOT detected — column names may be dynamic; verify lineage manually.",
            ),
            PatternType.UNPIVOT: (
                "W009",
                "UNPIVOT detected — column names may be dynamic; verify lineage manually.",
            ),
            PatternType.LATERAL_JOIN: (
                "W010",
                "LATERAL / APPLY join detected — correlated subquery; inner tables included in lineage.",
            ),
        }

        emitted_codes: set[str] = set()
        for pattern in patterns:
            code, msg = _warning_meta.get(pattern.pattern_type, ("W999", "Unknown pattern detected."))
            if code not in emitted_codes:
                line = self._char_offset_to_line(sql, pattern.location)
                warnings.append(Warning(code=code, message=msg, line=line))
                emitted_codes.add(code)

        # --- Clean the SQL ---
        cleaned = sql

        # 1. Strip block comments /* ... */
        cleaned = re.sub(r"/\*.*?\*/", " ", cleaned, flags=re.DOTALL)

        # 2. Strip single-line comments -- ...
        cleaned = re.sub(r"--[^\n]*", " ", cleaned)

        # 3. Strip Jinja2 expressions (dbt templating) to avoid parse confusion
        cleaned = re.sub(r"\{\{.*?\}\}", " __JINJA_EXPR__ ", cleaned, flags=re.DOTALL)
        cleaned = re.sub(r"\{%-?.*?-?%\}", " ", cleaned, flags=re.DOTALL)

        # 4. Collapse whitespace (preserve newlines for line-count accuracy)
        cleaned = re.sub(r"[ \t]+", " ", cleaned)
        cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
        cleaned = cleaned.strip()

        return cleaned, warnings

    # ------------------------------------------------------------------
    # CTE dependency extraction
    # ------------------------------------------------------------------

    def extract_cte_dependencies(self, sql: str) -> list[str]:
        """
        Parse a multi-CTE ``WITH`` clause and return CTE names in dependency order.

        The function scans the SQL for all ``<name> AS (`` occurrences that
        follow a ``WITH`` keyword and returns the names in the order they appear
        (which is the valid definition order for standard SQL — each CTE may only
        reference CTEs defined earlier in the same ``WITH`` block).

        Parameters
        ----------
        sql:
            A SQL string that may contain one or more CTE blocks.

        Returns
        -------
        list[str]
            CTE alias names in the order they are defined, which is a valid
            topological dependency order.
        """
        # Find the WITH keyword
        with_match = _WITH_KEYWORD_RE.search(sql)
        if not with_match:
            return []

        # Scan from the WITH keyword onward for CTE definitions.
        # We look for patterns: [,] <name> AS (
        # and stop as soon as we hit the main query keyword (SELECT/INSERT/MERGE/UPDATE)
        # that is outside any parenthesis.
        after_with = sql[with_match.start():]

        cte_names: list[str] = []
        # Track balance of parentheses to detect when we leave the WITH clause
        depth = 0
        pos = 0
        tokens = re.split(r"(\(|\)|,|\bAS\b|\bSELECT\b|\bINSERT\b|\bMERGE\b|\bUPDATE\b|\bDELETE\b|\w+)",
                          after_with, flags=re.IGNORECASE)

        # Simpler approach: extract all <name> AS ( occurrences in order
        for m in _CTE_NAME_RE.finditer(after_with):
            name = m.group(1)
            # Exclude SQL keywords that could match
            if name.upper() not in {
                "WITH", "SELECT", "FROM", "WHERE", "JOIN", "ON", "AND", "OR",
                "AS", "RECURSIVE", "TABLE", "INTO", "SET", "GROUP", "ORDER",
                "HAVING", "UNION", "EXCEPT", "INTERSECT", "LIMIT", "OFFSET",
            }:
                cte_names.append(name)

        return cte_names

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _max_subquery_depth(sql: str) -> int:
        """
        Count the maximum nesting depth of subqueries by tracking parentheses
        that are followed by a SELECT keyword inside.

        Returns the maximum nesting level observed.
        """
        # Strip string literals to avoid counting parens inside quoted values
        cleaned = re.sub(r"'[^']*'", "''", sql)
        depth = 0
        max_depth = 0
        i = 0
        while i < len(cleaned):
            ch = cleaned[i]
            if ch == "(":
                depth += 1
                # Check if a SELECT follows within the next 50 chars
                lookahead = cleaned[i + 1: i + 60].lstrip()
                if re.match(r"\bSELECT\b", lookahead, re.IGNORECASE):
                    max_depth = max(max_depth, depth)
            elif ch == ")":
                depth = max(0, depth - 1)
            i += 1
        return max_depth

    @staticmethod
    def _char_offset_to_line(sql: str, offset: int) -> int:
        """Convert a character offset to a 1-based line number."""
        if offset <= 0:
            return 1
        return sql[:offset].count("\n") + 1
