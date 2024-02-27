"""
impact_analyzer.py — Column-level impact analysis across dbt models and Airflow DAG tasks.

Given a column name, find all downstream models and DAG tasks that depend on it.

Public API
----------
ImpactAnalyzer(sql)                              — build graph from SQL
    .analyze(column_name)                        → ImpactResult
    .what_if_rename(old_col, new_col)            → list[FileChange]
    .breaking_changes(sql_v1, sql_v2)            → BreakingChangeDiff
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

import sqlparse
import networkx as nx


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class DependencyNode:
    name: str
    node_type: str          # "table" | "cte" | "dbt_model" | "dag_task" | "column"
    sql_snippet: str = ""
    column_count: int = 0


@dataclass
class ImpactResult:
    column_name: str
    blast_radius: int
    affected_models: list[str]
    affected_dag_tasks: list[str]
    dependency_tree: dict[str, list[str]]  # parent → [children]
    critical_path: list[str]


@dataclass
class FileChange:
    file_path: str
    line_hint: str
    change_type: str         # "rename" | "update_ref"
    old_value: str
    new_value: str


@dataclass
class ColumnDiff:
    column_name: str
    change_type: str         # "added" | "removed" | "renamed" | "type_changed"
    old_value: str = ""
    new_value: str = ""


@dataclass
class BreakingChangeDiff:
    added_columns: list[str] = field(default_factory=list)
    removed_columns: list[str] = field(default_factory=list)
    renamed_columns: list[ColumnDiff] = field(default_factory=list)
    type_changes: list[ColumnDiff] = field(default_factory=list)
    is_breaking: bool = False

    @property
    def summary(self) -> str:
        parts = []
        if self.removed_columns:
            parts.append(f"{len(self.removed_columns)} column(s) removed: {self.removed_columns}")
        if self.renamed_columns:
            parts.append(f"{len(self.renamed_columns)} column(s) renamed")
        if self.type_changes:
            parts.append(f"{len(self.type_changes)} type change(s)")
        if self.added_columns:
            parts.append(f"{len(self.added_columns)} column(s) added (non-breaking)")
        return "; ".join(parts) if parts else "No changes detected"


# ---------------------------------------------------------------------------
# Regex helpers
# ---------------------------------------------------------------------------

_STRIP_SINGLE = re.compile(r"--[^\n]*", re.MULTILINE)
_STRIP_BLOCK = re.compile(r"/\*.*?\*/", re.DOTALL)
_CTAS_RE = re.compile(
    r"CREATE\s+(?:OR\s+REPLACE\s+)?TABLE\s+(\S+)\s+AS\s+",
    re.IGNORECASE,
)
_INSERT_RE = re.compile(r"INSERT\s+INTO\s+(\S+)", re.IGNORECASE)
_FROM_JOIN_RE = re.compile(
    r"(?:FROM|JOIN)\s+([\w]+(?:\.[\w]+)+|[\w]+)",
    re.IGNORECASE,
)
_SELECT_COLS_RE = re.compile(r"SELECT\s+(.*?)\s+FROM\b", re.IGNORECASE | re.DOTALL)
_AGG_RE = re.compile(
    r"\b(SUM|COUNT|AVG|MIN|MAX|STDDEV|VARIANCE|MEDIAN|LISTAGG)\s*\(",
    re.IGNORECASE,
)
_CAST_RE = re.compile(r"CAST\s*\((.+?)\s+AS\s+(\w+)\)", re.IGNORECASE)
_WITH_RE = re.compile(r"^\s*WITH\b", re.IGNORECASE)


# ---------------------------------------------------------------------------
# ImpactAnalyzer
# ---------------------------------------------------------------------------

class ImpactAnalyzer:
    """Build a column-level dependency graph from SQL and answer impact queries."""

    def __init__(self, sql: str) -> None:
        self._sql = sql
        self._graph: nx.DiGraph = nx.DiGraph()
        self._col_to_models: dict[str, list[str]] = {}  # col_name → models referencing it
        self._model_columns: dict[str, list[str]] = {}  # model_name → output columns
        self._model_sql: dict[str, str] = {}            # model_name → sql text
        self._model_type: dict[str, str] = {}           # model_name → node type
        self._build_graph()

    # ------------------------------------------------------------------
    # Public methods
    # ------------------------------------------------------------------

    def analyze(self, column_name: str) -> ImpactResult:
        """Return impact of *column_name* across all downstream models."""
        col_lower = column_name.lower()
        affected_models: list[str] = []
        affected_dag_tasks: list[str] = []

        # Find all models that project this column
        for model, cols in self._model_columns.items():
            if col_lower in [c.lower() for c in cols]:
                affected_models.append(model)
                # Downstream models that depend on this model
                for downstream in nx.descendants(self._graph, model):
                    if downstream not in affected_models:
                        downstream_cols = self._model_columns.get(downstream, [])
                        if col_lower in [c.lower() for c in downstream_cols]:
                            affected_models.append(downstream)
                    if self._model_type.get(downstream) == "dag_task":
                        if downstream not in affected_dag_tasks:
                            affected_dag_tasks.append(downstream)

        # Build dependency tree
        dep_tree = self._build_dep_tree(affected_models)

        # Critical path (longest path through affected models)
        critical_path = self._find_critical_path(affected_models)

        return ImpactResult(
            column_name=column_name,
            blast_radius=len(affected_models) + len(affected_dag_tasks),
            affected_models=affected_models,
            affected_dag_tasks=affected_dag_tasks,
            dependency_tree=dep_tree,
            critical_path=critical_path,
        )

    def what_if_rename(self, old_col: str, new_col: str) -> list[FileChange]:
        """Return files that would need updating if *old_col* is renamed to *new_col*."""
        changes: list[FileChange] = []
        col_lower = old_col.lower()

        for model, cols in self._model_columns.items():
            if col_lower in [c.lower() for c in cols]:
                sql = self._model_sql.get(model, "")
                # Find all line snippets that reference the column
                for line in sql.splitlines():
                    pattern = re.compile(r"\b" + re.escape(old_col) + r"\b", re.IGNORECASE)
                    if pattern.search(line):
                        changes.append(FileChange(
                            file_path=f"models/{model}.sql",
                            line_hint=line.strip(),
                            change_type="rename",
                            old_value=old_col,
                            new_value=new_col,
                        ))
                        break  # one entry per model

        return changes

    def breaking_changes(self, sql_v1: str, sql_v2: str) -> BreakingChangeDiff:
        """Diff column sets between two SQL versions and classify breaking changes."""
        cols_v1 = self._extract_all_output_cols(sql_v1)
        cols_v2 = self._extract_all_output_cols(sql_v2)

        v1_set = {c.lower() for c in cols_v1}
        v2_set = {c.lower() for c in cols_v2}

        removed = sorted(v1_set - v2_set)
        added = sorted(v2_set - v1_set)

        # Heuristic rename detection: single removal + single addition → likely rename
        renames: list[ColumnDiff] = []
        if len(removed) == 1 and len(added) == 1:
            renames.append(ColumnDiff(
                column_name=removed[0],
                change_type="renamed",
                old_value=removed[0],
                new_value=added[0],
            ))
            removed = []
            added = []

        # Type change detection
        type_changes: list[ColumnDiff] = []
        v1_types = self._extract_column_types(sql_v1)
        v2_types = self._extract_column_types(sql_v2)
        for col in v1_set & v2_set:
            t1 = v1_types.get(col, "")
            t2 = v2_types.get(col, "")
            if t1 and t2 and t1 != t2:
                type_changes.append(ColumnDiff(
                    column_name=col,
                    change_type="type_changed",
                    old_value=t1,
                    new_value=t2,
                ))

        diff = BreakingChangeDiff(
            added_columns=sorted(v2_set - v1_set) if not renames else [],
            removed_columns=removed,
            renamed_columns=renames,
            type_changes=type_changes,
        )
        diff.is_breaking = bool(removed or renames or type_changes)
        return diff

    # ------------------------------------------------------------------
    # Graph construction
    # ------------------------------------------------------------------

    def _build_graph(self) -> None:
        cleaned = _strip_comments(self._sql)
        stmts = sqlparse.split(cleaned)

        for raw in stmts:
            stripped = raw.strip()
            if not stripped:
                continue
            self._process_statement(stripped)

    def _process_statement(self, sql: str) -> None:
        target_table = _extract_target(sql)
        short_name = target_table.split(".")[-1] if target_table else None

        ctes = _parse_ctes(sql)
        final_select = _extract_final_select(sql)

        # Register CTE nodes
        for cte_name, cte_sql in ctes.items():
            self._register_model(cte_name, cte_sql, "cte")

        # Register final SELECT / target table
        if short_name and final_select:
            schema = target_table.split(".")[0] if "." in target_table else "staging"
            node_type = "dbt_model" if schema in ("mart", "marts") else "cte"
            self._register_model(short_name, final_select, node_type)

        # Add edges: CTE/table → consumer
        all_names = set(ctes.keys()) | ({short_name} if short_name else set())
        for model_name in all_names:
            model_sql = self._model_sql.get(model_name, "")
            for ref in _extract_table_refs(model_sql):
                ref_short = ref.split(".")[-1]
                if ref_short in self._graph:
                    if not self._graph.has_edge(ref_short, model_name):
                        self._graph.add_edge(ref_short, model_name)

    def _register_model(self, name: str, sql: str, node_type: str) -> None:
        cols = _extract_output_columns(sql)
        self._graph.add_node(name, node_type=node_type, sql=sql)
        self._model_columns[name] = cols
        self._model_sql[name] = sql
        self._model_type[name] = node_type

        # Update col → models index
        for col in cols:
            col_lower = col.lower()
            if col_lower not in self._col_to_models:
                self._col_to_models[col_lower] = []
            if name not in self._col_to_models[col_lower]:
                self._col_to_models[col_lower].append(name)

    # ------------------------------------------------------------------
    # Graph analysis helpers
    # ------------------------------------------------------------------

    def _build_dep_tree(self, affected_models: list[str]) -> dict[str, list[str]]:
        tree: dict[str, list[str]] = {}
        for model in affected_models:
            children = [
                s for s in self._graph.successors(model)
                if s in affected_models
            ]
            tree[model] = children
        return tree

    def _find_critical_path(self, affected_models: list[str]) -> list[str]:
        if not affected_models:
            return []
        subgraph = self._graph.subgraph(affected_models)
        try:
            # Longest path in affected subgraph
            return list(nx.dag_longest_path(subgraph))
        except (nx.NetworkXError, nx.NetworkXUnfeasible):
            return affected_models[:1]

    # ------------------------------------------------------------------
    # Column extraction helpers
    # ------------------------------------------------------------------

    def _extract_all_output_cols(self, sql: str) -> list[str]:
        cleaned = _strip_comments(sql)
        cols: list[str] = []
        for stmt in sqlparse.split(cleaned):
            cols.extend(_extract_output_columns(stmt))
        return list(dict.fromkeys(cols))  # deduplicate preserving order

    def _extract_column_types(self, sql: str) -> dict[str, str]:
        """Extract {column_name: cast_type} for all CAST expressions in sql."""
        types: dict[str, str] = {}
        for m in _CAST_RE.finditer(sql):
            inner = m.group(1).strip()
            cast_type = m.group(2).upper()
            # inner may be "col_name" or "schema.col_name"
            col_name = inner.split(".")[-1].strip().lower()
            types[col_name] = cast_type
        return types


# ---------------------------------------------------------------------------
# Standalone helpers
# ---------------------------------------------------------------------------

def _extract_target(sql: str) -> str | None:
    m = _CTAS_RE.search(sql)
    if m:
        return m.group(1).strip().lower()
    m = _INSERT_RE.search(sql)
    if m:
        return m.group(1).strip().lower()
    return None


def _extract_output_columns(sql: str) -> list[str]:
    m = _SELECT_COLS_RE.search(sql)
    if not m:
        return []
    col_clause = m.group(1).strip()
    if col_clause.upper() == "*":
        return ["*"]
    cols: list[str] = []
    for part in _split_commas(col_clause):
        part = part.strip()
        if not part:
            continue
        alias_m = re.search(r"\bAS\s+(\w+)\s*$", part, re.IGNORECASE)
        if alias_m:
            cols.append(alias_m.group(1).lower())
            continue
        tokens = re.split(r"[\s,.()+*/\-]", part)
        tokens = [t for t in tokens if t and re.match(r"^\w+$", t)]
        if tokens:
            cols.append(tokens[-1].lower())
    return cols


def _extract_table_refs(sql: str) -> list[str]:
    refs = []
    seen: set[str] = set()
    for m in _FROM_JOIN_RE.finditer(sql):
        name = m.group(1).strip().lower().rstrip(");,")
        if name and name not in seen and name != "dual":
            seen.add(name)
            refs.append(name)
    return refs


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
            sel = re.search(r"\bSELECT\b", stripped[m_ins.end():], re.IGNORECASE)
            if sel:
                return stripped[m_ins.end() + sel.start():]
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


def _split_commas(text: str) -> list[str]:
    parts: list[str] = []
    depth = 0
    current: list[str] = []
    for ch in text:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        if ch == "," and depth == 0:
            parts.append("".join(current))
            current = []
        else:
            current.append(ch)
    if current:
        parts.append("".join(current))
    return parts


def _strip_comments(sql: str) -> str:
    no_block = _STRIP_BLOCK.sub(" ", sql)
    return _STRIP_SINGLE.sub(" ", no_block)
