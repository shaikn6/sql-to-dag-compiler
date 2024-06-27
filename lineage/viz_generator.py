"""
viz_generator.py — Generate an interactive pyvis lineage graph HTML file.

Node colours
------------
- blue   (#4A90D9) : source tables
- yellow (#F5C518) : CTEs
- green  (#27AE60) : dbt models
- orange (#E67E22) : Airflow DAG tasks

Each node is clickable and shows: SQL snippet, column count, upstream/downstream count.

Public API
----------
generate_lineage_html(sql, output_path, dag_tasks=None) → str  (HTML content)
"""

from __future__ import annotations

import re
import json
import textwrap
from pathlib import Path
from typing import Any

import networkx as nx

# Maximum SQL input size to prevent DoS via unbounded regex on huge inputs
_MAX_SQL_BYTES = 5 * 1024 * 1024

try:
    from pyvis.network import Network
    _PYVIS_AVAILABLE = True
except ImportError:
    _PYVIS_AVAILABLE = False

import sqlparse


# ---------------------------------------------------------------------------
# Colour scheme
# ---------------------------------------------------------------------------

_COLOR = {
    "table":     "#4A90D9",   # blue
    "cte":       "#F5C518",   # yellow
    "dbt_model": "#27AE60",   # green
    "dag_task":  "#E67E22",   # orange
}

_SHAPE = {
    "table":     "database",
    "cte":       "ellipse",
    "dbt_model": "box",
    "dag_task":  "star",
}


# ---------------------------------------------------------------------------
# Regex helpers (minimal, scoped to this module)
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
_WITH_RE = re.compile(r"^\s*WITH\b", re.IGNORECASE)


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

class _GraphNode:
    def __init__(
        self,
        name: str,
        node_type: str,
        sql_snippet: str = "",
        column_count: int = 0,
    ) -> None:
        self.name = name
        self.node_type = node_type
        self.sql_snippet = sql_snippet
        self.column_count = column_count
        self.upstream: list[str] = []
        self.downstream: list[str] = []


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def generate_lineage_html(
    sql: str,
    output_path: str,
    dag_tasks: list[dict[str, Any]] | None = None,
) -> str:
    """
    Build a lineage graph from *sql*, optionally augmenting with Airflow *dag_tasks*.

    Parameters
    ----------
    sql:
        Raw SQL (may contain multiple statements with CTEs).
    output_path:
        Where to write the HTML file.
    dag_tasks:
        Optional list of dicts with keys: ``task_id``, ``source_tables``, ``target_table``.
        These become orange DAG task nodes.

    Returns
    -------
    str
        HTML content of the generated graph.
    """
    if len(sql.encode("utf-8")) > _MAX_SQL_BYTES:
        raise ValueError(
            f"SQL input is too large. Maximum allowed: "
            f"{_MAX_SQL_BYTES // 1_048_576} MB."
        )

    nodes, graph = _build_lineage_graph(sql, dag_tasks or [])

    if _PYVIS_AVAILABLE:
        html = _render_pyvis(nodes, graph, output_path)
    else:
        html = _render_fallback_html(nodes, graph, output_path)

    return html


# ---------------------------------------------------------------------------
# Graph construction
# ---------------------------------------------------------------------------

def _build_lineage_graph(
    sql: str,
    dag_tasks: list[dict[str, Any]],
) -> tuple[dict[str, _GraphNode], nx.DiGraph]:
    cleaned = _strip_comments(sql)
    stmts = sqlparse.split(cleaned)
    nodes: dict[str, _GraphNode] = {}
    graph = nx.DiGraph()

    for raw in stmts:
        stripped = raw.strip()
        if not stripped:
            continue
        _process_statement(stripped, nodes, graph)

    # Add DAG task nodes
    for task in dag_tasks:
        task_id = task.get("task_id", "unknown_task")
        task_node = _GraphNode(
            name=task_id,
            node_type="dag_task",
            sql_snippet=task.get("raw_sql", "")[:200],
            column_count=0,
        )
        nodes[task_id] = task_node
        graph.add_node(task_id, node_type="dag_task")

        for src in task.get("source_tables", []):
            src_short = src.split(".")[-1]
            if src_short in nodes:
                graph.add_edge(src_short, task_id)
                nodes[src_short].downstream.append(task_id)
                task_node.upstream.append(src_short)

    # Populate up/downstream lists from graph
    for node_name in graph.nodes:
        if node_name in nodes:
            nodes[node_name].upstream = list(graph.predecessors(node_name))
            nodes[node_name].downstream = list(graph.successors(node_name))

    # Add any referenced source tables that aren't already nodes
    for node_name in list(nodes.keys()):
        node = nodes[node_name]
        for upstream_name in node.upstream:
            if upstream_name not in nodes:
                source_node = _GraphNode(
                    name=upstream_name,
                    node_type="table",
                    sql_snippet=f"External source table: {upstream_name}",
                    column_count=0,
                )
                nodes[upstream_name] = source_node
                if not graph.has_node(upstream_name):
                    graph.add_node(upstream_name, node_type="table")

    return nodes, graph


def _process_statement(
    sql: str,
    nodes: dict[str, _GraphNode],
    graph: nx.DiGraph,
) -> None:
    target = _extract_target(sql)
    short_name = target.split(".")[-1] if target else None

    ctes = _parse_ctes(sql)
    final_select = _extract_final_select(sql)

    # Register CTE nodes
    cte_names: set[str] = set()
    for cte_name, cte_sql in ctes.items():
        col_count = _count_columns(cte_sql)
        node = _GraphNode(
            name=cte_name,
            node_type="cte",
            sql_snippet=textwrap.shorten(cte_sql.strip(), width=300),
            column_count=col_count,
        )
        nodes[cte_name] = node
        graph.add_node(cte_name, node_type="cte")
        cte_names.add(cte_name)

    # Register final model node
    if short_name and final_select:
        schema = target.split(".")[0] if target and "." in target else "staging"
        node_type = "dbt_model" if schema in ("mart", "marts", "dw") else "cte"
        col_count = _count_columns(final_select)
        node = _GraphNode(
            name=short_name,
            node_type=node_type,
            sql_snippet=textwrap.shorten(final_select.strip(), width=300),
            column_count=col_count,
        )
        nodes[short_name] = node
        graph.add_node(short_name, node_type=node_type)

    # Add edges from table references → their consumers
    all_names = cte_names | ({short_name} if short_name else set())
    for model_name in all_names:
        model_sql = ctes.get(model_name) or (final_select if model_name == short_name else "")
        if not model_sql:
            continue
        for ref in _extract_table_refs(model_sql):
            ref_short = ref.split(".")[-1]
            # Ensure source nodes exist
            if ref_short not in nodes:
                source_type = "table" if "." in ref else "cte"
                source_node = _GraphNode(
                    name=ref_short,
                    node_type=source_type,
                    sql_snippet=f"Referenced as: {ref}",
                    column_count=0,
                )
                nodes[ref_short] = source_node
                graph.add_node(ref_short, node_type=source_type)

            if not graph.has_edge(ref_short, model_name):
                graph.add_edge(ref_short, model_name)


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------

def _render_pyvis(
    nodes: dict[str, _GraphNode],
    graph: nx.DiGraph,
    output_path: str,
) -> str:
    net = Network(
        height="750px",
        width="100%",
        directed=True,
        bgcolor="#1a1a2e",
        font_color="#e0e0e0",
    )
    net.barnes_hut(
        gravity=-8000,
        central_gravity=0.3,
        spring_length=120,
        spring_strength=0.05,
    )

    for node_name, node in nodes.items():
        color = _COLOR.get(node.node_type, "#888888")
        shape = _SHAPE.get(node.node_type, "ellipse")
        # Escape user-derived content before embedding in HTML tooltip to
        # prevent XSS if the generated HTML file is opened in a browser.
        safe_name = _html_escape(node_name)
        safe_type = _html_escape(node.node_type)
        safe_snippet = _html_escape(node.sql_snippet[:200])
        tooltip = (
            f"<b>{safe_name}</b><br>"
            f"Type: {safe_type}<br>"
            f"Columns: {node.column_count}<br>"
            f"Upstream: {len(node.upstream)}<br>"
            f"Downstream: {len(node.downstream)}<br>"
            f"<hr><code>{safe_snippet}</code>"
        )
        net.add_node(
            node_name,
            label=node_name,
            color=color,
            shape=shape,
            title=tooltip,
            size=25 if node.node_type == "dag_task" else 20,
            font={"size": 12, "color": "#ffffff"},
        )

    for u, v in graph.edges:
        net.add_edge(u, v, arrows="to", color="#888888", width=2)

    # Options JSON
    net.set_options("""
    var options = {
      "nodes": {
        "borderWidth": 2,
        "shadow": true
      },
      "edges": {
        "smooth": {"type": "curvedCW", "roundness": 0.2},
        "shadow": true
      },
      "physics": {
        "enabled": true,
        "stabilization": {"iterations": 200}
      },
      "interaction": {
        "hover": true,
        "navigationButtons": true,
        "keyboard": true
      }
    }
    """)

    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    net.write_html(str(path))
    return path.read_text(encoding="utf-8")


def _render_fallback_html(
    nodes: dict[str, _GraphNode],
    graph: nx.DiGraph,
    output_path: str,
) -> str:
    """Render a simple static HTML table when pyvis is not installed."""
    legend_html = "".join(
        f'<span style="background:{color};padding:3px 8px;margin:4px;border-radius:4px;color:#fff">'
        f'{label}</span>'
        for label, color in [
            ("Source Table", _COLOR["table"]),
            ("CTE", _COLOR["cte"]),
            ("dbt Model", _COLOR["dbt_model"]),
            ("DAG Task", _COLOR["dag_task"]),
        ]
    )
    rows = []
    for node_name, node in nodes.items():
        color = _COLOR.get(node.node_type, "#888")
        # Escape all user-derived content before embedding in HTML.
        safe_node_name = _html_escape(node_name)
        safe_node_type = _html_escape(node.node_type)
        upstream_str = _html_escape(", ".join(node.upstream)) or "—"
        downstream_str = _html_escape(", ".join(node.downstream)) or "—"
        rows.append(
            f'<tr style="border-bottom:1px solid #333">'
            f'<td style="padding:8px;color:{color}"><b>{safe_node_name}</b></td>'
            f'<td style="padding:8px">{safe_node_type}</td>'
            f'<td style="padding:8px">{node.column_count}</td>'
            f'<td style="padding:8px">{upstream_str}</td>'
            f'<td style="padding:8px">{downstream_str}</td>'
            f'</tr>'
        )
    table_rows = "\n".join(rows)
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>SQL Lineage Graph</title>
  <style>
    body {{ background:#1a1a2e; color:#e0e0e0; font-family:monospace; padding:20px; }}
    table {{ border-collapse:collapse; width:100%; }}
    th {{ background:#2a2a4e; padding:10px; text-align:left; }}
    .legend {{ margin-bottom:20px; }}
  </style>
</head>
<body>
  <h1>SQL Lineage Graph</h1>
  <p style="color:#aaa">Install <code>pyvis</code> for an interactive graph.
     Showing static table fallback.</p>
  <div class="legend">{legend_html}</div>
  <table>
    <thead>
      <tr>
        <th>Node</th><th>Type</th><th>Columns</th><th>Upstream</th><th>Downstream</th>
      </tr>
    </thead>
    <tbody>
      {table_rows}
    </tbody>
  </table>
</body>
</html>"""
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(html, encoding="utf-8")
    return html


# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------

def _html_escape(text: str) -> str:
    """Escape HTML special characters to prevent XSS in generated HTML output."""
    return (
        text
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&#x27;")
    )


def _extract_target(sql: str) -> str | None:
    m = _CTAS_RE.search(sql)
    if m:
        return m.group(1).strip().lower()
    m = _INSERT_RE.search(sql)
    if m:
        return m.group(1).strip().lower()
    return None


def _count_columns(sql: str) -> int:
    m = _SELECT_COLS_RE.search(sql)
    if not m:
        return 0
    col_clause = m.group(1).strip()
    if col_clause.upper() == "*":
        return 1
    return len(_split_commas(col_clause))


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
