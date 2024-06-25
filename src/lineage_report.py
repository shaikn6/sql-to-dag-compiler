"""
lineage_report.py — Generate visual lineage reports from a networkx DAG.

Supports three export formats:
    - Mermaid flowchart (text) — renderable in GitHub Markdown, Notion, etc.
    - DOT (Graphviz)           — renderable with ``dot -Tpng``
    - JSON adjacency list      — machine-readable nodes + edges

Public API
----------
    generate_mermaid(dag)  → str
    generate_dot(dag)      → str
    generate_json(dag)     → dict
    LineageReportGenerator — class wrapping all three methods
"""

from __future__ import annotations

import json
import re
from typing import Any

import networkx as nx


# ---------------------------------------------------------------------------
# Public functions
# ---------------------------------------------------------------------------

def generate_mermaid(dag: nx.DiGraph) -> str:
    """
    Convert *dag* to a Mermaid ``flowchart LR`` diagram string.

    Each node becomes a Mermaid node with a safe identifier derived from the
    node's label (or the node key itself if no ``label`` attribute is present).
    Each directed edge becomes a Mermaid arrow ``A --> B``.

    Parameters
    ----------
    dag:
        A ``networkx.DiGraph``.  Node attributes are optional; if a ``label``
        attribute is present it is shown inside the node box.

    Returns
    -------
    str
        Valid Mermaid flowchart syntax, starting with ``flowchart LR``.
    """
    lines: list[str] = ["flowchart LR"]

    # Build safe Mermaid node IDs (alphanumeric + underscores only)
    node_id_map: dict[Any, str] = {}
    for node in dag.nodes():
        safe_id = re.sub(r"[^a-zA-Z0-9_]", "_", str(node))
        node_id_map[node] = safe_id

    # Node declarations with optional label
    for node in dag.nodes():
        mid = node_id_map[node]
        attrs = dag.nodes[node]
        label = attrs.get("label") or attrs.get("task_id") or str(node)
        # Escape double quotes inside the label
        label_escaped = str(label).replace('"', "'")
        lines.append(f'    {mid}["{label_escaped}"]')

    # Edge declarations
    for src, dst in dag.edges():
        lines.append(f"    {node_id_map[src]} --> {node_id_map[dst]}")

    return "\n".join(lines)


def generate_dot(dag: nx.DiGraph) -> str:
    """
    Convert *dag* to a Graphviz DOT format string.

    Parameters
    ----------
    dag:
        A ``networkx.DiGraph``.

    Returns
    -------
    str
        Valid DOT syntax starting with ``digraph {``.
    """
    lines: list[str] = [
        'digraph {',
        '    rankdir=LR;',
        '    node [shape=box, fontname="Helvetica", fontsize=11];',
        '    edge [fontname="Helvetica", fontsize=9];',
    ]

    # Node declarations
    for node in dag.nodes():
        attrs = dag.nodes[node]
        label = attrs.get("label") or attrs.get("task_id") or str(node)
        label_escaped = str(label).replace('"', '\\"')
        node_key = str(node).replace('"', '\\"')
        lines.append(f'    "{node_key}" [label="{label_escaped}"];')

    # Edge declarations
    for src, dst in dag.edges():
        src_key = str(src).replace('"', '\\"')
        dst_key = str(dst).replace('"', '\\"')
        lines.append(f'    "{src_key}" -> "{dst_key}";')

    lines.append("}")
    return "\n".join(lines)


def generate_json(dag: nx.DiGraph) -> dict[str, Any]:
    """
    Convert *dag* to a JSON-serialisable adjacency-list dict.

    Schema::

        {
            "nodes": [
                {"id": "<node_key>", "label": "<label>", ...attrs},
                ...
            ],
            "edges": [
                {"source": "<node_key>", "target": "<node_key>"},
                ...
            ]
        }

    Parameters
    ----------
    dag:
        A ``networkx.DiGraph``.

    Returns
    -------
    dict
        JSON-serialisable dict with ``"nodes"`` and ``"edges"`` keys.
    """
    nodes: list[dict[str, Any]] = []
    for node in dag.nodes():
        attrs = dict(dag.nodes[node])
        label = attrs.pop("label", None) or attrs.pop("task_id", None) or str(node)
        node_entry: dict[str, Any] = {"id": str(node), "label": str(label)}
        # Include remaining scalar attributes (skip non-serialisable values)
        for k, v in attrs.items():
            try:
                json.dumps(v)
                node_entry[k] = v
            except (TypeError, ValueError):
                node_entry[k] = str(v)
        nodes.append(node_entry)

    edges: list[dict[str, str]] = [
        {"source": str(src), "target": str(dst)}
        for src, dst in dag.edges()
    ]

    return {"nodes": nodes, "edges": edges}


# ---------------------------------------------------------------------------
# Class wrapper (convenience)
# ---------------------------------------------------------------------------

class LineageReportGenerator:
    """
    Convenience class wrapping all three lineage export formats.

    Usage::

        import networkx as nx
        from src.lineage_report import LineageReportGenerator

        dag = nx.DiGraph()
        dag.add_nodes_from(["orders", "order_items", "revenue"])
        dag.add_edges_from([("orders", "revenue"), ("order_items", "revenue")])

        gen = LineageReportGenerator(dag)
        print(gen.mermaid())
        print(gen.dot())
        print(gen.json())
    """

    def __init__(self, dag: nx.DiGraph) -> None:
        self._dag = dag

    def mermaid(self) -> str:
        """Return a Mermaid flowchart string."""
        return generate_mermaid(self._dag)

    def dot(self) -> str:
        """Return a Graphviz DOT string."""
        return generate_dot(self._dag)

    def json(self) -> dict[str, Any]:
        """Return a JSON-serialisable adjacency dict."""
        return generate_json(self._dag)

    def json_string(self, indent: int = 2) -> str:
        """Return the JSON adjacency dict serialised as a pretty string."""
        return json.dumps(self.json(), indent=indent)
