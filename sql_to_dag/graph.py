"""
graph.py — Builds a directed dependency graph from parsed SQL statement metadata.

A directed edge  A → B  means "statement A must complete before statement B can run"
(i.e. A produces a table that B consumes).

Public API
----------
build_dependency_graph(statements)  → networkx.DiGraph
topological_order(graph)            → list[str]  (statement ids in execution order)
"""

from __future__ import annotations

from typing import Any

import networkx as nx


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def build_dependency_graph(statements: list[dict[str, Any]]) -> nx.DiGraph:
    """
    Build a directed acyclic graph from *statements*.

    Each node is a statement id (e.g. ``"stmt_0"``).
    Node attributes store the full statement metadata dict.

    An edge from node X to node Y is added when:
        - statement X writes to a table T  (target_table == T)
        - statement Y reads from table T   (T in source_tables)
    """
    graph = nx.DiGraph()

    # Add all nodes first so isolated statements are included.
    for stmt in statements:
        graph.add_node(stmt["id"], **stmt)

    # Build a lookup: table_name → stmt_id that produced it.
    producers: dict[str, str] = {}
    for stmt in statements:
        target = stmt.get("target_table")
        if target:
            producers[target] = stmt["id"]

    # Add edges: producer → consumer.
    for stmt in statements:
        for src_table in stmt.get("source_tables", []):
            if src_table in producers:
                producer_id = producers[src_table]
                consumer_id = stmt["id"]
                if producer_id != consumer_id:
                    graph.add_edge(producer_id, consumer_id)

    _validate_dag(graph)
    return graph


def topological_order(graph: nx.DiGraph) -> list[str]:
    """
    Return statement ids in a valid topological execution order.

    Raises ``ValueError`` if the graph contains a cycle (which would mean
    circular table dependencies — not valid SQL).
    """
    try:
        return list(nx.topological_sort(graph))
    except nx.NetworkXUnfeasible as exc:
        cycle = nx.find_cycle(graph)
        raise ValueError(
            f"Circular dependency detected in SQL statements: {cycle}"
        ) from exc


def get_dependencies(graph: nx.DiGraph, stmt_id: str) -> list[str]:
    """Return the list of statement ids that *stmt_id* depends on (its predecessors)."""
    return list(graph.predecessors(stmt_id))


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _validate_dag(graph: nx.DiGraph) -> None:
    """Raise ValueError if *graph* is not a DAG."""
    if not nx.is_directed_acyclic_graph(graph):
        cycles = list(nx.simple_cycles(graph))
        raise ValueError(
            f"SQL statements contain circular table dependencies: {cycles}"
        )
