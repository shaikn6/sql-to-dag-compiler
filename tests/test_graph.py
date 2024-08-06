"""Tests for sql_to_dag.graph — dependency graph construction and topological sort."""
from __future__ import annotations

import pytest
import networkx as nx

from sql_to_dag.graph import (
    build_dependency_graph,
    topological_order,
    get_dependencies,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_stmt(
    stmt_id: str,
    target: str | None,
    sources: list[str],
    label: str | None = None,
) -> dict:
    return {
        "id": stmt_id,
        "label": label or target or stmt_id,
        "target_table": target,
        "source_tables": sources,
        "statement_type": "CTAS",
        "raw_sql": f"CREATE TABLE {target} AS SELECT 1",
        "has_where": False,
        "has_group_by": False,
        "aggregations": [],
    }


CHAIN_STMTS = [
    make_stmt("stmt_0", "staging.customer_txn", ["raw.transactions"]),
    make_stmt("stmt_1", "mart.customer_summary", ["staging.customer_txn", "raw.customers"]),
    make_stmt("stmt_2", "mart.high_value_customers", ["mart.customer_summary"]),
]

INDEPENDENT_STMTS = [
    make_stmt("stmt_0", "staging.table_a", ["raw.source1"]),
    make_stmt("stmt_1", "staging.table_b", ["raw.source2"]),
    make_stmt("stmt_2", "staging.table_c", ["raw.source3"]),
]


# ---------------------------------------------------------------------------
# build_dependency_graph tests
# ---------------------------------------------------------------------------

class TestBuildDependencyGraph:

    def test_returns_digraph(self):
        g = build_dependency_graph(CHAIN_STMTS)
        assert isinstance(g, nx.DiGraph)

    def test_node_count_matches_statements(self):
        g = build_dependency_graph(CHAIN_STMTS)
        assert g.number_of_nodes() == 3

    def test_edges_reflect_chain_dependencies(self):
        g = build_dependency_graph(CHAIN_STMTS)
        # stmt_0 → stmt_1 → stmt_2
        assert g.has_edge("stmt_0", "stmt_1")
        assert g.has_edge("stmt_1", "stmt_2")

    def test_no_spurious_edges(self):
        g = build_dependency_graph(CHAIN_STMTS)
        # stmt_0 should NOT directly connect to stmt_2
        assert not g.has_edge("stmt_0", "stmt_2")

    def test_independent_statements_have_no_edges(self):
        g = build_dependency_graph(INDEPENDENT_STMTS)
        assert g.number_of_edges() == 0

    def test_independent_statements_still_have_all_nodes(self):
        g = build_dependency_graph(INDEPENDENT_STMTS)
        assert g.number_of_nodes() == 3

    def test_node_attributes_stored(self):
        g = build_dependency_graph(CHAIN_STMTS)
        node_data = g.nodes["stmt_0"]
        assert node_data["target_table"] == "staging.customer_txn"

    def test_empty_list_returns_empty_graph(self):
        g = build_dependency_graph([])
        assert g.number_of_nodes() == 0
        assert g.number_of_edges() == 0

    def test_single_statement_no_edges(self):
        stmts = [make_stmt("stmt_0", "staging.result", ["raw.source"])]
        g = build_dependency_graph(stmts)
        assert g.number_of_nodes() == 1
        assert g.number_of_edges() == 0

    def test_graph_is_a_dag(self):
        g = build_dependency_graph(CHAIN_STMTS)
        assert nx.is_directed_acyclic_graph(g)

    def test_raises_on_circular_dependency(self):
        # A reads from B, B reads from A — cycle
        circular = [
            make_stmt("stmt_0", "table_a", ["table_b"]),
            make_stmt("stmt_1", "table_b", ["table_a"]),
        ]
        with pytest.raises(ValueError, match="circular"):
            build_dependency_graph(circular)

    def test_statement_without_target_is_not_a_producer(self):
        stmts = [
            {"id": "stmt_0", "label": "run_something", "target_table": None,
             "source_tables": ["raw.data"], "statement_type": "UNKNOWN",
             "raw_sql": "SELECT 1", "has_where": False, "has_group_by": False,
             "aggregations": []},
            make_stmt("stmt_1", "staging.result", ["raw.data"]),
        ]
        g = build_dependency_graph(stmts)
        # No edge because stmt_0 has no target_table
        assert g.number_of_edges() == 0


# ---------------------------------------------------------------------------
# topological_order tests
# ---------------------------------------------------------------------------

class TestTopologicalOrder:

    def test_chain_order_is_correct(self):
        g = build_dependency_graph(CHAIN_STMTS)
        order = topological_order(g)
        # stmt_0 must appear before stmt_1, stmt_1 before stmt_2
        assert order.index("stmt_0") < order.index("stmt_1")
        assert order.index("stmt_1") < order.index("stmt_2")

    def test_all_nodes_in_order(self):
        g = build_dependency_graph(CHAIN_STMTS)
        order = topological_order(g)
        assert set(order) == {"stmt_0", "stmt_1", "stmt_2"}

    def test_independent_order_contains_all(self):
        g = build_dependency_graph(INDEPENDENT_STMTS)
        order = topological_order(g)
        assert set(order) == {"stmt_0", "stmt_1", "stmt_2"}

    def test_empty_graph_returns_empty_list(self):
        g = nx.DiGraph()
        assert topological_order(g) == []


# ---------------------------------------------------------------------------
# get_dependencies tests
# ---------------------------------------------------------------------------

class TestGetDependencies:

    def test_stmt_with_upstream_dep(self):
        g = build_dependency_graph(CHAIN_STMTS)
        deps = get_dependencies(g, "stmt_1")
        assert "stmt_0" in deps

    def test_stmt_with_no_deps(self):
        g = build_dependency_graph(CHAIN_STMTS)
        deps = get_dependencies(g, "stmt_0")
        assert deps == []

    def test_stmt_with_multiple_upstream(self):
        stmts = [
            make_stmt("stmt_0", "staging.a", ["raw.x"]),
            make_stmt("stmt_1", "staging.b", ["raw.y"]),
            make_stmt("stmt_2", "mart.result", ["staging.a", "staging.b"]),
        ]
        g = build_dependency_graph(stmts)
        deps = get_dependencies(g, "stmt_2")
        assert set(deps) == {"stmt_0", "stmt_1"}
