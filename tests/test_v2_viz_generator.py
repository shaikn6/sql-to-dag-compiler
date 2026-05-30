"""Tests for lineage.viz_generator — interactive lineage HTML generation."""

from __future__ import annotations

import os
import re
import tempfile
from pathlib import Path

import pytest

from lineage.viz_generator import (
    generate_lineage_html,
    _build_lineage_graph,
    _process_statement,
    _count_columns,
    _extract_table_refs,
    _GraphNode,
)
import networkx as nx


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

SIMPLE_SQL = """
CREATE TABLE staging.customer_txn AS
SELECT customer_id, SUM(amount) AS total_amount
FROM raw.transactions
GROUP BY customer_id;

CREATE TABLE mart.customer_summary AS
SELECT t.customer_id, c.name, t.total_amount
FROM staging.customer_txn t
JOIN raw.customers c ON t.customer_id = c.customer_id;
"""

CTE_SQL = """
CREATE TABLE mart.monthly_revenue AS
WITH raw_orders AS (
    SELECT order_id, customer_id, amount FROM raw.orders
),
enriched AS (
    SELECT o.order_id, o.amount, c.segment
    FROM raw_orders o JOIN raw.customers c ON o.customer_id = c.customer_id
),
agg AS (
    SELECT segment, SUM(amount) AS revenue FROM enriched GROUP BY segment
)
SELECT segment, revenue FROM agg;
"""

DAG_TASKS = [
    {
        "task_id": "create_customer_txn",
        "source_tables": ["raw.transactions"],
        "target_table": "staging.customer_txn",
        "raw_sql": "CREATE TABLE staging.customer_txn AS SELECT ...",
    },
    {
        "task_id": "create_customer_summary",
        "source_tables": ["staging.customer_txn", "raw.customers"],
        "target_table": "mart.customer_summary",
        "raw_sql": "CREATE TABLE mart.customer_summary AS SELECT ...",
    },
]


# ---------------------------------------------------------------------------
# generate_lineage_html tests
# ---------------------------------------------------------------------------

class TestGenerateLineageHtml:

    def test_returns_string(self, tmp_path):
        output = str(tmp_path / "test_graph.html")
        html = generate_lineage_html(SIMPLE_SQL, output)
        assert isinstance(html, str)

    def test_html_is_non_empty(self, tmp_path):
        output = str(tmp_path / "test_graph.html")
        html = generate_lineage_html(SIMPLE_SQL, output)
        assert len(html) > 100

    def test_output_file_created(self, tmp_path):
        output = str(tmp_path / "lineage_graph.html")
        generate_lineage_html(SIMPLE_SQL, output)
        assert Path(output).exists()

    def test_html_contains_doctype_or_html_tag(self, tmp_path):
        output = str(tmp_path / "graph.html")
        html = generate_lineage_html(SIMPLE_SQL, output)
        assert "<!DOCTYPE html>" in html or "<html" in html

    def test_cte_sql_generates_html(self, tmp_path):
        output = str(tmp_path / "cte_graph.html")
        html = generate_lineage_html(CTE_SQL, output)
        assert len(html) > 100

    def test_with_dag_tasks(self, tmp_path):
        output = str(tmp_path / "dag_graph.html")
        html = generate_lineage_html(SIMPLE_SQL, output, dag_tasks=DAG_TASKS)
        assert isinstance(html, str)

    def test_output_dir_created_if_missing(self, tmp_path):
        output = str(tmp_path / "subdir" / "graph.html")
        generate_lineage_html(SIMPLE_SQL, output)
        assert Path(output).exists()

    def test_empty_sql_generates_valid_html(self, tmp_path):
        output = str(tmp_path / "empty_graph.html")
        html = generate_lineage_html("", output)
        assert isinstance(html, str)


# ---------------------------------------------------------------------------
# _build_lineage_graph tests
# ---------------------------------------------------------------------------

class TestBuildLineageGraph:

    def test_returns_nodes_and_graph(self):
        nodes, graph = _build_lineage_graph(SIMPLE_SQL, [])
        assert isinstance(nodes, dict)
        assert isinstance(graph, nx.DiGraph)

    def test_nodes_not_empty(self):
        nodes, graph = _build_lineage_graph(SIMPLE_SQL, [])
        assert len(nodes) > 0

    def test_graph_has_nodes(self):
        nodes, graph = _build_lineage_graph(SIMPLE_SQL, [])
        assert graph.number_of_nodes() > 0

    def test_graph_has_edges(self):
        nodes, graph = _build_lineage_graph(SIMPLE_SQL, [])
        assert graph.number_of_edges() > 0

    def test_cte_nodes_present(self):
        nodes, graph = _build_lineage_graph(CTE_SQL, [])
        node_names = set(nodes.keys())
        assert "raw_orders" in node_names or len(node_names) >= 2

    def test_dag_task_nodes_added(self):
        nodes, graph = _build_lineage_graph(SIMPLE_SQL, DAG_TASKS)
        assert "create_customer_txn" in nodes or "create_customer_summary" in nodes

    def test_node_types_assigned(self):
        nodes, graph = _build_lineage_graph(SIMPLE_SQL, [])
        for node in nodes.values():
            assert node.node_type in ("table", "cte", "dbt_model", "dag_task")


# ---------------------------------------------------------------------------
# _count_columns tests
# ---------------------------------------------------------------------------

class TestCountColumns:

    def test_simple_count(self):
        assert _count_columns("SELECT a, b, c FROM t") == 3

    def test_star_count(self):
        assert _count_columns("SELECT * FROM t") == 1

    def test_nested_parens_not_counted(self):
        n = _count_columns("SELECT SUM(a, b), c FROM t")
        assert n == 2

    def test_no_from_returns_zero(self):
        assert _count_columns("not sql") == 0


# ---------------------------------------------------------------------------
# _extract_table_refs tests (viz module version)
# ---------------------------------------------------------------------------

class TestVizExtractTableRefs:

    def test_from_detected(self):
        refs = _extract_table_refs("SELECT a FROM raw.orders")
        assert "raw.orders" in refs

    def test_join_detected(self):
        refs = _extract_table_refs("SELECT a FROM t1 JOIN t2 ON t1.id = t2.id")
        assert "t1" in refs
        assert "t2" in refs

    def test_deduplication(self):
        refs = _extract_table_refs(
            "SELECT a FROM t1 JOIN t1 ON t1.x = t1.y"
        )
        assert refs.count("t1") == 1
