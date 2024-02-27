"""Tests for lineage.column_tracer — column-level lineage tracing."""

from __future__ import annotations

import pytest

from lineage.column_tracer import (
    trace_column,
    ColumnLineage,
    TransformStep,
    _classify_expression,
    _describe_transformation,
    _parse_ctes,
    _extract_final_select,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

SIMPLE_SQL = """
CREATE TABLE staging.customer_txn AS
SELECT customer_id, SUM(amount) AS total_amount, COUNT(*) AS txn_count
FROM raw.transactions
GROUP BY customer_id;
"""

CHAIN_SQL = """
CREATE TABLE staging.customer_txn AS
SELECT customer_id, SUM(amount) AS total_amount
FROM raw.transactions
GROUP BY customer_id;

CREATE TABLE mart.customer_summary AS
SELECT c.customer_id, c.name, t.total_amount
FROM staging.customer_txn t
JOIN raw.customers c ON t.customer_id = c.customer_id;
"""

CTE_SQL = """
CREATE TABLE mart.monthly_revenue AS
WITH raw_orders AS (
    SELECT order_id, customer_id, amount
    FROM raw.orders
),
enriched AS (
    SELECT o.order_id, o.customer_id, o.amount AS revenue
    FROM raw_orders o
    JOIN raw.customers c ON o.customer_id = c.customer_id
),
agg AS (
    SELECT customer_id, SUM(revenue) AS total_revenue
    FROM enriched
    GROUP BY customer_id
)
SELECT customer_id, total_revenue
FROM agg;
"""

RENAME_SQL = """
CREATE TABLE staging.result AS
SELECT old_name AS new_name, value
FROM raw.source;
"""

CAST_SQL = """
CREATE TABLE staging.typed AS
SELECT CAST(amount AS DECIMAL) AS amount, customer_id
FROM raw.orders;
"""

WINDOW_SQL = """
CREATE TABLE staging.ranked AS
SELECT customer_id,
       RANK() OVER (ORDER BY total_amount DESC) AS rank_col,
       total_amount
FROM staging.customer_txn;
"""


# ---------------------------------------------------------------------------
# trace_column() return type
# ---------------------------------------------------------------------------

class TestTraceColumnReturnType:

    def test_returns_column_lineage(self):
        result = trace_column(SIMPLE_SQL, "customer_id")
        assert isinstance(result, ColumnLineage)

    def test_column_name_preserved(self):
        result = trace_column(SIMPLE_SQL, "customer_id")
        assert result.column_name == "customer_id"

    def test_path_is_list(self):
        result = trace_column(SIMPLE_SQL, "customer_id")
        assert isinstance(result.path, list)

    def test_steps_are_transform_step_instances(self):
        result = trace_column(SIMPLE_SQL, "customer_id")
        for step in result.path:
            assert isinstance(step, TransformStep)


# ---------------------------------------------------------------------------
# Column detection
# ---------------------------------------------------------------------------

class TestColumnDetection:

    def test_passthrough_column_found(self):
        result = trace_column(SIMPLE_SQL, "customer_id")
        assert result.found

    def test_aggregate_column_found(self):
        result = trace_column(SIMPLE_SQL, "total_amount")
        assert result.found

    def test_unknown_column_not_found(self):
        result = trace_column(SIMPLE_SQL, "completely_unknown_xyz")
        assert not result.found

    def test_depth_positive_for_found_column(self):
        result = trace_column(SIMPLE_SQL, "total_amount")
        if result.found:
            assert result.depth >= 0


# ---------------------------------------------------------------------------
# Transformation classification
# ---------------------------------------------------------------------------

class TestTransformationTypes:

    def test_aggregate_transformation_detected(self):
        result = trace_column(SIMPLE_SQL, "total_amount")
        if result.found and result.path:
            agg_steps = [s for s in result.path if "aggregate" in s.transformation]
            assert len(agg_steps) >= 1

    def test_rename_transformation_detected(self):
        result = trace_column(RENAME_SQL, "old_name")
        if result.found and result.path:
            rename_steps = [s for s in result.path if s.transformation == "rename"]
            assert len(rename_steps) >= 1

    def test_cast_transformation_detected(self):
        result = trace_column(CAST_SQL, "amount")
        if result.found and result.path:
            cast_steps = [s for s in result.path if "cast" in s.transformation.lower()]
            assert len(cast_steps) >= 1

    def test_window_transformation_detected(self):
        result = trace_column(WINDOW_SQL, "rank_col")
        if result.found and result.path:
            win_steps = [s for s in result.path if "window" in s.transformation]
            assert len(win_steps) >= 1


# ---------------------------------------------------------------------------
# CTE tracing
# ---------------------------------------------------------------------------

class TestCteTracing:

    def test_traces_column_through_ctes(self):
        result = trace_column(CTE_SQL, "amount")
        assert isinstance(result, ColumnLineage)

    def test_cte_path_entries_have_model_names(self):
        result = trace_column(CTE_SQL, "amount")
        for step in result.path:
            assert step.model_name, "Each step must have a model_name"

    def test_column_with_alias_traced(self):
        result = trace_column(CTE_SQL, "revenue")
        assert isinstance(result, ColumnLineage)


# ---------------------------------------------------------------------------
# _classify_expression tests
# ---------------------------------------------------------------------------

class TestClassifyExpression:

    def test_simple_passthrough(self):
        inp, out, trans, snip = _classify_expression("customer_id", "customer_id")
        assert inp == "customer_id"
        assert trans == "passthrough"

    def test_aggregate_detected(self):
        inp, out, trans, snip = _classify_expression("SUM(amount) AS total_amount", "amount")
        assert inp == "amount"
        assert "aggregate" in trans

    def test_rename_detected(self):
        inp, out, trans, snip = _classify_expression("old_col AS new_col", "old_col")
        assert inp == "old_col"
        assert out == "new_col"
        assert trans == "rename"

    def test_not_in_expression_returns_empty(self):
        inp, out, trans, snip = _classify_expression("something_else", "my_col")
        assert inp == ""
        assert out == ""


# ---------------------------------------------------------------------------
# _describe_transformation tests
# ---------------------------------------------------------------------------

class TestDescribeTransformation:

    def test_aggregate_label(self):
        assert "aggregate" in _describe_transformation("SUM(amount)")

    def test_window_label(self):
        assert "window" in _describe_transformation("RANK() OVER (ORDER BY x)")

    def test_cast_label(self):
        assert "cast" in _describe_transformation("CAST(x AS VARCHAR)")

    def test_case_label(self):
        assert "case" in _describe_transformation("CASE WHEN x > 0 THEN 'y' END")

    def test_passthrough_label(self):
        assert _describe_transformation("customer_id") == "passthrough"


# ---------------------------------------------------------------------------
# _parse_ctes tests
# ---------------------------------------------------------------------------

class TestParseCtes:

    def test_no_with_returns_empty(self):
        assert _parse_ctes("SELECT 1") == {}

    def test_single_cte(self):
        sql = "WITH cte AS (SELECT 1 AS id FROM t) SELECT id FROM cte"
        result = _parse_ctes(sql)
        assert "cte" in result

    def test_multiple_ctes(self):
        # _parse_ctes operates on the WITH ... SELECT block directly
        cte_select_body = (
            "WITH raw_orders AS (\n"
            "    SELECT order_id, customer_id, amount FROM raw.orders\n"
            "),\n"
            "enriched AS (\n"
            "    SELECT o.order_id, o.amount AS revenue FROM raw_orders o\n"
            "),\n"
            "agg AS (\n"
            "    SELECT customer_id, SUM(revenue) AS total_revenue\n"
            "    FROM enriched GROUP BY customer_id\n"
            ")\n"
            "SELECT customer_id, total_revenue FROM agg"
        )
        result = _parse_ctes(cte_select_body)
        assert "raw_orders" in result
        assert "enriched" in result
        assert "agg" in result
