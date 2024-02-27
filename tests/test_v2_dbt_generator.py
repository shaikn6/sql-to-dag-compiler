"""Tests for dbt_compiler.dbt_generator — SQL → dbt model generation."""

from __future__ import annotations

import pytest

from dbt_compiler.dbt_generator import (
    compile_sql_to_dbt,
    write_dbt_project,
    DbtModel,
    DbtSource,
    DbtCompileResult,
    _extract_ctes,
    _extract_final_select,
    _extract_column_metadata,
    _split_top_level_commas,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

SIMPLE_CTAS = """
CREATE TABLE staging.customer_txn AS
SELECT customer_id, SUM(amount) AS total_amount, COUNT(*) AS txn_count
FROM raw.transactions
WHERE txn_date >= '2024-01-01'
GROUP BY customer_id;
"""

CTAS_WITH_JOIN = """
CREATE TABLE mart.customer_summary AS
SELECT c.customer_id, c.name, c.segment, t.total_amount
FROM staging.customer_txn t
JOIN raw.customers c ON t.customer_id = c.customer_id
WHERE t.total_amount > 100;
"""

CTE_SQL = """
CREATE TABLE mart.monthly_revenue AS
WITH raw_orders AS (
    SELECT order_id, customer_id, amount, order_date
    FROM raw.orders
    WHERE order_date >= '2024-01-01'
),
enriched AS (
    SELECT o.order_id, o.customer_id, o.amount, c.segment
    FROM raw_orders o
    JOIN raw.customers c ON o.customer_id = c.customer_id
),
monthly_agg AS (
    SELECT segment, SUM(amount) AS revenue, COUNT(DISTINCT customer_id) AS unique_customers
    FROM enriched
    GROUP BY segment
)
SELECT segment, revenue, unique_customers
FROM monthly_agg;
"""

UNION_SQL = """
CREATE TABLE staging.all_events AS
SELECT event_id, user_id, 'click' AS event_type FROM raw.click_events
UNION ALL
SELECT event_id, user_id, 'view' AS event_type FROM raw.view_events;
"""

INSERT_SELECT = """
INSERT INTO mart.high_value_customers
SELECT customer_id, name, total_amount
FROM mart.customer_summary
WHERE total_amount > 10000;
"""

WINDOW_SQL = """
CREATE TABLE staging.ranked_customers AS
SELECT customer_id, total_amount,
       RANK() OVER (ORDER BY total_amount DESC) AS rank_by_spend
FROM staging.customer_txn;
"""


# ---------------------------------------------------------------------------
# DbtCompileResult tests
# ---------------------------------------------------------------------------

class TestCompileSqlToDbt:

    def test_returns_dbt_compile_result(self):
        result = compile_sql_to_dbt(SIMPLE_CTAS)
        assert isinstance(result, DbtCompileResult)

    def test_simple_ctas_produces_at_least_one_model(self):
        result = compile_sql_to_dbt(SIMPLE_CTAS)
        assert len(result.models) >= 1

    def test_aggregated_model_materialized_as_table(self):
        result = compile_sql_to_dbt(SIMPLE_CTAS)
        agg_model = next(
            (m for m in result.models if "customer_txn" in m.name.lower()), None
        )
        assert agg_model is not None
        assert agg_model.materialized == "table"

    def test_non_aggregated_model_materialized_as_view(self):
        result = compile_sql_to_dbt(INSERT_SELECT)
        model = next(
            (m for m in result.models if "high_value" in m.name.lower()), None
        )
        assert model is not None
        assert model.materialized == "view"

    def test_sources_detected_from_raw_schema(self):
        result = compile_sql_to_dbt(SIMPLE_CTAS)
        source_schemas = [s.schema for s in result.sources]
        assert "raw" in source_schemas

    def test_cte_sql_creates_cte_models(self):
        result = compile_sql_to_dbt(CTE_SQL)
        model_names = [m.name.lower() for m in result.models]
        assert "raw_orders" in model_names
        assert "enriched" in model_names
        assert "monthly_agg" in model_names

    def test_final_target_model_included(self):
        result = compile_sql_to_dbt(CTE_SQL)
        model_names = [m.name.lower() for m in result.models]
        assert "monthly_revenue" in model_names

    def test_union_all_detected(self):
        result = compile_sql_to_dbt(UNION_SQL)
        assert len(result.models) >= 1

    def test_window_function_model_materialized_as_table(self):
        result = compile_sql_to_dbt(WINDOW_SQL)
        model = next(
            (m for m in result.models if "ranked" in m.name.lower()), None
        )
        assert model is not None
        assert model.materialized == "table"

    def test_source_tables_populated(self):
        result = compile_sql_to_dbt(CTE_SQL)
        all_tables = []
        for src in result.sources:
            all_tables.extend(src.tables)
        # raw.orders and raw.customers should be detected
        assert "orders" in all_tables or "customers" in all_tables

    def test_model_has_description(self):
        result = compile_sql_to_dbt(SIMPLE_CTAS)
        for model in result.models:
            assert model.description, "Every model should have a description"

    def test_empty_sql_returns_empty_result(self):
        result = compile_sql_to_dbt("")
        assert result.models == []
        assert result.sources == []


# ---------------------------------------------------------------------------
# DbtModel.dbt_sql tests
# ---------------------------------------------------------------------------

class TestDbtModelDbtSql:

    def test_source_ref_replaced(self):
        result = compile_sql_to_dbt(SIMPLE_CTAS)
        model = result.models[0]
        # Should contain source() macro for raw schema tables
        if model.source_refs:
            assert "{{" in model.dbt_sql or "source(" in model.dbt_sql

    def test_model_ref_replaced(self):
        result = compile_sql_to_dbt(CTE_SQL)
        monthly_model = next(
            (m for m in result.models if m.name.lower() == "monthly_revenue"), None
        )
        if monthly_model and monthly_model.model_refs:
            assert "ref(" in monthly_model.dbt_sql


# ---------------------------------------------------------------------------
# Column extraction tests
# ---------------------------------------------------------------------------

class TestColumnExtraction:

    def test_columns_extracted_from_select(self):
        result = compile_sql_to_dbt(SIMPLE_CTAS)
        model = next(m for m in result.models if "customer_txn" in m.name.lower())
        col_names = [c.name for c in model.columns]
        assert "customer_id" in col_names or len(col_names) >= 1

    def test_aggregate_column_description(self):
        result = compile_sql_to_dbt(SIMPLE_CTAS)
        model = next(m for m in result.models if "customer_txn" in m.name.lower())
        agg_cols = [c for c in model.columns if "sum" in c.description.lower() or "count" in c.description.lower()]
        assert len(agg_cols) >= 1


# ---------------------------------------------------------------------------
# write_dbt_project tests
# ---------------------------------------------------------------------------

class TestWriteDbtProject:

    def test_writes_model_sql_files(self, tmp_path):
        result = compile_sql_to_dbt(SIMPLE_CTAS)
        written = write_dbt_project(result, str(tmp_path))
        sql_files = [k for k in written if k.endswith(".sql")]
        assert len(sql_files) >= 1

    def test_writes_schema_yml(self, tmp_path):
        result = compile_sql_to_dbt(SIMPLE_CTAS)
        written = write_dbt_project(result, str(tmp_path))
        yml_files = [k for k in written if k.endswith("schema.yml")]
        assert len(yml_files) >= 1

    def test_writes_sources_yml(self, tmp_path):
        result = compile_sql_to_dbt(SIMPLE_CTAS)
        written = write_dbt_project(result, str(tmp_path))
        assert "sources.yml" in written

    def test_schema_yml_contains_version(self, tmp_path):
        result = compile_sql_to_dbt(SIMPLE_CTAS)
        written = write_dbt_project(result, str(tmp_path))
        for key, content in written.items():
            if key.endswith("schema.yml"):
                assert "version: 2" in content

    def test_sources_yml_has_source_block(self, tmp_path):
        result = compile_sql_to_dbt(SIMPLE_CTAS)
        written = write_dbt_project(result, str(tmp_path))
        if "sources.yml" in written:
            assert "sources:" in written["sources.yml"]

    def test_model_sql_contains_config_block(self, tmp_path):
        result = compile_sql_to_dbt(SIMPLE_CTAS)
        written = write_dbt_project(result, str(tmp_path))
        for key, content in written.items():
            if key.endswith(".sql"):
                assert "config(" in content
                break


# ---------------------------------------------------------------------------
# Utility function tests
# ---------------------------------------------------------------------------

class TestExtractCtes:

    def test_no_with_returns_empty(self):
        result = _extract_ctes("SELECT id FROM foo")
        assert result == {}

    def test_single_cte_extracted(self):
        sql = "WITH cte AS (SELECT 1 AS id) SELECT id FROM cte"
        result = _extract_ctes(sql)
        assert "cte" in result

    def test_multiple_ctes_extracted(self):
        # _extract_ctes operates on the SELECT body after the CTAS wrapper
        cte_select_body = (
            "WITH raw_orders AS (\n"
            "    SELECT order_id, customer_id, amount, order_date\n"
            "    FROM raw.orders\n"
            "    WHERE order_date >= '2024-01-01'\n"
            "),\n"
            "enriched AS (\n"
            "    SELECT o.order_id, o.amount AS revenue\n"
            "    FROM raw_orders o\n"
            "),\n"
            "monthly_agg AS (\n"
            "    SELECT segment, SUM(amount) AS revenue FROM enriched GROUP BY segment\n"
            ")\n"
            "SELECT segment, revenue FROM monthly_agg"
        )
        result = _extract_ctes(cte_select_body)
        assert "raw_orders" in result
        assert "enriched" in result
        assert "monthly_agg" in result


class TestSplitTopLevelCommas:

    def test_simple_split(self):
        result = _split_top_level_commas("a, b, c")
        assert len(result) == 3

    def test_nested_parens_not_split(self):
        result = _split_top_level_commas("SUM(a, b), c")
        assert len(result) == 2

    def test_empty_string(self):
        # empty string returns a list with one empty string or empty list — implementation-defined
        result = _split_top_level_commas("")
        assert result == [] or result == [""]
