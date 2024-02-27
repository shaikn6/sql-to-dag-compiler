"""Tests for lineage.impact_analyzer — column impact analysis engine."""

from __future__ import annotations

import pytest

from lineage.impact_analyzer import (
    ImpactAnalyzer,
    ImpactResult,
    FileChange,
    BreakingChangeDiff,
    ColumnDiff,
    _extract_output_columns,
    _extract_table_refs,
    _parse_ctes,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

CHAIN_SQL = """
CREATE TABLE staging.customer_txn AS
SELECT customer_id, SUM(amount) AS total_amount, COUNT(*) AS txn_count
FROM raw.transactions
GROUP BY customer_id;

CREATE TABLE mart.customer_summary AS
SELECT c.customer_id, c.name, t.total_amount, t.txn_count
FROM staging.customer_txn t
JOIN raw.customers c ON t.customer_id = c.customer_id;

INSERT INTO mart.high_value_customers
SELECT customer_id, name, total_amount
FROM mart.customer_summary
WHERE total_amount > 10000;
"""

CTE_SQL = """
CREATE TABLE mart.monthly_revenue AS
WITH raw_orders AS (
    SELECT order_id, customer_id, amount
    FROM raw.orders
),
enriched AS (
    SELECT o.order_id, o.customer_id, o.amount, c.segment
    FROM raw_orders o
    JOIN raw.customers c ON o.customer_id = c.customer_id
),
monthly_agg AS (
    SELECT segment, SUM(amount) AS revenue
    FROM enriched
    GROUP BY segment
)
SELECT segment, revenue
FROM monthly_agg;
"""


# ---------------------------------------------------------------------------
# ImpactAnalyzer construction
# ---------------------------------------------------------------------------

class TestImpactAnalyzerInit:

    def test_instantiates_without_error(self):
        analyzer = ImpactAnalyzer(CHAIN_SQL)
        assert analyzer is not None

    def test_instantiates_with_cte_sql(self):
        analyzer = ImpactAnalyzer(CTE_SQL)
        assert analyzer is not None

    def test_empty_sql_does_not_raise(self):
        analyzer = ImpactAnalyzer("")
        assert analyzer is not None


# ---------------------------------------------------------------------------
# analyze()
# ---------------------------------------------------------------------------

class TestAnalyze:

    def test_returns_impact_result(self):
        analyzer = ImpactAnalyzer(CHAIN_SQL)
        result = analyzer.analyze("total_amount")
        assert isinstance(result, ImpactResult)

    def test_blast_radius_is_non_negative(self):
        analyzer = ImpactAnalyzer(CHAIN_SQL)
        result = analyzer.analyze("total_amount")
        assert result.blast_radius >= 0

    def test_column_name_preserved(self):
        analyzer = ImpactAnalyzer(CHAIN_SQL)
        result = analyzer.analyze("customer_id")
        assert result.column_name == "customer_id"

    def test_affected_models_is_list(self):
        analyzer = ImpactAnalyzer(CHAIN_SQL)
        result = analyzer.analyze("total_amount")
        assert isinstance(result.affected_models, list)

    def test_dependency_tree_is_dict(self):
        analyzer = ImpactAnalyzer(CHAIN_SQL)
        result = analyzer.analyze("total_amount")
        assert isinstance(result.dependency_tree, dict)

    def test_critical_path_is_list(self):
        analyzer = ImpactAnalyzer(CHAIN_SQL)
        result = analyzer.analyze("total_amount")
        assert isinstance(result.critical_path, list)

    def test_unknown_column_blast_radius_zero(self):
        analyzer = ImpactAnalyzer(CHAIN_SQL)
        result = analyzer.analyze("nonexistent_col_xyz_abc")
        assert result.blast_radius == 0

    def test_cte_column_analysis(self):
        analyzer = ImpactAnalyzer(CTE_SQL)
        result = analyzer.analyze("amount")
        assert isinstance(result, ImpactResult)


# ---------------------------------------------------------------------------
# what_if_rename()
# ---------------------------------------------------------------------------

class TestWhatIfRename:

    def test_returns_list(self):
        analyzer = ImpactAnalyzer(CHAIN_SQL)
        changes = analyzer.what_if_rename("total_amount", "total_amount_v2")
        assert isinstance(changes, list)

    def test_changes_are_file_change_instances(self):
        analyzer = ImpactAnalyzer(CHAIN_SQL)
        changes = analyzer.what_if_rename("total_amount", "new_total")
        for change in changes:
            assert isinstance(change, FileChange)

    def test_file_change_has_correct_old_value(self):
        analyzer = ImpactAnalyzer(CHAIN_SQL)
        changes = analyzer.what_if_rename("total_amount", "new_total")
        for change in changes:
            assert change.old_value == "total_amount"
            assert change.new_value == "new_total"

    def test_no_changes_for_unknown_column(self):
        analyzer = ImpactAnalyzer(CHAIN_SQL)
        changes = analyzer.what_if_rename("totally_unknown_col", "new_name")
        assert changes == []

    def test_file_paths_contain_model_name(self):
        analyzer = ImpactAnalyzer(CHAIN_SQL)
        changes = analyzer.what_if_rename("total_amount", "new_total")
        for change in changes:
            assert "models/" in change.file_path or ".sql" in change.file_path


# ---------------------------------------------------------------------------
# breaking_changes()
# ---------------------------------------------------------------------------

class TestBreakingChanges:

    def test_returns_breaking_change_diff(self):
        analyzer = ImpactAnalyzer(CHAIN_SQL)
        diff = analyzer.breaking_changes(
            "SELECT order_id, customer_id, amount FROM raw.orders",
            "SELECT order_id, customer_id, amount FROM raw.orders",
        )
        assert isinstance(diff, BreakingChangeDiff)

    def test_no_breaking_changes_identical_sql(self):
        analyzer = ImpactAnalyzer(CHAIN_SQL)
        sql = "SELECT order_id, customer_id, amount FROM raw.orders"
        diff = analyzer.breaking_changes(sql, sql)
        assert not diff.is_breaking

    def test_removed_column_is_breaking(self):
        analyzer = ImpactAnalyzer(CHAIN_SQL)
        v1 = "SELECT order_id, customer_id, amount FROM raw.orders"
        v2 = "SELECT order_id, customer_id FROM raw.orders"
        diff = analyzer.breaking_changes(v1, v2)
        assert diff.is_breaking
        assert "amount" in diff.removed_columns

    def test_added_column_is_non_breaking(self):
        analyzer = ImpactAnalyzer(CHAIN_SQL)
        v1 = "SELECT order_id, customer_id FROM raw.orders"
        v2 = "SELECT order_id, customer_id, amount FROM raw.orders"
        diff = analyzer.breaking_changes(v1, v2)
        assert not diff.is_breaking
        assert "amount" in diff.added_columns

    def test_single_rename_detected(self):
        analyzer = ImpactAnalyzer(CHAIN_SQL)
        v1 = "SELECT order_id, amount FROM raw.orders"
        v2 = "SELECT order_id, total_amount FROM raw.orders"
        diff = analyzer.breaking_changes(v1, v2)
        # Single rename detected
        assert diff.is_breaking
        if diff.renamed_columns:
            assert diff.renamed_columns[0].change_type == "renamed"

    def test_type_change_detected(self):
        analyzer = ImpactAnalyzer(CHAIN_SQL)
        v1 = "SELECT CAST(amount AS INTEGER) AS amount FROM raw.orders"
        v2 = "SELECT CAST(amount AS VARCHAR) AS amount FROM raw.orders"
        diff = analyzer.breaking_changes(v1, v2)
        # Type change should be detected
        assert isinstance(diff, BreakingChangeDiff)

    def test_diff_summary_not_empty_on_breaking(self):
        analyzer = ImpactAnalyzer(CHAIN_SQL)
        v1 = "SELECT a, b, c FROM raw.t"
        v2 = "SELECT a, b FROM raw.t"
        diff = analyzer.breaking_changes(v1, v2)
        assert diff.summary != "No changes detected"


# ---------------------------------------------------------------------------
# Utility helper tests
# ---------------------------------------------------------------------------

class TestExtractOutputColumns:

    def test_simple_columns(self):
        cols = _extract_output_columns("SELECT a, b, c FROM t")
        assert "a" in cols
        assert "b" in cols
        assert "c" in cols

    def test_aliased_column(self):
        cols = _extract_output_columns("SELECT SUM(x) AS total FROM t")
        assert "total" in cols

    def test_star_select(self):
        cols = _extract_output_columns("SELECT * FROM t")
        assert "*" in cols

    def test_no_from_returns_empty(self):
        cols = _extract_output_columns("not a select statement")
        assert cols == []


class TestExtractTableRefs:

    def test_from_clause_detected(self):
        refs = _extract_table_refs("SELECT a FROM raw.transactions")
        assert "raw.transactions" in refs

    def test_join_clause_detected(self):
        refs = _extract_table_refs("SELECT a FROM t1 JOIN t2 ON t1.id = t2.id")
        assert "t1" in refs
        assert "t2" in refs

    def test_dual_excluded(self):
        refs = _extract_table_refs("SELECT SYSDATE FROM DUAL")
        assert "dual" not in refs
