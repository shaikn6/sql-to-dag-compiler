"""Tests for sql_to_dag.parser — SQL/PLSQL statement parsing."""

import pytest

from sql_to_dag.parser import parse_sql_string, parse_sql_file


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

SAMPLE_CTAS = """
CREATE TABLE staging.customer_txn AS
SELECT customer_id, SUM(amount) as total_amount, COUNT(*) as txn_count
FROM raw.transactions
WHERE txn_date >= TRUNC(SYSDATE) - 30
GROUP BY customer_id;
"""

SAMPLE_INSERT_SELECT = """
INSERT INTO mart.high_value_customers
SELECT customer_id, name, total_amount
FROM mart.customer_summary
WHERE total_amount > 10000;
"""

MULTI_STATEMENT_SQL = """
CREATE TABLE staging.customer_txn AS
SELECT customer_id, SUM(amount) as total_amount
FROM raw.transactions
GROUP BY customer_id;

CREATE TABLE mart.customer_summary AS
SELECT c.customer_id, c.name, t.total_amount
FROM staging.customer_txn t
JOIN raw.customers c ON t.customer_id = c.customer_id
WHERE t.total_amount > 100;

INSERT INTO mart.high_value_customers
SELECT customer_id, name, total_amount
FROM mart.customer_summary
WHERE total_amount > 10000;
"""

SQL_WITH_COMMENTS = """
-- Step 1: aggregate from raw transaction data
CREATE TABLE staging.result AS
SELECT id, SUM(val) as total
FROM raw.source_table
GROUP BY id;
"""


# ---------------------------------------------------------------------------
# parse_sql_string tests
# ---------------------------------------------------------------------------

class TestParseSqlString:

    def test_returns_list(self):
        result = parse_sql_string(SAMPLE_CTAS)
        assert isinstance(result, list)

    def test_single_ctas_yields_one_statement(self):
        result = parse_sql_string(SAMPLE_CTAS)
        assert len(result) == 1

    def test_multi_statement_yields_correct_count(self):
        result = parse_sql_string(MULTI_STATEMENT_SQL)
        assert len(result) == 3

    def test_empty_string_returns_empty_list(self):
        result = parse_sql_string("")
        assert result == []

    def test_whitespace_only_returns_empty_list(self):
        result = parse_sql_string("   \n\n  ")
        assert result == []


class TestStatementIds:

    def test_ids_are_sequential(self):
        result = parse_sql_string(MULTI_STATEMENT_SQL)
        ids = [s["id"] for s in result]
        assert ids == ["stmt_0", "stmt_1", "stmt_2"]

    def test_id_format(self):
        result = parse_sql_string(SAMPLE_CTAS)
        assert result[0]["id"] == "stmt_0"


class TestStatementType:

    def test_ctas_detected(self):
        result = parse_sql_string(SAMPLE_CTAS)
        assert result[0]["statement_type"] == "CTAS"

    def test_insert_select_detected(self):
        result = parse_sql_string(SAMPLE_INSERT_SELECT)
        assert result[0]["statement_type"] == "INSERT_SELECT"

    def test_multi_statement_types(self):
        result = parse_sql_string(MULTI_STATEMENT_SQL)
        assert result[0]["statement_type"] == "CTAS"
        assert result[1]["statement_type"] == "CTAS"
        assert result[2]["statement_type"] == "INSERT_SELECT"


class TestTargetTable:

    def test_ctas_target_table(self):
        result = parse_sql_string(SAMPLE_CTAS)
        assert result[0]["target_table"] == "staging.customer_txn"

    def test_insert_target_table(self):
        result = parse_sql_string(SAMPLE_INSERT_SELECT)
        assert result[0]["target_table"] == "mart.high_value_customers"

    def test_target_tables_in_multi_statement(self):
        result = parse_sql_string(MULTI_STATEMENT_SQL)
        targets = [s["target_table"] for s in result]
        assert targets == [
            "staging.customer_txn",
            "mart.customer_summary",
            "mart.high_value_customers",
        ]

    def test_target_table_is_lowercased(self):
        sql = "CREATE TABLE STAGING.MY_TABLE AS SELECT id FROM raw.src;"
        result = parse_sql_string(sql)
        assert result[0]["target_table"] == "staging.my_table"


class TestSourceTables:

    def test_from_clause_detected(self):
        result = parse_sql_string(SAMPLE_CTAS)
        assert "raw.transactions" in result[0]["source_tables"]

    def test_join_clause_detected(self):
        result = parse_sql_string(MULTI_STATEMENT_SQL)
        # stmt_1 has both FROM and JOIN
        assert "staging.customer_txn" in result[1]["source_tables"]
        assert "raw.customers" in result[1]["source_tables"]

    def test_target_not_in_sources(self):
        result = parse_sql_string(SAMPLE_CTAS)
        stmt = result[0]
        assert stmt["target_table"] not in stmt["source_tables"]

    def test_comments_not_mistaken_for_table_refs(self):
        """The comment 'aggregate from raw transaction data' must not yield 'raw' as a source."""
        result = parse_sql_string(SQL_WITH_COMMENTS)
        sources = result[0]["source_tables"]
        # Only 'raw.source_table' should appear, not bare 'raw'
        assert "raw" not in sources
        assert "raw.source_table" in sources


class TestTransformationFlags:

    def test_has_where_true(self):
        result = parse_sql_string(SAMPLE_CTAS)
        assert result[0]["has_where"] is True

    def test_has_where_false(self):
        sql = "CREATE TABLE t AS SELECT id FROM raw.src;"
        result = parse_sql_string(sql)
        assert result[0]["has_where"] is False

    def test_has_group_by_true(self):
        result = parse_sql_string(SAMPLE_CTAS)
        assert result[0]["has_group_by"] is True

    def test_has_group_by_false(self):
        result = parse_sql_string(SAMPLE_INSERT_SELECT)
        assert result[0]["has_group_by"] is False

    def test_aggregations_detected(self):
        result = parse_sql_string(SAMPLE_CTAS)
        aggs = result[0]["aggregations"]
        assert "SUM" in aggs
        assert "COUNT" in aggs

    def test_no_aggregations(self):
        result = parse_sql_string(SAMPLE_INSERT_SELECT)
        assert result[0]["aggregations"] == []


class TestLabel:

    def test_ctas_label_uses_target_table_stem(self):
        result = parse_sql_string(SAMPLE_CTAS)
        # label should be create_<table_stem>
        assert result[0]["label"] == "create_customer_txn"

    def test_insert_label_uses_target_table_stem(self):
        result = parse_sql_string(SAMPLE_INSERT_SELECT)
        assert result[0]["label"] == "insert_high_value_customers"


class TestParseSqlFile:

    def test_parses_example_file(self, tmp_path):
        sql_file = tmp_path / "test.sql"
        sql_file.write_text(MULTI_STATEMENT_SQL, encoding="utf-8")
        result = parse_sql_file(str(sql_file))
        assert len(result) == 3

    def test_raises_on_missing_file(self):
        with pytest.raises(FileNotFoundError):
            parse_sql_file("/nonexistent/path/file.sql")
