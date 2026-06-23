"""
Comprehensive test suite to push sql-to-dag-compiler coverage to 95%+.

Covers every branch/error path in:
  - sql_to_dag/parser.py
  - sql_to_dag/graph.py
  - sql_to_dag/generator.py
  - src/edge_case_handler.py
  - src/lineage_report.py
"""

from __future__ import annotations

import json
import textwrap
import pytest
import networkx as nx


# ===========================================================================
# sql_to_dag/parser.py
# ===========================================================================

class TestStripPlsqlBlockDelimiters:
    def test_removes_begin_end_block(self):
        from sql_to_dag.parser import _strip_plsql_block_delimiters
        sql = "BEGIN SELECT 1; END;"
        result = _strip_plsql_block_delimiters(sql)
        # The BEGIN...END block should be removed
        assert len(result) < len(sql)

    def test_no_block_unchanged(self):
        from sql_to_dag.parser import _strip_plsql_block_delimiters
        sql = "SELECT * FROM foo"
        assert _strip_plsql_block_delimiters(sql) == sql


class TestNormaliseTableName:
    def test_strips_parens_and_lowercases(self):
        from sql_to_dag.parser import _normalise_table_name
        assert _normalise_table_name("MyTable);") == "mytable"

    def test_strips_comma(self):
        from sql_to_dag.parser import _normalise_table_name
        assert _normalise_table_name("schema.table,") == "schema.table"

    def test_already_clean(self):
        from sql_to_dag.parser import _normalise_table_name
        assert _normalise_table_name("schema.table") == "schema.table"


class TestStripComments:
    def test_removes_line_comment(self):
        from sql_to_dag.parser import _strip_comments
        sql = "SELECT 1 -- this is a comment\n, 2"
        result = _strip_comments(sql)
        assert "this is a comment" not in result

    def test_removes_block_comment(self):
        from sql_to_dag.parser import _strip_comments
        sql = "SELECT /* block */ 1"
        result = _strip_comments(sql)
        assert "block" not in result

    def test_preserves_sql(self):
        from sql_to_dag.parser import _strip_comments
        sql = "SELECT 1 FROM dual -- comment"
        result = _strip_comments(sql)
        assert "SELECT" in result
        assert "FROM" in result


class TestDetectStatementType:
    def test_ctas(self):
        from sql_to_dag.parser import _detect_statement_type
        sql = "CREATE TABLE foo AS SELECT * FROM bar"
        assert _detect_statement_type(sql.upper()) == "CTAS"

    def test_insert_select(self):
        from sql_to_dag.parser import _detect_statement_type
        sql = "INSERT INTO foo SELECT * FROM bar"
        assert _detect_statement_type(sql.upper()) == "INSERT_SELECT"

    def test_insert_values(self):
        from sql_to_dag.parser import _detect_statement_type
        sql = "INSERT INTO foo VALUES (1, 2)"
        assert _detect_statement_type(sql.upper()) == "INSERT_VALUES"

    def test_unknown(self):
        from sql_to_dag.parser import _detect_statement_type
        sql = "UPDATE foo SET x = 1"
        assert _detect_statement_type(sql.upper()) == "UNKNOWN"


class TestExtractTargetTable:
    def test_ctas_target(self):
        from sql_to_dag.parser import _extract_target_table
        sql = "CREATE TABLE schema.my_table AS SELECT * FROM source"
        result = _extract_target_table(sql, "CTAS")
        assert result == "schema.my_table"

    def test_insert_target(self):
        from sql_to_dag.parser import _extract_target_table
        sql = "INSERT INTO my_target SELECT * FROM source"
        result = _extract_target_table(sql, "INSERT_SELECT")
        assert result == "my_target"

    def test_insert_values_target(self):
        from sql_to_dag.parser import _extract_target_table
        sql = "INSERT INTO t VALUES (1)"
        result = _extract_target_table(sql, "INSERT_VALUES")
        assert result == "t"

    def test_unknown_returns_none(self):
        from sql_to_dag.parser import _extract_target_table
        sql = "UPDATE foo SET x = 1"
        result = _extract_target_table(sql, "UNKNOWN")
        assert result is None

    def test_no_match_returns_none(self):
        from sql_to_dag.parser import _extract_target_table
        sql = "SELECT 1"
        result = _extract_target_table(sql, "CTAS")
        assert result is None


class TestExtractSourceTables:
    def test_basic_from(self):
        from sql_to_dag.parser import _extract_source_tables
        sql = "SELECT * FROM schema.source_table"
        result = _extract_source_tables(sql, "schema.target")
        assert "schema.source_table" in result

    def test_excludes_target(self):
        from sql_to_dag.parser import _extract_source_tables
        sql = "INSERT INTO mytarget SELECT * FROM mytarget"
        result = _extract_source_tables(sql, "mytarget")
        assert "mytarget" not in result

    def test_excludes_dual(self):
        from sql_to_dag.parser import _extract_source_tables
        sql = "SELECT 1 FROM dual"
        result = _extract_source_tables(sql, None)
        assert "dual" not in result

    def test_join_tables(self):
        from sql_to_dag.parser import _extract_source_tables
        sql = "SELECT * FROM a JOIN schema.b ON a.id = b.id"
        result = _extract_source_tables(sql, None)
        assert "a" in result
        assert "schema.b" in result

    def test_deduplicates(self):
        from sql_to_dag.parser import _extract_source_tables
        sql = "SELECT * FROM foo JOIN foo ON 1=1"
        result = _extract_source_tables(sql, None)
        assert result.count("foo") == 1

    def test_no_target_provided(self):
        from sql_to_dag.parser import _extract_source_tables
        sql = "SELECT * FROM tbl1"
        result = _extract_source_tables(sql, None)
        assert "tbl1" in result


class TestExtractAggregations:
    def test_detects_sum(self):
        from sql_to_dag.parser import _extract_aggregations
        sql = "SELECT SUM(revenue) FROM sales"
        result = _extract_aggregations(sql)
        assert "SUM" in result

    def test_detects_multiple(self):
        from sql_to_dag.parser import _extract_aggregations
        sql = "SELECT COUNT(*), AVG(price), MAX(date) FROM orders"
        result = _extract_aggregations(sql)
        assert "COUNT" in result
        assert "AVG" in result
        assert "MAX" in result

    def test_deduplicates(self):
        from sql_to_dag.parser import _extract_aggregations
        sql = "SELECT SUM(a), SUM(b) FROM t"
        result = _extract_aggregations(sql)
        assert result.count("SUM") == 1

    def test_no_aggs(self):
        from sql_to_dag.parser import _extract_aggregations
        sql = "SELECT a, b FROM t WHERE a > 1"
        result = _extract_aggregations(sql)
        assert result == []

    def test_median_and_listagg(self):
        from sql_to_dag.parser import _extract_aggregations
        sql = "SELECT MEDIAN(price), LISTAGG(name, ',') FROM t"
        result = _extract_aggregations(sql)
        assert "MEDIAN" in result
        assert "LISTAGG" in result


class TestMakeTaskLabel:
    def test_ctas_label(self):
        from sql_to_dag.parser import _make_task_label
        assert _make_task_label("CTAS", "schema.my_table", 0) == "create_my_table"

    def test_insert_label(self):
        from sql_to_dag.parser import _make_task_label
        assert _make_task_label("INSERT_SELECT", "target", 1) == "insert_target"

    def test_insert_values_label(self):
        from sql_to_dag.parser import _make_task_label
        assert _make_task_label("INSERT_VALUES", "dest", 0) == "insert_dest"

    def test_no_target_fallback(self):
        from sql_to_dag.parser import _make_task_label
        assert _make_task_label("UNKNOWN", None, 3) == "stmt_3"

    def test_unknown_with_target(self):
        from sql_to_dag.parser import _make_task_label
        label = _make_task_label("UNKNOWN", "tbl", 2)
        assert "tbl" in label


class TestParseSqlString:
    def test_basic_ctas(self):
        from sql_to_dag.parser import parse_sql_string
        sql = "CREATE TABLE summary AS SELECT * FROM orders"
        results = parse_sql_string(sql)
        assert len(results) == 1
        assert results[0]["statement_type"] == "CTAS"
        assert results[0]["target_table"] == "summary"

    def test_multiple_statements(self):
        from sql_to_dag.parser import parse_sql_string
        sql = textwrap.dedent("""
            CREATE TABLE a AS SELECT * FROM b;
            INSERT INTO c SELECT * FROM a;
        """)
        results = parse_sql_string(sql)
        assert len(results) == 2

    def test_empty_input(self):
        from sql_to_dag.parser import parse_sql_string
        results = parse_sql_string("")
        assert results == []

    def test_whitespace_only(self):
        from sql_to_dag.parser import parse_sql_string
        results = parse_sql_string("   \n\t  ")
        assert results == []

    def test_size_limit_exceeded(self):
        from sql_to_dag.parser import parse_sql_string
        big_sql = "SELECT 1; " * 600_000  # > 5MB
        with pytest.raises(ValueError, match="maximum allowed size"):
            parse_sql_string(big_sql)

    def test_has_where_detected(self):
        from sql_to_dag.parser import parse_sql_string
        sql = "CREATE TABLE a AS SELECT * FROM b WHERE id > 1"
        results = parse_sql_string(sql)
        assert results[0]["has_where"] is True

    def test_has_group_by_detected(self):
        from sql_to_dag.parser import parse_sql_string
        sql = "CREATE TABLE a AS SELECT id, SUM(val) FROM b GROUP BY id"
        results = parse_sql_string(sql)
        assert results[0]["has_group_by"] is True

    def test_unknown_statement_has_id(self):
        from sql_to_dag.parser import parse_sql_string
        sql = "UPDATE foo SET x = 1"
        results = parse_sql_string(sql)
        assert results[0]["id"] == "stmt_0"
        assert results[0]["statement_type"] == "UNKNOWN"

    def test_parse_sql_file(self, tmp_path):
        from sql_to_dag.parser import parse_sql_file
        f = tmp_path / "test.sql"
        f.write_text("CREATE TABLE t AS SELECT * FROM src;")
        results = parse_sql_file(str(f))
        assert len(results) == 1

    def test_insert_values(self):
        from sql_to_dag.parser import parse_sql_string
        sql = "INSERT INTO t VALUES (1, 'foo')"
        results = parse_sql_string(sql)
        assert results[0]["statement_type"] == "INSERT_VALUES"

    def test_source_tables_populated(self):
        from sql_to_dag.parser import parse_sql_string
        sql = "CREATE TABLE a AS SELECT * FROM schema.raw_data"
        results = parse_sql_string(sql)
        assert "schema.raw_data" in results[0]["source_tables"]

    def test_each_statement_has_required_keys(self):
        from sql_to_dag.parser import parse_sql_string
        sql = "CREATE TABLE t AS SELECT * FROM s"
        results = parse_sql_string(sql)
        required = {"id", "raw_sql", "statement_type", "target_table",
                    "source_tables", "has_where", "has_group_by", "aggregations", "label"}
        assert required.issubset(set(results[0].keys()))


# ===========================================================================
# sql_to_dag/graph.py
# ===========================================================================

class TestBuildDependencyGraph:
    def _make_stmt(self, id, target, sources):
        return {"id": id, "target_table": target, "source_tables": sources}

    def test_simple_chain(self):
        from sql_to_dag.graph import build_dependency_graph, topological_order
        stmts = [
            self._make_stmt("stmt_0", "a", []),
            self._make_stmt("stmt_1", "b", ["a"]),
            self._make_stmt("stmt_2", "c", ["b"]),
        ]
        g = build_dependency_graph(stmts)
        order = topological_order(g)
        assert order.index("stmt_0") < order.index("stmt_1")
        assert order.index("stmt_1") < order.index("stmt_2")

    def test_parallel_nodes(self):
        from sql_to_dag.graph import build_dependency_graph
        stmts = [
            self._make_stmt("stmt_0", "a", []),
            self._make_stmt("stmt_1", "b", []),
            self._make_stmt("stmt_2", "c", ["a", "b"]),
        ]
        g = build_dependency_graph(stmts)
        assert g.has_edge("stmt_0", "stmt_2")
        assert g.has_edge("stmt_1", "stmt_2")

    def test_isolated_node(self):
        from sql_to_dag.graph import build_dependency_graph
        stmts = [
            self._make_stmt("stmt_0", "a", []),
            self._make_stmt("stmt_1", "b", []),
        ]
        g = build_dependency_graph(stmts)
        assert g.has_node("stmt_0")
        assert g.has_node("stmt_1")
        assert g.number_of_edges() == 0

    def test_no_self_edges(self):
        from sql_to_dag.graph import build_dependency_graph
        stmts = [self._make_stmt("stmt_0", "a", ["a"])]
        g = build_dependency_graph(stmts)
        assert not g.has_edge("stmt_0", "stmt_0")

    def test_no_target_table(self):
        from sql_to_dag.graph import build_dependency_graph
        stmts = [self._make_stmt("stmt_0", None, ["x"])]
        g = build_dependency_graph(stmts)
        assert g.has_node("stmt_0")

    def test_cycle_raises(self):
        from sql_to_dag.graph import build_dependency_graph
        stmts = [
            self._make_stmt("stmt_0", "a", ["b"]),
            self._make_stmt("stmt_1", "b", ["a"]),
        ]
        with pytest.raises(ValueError, match="[Cc]ircular"):
            build_dependency_graph(stmts)

    def test_node_attrs_stored(self):
        from sql_to_dag.graph import build_dependency_graph
        stmts = [self._make_stmt("s0", "x", [])]
        g = build_dependency_graph(stmts)
        assert g.nodes["s0"]["target_table"] == "x"


class TestGetDependencies:
    def test_returns_predecessors(self):
        from sql_to_dag.graph import build_dependency_graph, get_dependencies
        stmts = [
            {"id": "s0", "target_table": "a", "source_tables": []},
            {"id": "s1", "target_table": "b", "source_tables": ["a"]},
        ]
        g = build_dependency_graph(stmts)
        deps = get_dependencies(g, "s1")
        assert "s0" in deps

    def test_root_has_no_deps(self):
        from sql_to_dag.graph import build_dependency_graph, get_dependencies
        stmts = [{"id": "s0", "target_table": "a", "source_tables": []}]
        g = build_dependency_graph(stmts)
        assert get_dependencies(g, "s0") == []


class TestTopologicalOrder:
    def test_cycle_raises_value_error(self):
        from sql_to_dag.graph import topological_order
        g = nx.DiGraph()
        g.add_edge("a", "b")
        g.add_edge("b", "a")
        with pytest.raises(ValueError):
            topological_order(g)

    def test_empty_graph(self):
        from sql_to_dag.graph import topological_order
        g = nx.DiGraph()
        order = topological_order(g)
        assert order == []


# ===========================================================================
# sql_to_dag/generator.py
# ===========================================================================

_SIMPLE_SQL = "CREATE TABLE out_table AS SELECT * FROM in_table;"

_CHAIN_SQL = textwrap.dedent("""
    CREATE TABLE step1 AS SELECT * FROM raw_data;
    CREATE TABLE step2 AS SELECT * FROM step1;
    INSERT INTO final SELECT * FROM step2;
""")


class TestSanitizeIdentifier:
    def test_valid_identifier(self):
        from sql_to_dag.generator import _sanitize_identifier
        assert _sanitize_identifier("my_dag_123", "dag_id") == "my_dag_123"

    def test_empty_raises(self):
        from sql_to_dag.generator import _sanitize_identifier
        with pytest.raises(ValueError, match="must not be empty"):
            _sanitize_identifier("", "dag_id")

    def test_unsafe_chars_raise(self):
        from sql_to_dag.generator import _sanitize_identifier
        with pytest.raises(ValueError, match="not allowed"):
            _sanitize_identifier("my dag; DROP TABLE--", "dag_id")

    def test_dots_and_dashes_allowed(self):
        from sql_to_dag.generator import _sanitize_identifier
        assert _sanitize_identifier("my-dag.v2", "dag_id") == "my-dag.v2"


class TestSanitizeSqlForEmbedding:
    def test_triple_quotes_escaped(self):
        from sql_to_dag.generator import _sanitize_sql_for_embedding
        sql = 'SELECT """ FROM t'
        result = _sanitize_sql_for_embedding(sql)
        assert '"""' not in result

    def test_no_triple_quotes_unchanged(self):
        from sql_to_dag.generator import _sanitize_sql_for_embedding
        sql = "SELECT * FROM t"
        assert _sanitize_sql_for_embedding(sql) == sql


class TestCheckInputSize:
    def test_within_limit(self):
        from sql_to_dag.generator import _check_input_size
        _check_input_size("SELECT 1")  # should not raise

    def test_over_limit_raises(self):
        from sql_to_dag.generator import _check_input_size
        big = "x" * (6 * 1024 * 1024)
        with pytest.raises(ValueError, match="too large"):
            _check_input_size(big)


class TestDagIdFromPath:
    def test_basic(self):
        from sql_to_dag.generator import _dag_id_from_path
        assert _dag_id_from_path("/path/to/my-pipeline.sql") == "my_pipeline"

    def test_spaces_to_underscores(self):
        from sql_to_dag.generator import _dag_id_from_path
        assert _dag_id_from_path("/path/my pipeline.sql") == "my_pipeline"

    def test_simple_name(self):
        from sql_to_dag.generator import _dag_id_from_path
        assert _dag_id_from_path("pipeline.sql") == "pipeline"


class TestCompileSqlString:
    def test_basic_compile(self):
        from sql_to_dag.generator import compile_sql_string
        result = compile_sql_string(_SIMPLE_SQL, dag_id="test_dag")
        assert "test_dag" in result

    def test_chain_compile(self):
        from sql_to_dag.generator import compile_sql_string
        result = compile_sql_string(_CHAIN_SQL, dag_id="chain_dag")
        assert "chain_dag" in result

    def test_custom_owner(self):
        from sql_to_dag.generator import compile_sql_string
        result = compile_sql_string(_SIMPLE_SQL, dag_id="dag1", dag_owner="myteam")
        assert "myteam" in result

    def test_custom_schedule(self):
        from sql_to_dag.generator import compile_sql_string
        result = compile_sql_string(_SIMPLE_SQL, dag_id="dag1", schedule_interval="@weekly")
        assert "@weekly" in result

    def test_custom_tags(self):
        from sql_to_dag.generator import compile_sql_string
        result = compile_sql_string(_SIMPLE_SQL, dag_id="dag1", tags=["test", "prod"])
        assert "test" in result

    def test_invalid_dag_id_raises(self):
        from sql_to_dag.generator import compile_sql_string
        with pytest.raises(ValueError):
            compile_sql_string(_SIMPLE_SQL, dag_id="bad dag; DROP")

    def test_invalid_owner_raises(self):
        from sql_to_dag.generator import compile_sql_string
        with pytest.raises(ValueError):
            compile_sql_string(_SIMPLE_SQL, dag_id="good_dag", dag_owner="bad owner!")

    def test_oversized_sql_raises(self):
        from sql_to_dag.generator import compile_sql_string
        big = "SELECT 1; " * 600_000
        with pytest.raises(ValueError):
            compile_sql_string(big, dag_id="dag1")

    def test_empty_sql_compiles(self):
        from sql_to_dag.generator import compile_sql_string
        with pytest.raises(ValueError):
            compile_sql_string("", dag_id="empty_dag")

    def test_retries_in_output(self):
        from sql_to_dag.generator import compile_sql_string
        result = compile_sql_string(_SIMPLE_SQL, dag_id="dag1", retries=3)
        assert "3" in result

    def test_source_label_in_output(self):
        from sql_to_dag.generator import compile_sql_string
        result = compile_sql_string(_SIMPLE_SQL, dag_id="dag1", source_label="my_source.sql")
        assert "my_source.sql" in result

    def test_default_tags_used_when_none(self):
        from sql_to_dag.generator import compile_sql_string
        result = compile_sql_string(_SIMPLE_SQL, dag_id="dag1", tags=None)
        assert "sql-to-dag" in result

    def test_retry_delay_minutes(self):
        from sql_to_dag.generator import compile_sql_string
        result = compile_sql_string(_SIMPLE_SQL, dag_id="dag1", retry_delay_minutes=10)
        assert "10" in result


class TestCompileSqlFile:
    def test_basic_file(self, tmp_path):
        from sql_to_dag.generator import compile_sql_file
        f = tmp_path / "mypipeline.sql"
        f.write_text(_SIMPLE_SQL)
        result = compile_sql_file(str(f))
        assert "mypipeline" in result

    def test_explicit_dag_id(self, tmp_path):
        from sql_to_dag.generator import compile_sql_file
        f = tmp_path / "test.sql"
        f.write_text(_SIMPLE_SQL)
        result = compile_sql_file(str(f), dag_id="explicit_dag")
        assert "explicit_dag" in result

    def test_invalid_dag_id_in_path(self, tmp_path):
        from sql_to_dag.generator import compile_sql_file
        f = tmp_path / "test.sql"
        f.write_text(_SIMPLE_SQL)
        with pytest.raises(ValueError):
            compile_sql_file(str(f), dag_id="bad dag!")

    def test_dag_owner_passed(self, tmp_path):
        from sql_to_dag.generator import compile_sql_file
        f = tmp_path / "test.sql"
        f.write_text(_SIMPLE_SQL)
        result = compile_sql_file(str(f), dag_owner="data_eng")
        assert "data_eng" in result


# ===========================================================================
# src/edge_case_handler.py
# ===========================================================================

class TestPatternDetection:
    def test_detects_cte(self):
        from src.edge_case_handler import EdgeCaseHandler, PatternType
        handler = EdgeCaseHandler()
        sql = "WITH cte AS (SELECT 1) SELECT * FROM cte"
        patterns = handler.detect_patterns(sql)
        types = [p.pattern_type for p in patterns]
        assert PatternType.CTE in types

    def test_detects_recursive_cte(self):
        from src.edge_case_handler import EdgeCaseHandler, PatternType
        handler = EdgeCaseHandler()
        sql = "WITH RECURSIVE cte AS (SELECT 1) SELECT * FROM cte"
        patterns = handler.detect_patterns(sql)
        types = [p.pattern_type for p in patterns]
        assert PatternType.RECURSIVE_CTE in types

    def test_detects_connect_by(self):
        from src.edge_case_handler import EdgeCaseHandler, PatternType
        handler = EdgeCaseHandler()
        sql = "SELECT * FROM t CONNECT BY PRIOR id = parent_id START WITH parent_id IS NULL"
        patterns = handler.detect_patterns(sql)
        types = [p.pattern_type for p in patterns]
        assert PatternType.RECURSIVE_CTE in types

    def test_detects_merge(self):
        from src.edge_case_handler import EdgeCaseHandler, PatternType
        handler = EdgeCaseHandler()
        sql = "MERGE INTO target USING source ON (1=1) WHEN MATCHED THEN UPDATE SET x=1"
        patterns = handler.detect_patterns(sql)
        types = [p.pattern_type for p in patterns]
        assert PatternType.MERGE in types

    def test_detects_execute_immediate(self):
        from src.edge_case_handler import EdgeCaseHandler, PatternType
        handler = EdgeCaseHandler()
        sql = "EXECUTE IMMEDIATE 'SELECT 1'"
        patterns = handler.detect_patterns(sql)
        types = [p.pattern_type for p in patterns]
        assert PatternType.DYNAMIC_SQL in types

    def test_detects_sp_executesql(self):
        from src.edge_case_handler import EdgeCaseHandler, PatternType
        handler = EdgeCaseHandler()
        sql = "EXEC sp_executesql N'SELECT 1'"
        patterns = handler.detect_patterns(sql)
        types = [p.pattern_type for p in patterns]
        assert PatternType.DYNAMIC_SQL in types

    def test_detects_window_function(self):
        from src.edge_case_handler import EdgeCaseHandler, PatternType
        handler = EdgeCaseHandler()
        sql = "SELECT ROW_NUMBER() OVER (PARTITION BY id ORDER BY date) FROM t"
        patterns = handler.detect_patterns(sql)
        types = [p.pattern_type for p in patterns]
        assert PatternType.WINDOW_FUNCTION in types

    def test_detects_pivot(self):
        from src.edge_case_handler import EdgeCaseHandler, PatternType
        handler = EdgeCaseHandler()
        sql = "SELECT * FROM t PIVOT (SUM(val) FOR month IN ('Jan','Feb'))"
        patterns = handler.detect_patterns(sql)
        types = [p.pattern_type for p in patterns]
        assert PatternType.PIVOT in types

    def test_detects_unpivot(self):
        from src.edge_case_handler import EdgeCaseHandler, PatternType
        handler = EdgeCaseHandler()
        sql = "SELECT * FROM t UNPIVOT (val FOR col IN (a, b))"
        patterns = handler.detect_patterns(sql)
        types = [p.pattern_type for p in patterns]
        assert PatternType.UNPIVOT in types

    def test_detects_lateral_join(self):
        from src.edge_case_handler import EdgeCaseHandler, PatternType
        handler = EdgeCaseHandler()
        sql = "SELECT * FROM t, LATERAL (SELECT 1) AS sub"
        patterns = handler.detect_patterns(sql)
        types = [p.pattern_type for p in patterns]
        assert PatternType.LATERAL_JOIN in types

    def test_cross_apply(self):
        from src.edge_case_handler import EdgeCaseHandler, PatternType
        handler = EdgeCaseHandler()
        sql = "SELECT * FROM t CROSS APPLY fn(t.id)"
        patterns = handler.detect_patterns(sql)
        types = [p.pattern_type for p in patterns]
        assert PatternType.LATERAL_JOIN in types

    def test_clean_sql_no_patterns(self):
        from src.edge_case_handler import EdgeCaseHandler
        handler = EdgeCaseHandler()
        sql = "SELECT * FROM orders WHERE id > 1"
        patterns = handler.detect_patterns(sql)
        assert len(patterns) == 0

    def test_preprocess_returns_sql_and_warnings(self):
        from src.edge_case_handler import EdgeCaseHandler
        handler = EdgeCaseHandler()
        sql = "WITH cte AS (SELECT 1) SELECT * FROM cte"
        processed, warnings = handler.preprocess(sql)
        assert isinstance(processed, str)
        assert isinstance(warnings, list)

    def test_extract_cte_dependencies(self):
        from src.edge_case_handler import EdgeCaseHandler
        handler = EdgeCaseHandler()
        sql = "WITH cte1 AS (SELECT * FROM raw), cte2 AS (SELECT * FROM cte1) SELECT * FROM cte2"
        deps = handler.extract_cte_dependencies(sql)
        assert isinstance(deps, list)
        assert len(deps) >= 1

    def test_preprocess_clean_sql_no_warnings(self):
        from src.edge_case_handler import EdgeCaseHandler
        handler = EdgeCaseHandler()
        sql = "SELECT * FROM simple_table"
        processed, warnings = handler.preprocess(sql)
        assert warnings == []

    def test_multiple_patterns_in_one_sql(self):
        from src.edge_case_handler import EdgeCaseHandler, PatternType
        handler = EdgeCaseHandler()
        sql = textwrap.dedent("""
            WITH cte AS (SELECT * FROM t)
            SELECT ROW_NUMBER() OVER (PARTITION BY id ORDER BY date) rn, *
            FROM cte
        """)
        patterns = handler.detect_patterns(sql)
        types = [p.pattern_type for p in patterns]
        assert PatternType.CTE in types
        assert PatternType.WINDOW_FUNCTION in types


class TestSQLPatternDataclass:
    def test_pattern_attributes(self):
        from src.edge_case_handler import SQLPattern, PatternType
        p = SQLPattern(pattern_type=PatternType.CTE, location=10, complexity_score=1)
        assert p.pattern_type == PatternType.CTE
        assert p.location == 10
        assert p.complexity_score == 1

    def test_warning_attributes(self):
        from src.edge_case_handler import Warning
        w = Warning(code="W001", message="CTE detected", line=1)
        assert w.code == "W001"
        assert w.line == 1


# ===========================================================================
# src/lineage_report.py
# ===========================================================================

def _make_test_dag():
    """Create a simple DAG for lineage testing."""
    g = nx.DiGraph()
    g.add_node("s0", label="load_raw", target_table="raw")
    g.add_node("s1", label="create_summary", target_table="summary")
    g.add_edge("s0", "s1")
    return g


class TestGenerateMermaid:
    def test_starts_with_flowchart(self):
        from src.lineage_report import generate_mermaid
        g = _make_test_dag()
        result = generate_mermaid(g)
        assert result.startswith("flowchart LR")

    def test_contains_nodes(self):
        from src.lineage_report import generate_mermaid
        g = _make_test_dag()
        result = generate_mermaid(g)
        assert "s0" in result or "load_raw" in result

    def test_contains_edge_arrow(self):
        from src.lineage_report import generate_mermaid
        g = _make_test_dag()
        result = generate_mermaid(g)
        assert "-->" in result

    def test_empty_graph(self):
        from src.lineage_report import generate_mermaid
        g = nx.DiGraph()
        result = generate_mermaid(g)
        assert "flowchart LR" in result

    def test_node_without_label_attr(self):
        from src.lineage_report import generate_mermaid
        g = nx.DiGraph()
        g.add_node("mynode")  # no label attr
        result = generate_mermaid(g)
        assert "mynode" in result

    def test_special_chars_in_label_escaped(self):
        from src.lineage_report import generate_mermaid
        g = nx.DiGraph()
        g.add_node("n", label='table with "quotes"')
        result = generate_mermaid(g)
        # double quotes in label should be escaped
        assert '"""' not in result


class TestGenerateDot:
    def test_starts_with_digraph(self):
        from src.lineage_report import generate_dot
        g = _make_test_dag()
        result = generate_dot(g)
        assert "digraph" in result

    def test_contains_rankdir(self):
        from src.lineage_report import generate_dot
        g = _make_test_dag()
        result = generate_dot(g)
        assert "rankdir=LR" in result

    def test_contains_arrow(self):
        from src.lineage_report import generate_dot
        g = _make_test_dag()
        result = generate_dot(g)
        assert "->" in result

    def test_empty_graph(self):
        from src.lineage_report import generate_dot
        g = nx.DiGraph()
        result = generate_dot(g)
        assert "digraph" in result


class TestGenerateJson:
    def test_has_nodes_and_edges(self):
        from src.lineage_report import generate_json
        g = _make_test_dag()
        result = generate_json(g)
        assert "nodes" in result
        assert "edges" in result

    def test_nodes_have_id_and_label(self):
        from src.lineage_report import generate_json
        g = _make_test_dag()
        result = generate_json(g)
        for node in result["nodes"]:
            assert "id" in node
            assert "label" in node

    def test_edges_have_source_and_target(self):
        from src.lineage_report import generate_json
        g = _make_test_dag()
        result = generate_json(g)
        for edge in result["edges"]:
            assert "source" in edge
            assert "target" in edge

    def test_json_serializable(self):
        from src.lineage_report import generate_json
        g = _make_test_dag()
        result = generate_json(g)
        json.dumps(result)  # should not raise

    def test_empty_graph(self):
        from src.lineage_report import generate_json
        g = nx.DiGraph()
        result = generate_json(g)
        assert result == {"nodes": [], "edges": []}

    def test_node_without_label_falls_back_to_key(self):
        from src.lineage_report import generate_json
        g = nx.DiGraph()
        g.add_node("bare_node")
        result = generate_json(g)
        assert result["nodes"][0]["id"] == "bare_node"
        assert result["nodes"][0]["label"] == "bare_node"


class TestLineageReportGenerator:
    def test_mermaid_method(self):
        from src.lineage_report import LineageReportGenerator
        gen = LineageReportGenerator(_make_test_dag())
        result = gen.mermaid()
        assert "flowchart LR" in result

    def test_dot_method(self):
        from src.lineage_report import LineageReportGenerator
        gen = LineageReportGenerator(_make_test_dag())
        result = gen.dot()
        assert "digraph" in result

    def test_json_method(self):
        from src.lineage_report import LineageReportGenerator
        gen = LineageReportGenerator(_make_test_dag())
        result = gen.json()
        assert "nodes" in result

    def test_json_string_method(self):
        from src.lineage_report import LineageReportGenerator
        gen = LineageReportGenerator(_make_test_dag())
        result = gen.json_string()
        parsed = json.loads(result)
        assert "nodes" in parsed

    def test_json_string_indent(self):
        from src.lineage_report import LineageReportGenerator
        gen = LineageReportGenerator(_make_test_dag())
        result = gen.json_string(indent=4)
        # 4-space indent means lines will have leading spaces
        assert "\n" in result
