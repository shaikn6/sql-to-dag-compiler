"""
tests/test_v2.py — V2 test suite.

Covers:
    - DbtModelParser: ref(), source(), config(), incremental detection
    - DAG generation: valid Python syntax, task count, dependency wiring
    - EdgeCaseHandler: CTE, MERGE, recursive CTE detection, preprocess
    - LineageReportGenerator: Mermaid, DOT, JSON outputs
    - Full pipeline: dbt SQL → parse → to_airflow_dag → valid Python
"""

from __future__ import annotations

import ast
import os
import sys
import textwrap
import json

import networkx as nx
import pytest

# Make sure src/ is importable regardless of working directory
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.dbt_parser import DbtModel, DbtModelParser, DbtProject
from src.edge_case_handler import EdgeCaseHandler, PatternType, SQLPattern, Warning
from src.lineage_report import generate_dot, generate_json, generate_mermaid, LineageReportGenerator


# ===========================================================================
# Fixtures
# ===========================================================================

@pytest.fixture
def parser() -> DbtModelParser:
    return DbtModelParser()


@pytest.fixture
def handler() -> EdgeCaseHandler:
    return EdgeCaseHandler()


@pytest.fixture
def simple_dag() -> nx.DiGraph:
    dag = nx.DiGraph()
    dag.add_node("orders", label="orders")
    dag.add_node("revenue", label="revenue")
    dag.add_edge("orders", "revenue")
    return dag


@pytest.fixture
def multi_node_dag() -> nx.DiGraph:
    dag = nx.DiGraph()
    for name in ["raw_orders", "stg_orders", "fct_orders", "dim_customers"]:
        dag.add_node(name, label=name)
    dag.add_edge("raw_orders", "stg_orders")
    dag.add_edge("stg_orders", "fct_orders")
    dag.add_edge("dim_customers", "fct_orders")
    return dag


# ===========================================================================
# DbtModelParser — ref() extraction
# ===========================================================================

class TestDbtParserRefs:

    def test_single_ref_extracted(self, parser):
        sql = "SELECT * FROM {{ ref('orders') }}"
        model = parser.parse_model(sql, "revenue")
        assert "orders" in model.deps

    def test_multiple_refs_extracted(self, parser):
        sql = """
        SELECT o.*, c.name
        FROM {{ ref('stg_orders') }} o
        JOIN {{ ref('stg_customers') }} c ON o.customer_id = c.id
        """
        model = parser.parse_model(sql, "fct_orders")
        assert "stg_orders" in model.deps
        assert "stg_customers" in model.deps
        assert len(model.deps) == 2

    def test_ref_double_quotes(self, parser):
        sql = 'SELECT * FROM {{ ref("my_model") }}'
        model = parser.parse_model(sql, "downstream")
        assert "my_model" in model.deps

    def test_no_refs_returns_empty_list(self, parser):
        sql = "SELECT 1 AS id"
        model = parser.parse_model(sql, "simple")
        assert model.deps == []

    def test_duplicate_refs_deduped(self, parser):
        sql = """
        SELECT a.*, b.*
        FROM {{ ref('orders') }} a
        JOIN {{ ref('orders') }} b ON a.id = b.parent_id
        """
        model = parser.parse_model(sql, "self_join")
        assert model.deps.count("orders") == 1


# ===========================================================================
# DbtModelParser — source() extraction
# ===========================================================================

class TestDbtParserSources:

    def test_single_source_extracted(self, parser):
        sql = "SELECT * FROM {{ source('raw', 'transactions') }}"
        model = parser.parse_model(sql, "stg_transactions")
        assert ("raw", "transactions") in model.sources

    def test_multiple_sources(self, parser):
        sql = """
        SELECT t.*, c.name
        FROM {{ source('raw', 'transactions') }} t
        JOIN {{ source('raw', 'customers') }} c ON t.customer_id = c.id
        """
        model = parser.parse_model(sql, "stg_enriched")
        assert ("raw", "transactions") in model.sources
        assert ("raw", "customers") in model.sources

    def test_no_sources_returns_empty_list(self, parser):
        sql = "SELECT * FROM {{ ref('other_model') }}"
        model = parser.parse_model(sql, "derived")
        assert model.sources == []

    def test_source_with_double_quotes(self, parser):
        sql = 'SELECT * FROM {{ source("jaffle_shop", "orders") }}'
        model = parser.parse_model(sql, "stg_orders")
        assert ("jaffle_shop", "orders") in model.sources


# ===========================================================================
# DbtModelParser — config() parsing
# ===========================================================================

class TestDbtParserConfig:

    def test_materialized_table(self, parser):
        sql = "{{ config(materialized='table') }}\nSELECT 1"
        model = parser.parse_model(sql, "my_table")
        assert model.materialization == "table"

    def test_materialized_view(self, parser):
        sql = "{{ config(materialized='view') }}\nSELECT 1"
        model = parser.parse_model(sql, "my_view")
        assert model.materialization == "view"

    def test_materialized_incremental(self, parser):
        sql = "{{ config(materialized='incremental') }}\nSELECT 1"
        model = parser.parse_model(sql, "my_inc")
        assert model.materialization == "incremental"

    def test_default_materialization_is_view(self, parser):
        sql = "SELECT * FROM some_table"
        model = parser.parse_model(sql, "no_config")
        assert model.materialization == "view"

    def test_config_with_extra_params(self, parser):
        sql = "{{ config(materialized='table', unique_key='id', tags=['daily']) }}\nSELECT 1"
        model = parser.parse_model(sql, "complex_config")
        assert model.materialization == "table"


# ===========================================================================
# DbtModelParser — incremental detection
# ===========================================================================

class TestDbtParserIncremental:

    def test_is_incremental_detected(self, parser):
        sql = textwrap.dedent("""\
            {{ config(materialized='incremental') }}
            SELECT *
            FROM raw.events
            {% if is_incremental() %}
            WHERE event_time > (SELECT MAX(event_time) FROM {{ this }})
            {% endif %}
        """)
        model = parser.parse_model(sql, "events_incremental")
        assert model.is_incremental is True

    def test_not_incremental(self, parser):
        sql = "SELECT * FROM raw.customers"
        model = parser.parse_model(sql, "customers_full")
        assert model.is_incremental is False

    def test_incremental_with_whitespace_variants(self, parser):
        sql = "{%- if is_incremental() -%}\nWHERE id > 0\n{%- endif -%}"
        model = parser.parse_model(sql, "inc_model")
        assert model.is_incremental is True


# ===========================================================================
# DAG generation — valid Python, task count, dependency wiring
# ===========================================================================

class TestDbtDagGeneration:

    def _make_two_model_project(self, parser) -> DbtProject:
        sql_a = "SELECT * FROM {{ source('raw', 'orders') }}"
        sql_b = "SELECT * FROM {{ ref('stg_orders') }}"
        model_a = parser.parse_model(sql_a, "stg_orders")
        model_b = parser.parse_model(sql_b, "fct_orders")
        dep_graph = {
            "stg_orders": [],
            "fct_orders": ["stg_orders"],
        }
        return DbtProject(models=[model_a, model_b], dependency_graph=dep_graph)

    def test_dag_is_valid_python(self, parser):
        project = self._make_two_model_project(parser)
        dag_source = parser.to_airflow_dag(project, dag_id="test_dag")
        # Should parse without SyntaxError
        tree = ast.parse(dag_source)
        assert tree is not None

    def test_dag_contains_correct_task_count(self, parser):
        project = self._make_two_model_project(parser)
        dag_source = parser.to_airflow_dag(project)
        # Two models → two BashOperator assignments
        assert dag_source.count("BashOperator(") == 2

    def test_dag_wires_dependencies(self, parser):
        project = self._make_two_model_project(parser)
        dag_source = parser.to_airflow_dag(project)
        assert "set_downstream" in dag_source

    def test_dag_contains_dag_id(self, parser):
        project = self._make_two_model_project(parser)
        dag_source = parser.to_airflow_dag(project, dag_id="my_custom_dag")
        assert "my_custom_dag" in dag_source

    def test_dag_imports_bash_operator(self, parser):
        project = self._make_two_model_project(parser)
        dag_source = parser.to_airflow_dag(project)
        assert "BashOperator" in dag_source
        assert "from airflow" in dag_source

    def test_dag_contains_dbt_run_command(self, parser):
        project = self._make_two_model_project(parser)
        dag_source = parser.to_airflow_dag(project)
        assert "dbt run" in dag_source

    def test_empty_project_raises(self, parser):
        empty_project = DbtProject(models=[], dependency_graph={})
        with pytest.raises(ValueError, match="no models"):
            parser.to_airflow_dag(empty_project)

    def test_single_model_no_dependencies(self, parser):
        sql = "SELECT 1 AS id"
        model = parser.parse_model(sql, "standalone")
        project = DbtProject(models=[model], dependency_graph={"standalone": []})
        dag_source = parser.to_airflow_dag(project)
        assert "standalone" in dag_source
        # No dependency lines expected
        assert "set_downstream" not in dag_source


# ===========================================================================
# EdgeCaseHandler — CTE detection
# ===========================================================================

class TestEdgeCaseCteDetection:

    def test_single_cte_detected(self, handler):
        sql = "WITH cte AS (SELECT 1) SELECT * FROM cte"
        patterns = handler.detect_patterns(sql)
        types = [p.pattern_type for p in patterns]
        assert PatternType.CTE in types

    def test_multiple_ctes_detected(self, handler):
        sql = """
        WITH
          cte_a AS (SELECT 1 AS a),
          cte_b AS (SELECT a FROM cte_a)
        SELECT * FROM cte_b
        """
        patterns = handler.detect_patterns(sql)
        types = [p.pattern_type for p in patterns]
        assert PatternType.CTE in types

    def test_no_cte_no_detection(self, handler):
        sql = "SELECT * FROM orders WHERE amount > 100"
        patterns = handler.detect_patterns(sql)
        types = [p.pattern_type for p in patterns]
        assert PatternType.CTE not in types


# ===========================================================================
# EdgeCaseHandler — MERGE detection
# ===========================================================================

class TestEdgeCaseMergeDetection:

    def test_merge_detected(self, handler):
        sql = """
        MERGE INTO target_table t
        USING source_table s ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET t.val = s.val
        WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.val)
        """
        patterns = handler.detect_patterns(sql)
        types = [p.pattern_type for p in patterns]
        assert PatternType.MERGE in types

    def test_no_merge_in_plain_select(self, handler):
        sql = "SELECT * FROM orders"
        patterns = handler.detect_patterns(sql)
        types = [p.pattern_type for p in patterns]
        assert PatternType.MERGE not in types


# ===========================================================================
# EdgeCaseHandler — Recursive CTE detection
# ===========================================================================

class TestEdgeCaseRecursiveCte:

    def test_with_recursive_detected(self, handler):
        sql = """
        WITH RECURSIVE cte(n) AS (
          SELECT 1
          UNION ALL
          SELECT n + 1 FROM cte WHERE n < 10
        )
        SELECT * FROM cte
        """
        patterns = handler.detect_patterns(sql)
        types = [p.pattern_type for p in patterns]
        assert PatternType.RECURSIVE_CTE in types

    def test_connect_by_detected_as_recursive(self, handler):
        sql = """
        SELECT employee_id, manager_id, LEVEL
        FROM employees
        START WITH manager_id IS NULL
        CONNECT BY PRIOR employee_id = manager_id
        """
        patterns = handler.detect_patterns(sql)
        types = [p.pattern_type for p in patterns]
        assert PatternType.RECURSIVE_CTE in types

    def test_window_function_detected(self, handler):
        sql = "SELECT ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) FROM employees"
        patterns = handler.detect_patterns(sql)
        types = [p.pattern_type for p in patterns]
        assert PatternType.WINDOW_FUNCTION in types


# ===========================================================================
# EdgeCaseHandler — preprocess
# ===========================================================================

class TestEdgeCasePreprocess:

    def test_preprocess_removes_block_comments(self, handler):
        sql = "SELECT /* this is a comment */ 1 AS id"
        cleaned, warnings = handler.preprocess(sql)
        assert "/*" not in cleaned
        assert "*/" not in cleaned

    def test_preprocess_removes_line_comments(self, handler):
        sql = "SELECT 1 -- inline comment\nFROM dual"
        cleaned, warnings = handler.preprocess(sql)
        assert "--" not in cleaned
        assert "inline comment" not in cleaned

    def test_preprocess_does_not_crash_on_complex_sql(self, handler):
        sql = textwrap.dedent("""\
            WITH RECURSIVE cte AS (SELECT 1)
            MERGE INTO t USING s ON t.id = s.id
            WHEN MATCHED THEN UPDATE SET t.v = s.v;
        """)
        cleaned, warnings = handler.preprocess(sql)
        assert isinstance(cleaned, str)
        assert isinstance(warnings, list)

    def test_preprocess_returns_warnings_for_dynamic_sql(self, handler):
        sql = "EXECUTE IMMEDIATE 'SELECT 1'"
        cleaned, warnings = handler.preprocess(sql)
        codes = [w.code for w in warnings]
        assert "W005" in codes

    def test_preprocess_strips_jinja_expressions(self, handler):
        sql = "SELECT * FROM {{ ref('my_model') }}"
        cleaned, warnings = handler.preprocess(sql)
        assert "{{" not in cleaned
        assert "}}" not in cleaned

    def test_preprocess_warning_has_line_number(self, handler):
        sql = "SELECT 1\nMERGE INTO t USING s ON t.id = s.id"
        cleaned, warnings = handler.preprocess(sql)
        merge_warnings = [w for w in warnings if w.code == "W004"]
        assert len(merge_warnings) > 0
        assert merge_warnings[0].line >= 1


# ===========================================================================
# LineageReport — Mermaid
# ===========================================================================

class TestLineageReportMermaid:

    def test_mermaid_starts_with_flowchart(self, simple_dag):
        result = generate_mermaid(simple_dag)
        assert result.startswith("flowchart")

    def test_mermaid_contains_node_ids(self, simple_dag):
        result = generate_mermaid(simple_dag)
        assert "orders" in result
        assert "revenue" in result

    def test_mermaid_contains_arrow(self, simple_dag):
        result = generate_mermaid(simple_dag)
        assert "-->" in result

    def test_mermaid_multi_node_dag(self, multi_node_dag):
        result = generate_mermaid(multi_node_dag)
        assert result.startswith("flowchart")
        assert "raw_orders" in result
        assert "fct_orders" in result


# ===========================================================================
# LineageReport — DOT
# ===========================================================================

class TestLineageReportDot:

    def test_dot_contains_digraph(self, simple_dag):
        result = generate_dot(simple_dag)
        assert "digraph" in result

    def test_dot_contains_nodes(self, simple_dag):
        result = generate_dot(simple_dag)
        assert "orders" in result
        assert "revenue" in result

    def test_dot_contains_edge_arrow(self, simple_dag):
        result = generate_dot(simple_dag)
        assert "->" in result

    def test_dot_closes_brace(self, simple_dag):
        result = generate_dot(simple_dag)
        assert result.strip().endswith("}")


# ===========================================================================
# LineageReport — JSON
# ===========================================================================

class TestLineageReportJson:

    def test_json_has_nodes_key(self, simple_dag):
        result = generate_json(simple_dag)
        assert "nodes" in result

    def test_json_has_edges_key(self, simple_dag):
        result = generate_json(simple_dag)
        assert "edges" in result

    def test_json_nodes_correct_count(self, simple_dag):
        result = generate_json(simple_dag)
        assert len(result["nodes"]) == 2

    def test_json_edges_correct_count(self, simple_dag):
        result = generate_json(simple_dag)
        assert len(result["edges"]) == 1

    def test_json_is_serialisable(self, simple_dag):
        result = generate_json(simple_dag)
        serialised = json.dumps(result)
        assert isinstance(serialised, str)

    def test_json_edge_has_source_and_target(self, simple_dag):
        result = generate_json(simple_dag)
        edge = result["edges"][0]
        assert "source" in edge
        assert "target" in edge

    def test_json_multi_dag(self, multi_node_dag):
        result = generate_json(multi_node_dag)
        assert len(result["nodes"]) == 4
        assert len(result["edges"]) == 3


# ===========================================================================
# LineageReportGenerator class
# ===========================================================================

class TestLineageReportGeneratorClass:

    def test_class_mermaid(self, simple_dag):
        gen = LineageReportGenerator(simple_dag)
        assert gen.mermaid().startswith("flowchart")

    def test_class_dot(self, simple_dag):
        gen = LineageReportGenerator(simple_dag)
        assert "digraph" in gen.dot()

    def test_class_json(self, simple_dag):
        gen = LineageReportGenerator(simple_dag)
        result = gen.json()
        assert "nodes" in result and "edges" in result

    def test_class_json_string_is_valid_json(self, simple_dag):
        gen = LineageReportGenerator(simple_dag)
        parsed = json.loads(gen.json_string())
        assert "nodes" in parsed


# ===========================================================================
# Full pipeline: dbt SQL → parse → to_airflow_dag → valid Python
# ===========================================================================

class TestFullPipeline:

    def test_full_pipeline_three_models(self, parser):
        """End-to-end: parse 3 interconnected dbt models and generate a valid DAG."""
        sql_raw = "SELECT * FROM {{ source('prod', 'raw_events') }}"
        sql_stg = "{{ config(materialized='view') }}\nSELECT * FROM {{ ref('raw_events') }}"
        sql_fct = textwrap.dedent("""\
            {{ config(materialized='incremental') }}
            SELECT *
            FROM {{ ref('stg_events') }}
            {% if is_incremental() %}
            WHERE created_at > (SELECT MAX(created_at) FROM {{ this }})
            {% endif %}
        """)

        model_raw = parser.parse_model(sql_raw, "raw_events")
        model_stg = parser.parse_model(sql_stg, "stg_events")
        model_fct = parser.parse_model(sql_fct, "fct_events")

        dep_graph = {
            "raw_events": [],
            "stg_events": ["raw_events"],
            "fct_events": ["stg_events"],
        }
        project = DbtProject(
            models=[model_raw, model_stg, model_fct],
            dependency_graph=dep_graph,
        )

        dag_source = parser.to_airflow_dag(project, dag_id="events_pipeline")

        # Must parse as valid Python
        tree = ast.parse(dag_source)
        assert tree is not None

        # Must contain all three model names
        assert "raw_events" in dag_source
        assert "stg_events" in dag_source
        assert "fct_events" in dag_source

        # Must have 3 BashOperator tasks
        assert dag_source.count("BashOperator(") == 3

        # Must wire the two dependency edges
        assert dag_source.count("set_downstream") == 2

    def test_full_pipeline_incremental_flag_preserved(self, parser):
        sql = textwrap.dedent("""\
            {{ config(materialized='incremental') }}
            SELECT id FROM {{ source('raw', 'events') }}
            {% if is_incremental() %}WHERE id > 0{% endif %}
        """)
        model = parser.parse_model(sql, "inc_model")
        assert model.is_incremental is True
        assert model.materialization == "incremental"

    def test_full_pipeline_dag_with_schedule(self, parser):
        sql = "SELECT 1 AS id"
        model = parser.parse_model(sql, "single_model")
        project = DbtProject(models=[model], dependency_graph={"single_model": []})
        dag_source = parser.to_airflow_dag(project, schedule_interval="0 6 * * *")
        assert "0 6 * * *" in dag_source

    def test_extract_cte_dependencies_order(self, handler):
        sql = textwrap.dedent("""\
            WITH
              base AS (SELECT * FROM raw),
              enriched AS (SELECT b.*, c.name FROM base b JOIN customers c ON b.id = c.id),
              final AS (SELECT * FROM enriched WHERE amount > 0)
            SELECT * FROM final
        """)
        cte_names = handler.extract_cte_dependencies(sql)
        assert "base" in cte_names
        assert "enriched" in cte_names
        assert "final" in cte_names
        # base must appear before enriched, enriched before final
        assert cte_names.index("base") < cte_names.index("enriched")
        assert cte_names.index("enriched") < cte_names.index("final")
