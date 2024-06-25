# Changelog

## v2.0.0 — 2026-05-30

### Added

- **dbt model parser** (`src/dbt_parser.py`): parses `{{ ref() }}`, `{{ source() }}`, `{{ config(materialized=...) }}`, and `{% if is_incremental() %}` blocks from dbt model SQL; scans project directories recursively; generates Airflow 2.x DAGs with `BashOperator` tasks running `dbt run --select {model_name}` and dependency wiring derived from `{{ ref() }}` relationships.
- **Edge-case SQL handler** (`src/edge_case_handler.py`): detects and preprocesses CTEs (single and multi-CTE `WITH` clauses), recursive CTEs (`WITH RECURSIVE`, Oracle `CONNECT BY`), `MERGE` statements, dynamic SQL (`EXECUTE IMMEDIATE`, `sp_executesql`), stored-procedure `OUT`/`OUTPUT` parameters, window functions (`OVER`), `PIVOT`/`UNPIVOT`, and lateral joins (`LATERAL`, `CROSS APPLY`, `OUTER APPLY`); emits structured warnings with line numbers; strips comments and Jinja2 expressions.
- **Lineage report generator** (`src/lineage_report.py`): exports a `networkx.DiGraph` dependency graph as a Mermaid `flowchart LR` diagram, Graphviz DOT format, and a JSON adjacency list (`{"nodes": [...], "edges": [...]}`); available as standalone functions and as the `LineageReportGenerator` convenience class.
- **V2 test suite** (`tests/test_v2.py`): 62 tests covering all new modules end-to-end.

---

## v1.0.0 — 2026-05-30

### Initial Release

- **SQL parser** (`sql_to_dag/parser.py`): splits Oracle SQL/PLSQL files into individual statements using `sqlparse`; extracts statement type (`CTAS`, `INSERT_SELECT`, `INSERT_VALUES`), target table, source tables, aggregation functions, `WHERE`/`GROUP BY` presence; strips PL/SQL block delimiters and SQL comments.
- **Dependency graph** (`sql_to_dag/graph.py`): builds a `networkx.DiGraph` from parsed statement metadata; adds a directed edge wherever one statement produces a table consumed by another; validates acyclicity; exposes topological sort and predecessor lookup.
- **DAG generator** (`sql_to_dag/generator.py`): renders a complete Airflow 2.x Python DAG file via Jinja2 from the topologically-ordered statement list; embeds SQL metadata as `doc_md`; emits `set_upstream()` calls matching graph edges; provides a CLI entry point (`python -m sql_to_dag.generator`).
- **74 tests** across parser, graph, and generator modules — all passing.
