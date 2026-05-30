# Architecture

## Overview

`sql-to-dag-compiler` is a three-stage compiler:

```
Oracle SQL / PLSQL File
        │
        ▼
┌───────────────┐
│   parser.py   │  sqlparse splits statements; regex extracts
│               │  source/target tables, aggregations, flags.
└──────┬────────┘
       │  list[StatementDict]
       ▼
┌───────────────┐
│    graph.py   │  networkx DiGraph; edge A→B when A produces
│               │  a table that B consumes. Topological sort
│               │  gives a valid execution order.
└──────┬────────┘
       │  DiGraph + ordered node list
       ▼
┌───────────────┐
│  generator.py │  Jinja2 renders dag_template.py.j2 with
│               │  task list + dependency lines → valid .py
└──────┬────────┘
       │
       ▼
  Airflow 2.x DAG Python file
```

## Component Details

### parser.py

- Uses `sqlparse.split()` to tokenize the file into individual statements.
- Strips SQL single-line (`--`) and block (`/* */`) comments before applying regexes to avoid false table matches from comment text.
- Detects statement types: `CTAS` (CREATE TABLE AS SELECT), `INSERT_SELECT`, `INSERT_VALUES`.
- Extracts source tables via `FROM`/`JOIN` clause regex; extracts target via `CREATE TABLE`/`INSERT INTO` regex.
- Returns a list of metadata dicts — one per statement.

### graph.py

- Receives the metadata list and builds a `networkx.DiGraph`.
- A directed edge `A → B` is created when statement A's `target_table` appears in statement B's `source_tables`.
- Validates the graph is a DAG (raises `ValueError` on circular dependencies).
- `topological_order()` returns statement IDs in a valid execution sequence.

### generator.py

- Orchestrates parser → graph → renderer.
- Builds a `tasks` list ordered by topological sort, enriched with all metadata.
- Converts graph edges into Airflow `set_upstream()` calls.
- Renders via Jinja2 `dag_template.py.j2` → a syntactically valid Python file.
- Exposes both a Python API (`compile_sql_file`, `compile_sql_string`) and a CLI (`python -m sql_to_dag.generator`).

### dag_template.py.j2

- Jinja2 template producing an Airflow 2.x DAG.
- Uses `PythonOperator` with an `execute_sql()` stub — swap for `RedshiftSQLHook` in production.
- Embeds all SQL as a `SQL_STATEMENTS` dict; each task's `doc_md` includes lineage metadata.
- Dependency lines (`set_upstream`) are injected at the bottom of the `with DAG(...)` block.

## Data Flow Example

Input: three Oracle SQL statements (`staging.customer_txn` → `mart.customer_summary` → `mart.high_value_customers`).

```
parser detects:
  stmt_0: target=staging.customer_txn, sources=[raw.transactions]
  stmt_1: target=mart.customer_summary, sources=[staging.customer_txn, raw.customers]
  stmt_2: target=mart.high_value_customers, sources=[mart.customer_summary]

graph adds edges:
  stmt_0 → stmt_1  (staging.customer_txn is consumed by stmt_1)
  stmt_1 → stmt_2  (mart.customer_summary is consumed by stmt_2)

topological order: [stmt_0, stmt_1, stmt_2]

generator emits:
  create_customer_summary.set_upstream(create_customer_txn)
  insert_high_value_customers.set_upstream(create_customer_summary)
```

## Extension Points

| Goal | Where to change |
|------|----------------|
| Support `MERGE` statements | `parser.py` — add detection in `_detect_statement_type` |
| Output `SQLExecuteQueryOperator` | `dag_template.py.j2` — swap operator import + instantiation |
| Add schema validation | `graph.py` — post-processing after `build_dependency_graph` |
| Support multiple files | `generator.py` — merge statement lists before graph build |
| Redshift-specific SQL rewrites | New `rewriter.py` module between parser and generator |
