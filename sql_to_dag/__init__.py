"""
sql_to_dag — Converts Oracle SQL/PLSQL stored procedures into Apache Airflow 2.x DAGs.

Components:
    parser    — Parses SQL statements into structured metadata dicts.
    graph     — Builds a directed dependency graph using networkx.
    generator — Renders a valid Airflow 2.x DAG Python file via Jinja2.
"""

__version__ = "1.0.0"
__author__ = "Nagizaaz Shaik"
