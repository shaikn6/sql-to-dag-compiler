"""
src — V2 extensions for sql-to-dag-compiler.

Modules:
    dbt_parser       — Parse dbt models and project directories → Airflow DAGs
    edge_case_handler — Detect and preprocess complex SQL patterns
    lineage_report   — Export dependency graphs as Mermaid, DOT, and JSON
"""
