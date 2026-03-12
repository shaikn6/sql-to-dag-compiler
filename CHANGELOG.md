# Changelog

## [Unreleased]

## [1.0.0] - 2026-06-17
### Added
- SQL query parser that compiles SELECT statements into Airflow DAG definitions
- Dependency resolution engine that identifies upstream table lineage
- Auto-generated task operators for BigQuery, Snowflake, and Redshift targets
- Visual DAG preview rendered from compiled dependency graph
- Support for CTEs and subquery decomposition into modular tasks
- Validation layer that catches circular dependencies at compile time
