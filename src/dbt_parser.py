"""
dbt_parser.py — Parse dbt model SQL files and project directories.

Extracts:
    - {{ ref('other_model') }}             — inter-model dependencies
    - {{ source('schema', 'table') }}      — external source references
    - {{ config(materialized='...') }}     — model materialization config
    - {% if is_incremental() %} blocks     — incremental logic detection

Generates valid Airflow 2.x DAG Python source where each dbt model becomes
a BashOperator running ``dbt run --select {model_name}``.
"""

from __future__ import annotations

import os
import re
import textwrap
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------

@dataclass
class DbtModel:
    """Parsed representation of a single dbt model."""
    name: str
    sql: str
    deps: list[str] = field(default_factory=list)        # ref() dependencies
    sources: list[tuple[str, str]] = field(default_factory=list)  # (schema, table) pairs
    materialization: str = "view"                         # table | view | incremental | ephemeral
    is_incremental: bool = False


@dataclass
class DbtProject:
    """A parsed dbt project: all models + their dependency graph."""
    models: list[DbtModel] = field(default_factory=list)
    # Maps model_name → list of model names it depends on (upstream)
    dependency_graph: dict[str, list[str]] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Regex patterns
# ---------------------------------------------------------------------------

# {{ ref('model_name') }} or {{ ref("model_name") }}
_REF_RE = re.compile(
    r"\{\{\s*ref\(\s*['\"]([^'\"]+)['\"]\s*\)\s*\}\}",
    re.IGNORECASE,
)

# {{ source('schema', 'table') }} or double-quoted variants
_SOURCE_RE = re.compile(
    r"\{\{\s*source\(\s*['\"]([^'\"]+)['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*\)\s*\}\}",
    re.IGNORECASE,
)

# {{ config(materialized='...') }} — captures the value
_CONFIG_MATERIALIZED_RE = re.compile(
    r"\{\{\s*config\s*\([^)]*materialized\s*=\s*['\"]([^'\"]+)['\"][^)]*\)\s*\}\}",
    re.IGNORECASE,
)

# {% if is_incremental() %}
_INCREMENTAL_RE = re.compile(
    r"\{%-?\s*if\s+is_incremental\(\)\s*-?%\}",
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

class DbtModelParser:
    """Parses individual dbt model SQL and entire dbt project directories."""

    # ------------------------------------------------------------------
    # Single-model parsing
    # ------------------------------------------------------------------

    def parse_model(self, dbt_sql: str, model_name: str) -> DbtModel:
        """
        Parse a dbt model SQL string.

        Parameters
        ----------
        dbt_sql:
            Raw contents of a ``.sql`` dbt model file (may contain Jinja2 blocks).
        model_name:
            The logical name of the model (usually the filename stem).

        Returns
        -------
        DbtModel
            Populated dataclass with dependencies, sources, materialization, and
            incremental flag extracted from the Jinja2 templating layer.
        """
        deps = self._extract_refs(dbt_sql)
        sources = self._extract_sources(dbt_sql)
        materialization = self._extract_materialization(dbt_sql)
        is_incremental = self._detect_incremental(dbt_sql)

        return DbtModel(
            name=model_name,
            sql=dbt_sql,
            deps=deps,
            sources=sources,
            materialization=materialization,
            is_incremental=is_incremental,
        )

    # ------------------------------------------------------------------
    # Project parsing
    # ------------------------------------------------------------------

    def parse_project(self, models_dir: str) -> DbtProject:
        """
        Scan *models_dir* for ``.sql`` files, parse each as a dbt model,
        and build a dependency graph.

        Parameters
        ----------
        models_dir:
            Path to the directory containing ``.sql`` dbt model files.
            The directory is scanned recursively.

        Returns
        -------
        DbtProject
            All parsed models plus a ``dependency_graph`` mapping each model
            name to the list of upstream model names it depends on.
        """
        models_path = Path(models_dir)
        if not models_path.is_dir():
            raise FileNotFoundError(f"models_dir not found: {models_dir}")

        models: list[DbtModel] = []
        for sql_file in sorted(models_path.rglob("*.sql")):
            model_name = sql_file.stem
            sql_text = sql_file.read_text(encoding="utf-8")
            model = self.parse_model(sql_text, model_name)
            models.append(model)

        # Build dependency graph restricted to known model names.
        known_names = {m.name for m in models}
        dependency_graph: dict[str, list[str]] = {}
        for model in models:
            # Only keep deps that resolve to another model in this project.
            resolved_deps = [d for d in model.deps if d in known_names]
            dependency_graph[model.name] = resolved_deps

        return DbtProject(models=models, dependency_graph=dependency_graph)

    # ------------------------------------------------------------------
    # DAG generation
    # ------------------------------------------------------------------

    def to_airflow_dag(
        self,
        project: DbtProject,
        dag_id: str = "dbt_project_dag",
        dag_owner: str = "airflow",
        schedule_interval: str = "@daily",
        retries: int = 1,
        retry_delay_minutes: int = 5,
        tags: list[str] | None = None,
        dbt_profiles_dir: str = "~/.dbt",
        dbt_project_dir: str = ".",
    ) -> str:
        """
        Generate an Airflow 2.x DAG Python source string from a parsed DbtProject.

        Each dbt model becomes a ``BashOperator`` that runs::

            dbt run --select {model_name}

        Dependencies expressed as ``{{ ref(...) }}`` are wired as Airflow task
        dependencies using ``set_downstream``.

        Parameters
        ----------
        project:
            A ``DbtProject`` returned by :meth:`parse_project`.
        dag_id:
            Airflow ``dag_id`` for the generated DAG.
        dag_owner:
            ``owner`` field in ``default_args``.
        schedule_interval:
            Cron or preset (``"@daily"`` etc.).
        retries, retry_delay_minutes:
            Task retry configuration.
        tags:
            Airflow DAG tags.
        dbt_profiles_dir, dbt_project_dir:
            Passed as ``--profiles-dir`` / ``--project-dir`` to the ``dbt`` CLI.

        Returns
        -------
        str
            Valid Python source for an Airflow DAG file.
        """
        if not project.models:
            raise ValueError("DbtProject contains no models — nothing to compile.")

        now = datetime.now(tz=timezone.utc)
        effective_tags = tags or ["dbt", "sql-to-dag", "generated"]

        # Topologically sort models so upstream tasks are defined before downstream.
        topo_order = self._topological_sort(project)

        # Build task variable name → model name mapping (safe Python identifier)
        def task_var(name: str) -> str:
            return re.sub(r"[^a-zA-Z0-9_]", "_", name)

        lines: list[str] = []

        # ---- header ----
        lines.append('"""')
        lines.append(f"Auto-generated dbt Airflow DAG: {dag_id}")
        lines.append(f"Generated at: {now.strftime('%Y-%m-%d %H:%M:%S UTC')}")
        lines.append(f"Models: {len(project.models)}")
        lines.append('"""')
        lines.append("")
        lines.append("from datetime import datetime, timedelta")
        lines.append("")
        lines.append("from airflow import DAG")
        lines.append("from airflow.operators.bash import BashOperator")
        lines.append("")

        # ---- default_args ----
        lines.append("default_args = {")
        lines.append(f'    "owner": "{dag_owner}",')
        lines.append(f'    "retries": {retries},')
        lines.append(f'    "retry_delay": timedelta(minutes={retry_delay_minutes}),')
        lines.append("}")
        lines.append("")

        # ---- DAG context manager ----
        lines.append("with DAG(")
        lines.append(f'    dag_id="{dag_id}",')
        lines.append(f'    description="dbt pipeline — auto-generated by sql-to-dag-compiler v2",')
        lines.append(f'    schedule_interval="{schedule_interval}",')
        lines.append(
            f'    start_date=datetime({now.year}, {now.month}, 1),'
        )
        lines.append('    catchup=False,')
        lines.append(f'    tags={effective_tags!r},')
        lines.append('    default_args=default_args,')
        lines.append(") as dag:")
        lines.append("")

        # ---- task definitions ----
        model_by_name = {m.name: m for m in project.models}
        for model_name in topo_order:
            model = model_by_name[model_name]
            var = task_var(model_name)
            mat_comment = f"  # materialized={model.materialization}"
            if model.is_incremental:
                mat_comment += ", incremental"
            lines.append(f"    {var} = BashOperator(")
            lines.append(f'        task_id="{model_name}",')
            lines.append(
                f'        bash_command=('
                f'"dbt run --select {model_name}'
                f' --profiles-dir {dbt_profiles_dir}'
                f' --project-dir {dbt_project_dir}"'
                f'),'
            )
            lines.append(f"    ){mat_comment}")
            lines.append("")

        # ---- dependency wiring ----
        dep_lines: list[str] = []
        for model_name in topo_order:
            upstream_deps = project.dependency_graph.get(model_name, [])
            for dep_name in upstream_deps:
                if dep_name in {m.name for m in project.models}:
                    up_var = task_var(dep_name)
                    dn_var = task_var(model_name)
                    dep_lines.append(f"    {up_var}.set_downstream({dn_var})")

        if dep_lines:
            lines.append("    # Task dependencies derived from {{ ref() }} relationships")
            lines.extend(dep_lines)
            lines.append("")

        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _extract_refs(self, sql: str) -> list[str]:
        """Return list of model names from all {{ ref('...') }} calls."""
        return list(dict.fromkeys(_REF_RE.findall(sql)))  # dedup, preserve order

    def _extract_sources(self, sql: str) -> list[tuple[str, str]]:
        """Return list of (schema, table) pairs from {{ source('...', '...') }} calls."""
        raw = _SOURCE_RE.findall(sql)
        seen: set[tuple[str, str]] = set()
        result: list[tuple[str, str]] = []
        for pair in raw:
            t = (pair[0], pair[1])
            if t not in seen:
                seen.add(t)
                result.append(t)
        return result

    def _extract_materialization(self, sql: str) -> str:
        """Return the materialization type from {{ config(...) }}, defaulting to 'view'."""
        m = _CONFIG_MATERIALIZED_RE.search(sql)
        if m:
            return m.group(1).lower()
        return "view"

    def _detect_incremental(self, sql: str) -> bool:
        """Return True if the model contains {% if is_incremental() %} blocks."""
        return bool(_INCREMENTAL_RE.search(sql))

    def _topological_sort(self, project: DbtProject) -> list[str]:
        """
        Return model names in topological order (upstream first) using Kahn's algorithm.

        Raises ValueError on circular dependencies.
        """
        graph = project.dependency_graph
        all_names = [m.name for m in project.models]

        # in-degree count
        in_degree: dict[str, int] = {name: 0 for name in all_names}
        # adjacency: upstream → list of downstream
        children: dict[str, list[str]] = {name: [] for name in all_names}

        for name in all_names:
            for dep in graph.get(name, []):
                if dep in in_degree:
                    in_degree[name] += 1
                    children[dep].append(name)

        queue = [n for n in all_names if in_degree[n] == 0]
        queue.sort()  # deterministic ordering for nodes with no deps
        result: list[str] = []

        while queue:
            node = queue.pop(0)
            result.append(node)
            for child in sorted(children[node]):
                in_degree[child] -= 1
                if in_degree[child] == 0:
                    queue.append(child)

        if len(result) != len(all_names):
            cycle_nodes = [n for n in all_names if n not in result]
            raise ValueError(
                f"Circular dependency detected in dbt models: {cycle_nodes}"
            )

        return result
