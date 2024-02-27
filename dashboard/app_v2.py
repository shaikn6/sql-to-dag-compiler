"""
app_v2.py — Streamlit V2 dashboard for sql-to-dag-compiler.

Tabs
----
  1. DAG Preview          (V1 preserved) — SQL → Airflow DAG
  2. dbt Model Preview    (new)          — SQL → dbt model YAML + SQL
  3. Lineage Explorer     (new)          — SQL → interactive HTML graph
  4. Impact Analysis      (new)          — column name → blast radius tree

Run
---
    streamlit run dashboard/app_v2.py
"""

from __future__ import annotations

import sys
import os
import textwrap
import tempfile
from pathlib import Path

# Ensure project root is importable when running from any directory
_ROOT = Path(__file__).parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import streamlit as st

from sql_to_dag.generator import compile_sql_string
from dbt_compiler.dbt_generator import compile_sql_to_dbt, write_dbt_project
from lineage.viz_generator import generate_lineage_html
from lineage.impact_analyzer import ImpactAnalyzer
from lineage.column_tracer import trace_column


# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="SQL → DAG / dbt Compiler V2",
    page_icon="",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ---------------------------------------------------------------------------
# Shared sample SQL
# ---------------------------------------------------------------------------

SAMPLE_SQL = textwrap.dedent("""
    -- Step 1: aggregate transactions
    CREATE TABLE staging.customer_txn AS
    SELECT customer_id, SUM(amount) AS total_amount, COUNT(*) AS txn_count
    FROM raw.transactions
    WHERE txn_date >= TRUNC(SYSDATE) - 30
    GROUP BY customer_id;

    -- Step 2: enrich with customer data
    CREATE TABLE mart.customer_summary AS
    SELECT c.customer_id, c.name, c.segment,
           t.total_amount, t.txn_count
    FROM staging.customer_txn t
    JOIN raw.customers c ON t.customer_id = c.customer_id
    WHERE t.total_amount > 100;

    -- Step 3: high-value segment
    INSERT INTO mart.high_value_customers
    SELECT customer_id, name, total_amount
    FROM mart.customer_summary
    WHERE total_amount > 10000;
""").strip()

CTE_SAMPLE_SQL = textwrap.dedent("""
    CREATE TABLE mart.monthly_revenue AS
    WITH raw_orders AS (
        SELECT order_id, customer_id, amount, order_date
        FROM raw.orders
        WHERE order_date >= '2024-01-01'
    ),
    enriched AS (
        SELECT o.order_id, o.customer_id, o.amount,
               c.segment, c.region,
               DATE_TRUNC('month', o.order_date) AS month
        FROM raw_orders o
        JOIN raw.customers c ON o.customer_id = c.customer_id
    ),
    monthly_agg AS (
        SELECT month, segment, region,
               SUM(amount) AS revenue,
               COUNT(DISTINCT customer_id) AS unique_customers,
               COUNT(order_id) AS order_count
        FROM enriched
        GROUP BY month, segment, region
    )
    SELECT month, segment, region, revenue,
           unique_customers, order_count,
           revenue / NULLIF(unique_customers, 0) AS revenue_per_customer
    FROM monthly_agg
    ORDER BY month DESC;
""").strip()


# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------

with st.sidebar:
    st.title("sql-to-dag-compiler")
    st.caption("V2 — dbt + lineage intelligence")
    st.divider()
    st.markdown("**Quick links**")
    st.markdown("- [GitHub repo](https://github.com/shaikn6/sql-to-dag-compiler)")
    st.markdown("- V1: Oracle SQL → Airflow DAG")
    st.markdown("- V2: + dbt models + lineage graph + impact analysis")


# ---------------------------------------------------------------------------
# Tabs
# ---------------------------------------------------------------------------

tab1, tab2, tab3, tab4 = st.tabs([
    "DAG Preview",
    "dbt Model Preview",
    "Lineage Explorer",
    "Impact Analysis",
])


# ===== TAB 1 — DAG PREVIEW (V1 preserved) ==================================

with tab1:
    st.header("SQL → Airflow DAG Compiler")
    st.caption("V1 feature — preserved. Paste Oracle SQL/PLSQL below.")

    col_left, col_right = st.columns([1, 1], gap="large")

    with col_left:
        sql_input = st.text_area(
            "Oracle SQL / PLSQL",
            value=SAMPLE_SQL,
            height=320,
            key="dag_sql_input",
        )
        dag_id = st.text_input("DAG ID", value="generated_dag", key="dag_id_input")
        dag_owner = st.text_input("Owner", value="data_team", key="dag_owner_input")
        schedule = st.text_input("Schedule", value="@daily", key="dag_schedule_input")

        if st.button("Compile → DAG", type="primary", key="compile_dag_btn"):
            try:
                dag_source = compile_sql_string(
                    sql_input,
                    dag_id=dag_id,
                    dag_owner=dag_owner,
                    schedule_interval=schedule,
                )
                st.session_state["dag_output"] = dag_source
                st.success("DAG compiled successfully.")
            except Exception as exc:
                st.error(f"Compilation error: {exc}")

    with col_right:
        if "dag_output" in st.session_state:
            st.subheader("Generated Airflow DAG")
            st.code(st.session_state["dag_output"], language="python")
            st.download_button(
                "Download DAG",
                data=st.session_state["dag_output"],
                file_name=f"{dag_id}.py",
                mime="text/plain",
            )
        else:
            st.info("Click 'Compile → DAG' to see the generated Airflow DAG here.")


# ===== TAB 2 — dbt MODEL PREVIEW ===========================================

with tab2:
    st.header("SQL → dbt Model Generator")
    st.caption("Parses CTEs and SELECT statements into dbt YAML + SQL files.")

    col_l2, col_r2 = st.columns([1, 1], gap="large")

    with col_l2:
        dbt_sql_input = st.text_area(
            "Input SQL (supports CTEs, CTAS, INSERT … SELECT)",
            value=CTE_SAMPLE_SQL,
            height=320,
            key="dbt_sql_input",
        )

        if st.button("Generate dbt Models", type="primary", key="gen_dbt_btn"):
            try:
                dbt_result = compile_sql_to_dbt(dbt_sql_input)
                st.session_state["dbt_result"] = dbt_result
                st.success(
                    f"Generated {len(dbt_result.models)} model(s), "
                    f"{len(dbt_result.sources)} source(s)."
                )
            except Exception as exc:
                st.error(f"dbt generation error: {exc}")

    with col_r2:
        if "dbt_result" in st.session_state:
            dbt_result = st.session_state["dbt_result"]

            model_names = [m.name for m in dbt_result.models]
            selected_model = st.selectbox("Select model to preview", options=model_names, key="dbt_model_select")

            if selected_model:
                model = next(m for m in dbt_result.models if m.name == selected_model)

                st.markdown(f"**Layer:** `{model.layer}` | **Materialized:** `{model.materialized}`")
                st.markdown(f"**Description:** {model.description}")

                col_sql, col_yaml = st.columns(2)
                with col_sql:
                    st.subheader("Model SQL")
                    from dbt_compiler.dbt_generator import _render_model_sql
                    st.code(_render_model_sql(model), language="sql")
                with col_yaml:
                    st.subheader("Schema YAML")
                    from dbt_compiler.dbt_generator import _render_schema_yml
                    st.code(_render_schema_yml([model]), language="yaml")

            if dbt_result.sources:
                st.subheader("sources.yml")
                from dbt_compiler.dbt_generator import _render_sources_yml
                st.code(_render_sources_yml(dbt_result.sources), language="yaml")

            # Download all artefacts as a zip
            if st.button("Download all dbt artefacts", key="dbt_download_btn"):
                import io, zipfile
                with tempfile.TemporaryDirectory() as tmp:
                    written = write_dbt_project(dbt_result, tmp)
                buf = io.BytesIO()
                with zipfile.ZipFile(buf, "w") as zf:
                    for rel_path, content in written.items():
                        zf.writestr(rel_path, content)
                st.download_button(
                    "Download zip",
                    data=buf.getvalue(),
                    file_name="dbt_models.zip",
                    mime="application/zip",
                )
        else:
            st.info("Click 'Generate dbt Models' to preview the generated artefacts here.")


# ===== TAB 3 — LINEAGE EXPLORER ============================================

with tab3:
    st.header("Lineage Explorer")
    st.caption(
        "Upload SQL and get an interactive lineage graph. "
        "Nodes: blue=tables, yellow=CTEs, green=dbt models, orange=DAG tasks."
    )

    col_l3, col_r3 = st.columns([1, 2], gap="large")

    with col_l3:
        lineage_sql = st.text_area(
            "SQL (paste or upload)",
            value=CTE_SAMPLE_SQL,
            height=260,
            key="lineage_sql_input",
        )
        uploaded = st.file_uploader("Or upload a .sql file", type=["sql"], key="lineage_upload")
        if uploaded is not None:
            lineage_sql = uploaded.read().decode("utf-8")
            st.success(f"Loaded: {uploaded.name}")

        if st.button("Generate Lineage Graph", type="primary", key="gen_lineage_btn"):
            try:
                with tempfile.NamedTemporaryFile(
                    suffix=".html", delete=False, mode="w", encoding="utf-8"
                ) as tmp_f:
                    tmp_path = tmp_f.name
                html_content = generate_lineage_html(lineage_sql, tmp_path)
                st.session_state["lineage_html"] = html_content
                st.session_state["lineage_html_path"] = tmp_path
                st.success("Lineage graph generated.")
            except Exception as exc:
                st.error(f"Lineage error: {exc}")

    with col_r3:
        if "lineage_html" in st.session_state:
            st.subheader("Interactive Lineage Graph")
            st.components.v1.html(
                st.session_state["lineage_html"],
                height=700,
                scrolling=True,
            )
            st.download_button(
                "Download lineage_graph.html",
                data=st.session_state["lineage_html"],
                file_name="lineage_graph.html",
                mime="text/html",
            )
        else:
            st.info("Click 'Generate Lineage Graph' to see the interactive graph here.")


# ===== TAB 4 — IMPACT ANALYSIS =============================================

with tab4:
    st.header("Impact Analysis")
    st.caption(
        "Enter a column name to see its blast radius — all downstream models "
        "and DAG tasks that depend on it."
    )

    col_l4, col_r4 = st.columns([1, 1], gap="large")

    with col_l4:
        impact_sql = st.text_area(
            "SQL",
            value=CTE_SAMPLE_SQL,
            height=200,
            key="impact_sql_input",
        )
        column_name = st.text_input(
            "Column name to analyze",
            value="amount",
            key="impact_col_input",
        )
        col_v1, col_v2 = st.columns(2)
        with col_v1:
            sql_v1 = st.text_area(
                "SQL v1 (for breaking changes)",
                value="SELECT order_id, customer_id, amount FROM raw.orders",
                height=100,
                key="sql_v1_input",
            )
        with col_v2:
            sql_v2 = st.text_area(
                "SQL v2 (for breaking changes)",
                value="SELECT order_id, customer_id, total_amount FROM raw.orders",
                height=100,
                key="sql_v2_input",
            )

        run_impact = st.button("Run Impact Analysis", type="primary", key="run_impact_btn")
        run_rename = st.button("What-if Rename →", key="run_rename_btn")
        run_breaking = st.button("Breaking Changes", key="run_breaking_btn")

    with col_r4:
        if run_impact:
            try:
                analyzer = ImpactAnalyzer(impact_sql)
                result = analyzer.analyze(column_name)

                st.subheader(f"Impact: `{column_name}`")
                col_m1, col_m2, col_m3 = st.columns(3)
                col_m1.metric("Blast Radius", result.blast_radius)
                col_m2.metric("Affected Models", len(result.affected_models))
                col_m3.metric("Affected DAG Tasks", len(result.affected_dag_tasks))

                if result.affected_models:
                    st.markdown("**Affected models:**")
                    for m in result.affected_models:
                        st.markdown(f"- `{m}`")

                if result.critical_path:
                    st.markdown("**Critical path:**")
                    st.code(" → ".join(result.critical_path))

                if result.dependency_tree:
                    st.markdown("**Dependency tree:**")
                    for parent, children in result.dependency_tree.items():
                        if children:
                            st.markdown(f"- `{parent}` → {[f'`{c}`' for c in children]}")
                        else:
                            st.markdown(f"- `{parent}` (leaf)")
            except Exception as exc:
                st.error(f"Impact analysis error: {exc}")

        if run_rename and column_name:
            try:
                analyzer = ImpactAnalyzer(impact_sql)
                new_col = f"{column_name}_v2"
                changes = analyzer.what_if_rename(column_name, new_col)
                st.subheader(f"What-if rename: `{column_name}` → `{new_col}`")
                if changes:
                    for change in changes:
                        st.markdown(
                            f"- **{change.file_path}** — `{change.line_hint[:80]}`"
                        )
                else:
                    st.info(f"No references to `{column_name}` found in model SQL.")
            except Exception as exc:
                st.error(f"Rename analysis error: {exc}")

        if run_breaking:
            try:
                analyzer = ImpactAnalyzer(impact_sql)
                diff = analyzer.breaking_changes(sql_v1, sql_v2)
                st.subheader("Breaking Changes Diff")
                if diff.is_breaking:
                    st.error(f"BREAKING: {diff.summary}")
                else:
                    st.success("No breaking changes detected.")

                if diff.removed_columns:
                    st.markdown(f"**Removed:** {diff.removed_columns}")
                if diff.added_columns:
                    st.markdown(f"**Added (non-breaking):** {diff.added_columns}")
                if diff.renamed_columns:
                    for r in diff.renamed_columns:
                        st.markdown(f"**Renamed:** `{r.old_value}` → `{r.new_value}`")
                if diff.type_changes:
                    for tc in diff.type_changes:
                        st.markdown(f"**Type change:** `{tc.column_name}`: `{tc.old_value}` → `{tc.new_value}`")
            except Exception as exc:
                st.error(f"Breaking change analysis error: {exc}")

        if not (run_impact or run_rename or run_breaking):
            st.info("Use the buttons on the left to run analysis.")

        # Column tracer section
        st.divider()
        st.subheader("Column Lineage Tracer")
        trace_col = st.text_input(
            "Column to trace through CTEs",
            value="amount",
            key="trace_col_input",
        )
        if st.button("Trace Column", key="trace_btn"):
            try:
                lineage = trace_column(impact_sql, trace_col)
                if lineage.found:
                    st.success(f"Found {len(lineage.path)} hop(s) for `{trace_col}`")
                    for i, step in enumerate(lineage.path, 1):
                        with st.expander(f"Step {i}: {step.model_name} ({step.transformation})"):
                            st.markdown(
                                f"**{step.input_col}** → `{step.transformation}` → **{step.output_col}**"
                            )
                            st.code(step.sql_snippet, language="sql")
                else:
                    st.warning(f"Column `{trace_col}` not found in the provided SQL.")
            except Exception as exc:
                st.error(f"Column trace error: {exc}")
