"""
generate_v2_screenshots.py — Generate V2 PNG screenshots using matplotlib.

Outputs
-------
docs/screenshots/v2_dbt_model_output.png
docs/screenshots/v2_lineage_graph.png
docs/screenshots/v2_impact_analysis.png
docs/screenshots/v2_column_trace.png

Usage
-----
    python scripts/generate_v2_screenshots.py
"""

from __future__ import annotations

import sys
import os
from pathlib import Path

# Ensure project root is on path
ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import networkx as nx

SCREENSHOTS_DIR = ROOT / "docs" / "screenshots"
SCREENSHOTS_DIR.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------------
# Colour palette (dark background)
# ---------------------------------------------------------------------------
BG = "#0f1117"
SURFACE = "#1a1b26"
ACCENT = "#7c6af7"
TEXT = "#c9d1d9"
MUTED = "#8b949e"
GREEN = "#27AE60"
YELLOW = "#F5C518"
BLUE = "#4A90D9"
ORANGE = "#E67E22"
RED = "#e74c3c"


def _fig(w=14, h=8):
    fig, ax = plt.subplots(figsize=(w, h))
    fig.patch.set_facecolor(BG)
    ax.set_facecolor(SURFACE)
    for spine in ax.spines.values():
        spine.set_edgecolor(ACCENT)
    ax.tick_params(colors=MUTED)
    return fig, ax


# ---------------------------------------------------------------------------
# 1. v2_dbt_model_output.png
# ---------------------------------------------------------------------------

def gen_dbt_model_output():
    fig, ax = _fig(14, 9)
    ax.set_xlim(0, 14)
    ax.set_ylim(0, 9)
    ax.axis("off")
    ax.set_title("dbt Model Generator — Output Preview", fontsize=14,
                 color=TEXT, pad=10, fontfamily="monospace")

    # Left panel: schema.yml
    yaml_lines = [
        "# models/staging/schema.yml",
        "version: 2",
        "",
        "models:",
        "  - name: customer_txn",
        "    description: \"dbt model: customer_txn.",
        "                  Aggregated grain.\"",
        "    config:",
        "      materialized: table",
        "    columns:",
        "      - name: customer_id",
        "        description: \"Column: customer_id\"",
        "      - name: total_amount",
        "        description: \"SUM of total_amount\"",
        "      - name: txn_count",
        "        description: \"COUNT of txn_count\"",
        "",
        "  - name: customer_summary",
        "    description: \"dbt model: customer_summary.\"",
        "    config:",
        "      materialized: view",
        "    columns:",
        "      - name: customer_id",
        "      - name: name",
        "      - name: segment",
        "      - name: total_amount",
    ]

    yaml_text = "\n".join(yaml_lines)
    ax.text(0.3, 8.6, yaml_text, fontsize=8.5, color="#e3c06b",
            fontfamily="monospace", va="top", linespacing=1.5)

    # Right panel: model SQL
    sql_lines = [
        "# models/staging/customer_txn.sql",
        "",
        "{{ config(materialized='table') }}",
        "",
        "SELECT",
        "    customer_id,",
        "    SUM(amount) AS total_amount,",
        "    COUNT(*) AS txn_count",
        "FROM {{ source('raw', 'transactions') }}",
        "WHERE txn_date >= '2024-01-01'",
        "GROUP BY customer_id",
        "",
        "---",
        "# models/marts/customer_summary.sql",
        "",
        "{{ config(materialized='view') }}",
        "",
        "SELECT",
        "    c.customer_id,",
        "    c.name,",
        "    c.segment,",
        "    t.total_amount",
        "FROM {{ ref('customer_txn') }} t",
        "JOIN {{ source('raw', 'customers') }} c",
        "    ON t.customer_id = c.customer_id",
        "WHERE t.total_amount > 100",
    ]
    sql_text = "\n".join(sql_lines)
    ax.text(7.5, 8.6, sql_text, fontsize=8.5, color="#79c0ff",
            fontfamily="monospace", va="top", linespacing=1.5)

    # Divider
    ax.axvline(x=7.2, color=ACCENT, linewidth=1, linestyle="--", alpha=0.5)

    # Header labels
    ax.text(0.3, 8.8, "schema.yml", fontsize=10, color=YELLOW,
            fontfamily="monospace", fontweight="bold")
    ax.text(7.5, 8.8, "model SQL", fontsize=10, color=BLUE,
            fontfamily="monospace", fontweight="bold")

    # sources.yml snippet bottom
    ax.text(0.3, 0.7,
            "sources.yml → sources:\n  - name: raw\n    tables:\n"
            "      - name: transactions\n      - name: customers",
            fontsize=8, color=GREEN, fontfamily="monospace")

    fig.tight_layout()
    out = SCREENSHOTS_DIR / "v2_dbt_model_output.png"
    fig.savefig(out, dpi=150, bbox_inches="tight", facecolor=BG)
    plt.close(fig)
    print(f"Saved: {out}")


# ---------------------------------------------------------------------------
# 2. v2_lineage_graph.png  (15+ nodes)
# ---------------------------------------------------------------------------

def gen_lineage_graph():
    fig, ax = _fig(16, 10)
    ax.set_facecolor(BG)
    fig.patch.set_facecolor(BG)
    ax.set_title("Interactive Lineage Graph — pyvis HTML export (static preview)",
                 fontsize=13, color=TEXT, pad=12)

    # Build a rich DAG with 15+ nodes
    G = nx.DiGraph()

    tables = [
        ("raw.transactions", "table"),
        ("raw.customers", "table"),
        ("raw.orders", "table"),
        ("raw.products", "table"),
        ("raw.regions", "table"),
    ]
    ctes = [
        ("txn_agg", "cte"),
        ("customer_enriched", "cte"),
        ("order_enriched", "cte"),
        ("product_lookup", "cte"),
        ("regional_summary", "cte"),
    ]
    dbt_models = [
        ("customer_txn", "dbt_model"),
        ("customer_summary", "dbt_model"),
        ("order_summary", "dbt_model"),
    ]
    dag_tasks = [
        ("dag:load_customers", "dag_task"),
        ("dag:load_orders", "dag_task"),
        ("dag:export_summary", "dag_task"),
    ]

    all_nodes = tables + ctes + dbt_models + dag_tasks
    for name, ntype in all_nodes:
        G.add_node(name, ntype=ntype)

    edges = [
        ("raw.transactions", "txn_agg"),
        ("raw.customers", "txn_agg"),
        ("raw.customers", "customer_enriched"),
        ("txn_agg", "customer_enriched"),
        ("raw.orders", "order_enriched"),
        ("raw.products", "product_lookup"),
        ("raw.regions", "regional_summary"),
        ("customer_enriched", "customer_txn"),
        ("order_enriched", "order_summary"),
        ("product_lookup", "order_summary"),
        ("customer_txn", "customer_summary"),
        ("order_summary", "customer_summary"),
        ("regional_summary", "customer_summary"),
        ("customer_txn", "dag:load_customers"),
        ("order_summary", "dag:load_orders"),
        ("customer_summary", "dag:export_summary"),
    ]
    G.add_edges_from(edges)

    color_map = {"table": BLUE, "cte": YELLOW, "dbt_model": GREEN, "dag_task": ORANGE}
    node_colors = [color_map.get(G.nodes[n].get("ntype", "table"), MUTED) for n in G.nodes]

    try:
        pos = nx.nx_agraph.graphviz_layout(G, prog="dot")
    except Exception:
        pos = nx.spring_layout(G, seed=42, k=2.5)

    nx.draw_networkx_nodes(G, pos, node_color=node_colors, node_size=900, alpha=0.95, ax=ax)
    nx.draw_networkx_edges(G, pos, edge_color=MUTED, arrows=True,
                           arrowstyle="-|>", arrowsize=15,
                           connectionstyle="arc3,rad=0.05",
                           ax=ax, width=1.5, alpha=0.7)
    short_labels = {n: n.split(":")[-1].replace("raw.", "") for n in G.nodes}
    nx.draw_networkx_labels(G, pos, labels=short_labels,
                            font_size=7.5, font_color="white", ax=ax)
    ax.axis("off")

    legend_patches = [
        mpatches.Patch(color=BLUE, label="Source Table"),
        mpatches.Patch(color=YELLOW, label="CTE"),
        mpatches.Patch(color=GREEN, label="dbt Model"),
        mpatches.Patch(color=ORANGE, label="DAG Task"),
    ]
    ax.legend(handles=legend_patches, loc="lower right",
              facecolor=SURFACE, edgecolor=ACCENT,
              labelcolor=TEXT, fontsize=10)

    node_count = G.number_of_nodes()
    edge_count = G.number_of_edges()
    ax.set_title(
        f"Interactive Lineage Graph — {node_count} nodes, {edge_count} edges  "
        f"(pyvis HTML export preview)",
        fontsize=12, color=TEXT, pad=10,
    )

    out = SCREENSHOTS_DIR / "v2_lineage_graph.png"
    fig.savefig(out, dpi=150, bbox_inches="tight", facecolor=BG)
    plt.close(fig)
    print(f"Saved: {out}")


# ---------------------------------------------------------------------------
# 3. v2_impact_analysis.png
# ---------------------------------------------------------------------------

def gen_impact_analysis():
    fig, axes = plt.subplots(1, 2, figsize=(16, 9))
    fig.patch.set_facecolor(BG)

    # Left: blast radius tree
    ax = axes[0]
    ax.set_facecolor(SURFACE)
    ax.set_title("Blast Radius: column 'amount'", fontsize=12, color=YELLOW, pad=8)
    ax.axis("off")

    tree_data = {
        "raw.transactions\n(source)": ["txn_agg\n(CTE)", "order_enriched\n(CTE)"],
        "txn_agg\n(CTE)": ["customer_txn\n(dbt_model)"],
        "order_enriched\n(CTE)": ["order_summary\n(dbt_model)"],
        "customer_txn\n(dbt_model)": ["customer_summary\n(dbt_model)"],
        "order_summary\n(dbt_model)": ["customer_summary\n(dbt_model)"],
        "customer_summary\n(dbt_model)": ["dag:export_summary\n(dag_task)"],
    }
    node_types = {
        "raw.transactions\n(source)": "table",
        "txn_agg\n(CTE)": "cte",
        "order_enriched\n(CTE)": "cte",
        "customer_txn\n(dbt_model)": "dbt_model",
        "order_summary\n(dbt_model)": "dbt_model",
        "customer_summary\n(dbt_model)": "dbt_model",
        "dag:export_summary\n(dag_task)": "dag_task",
    }
    G2 = nx.DiGraph(tree_data)
    pos2 = nx.nx_agraph.graphviz_layout(G2, prog="dot") if False else nx.spring_layout(G2, seed=7, k=3)
    color_map2 = {"table": BLUE, "cte": YELLOW, "dbt_model": GREEN, "dag_task": ORANGE}
    colors2 = [color_map2.get(node_types.get(n, "table"), MUTED) for n in G2.nodes]
    nx.draw_networkx_nodes(G2, pos2, node_color=colors2, node_size=1200, alpha=0.9, ax=ax)
    nx.draw_networkx_edges(G2, pos2, edge_color=MUTED, arrows=True,
                           arrowstyle="-|>", arrowsize=12, ax=ax, width=1.5)
    nx.draw_networkx_labels(G2, pos2, font_size=7, font_color="white", ax=ax)

    # Right: metrics panel
    ax2 = axes[1]
    ax2.set_facecolor(SURFACE)
    ax2.axis("off")
    ax2.set_title("Impact Summary", fontsize=12, color=TEXT, pad=8)

    metrics = [
        ("Blast Radius", "7 nodes"),
        ("Affected Models", "5"),
        ("Affected DAG Tasks", "1"),
        ("Breaking if removed?", "YES — column propagates"),
        ("Critical Path", "txn_agg → customer_txn → customer_summary → export"),
    ]
    for spine in ax2.spines.values():
        spine.set_edgecolor(ACCENT)

    y = 0.88
    for label, value in metrics:
        ax2.text(0.05, y, label + ":", fontsize=10, color=MUTED,
                 transform=ax2.transAxes, fontfamily="monospace")
        color = RED if "YES" in value else TEXT
        ax2.text(0.45, y, value, fontsize=10, color=color, fontweight="bold",
                 transform=ax2.transAxes, fontfamily="monospace")
        y -= 0.12

    # What-if rename section
    ax2.text(0.05, 0.35, "what_if_rename('amount', 'revenue'):", fontsize=9.5,
             color=YELLOW, transform=ax2.transAxes, fontfamily="monospace")
    rename_results = [
        "→ models/txn_agg.sql: line 2",
        "→ models/order_enriched.sql: line 3",
        "→ models/customer_txn.sql: line 4",
        "→ models/order_summary.sql: line 2",
    ]
    for i, r in enumerate(rename_results):
        ax2.text(0.05, 0.26 - i * 0.07, r, fontsize=8.5, color=GREEN,
                 transform=ax2.transAxes, fontfamily="monospace")

    plt.tight_layout(pad=1.5)
    out = SCREENSHOTS_DIR / "v2_impact_analysis.png"
    fig.savefig(out, dpi=150, bbox_inches="tight", facecolor=BG)
    plt.close(fig)
    print(f"Saved: {out}")


# ---------------------------------------------------------------------------
# 4. v2_column_trace.png
# ---------------------------------------------------------------------------

def gen_column_trace():
    fig, ax = _fig(14, 8)
    ax.axis("off")
    ax.set_title("Column Lineage Trace: 'amount' through 5 CTEs",
                 fontsize=13, color=TEXT, pad=10)

    steps = [
        {
            "model": "raw.orders",
            "type": "table",
            "input": "amount",
            "output": "amount",
            "transform": "passthrough",
            "snippet": "SELECT order_id, customer_id, amount, order_date\nFROM raw.orders",
            "color": BLUE,
        },
        {
            "model": "raw_orders (CTE)",
            "type": "cte",
            "input": "amount",
            "output": "amount",
            "transform": "passthrough",
            "snippet": "SELECT order_id, customer_id, amount\nFROM raw.orders",
            "color": YELLOW,
        },
        {
            "model": "enriched (CTE)",
            "type": "cte",
            "input": "amount",
            "output": "revenue",
            "transform": "rename",
            "snippet": "SELECT o.order_id, o.amount AS revenue\nFROM raw_orders o JOIN raw.customers c ...",
            "color": YELLOW,
        },
        {
            "model": "monthly_agg (CTE)",
            "type": "cte",
            "input": "revenue",
            "output": "total_revenue",
            "transform": "aggregate:SUM",
            "snippet": "SELECT segment, SUM(revenue) AS total_revenue\nFROM enriched\nGROUP BY segment",
            "color": YELLOW,
        },
        {
            "model": "order_summary (dbt_model)",
            "type": "dbt_model",
            "input": "total_revenue",
            "output": "total_revenue",
            "transform": "passthrough",
            "snippet": "SELECT segment, total_revenue\nFROM monthly_agg",
            "color": GREEN,
        },
    ]

    box_w = 2.3
    box_h = 2.8
    gap = 0.5
    total_w = len(steps) * box_w + (len(steps) - 1) * gap
    start_x = (14 - total_w) / 2

    transform_colors = {
        "passthrough": MUTED,
        "rename": BLUE,
        "aggregate:SUM": ORANGE,
        "cast": ACCENT,
    }

    for i, step in enumerate(steps):
        x = start_x + i * (box_w + gap)
        y_bottom = 2.5

        # Box
        rect = mpatches.FancyBboxPatch(
            (x, y_bottom), box_w, box_h,
            boxstyle="round,pad=0.1",
            linewidth=2,
            edgecolor=step["color"],
            facecolor=SURFACE,
        )
        ax.add_patch(rect)

        # Model name header
        ax.text(x + box_w / 2, y_bottom + box_h - 0.2,
                step["model"], fontsize=7.5, color=step["color"],
                ha="center", va="top", fontweight="bold",
                fontfamily="monospace")

        # Type badge
        ax.text(x + box_w / 2, y_bottom + box_h - 0.55,
                f"[{step['type']}]", fontsize=7, color=MUTED,
                ha="center", va="top", fontfamily="monospace")

        # Transform badge
        t_color = transform_colors.get(step["transform"], TEXT)
        ax.text(x + box_w / 2, y_bottom + box_h - 0.95,
                step["transform"], fontsize=8, color=t_color,
                ha="center", va="top", fontweight="bold",
                fontfamily="monospace")

        # SQL snippet
        ax.text(x + 0.1, y_bottom + 1.2,
                step["snippet"], fontsize=6.5, color="#aaaaaa",
                ha="left", va="top", fontfamily="monospace",
                wrap=True)

        # In/out labels at bottom
        ax.text(x + 0.15, y_bottom + 0.25,
                f"in: {step['input']}", fontsize=7.5, color=MUTED,
                fontfamily="monospace")
        ax.text(x + 0.15, y_bottom + 0.05,
                f"out: {step['output']}", fontsize=7.5, color=GREEN,
                fontfamily="monospace")

        # Arrow to next
        if i < len(steps) - 1:
            ax_x = x + box_w
            ay = y_bottom + box_h / 2
            ax.annotate(
                "",
                xy=(ax_x + gap, ay),
                xytext=(ax_x, ay),
                arrowprops=dict(arrowstyle="-|>", color=ACCENT, lw=1.8),
            )

    # Footer annotation
    ax.text(7, 2.1,
            f"Total hops: {len(steps)}   |   Source: raw.orders.amount   →   "
            f"Final output: order_summary.total_revenue",
            fontsize=9, color=MUTED, ha="center", fontfamily="monospace")

    ax.set_xlim(0, 14)
    ax.set_ylim(0, 8)
    ax.set_facecolor(BG)
    fig.patch.set_facecolor(BG)

    out = SCREENSHOTS_DIR / "v2_column_trace.png"
    fig.savefig(out, dpi=150, bbox_inches="tight", facecolor=BG)
    plt.close(fig)
    print(f"Saved: {out}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("Generating V2 screenshots...")
    gen_dbt_model_output()
    gen_lineage_graph()
    gen_impact_analysis()
    gen_column_trace()
    print("Done.")
