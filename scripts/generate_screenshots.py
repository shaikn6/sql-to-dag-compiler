"""
Generate screenshot PNGs for the sql-to-dag-compiler README.

Run from the repo root:
    python scripts/generate_screenshots.py
"""

import os
import matplotlib
matplotlib.use("Agg")  # non-interactive backend
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch
import networkx as nx

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "docs", "screenshots")
os.makedirs(OUTPUT_DIR, exist_ok=True)


# ---------------------------------------------------------------------------
# 1. Pipeline overview (horizontal flow diagram)
# ---------------------------------------------------------------------------

def draw_pipeline_overview():
    fig, ax = plt.subplots(figsize=(12, 4))
    fig.patch.set_facecolor("#0f172a")
    ax.set_facecolor("#0f172a")
    ax.set_xlim(0, 12)
    ax.set_ylim(0, 4)
    ax.axis("off")

    stages = [
        ("Oracle SQL\n/ PLSQL",    "Input file",          0.6),
        ("SQL\nParser",            "sqlparse 0.4",        2.6),
        ("Dependency\nGraph",      "networkx 2.7",        4.6),
        ("DAG\nGenerator",         "Jinja2 3.x",          6.6),
        ("Airflow\nDAG",           "Python output",       8.6),
    ]

    box_w, box_h = 1.6, 1.4
    y_center = 2.0
    arrow_color = "#38bdf8"
    box_face = "#1e40af"
    subtitle_color = "#93c5fd"

    for i, (label, subtitle, x) in enumerate(stages):
        # Box
        fancy = FancyBboxPatch(
            (x, y_center - box_h / 2),
            box_w, box_h,
            boxstyle="round,pad=0.08",
            facecolor=box_face,
            edgecolor=arrow_color,
            linewidth=1.5,
            zorder=3,
        )
        ax.add_patch(fancy)

        # Main label
        ax.text(
            x + box_w / 2, y_center + 0.15,
            label,
            color="white",
            fontsize=10,
            fontweight="bold",
            ha="center",
            va="center",
            zorder=4,
        )
        # Subtitle
        ax.text(
            x + box_w / 2, y_center - 0.42,
            subtitle,
            color=subtitle_color,
            fontsize=7.5,
            ha="center",
            va="center",
            zorder=4,
        )

        # Arrow to next box
        if i < len(stages) - 1:
            ax.annotate(
                "",
                xy=(stages[i + 1][2], y_center),
                xytext=(x + box_w, y_center),
                arrowprops=dict(
                    arrowstyle="->",
                    color=arrow_color,
                    lw=2,
                ),
                zorder=2,
            )

    ax.set_title(
        "sql-to-dag-compiler  |  Compiler Pipeline",
        color="white",
        fontsize=13,
        fontweight="bold",
        pad=10,
    )

    out = os.path.join(OUTPUT_DIR, "pipeline_overview.png")
    fig.savefig(out, dpi=150, bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close(fig)
    print(f"Saved: {out}")


# ---------------------------------------------------------------------------
# 2. DAG output (Airflow-style task view)
# ---------------------------------------------------------------------------

def draw_dag_output():
    fig, ax = plt.subplots(figsize=(8, 6))
    fig.patch.set_facecolor("#0f172a")
    ax.set_facecolor("#17255a")
    ax.set_xlim(0, 8)
    ax.set_ylim(0, 6)
    ax.axis("off")

    tasks = [
        ("create_customer_txn",        4.0, 5.0),
        ("create_customer_summary",    4.0, 3.2),
        ("insert_high_value_customers",4.0, 1.4),
    ]

    box_w, box_h = 3.6, 0.72
    box_color = "#1d4ed8"
    edge_color = "#60a5fa"
    arrow_color = "#34d399"

    for label, x, y in tasks:
        fancy = FancyBboxPatch(
            (x - box_w / 2, y - box_h / 2),
            box_w, box_h,
            boxstyle="round,pad=0.06",
            facecolor=box_color,
            edgecolor=edge_color,
            linewidth=2,
            zorder=3,
        )
        ax.add_patch(fancy)
        ax.text(
            x, y,
            label,
            color="white",
            fontsize=9.5,
            fontweight="bold",
            ha="center",
            va="center",
            fontfamily="monospace",
            zorder=4,
        )

    # Arrows
    for i in range(len(tasks) - 1):
        x1, y1 = tasks[i][1], tasks[i][2] - box_h / 2
        x2, y2 = tasks[i + 1][1], tasks[i + 1][2] + box_h / 2
        ax.annotate(
            "",
            xy=(x2, y2),
            xytext=(x1, y1),
            arrowprops=dict(arrowstyle="-|>", color=arrow_color, lw=2),
            zorder=2,
        )

    ax.set_title(
        "Generated Airflow DAG  |  sample_oracle",
        color="white",
        fontsize=12,
        fontweight="bold",
        pad=10,
    )

    # Small status badge
    ax.text(
        0.18, 5.6,
        "  success  ",
        color="#16a34a",
        fontsize=8,
        bbox=dict(facecolor="#14532d", edgecolor="#16a34a", boxstyle="round,pad=0.3"),
    )

    out = os.path.join(OUTPUT_DIR, "dag_output.png")
    fig.savefig(out, dpi=150, bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close(fig)
    print(f"Saved: {out}")


# ---------------------------------------------------------------------------
# 3. Dependency graph (networkx)
# ---------------------------------------------------------------------------

def draw_dependency_graph():
    G = nx.DiGraph()

    nodes = [
        "raw.transactions",
        "raw.customers",
        "staging.customer_txn",
        "mart.customer_summary",
        "mart.high_value_customers",
    ]
    edges = [
        ("raw.transactions",   "staging.customer_txn"),
        ("raw.customers",      "staging.customer_txn"),
        ("staging.customer_txn","mart.customer_summary"),
        ("mart.customer_summary","mart.high_value_customers"),
    ]

    G.add_nodes_from(nodes)
    G.add_edges_from(edges)

    # Hierarchical layout by hand so it looks clean
    pos = {
        "raw.transactions":          (0.0, 1.0),
        "raw.customers":             (0.0, 0.0),
        "staging.customer_txn":      (2.0, 0.5),
        "mart.customer_summary":     (4.0, 0.5),
        "mart.high_value_customers": (6.0, 0.5),
    }

    # Node color by schema
    color_map = {
        "raw":     "#b45309",
        "staging": "#1d4ed8",
        "mart":    "#15803d",
    }
    node_colors = [color_map[n.split(".")[0]] for n in G.nodes()]

    fig, ax = plt.subplots(figsize=(10, 6))
    fig.patch.set_facecolor("#0f172a")
    ax.set_facecolor("#0f172a")

    nx.draw_networkx_nodes(
        G, pos, ax=ax,
        node_color=node_colors,
        node_size=2200,
        alpha=0.95,
    )
    nx.draw_networkx_labels(
        G, pos, ax=ax,
        font_color="white",
        font_size=7.5,
        font_weight="bold",
    )
    nx.draw_networkx_edges(
        G, pos, ax=ax,
        edge_color="#38bdf8",
        arrows=True,
        arrowsize=22,
        width=2,
        node_size=2200,
        connectionstyle="arc3,rad=0.05",
    )

    ax.set_title(
        "Table Dependency Graph  |  sql-to-dag-compiler",
        color="white",
        fontsize=12,
        fontweight="bold",
        pad=12,
    )
    ax.axis("off")

    # Legend
    legend_items = [
        mpatches.Patch(color="#b45309", label="raw schema"),
        mpatches.Patch(color="#1d4ed8", label="staging schema"),
        mpatches.Patch(color="#15803d", label="mart schema"),
    ]
    ax.legend(
        handles=legend_items,
        loc="lower right",
        facecolor="#1e293b",
        edgecolor="#334155",
        labelcolor="white",
        fontsize=9,
    )

    out = os.path.join(OUTPUT_DIR, "dependency_graph.png")
    fig.savefig(out, dpi=150, bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close(fig)
    print(f"Saved: {out}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    draw_pipeline_overview()
    draw_dag_output()
    draw_dependency_graph()
    print("All screenshots generated successfully.")
