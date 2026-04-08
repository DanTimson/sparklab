"""
compare_results.py  —  plots for the 12-variant × 3-size × 2-cluster matrix

Outputs:
  plot_time_vs_size.png        total time per variant across sizes
  plot_speedup_heatmap.png     speedup vs baseline grid
  plot_cache_order.png         cache post-filter vs pre-filter (order of ops)
  plot_partition_strategies.png repartition vs coalesce vs aqe vs baseline
  plot_step_breakdown.png      per-step stacked bar at 3M rows
  plot_ram_peak.png            peak JVM heap per variant at each size
  plot_ram_cache_effect.png    JVM heap per checkpoint for cache vs baseline
"""

import json
from pathlib import Path
from collections import defaultdict

import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

RESULTS_FILE = Path("results/experiment_results.json")
OUT_DIR      = Path("results")

SIZES    = ["100k", "1M", "3M"]
CLUSTERS = ["1dn", "3dn"]
VARIANTS = [
    "baseline",
    "cache",
    "cache_prefilter",
    "broadcast",
    "repartition",
    "coalesce",
    "aqe",
    "cache_broadcast",
    "cache_coalesce",
    "all_repart",
    "all_coalesce",
    "all_aqe",
]

COLORS = {
    "baseline":        "#4C72B0",
    "cache":           "#55A868",
    "cache_prefilter": "#C44E52",
    "broadcast":       "#8172B2",
    "repartition":     "#E88C30",
    "coalesce":        "#F0C040",
    "aqe":             "#CCB974",
    "cache_broadcast": "#3A9E8C",
    "cache_coalesce":  "#A0C878",
    "all_repart":      "#D46060",
    "all_coalesce":    "#8BC34A",
    "all_aqe":         "#5E9ED6",
}
MARKERS = {
    "baseline":        "o",
    "cache":           "s",
    "cache_prefilter": "X",
    "broadcast":       "D",
    "repartition":     "^",
    "coalesce":        "v",
    "aqe":             "p",
    "cache_broadcast": "P",
    "cache_coalesce":  "h",
    "all_repart":      "*",
    "all_coalesce":    "H",
    "all_aqe":         "8",
}

SIZE_X         = {"100k": 0, "1M": 1, "3M": 2}
SIZE_LABELS    = ["100k\n(~10 MB)", "1M\n(~100 MB)", "3M\n(~300 MB)"]
CLUSTER_LABELS = {"1dn": "1 DataNode", "3dn": "3 DataNodes"}

# All possible checkpoints in pipeline order; cache steps present only in
# their respective variants — step_t returns 0 for missing keys
STEPS = [
    "1_load",
    "2_features",
    "2_cache_prefilter",
    "3_filter",
    "3_cache",
    "4_join",
    "5_groupby",
    "6_window_rank",
    "7_write",
]
STEP_COLORS = [
    "#4878CF", "#6ACC65", "#88CC44",
    "#D65F5F", "#FF9955",
    "#B47CC7", "#C4AD66", "#77BEDB", "#aaa",
]

plt.rcParams.update({
    "figure.facecolor":  "white",
    "axes.facecolor":    "#f8f9f9",
    "axes.grid":         True,
    "grid.alpha":        0.4,
    "axes.spines.top":   False,
    "axes.spines.right": False,
    "font.size":         10,
})

with open(RESULTS_FILE) as f:
    raw = json.load(f)

data = defaultdict(lambda: defaultdict(dict))
for entry in raw:
    parts = entry["experiment"].split("_")
    if len(parts) < 3:
        continue
    size, cluster = parts[0], parts[1]
    data[size][cluster]["_".join(parts[2:])] = entry

def total(size, cluster, variant):
    try:    return data[size][cluster][variant]["total_s"]
    except: return None

def peak_ram(size, cluster, variant):
    try:    return data[size][cluster][variant]["peak_ram_mb"]
    except: return None

def step_t(size, cluster, variant, step):
    try:    return data[size][cluster][variant]["checkpoints"][step]["elapsed_s"]
    except: return 0.0

def step_ram(size, cluster, variant, step):
    try:    return data[size][cluster][variant]["checkpoints"][step]["ram_mb"]
    except: return None

def focused_plot(fig, axes, variants, annotate=True):
    for ax, cluster in zip(axes, CLUSTERS):
        for v in variants:
            pts = [(SIZE_X[s], total(s, cluster, v))
                   for s in SIZES if total(s, cluster, v) is not None]
            if not pts:
                continue
            xs, ys = zip(*pts)
            ax.plot(xs, ys, marker=MARKERS[v], color=COLORS[v],
                    label=v, linewidth=2.5, markersize=9)
            if annotate:
                for x, y in zip(xs, ys):
                    ax.annotate(f"{y:.1f}s", xy=(x, y), xytext=(0, 8),
                                textcoords="offset points",
                                fontsize=8, ha="center", color=COLORS[v])
        ax.set_title(CLUSTER_LABELS[cluster], fontweight="bold")
        ax.set_xticks(list(SIZE_X.values()))
        ax.set_xticklabels(SIZE_LABELS)
        ax.set_ylabel("Total time (s)")
        ax.legend(fontsize=9)

#  Plot 1 — Total time vs dataset size
fig, axes = plt.subplots(1, 2, figsize=(15, 5))
fig.suptitle("Total Execution Time vs Dataset Size", fontweight="bold", fontsize=13)

for ax, cluster in zip(axes, CLUSTERS):
    for v in VARIANTS:
        pts = [(SIZE_X[s], total(s, cluster, v))
               for s in SIZES if total(s, cluster, v) is not None]
        if not pts:
            continue
        xs, ys = zip(*pts)
        ax.plot(xs, ys, marker=MARKERS[v], color=COLORS[v],
                label=v, linewidth=2, markersize=7)
        ax.annotate(f"{ys[-1]:.0f}s", xy=(xs[-1], ys[-1]),
                    xytext=(4, 0), textcoords="offset points",
                    fontsize=7, color=COLORS[v])
    ax.set_title(CLUSTER_LABELS[cluster], fontweight="bold")
    ax.set_xticks(list(SIZE_X.values()))
    ax.set_xticklabels(SIZE_LABELS)
    ax.set_ylabel("Total time (s)")
    ax.legend(fontsize=7, loc="upper left", ncol=2)

plt.tight_layout()
fig.savefig(OUT_DIR / "plot_time_vs_size.png", dpi=150)
print("Saved: plot_time_vs_size.png")

#  Plot 2 — Speedup heatmap
fig, axes = plt.subplots(1, 2, figsize=(14, 6))
fig.suptitle("Speedup vs Baseline", fontweight="bold", fontsize=13)

for ax, cluster in zip(axes, CLUSTERS):
    matrix = np.array([
        [(total(s, cluster, "baseline") or 0) / (total(s, cluster, v) or float("inf"))
         if total(s, cluster, v) else np.nan
         for s in SIZES]
        for v in VARIANTS
    ])
    im = ax.imshow(matrix, aspect="auto", cmap="RdYlGn", vmin=0.5, vmax=2.0)
    ax.set_xticks(range(len(SIZES)))
    ax.set_xticklabels(SIZE_LABELS, fontsize=9)
    ax.set_yticks(range(len(VARIANTS)))
    ax.set_yticklabels(VARIANTS, fontsize=9)
    ax.set_title(CLUSTER_LABELS[cluster], fontweight="bold")
    for i in range(len(VARIANTS)):
        for j in range(len(SIZES)):
            v = matrix[i, j]
            if not np.isnan(v):
                ax.text(j, i, f"{v:.2f}×", ha="center", va="center", fontsize=8,
                        color="black" if 0.65 < v < 1.6 else "white")
    plt.colorbar(im, ax=ax, label=">1 = faster than baseline")

plt.tight_layout()
fig.savefig(OUT_DIR / "plot_speedup_heatmap.png", dpi=150)
print("Saved: plot_speedup_heatmap.png")

#  Plot 3 — Cache placement (order of operations)
fig, axes = plt.subplots(1, 2, figsize=(13, 5))
fig.suptitle("Cache Placement: Post-filter vs Pre-filter",
             fontweight="bold", fontsize=12)
focused_plot(fig, axes, ["baseline", "cache", "cache_prefilter"])
fig.text(0.5, 0.01,
         "cache = persists filtered DF  |  cache_prefilter = persists full featured DF",
         ha="center", fontsize=9, style="italic", color="#555")
plt.tight_layout(rect=[0, 0.05, 1, 1])
fig.savefig(OUT_DIR / "plot_cache_order.png", dpi=150)
print("Saved: plot_cache_order.png")

#  Plot 4 — Partition strategies
fig, axes = plt.subplots(1, 2, figsize=(13, 5))
fig.suptitle("Partition Strategies: Repartition vs Coalesce vs AQE",
             fontweight="bold", fontsize=12)
focused_plot(fig, axes, ["baseline", "repartition", "coalesce", "aqe"])
plt.tight_layout()
fig.savefig(OUT_DIR / "plot_partition_strategies.png", dpi=150)
print("Saved: plot_partition_strategies.png")

#  Plot 5 — Per-step stacked bar at 3M rows
fig, axes = plt.subplots(1, 2, figsize=(16, 5))
fig.suptitle("Per-Step Breakdown — 3M rows", fontweight="bold", fontsize=13)

for ax, cluster in zip(axes, CLUSTERS):
    x      = np.arange(len(VARIANTS))
    bottom = np.zeros(len(VARIANTS))
    for step, color in zip(STEPS, STEP_COLORS):
        heights = np.array([step_t("3M", cluster, v, step) for v in VARIANTS])
        ax.bar(x, heights, bottom=bottom, color=color, label=step, width=0.65)
        bottom += heights
    for i, v in enumerate(VARIANTS):
        t = total("3M", cluster, v)
        if t:
            ax.text(i, bottom[i] + 0.2, f"{t:.0f}s",
                    ha="center", fontsize=8, fontweight="bold")
    ax.set_xticks(x)
    ax.set_xticklabels(VARIANTS, rotation=30, ha="right", fontsize=8)
    ax.set_ylabel("Time (s)")
    ax.set_title(CLUSTER_LABELS[cluster], fontweight="bold")
    ax.legend(fontsize=7, loc="upper right")

plt.tight_layout()
fig.savefig(OUT_DIR / "plot_step_breakdown.png", dpi=150)
print("Saved: plot_step_breakdown.png")

#  Plot 6 — Peak JVM heap per variant × size
fig, axes = plt.subplots(1, 2, figsize=(15, 5))
fig.suptitle("Peak JVM Heap Usage vs Dataset Size", fontweight="bold", fontsize=13)

for ax, cluster in zip(axes, CLUSTERS):
    for v in VARIANTS:
        pts = [(SIZE_X[s], peak_ram(s, cluster, v))
               for s in SIZES if peak_ram(s, cluster, v) is not None]
        if not pts:
            continue
        xs, ys = zip(*pts)
        ax.plot(xs, ys, marker=MARKERS[v], color=COLORS[v],
                label=v, linewidth=2, markersize=7)
    ax.set_title(CLUSTER_LABELS[cluster], fontweight="bold")
    ax.set_xticks(list(SIZE_X.values()))
    ax.set_xticklabels(SIZE_LABELS)
    ax.set_ylabel("Peak JVM heap (MB)")
    ax.legend(fontsize=7, loc="upper left", ncol=2)

plt.tight_layout()
fig.savefig(OUT_DIR / "plot_ram_peak.png", dpi=150)
print("Saved: plot_ram_peak.png")

#  Plot 7 — JVM heap across checkpoints at 3M rows
#  Shows the heap jump when cache materialises vs baseline flat line.
fig, axes = plt.subplots(1, 2, figsize=(14, 5))
fig.suptitle("JVM Heap per Checkpoint — 3M rows (cache effect)",
             fontweight="bold", fontsize=12)

focus_variants = ["baseline", "cache", "cache_prefilter"]

for ax, cluster in zip(axes, CLUSTERS):
    for v in focus_variants:
        ram_pts = []
        for step in STEPS:
            r = step_ram("3M", cluster, v, step)
            if r is not None:
                ram_pts.append((step, r))
        if not ram_pts:
            continue
        labels, vals = zip(*ram_pts)
        xs = range(len(labels))
        ax.plot(xs, vals, marker=MARKERS[v], color=COLORS[v],
                label=v, linewidth=2.5, markersize=8)

    all_steps = []
    for step in STEPS:
        if any(step_ram("3M", cluster, v, step) is not None
               for v in focus_variants):
            all_steps.append(step)
    ax.set_xticks(range(len(all_steps)))
    ax.set_xticklabels(all_steps, rotation=30, ha="right", fontsize=8)
    ax.set_ylabel("JVM heap (MB)")
    ax.set_title(CLUSTER_LABELS[cluster], fontweight="bold")
    ax.legend(fontsize=9)

fig.text(0.5, 0.01,
         "Heap jump at 2_cache_prefilter / 3_cache shows materialisation cost; "
         "flat baseline confirms no persistence overhead",
         ha="center", fontsize=9, style="italic", color="#555")
plt.tight_layout(rect=[0, 0.05, 1, 1])
fig.savefig(OUT_DIR / "plot_ram_cache_effect.png", dpi=150)
print("Saved: plot_ram_cache_effect.png")

# Summary table
print("\n── Summary ──────────────────────────────────────────────────────────")
print(f"  {'experiment':<35}  {'total_s':>8}  {'peak_ram_mb':>12}")
for entry in sorted(raw, key=lambda e: e["experiment"]):
    print(f"  {entry['experiment']:<35}  {entry['total_s']:>8.2f}s  "
          f"{entry['peak_ram_mb']:>10.0f} MB")