"""
compare_results.py
──────────────────
Reads results/experiment_results.json and produces comparison charts
for all 4 experiments:
  1dn_base | 1dn_opt | 3dn_base | 3dn_opt

Outputs:
  results/comparison_total_time.png
  results/comparison_peak_ram.png
  results/comparison_steps.png
  results/comparison_speedup.png
"""

import json
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import numpy as np

RESULTS_FILE = Path("results/experiment_results.json")
OUT_DIR      = Path("results")

EXP_ORDER  = ["1dn_base", "1dn_opt", "3dn_base", "3dn_opt"]
EXP_LABELS = {
    "1dn_base": "1 DN\nBaseline",
    "1dn_opt":  "1 DN\nOptimized",
    "3dn_base": "3 DN\nBaseline",
    "3dn_opt":  "3 DN\nOptimized",
}
COLORS = {
    "1dn_base": "#4C72B0",
    "1dn_opt":  "#55A868",
    "3dn_base": "#C44E52",
    "3dn_opt":  "#8172B2",
}

STEP_ORDER = [
    "1_load_count",
    "1_load_repartition",
    "2_feature_engineering",
    "2_feature_engineering_cached",
    "3_filter",
    "3_broadcast_join",
    "4_groupby_agg",
    "5_window_rank",
    "6_write_hdfs",
]
STEP_DISPLAY = {
    "1_load_count":                   "Load + Count",
    "1_load_repartition":             "Load + Repartition",
    "2_feature_engineering":          "Feature Eng.",
    "2_feature_engineering_cached":   "Feature Eng.\n(cached)",
    "3_filter":                       "Filter",
    "3_broadcast_join":               "Broadcast Join",
    "4_groupby_agg":                  "GroupBy Agg.",
    "5_window_rank":                  "Window Rank",
    "6_write_hdfs":                   "Write HDFS",
}

# ── Load data ────────────────────────────────────────────────────────────────
if not RESULTS_FILE.exists():
    print(f"ERROR: {RESULTS_FILE} not found. Run all experiments first.")
    sys.exit(1)

with open(RESULTS_FILE) as f:
    raw = json.load(f)

# If multiple runs of same experiment exist, take the latest
data: dict = {}
for entry in raw:
    exp = entry["experiment"]
    data[exp] = entry   # later entries overwrite earlier

missing = [e for e in EXP_ORDER if e not in data]
if missing:
    print(f"WARNING: missing experiments: {missing}")
    print("Available:", list(data.keys()))

present = [e for e in EXP_ORDER if e in data]

def bar_labels(ax, bars, fmt="{:.1f}"):
    for bar in bars:
        h = bar.get_height()
        ax.text(bar.get_x() + bar.get_width() / 2, h + h * 0.02,
                fmt.format(h), ha="center", va="bottom", fontsize=9, fontweight="bold")

plt.rcParams.update({
    "figure.facecolor": "white",
    "axes.facecolor":   "#f8f8f8",
    "axes.grid":        True,
    "grid.alpha":       0.4,
    "axes.spines.top":  False,
    "axes.spines.right":False,
    "font.size":        11,
})

# ════════════════════════════════════════════════════════════════════════════
#  Plot 1: Total execution time
# ════════════════════════════════════════════════════════════════════════════
fig, ax = plt.subplots(figsize=(8, 5))
xs      = np.arange(len(present))
bars    = ax.bar(
    xs,
    [data[e]["total_s"] for e in present],
    color=[COLORS[e] for e in present],
    width=0.55, edgecolor="white", linewidth=1.2
)
bar_labels(ax, bars, "{:.1f}s")
ax.set_xticks(xs)
ax.set_xticklabels([EXP_LABELS[e] for e in present])
ax.set_ylabel("Total time (seconds)")
ax.set_title("Total Execution Time — 4 Experiments", fontweight="bold", pad=12)
plt.tight_layout()
fig.savefig(OUT_DIR / "comparison_total_time.png", dpi=150)
print("Saved: comparison_total_time.png")

# ════════════════════════════════════════════════════════════════════════════
#  Plot 2: Peak RAM
# ════════════════════════════════════════════════════════════════════════════
fig, ax = plt.subplots(figsize=(8, 5))
bars    = ax.bar(
    xs,
    [data[e]["peak_ram_mb"] for e in present],
    color=[COLORS[e] for e in present],
    width=0.55, edgecolor="white", linewidth=1.2
)
bar_labels(ax, bars, "{:.0f} MB")
ax.set_xticks(xs)
ax.set_xticklabels([EXP_LABELS[e] for e in present])
ax.set_ylabel("Peak driver RAM (MB)")
ax.set_title("Peak Memory Usage — 4 Experiments", fontweight="bold", pad=12)
plt.tight_layout()
fig.savefig(OUT_DIR / "comparison_peak_ram.png", dpi=150)
print("Saved: comparison_peak_ram.png")

# ════════════════════════════════════════════════════════════════════════════
#  Plot 3: Per-step timing heatmap
# ════════════════════════════════════════════════════════════════════════════
# Collect step timings — some steps only exist in base, some only in opt
all_step_keys = []
for e in present:
    for k in data[e]["checkpoints"]:
        if k not in all_step_keys:
            all_step_keys.append(k)

matrix = []
for e in present:
    row = [data[e]["checkpoints"].get(k, {}).get("elapsed_s", 0) for k in all_step_keys]
    matrix.append(row)
matrix = np.array(matrix)

fig, ax = plt.subplots(figsize=(max(10, len(all_step_keys) * 1.3), 4.5))
im = ax.imshow(matrix, aspect="auto", cmap="YlOrRd")
ax.set_xticks(range(len(all_step_keys)))
ax.set_xticklabels([STEP_DISPLAY.get(k, k) for k in all_step_keys],
                    rotation=35, ha="right", fontsize=9)
ax.set_yticks(range(len(present)))
ax.set_yticklabels([EXP_LABELS[e] for e in present])
for i in range(len(present)):
    for j in range(len(all_step_keys)):
        v = matrix[i, j]
        ax.text(j, i, f"{v:.1f}s" if v else "—", ha="center", va="center",
                fontsize=8, color="black" if v < matrix.max() * 0.6 else "white")
plt.colorbar(im, ax=ax, label="seconds")
ax.set_title("Per-Step Timing Heatmap", fontweight="bold", pad=12)
plt.tight_layout()
fig.savefig(OUT_DIR / "comparison_steps.png", dpi=150)
print("Saved: comparison_steps.png")

# ════════════════════════════════════════════════════════════════════════════
#  Plot 4: Speedup ratios (relative to 1dn_base)
# ════════════════════════════════════════════════════════════════════════════
if "1dn_base" in data:
    base_time = data["1dn_base"]["total_s"]
    speedups  = {e: base_time / data[e]["total_s"] for e in present}
    fig, ax   = plt.subplots(figsize=(8, 5))
    bars = ax.bar(
        xs,
        [speedups[e] for e in present],
        color=[COLORS[e] for e in present],
        width=0.55, edgecolor="white", linewidth=1.2
    )
    ax.axhline(1.0, color="gray", linestyle="--", linewidth=1, label="Baseline (1×)")
    bar_labels(ax, bars, "{:.2f}×")
    ax.set_xticks(xs)
    ax.set_xticklabels([EXP_LABELS[e] for e in present])
    ax.set_ylabel("Speedup (×) vs. 1 DN Baseline")
    ax.set_title("Speedup Relative to 1 DN Baseline", fontweight="bold", pad=12)
    ax.legend()
    plt.tight_layout()
    fig.savefig(OUT_DIR / "comparison_speedup.png", dpi=150)
    print("Saved: comparison_speedup.png")

print("\n── Summary ─────────────────────────────────────────────────────")
for e in present:
    d = data[e]
    print(f"  {e:<12}  total={d['total_s']:6.2f}s  peak_ram={d['peak_ram_mb']:6.0f} MB")