"""
spark_app.py  —  Baseline (unoptimized) Spark application
──────────────────────────────────────────────────────────
Usage:
  spark-submit \
    --master local[*] \
    --driver-memory 1g \
    --conf spark.driver.extraJavaOptions="-Dlog4j.configuration=file:config/log4j.properties" \
    spark/spark_app.py [hdfs://namenode:9000] [experiment_label]

Experiment labels: "1dn_base" | "3dn_base"
"""

import sys
import time
import json
import logging
import os
from datetime import datetime
from pathlib import Path

import psutil
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ── CLI args ────────────────────────────────────────────────────────────────
HDFS_URL   = sys.argv[1] if len(sys.argv) > 1 else "hdfs://namenode:9000"
EXP_LABEL  = sys.argv[2] if len(sys.argv) > 2 else "1dn_base"
HDFS_INPUT = f"{HDFS_URL}/data/transactions.csv"
HDFS_OUT   = f"{HDFS_URL}/output/{EXP_LABEL}"
RESULTS_FILE = Path("results/experiment_results.json")

# ── Logging setup ────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("SparkLab")

# ── Helpers ──────────────────────────────────────────────────────────────────
def get_ram_mb() -> float:
    proc = psutil.Process(os.getpid())
    return proc.memory_info().rss / 1_048_576

def checkpoint(label: str, t0: float, metrics: dict):
    elapsed = time.perf_counter() - t0
    ram     = get_ram_mb()
    log.info(f"[CHECKPOINT] {label:<35}  elapsed={elapsed:7.2f}s  RAM={ram:7.1f} MB")
    metrics[label] = {"elapsed_s": round(elapsed, 3), "ram_mb": round(ram, 1)}

def save_results(metrics: dict, total_s: float):
    RESULTS_FILE.parent.mkdir(parents=True, exist_ok=True)
    entry = {
        "experiment": EXP_LABEL,
        "timestamp":  datetime.now().isoformat(),
        "total_s":    round(total_s, 3),
        "peak_ram_mb": max(v["ram_mb"] for v in metrics.values()),
        "checkpoints": metrics,
    }
    existing = []
    if RESULTS_FILE.exists():
        with open(RESULTS_FILE) as f:
            existing = json.load(f)
    existing.append(entry)
    with open(RESULTS_FILE, "w") as f:
        json.dump(existing, f, indent=2)
    log.info(f"Results saved → {RESULTS_FILE}")

# ── Spark session ─────────────────────────────────────────────────────────────
log.info(f"Starting experiment: {EXP_LABEL}")
log.info(f"HDFS input: {HDFS_INPUT}")

spark = (
    SparkSession.builder
    .appName(f"SparkLab_{EXP_LABEL}")
    .config("spark.driver.memory", "1g")
    .config("spark.executor.memory", "1g")
    # Default parallelism — not tuned (baseline)
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")   # suppress Spark internal WARN/FATAL
# Re-enable scheduler logs for job/stage visibility
spark.sparkContext._jvm.org.apache.log4j.Logger \
    .getLogger("org.apache.spark.scheduler.DAGScheduler") \
    .setLevel(spark.sparkContext._jvm.org.apache.log4j.Level.INFO)

metrics: dict = {}
t_global = time.perf_counter()

# ═══════════════════════════════════════════════════════════════════════════
#  STEP 1 — Load CSV from HDFS
# ═══════════════════════════════════════════════════════════════════════════
log.info("── STEP 1: Loading CSV from HDFS")
t0 = time.perf_counter()

df_raw = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(HDFS_INPUT)
)
row_count = df_raw.count()   # first Action → triggers job
checkpoint("1_load_count", t0, metrics)
log.info(f"  Rows loaded: {row_count:,}  |  Schema: {df_raw.dtypes}")

# ═══════════════════════════════════════════════════════════════════════════
#  STEP 2 — Cleaning + derived columns
# ═══════════════════════════════════════════════════════════════════════════
log.info("── STEP 2: Feature engineering")
t0 = time.perf_counter()

df = (
    df_raw
    .dropna()
    .withColumn("revenue",      F.round(F.col("price") * F.col("quantity") *
                                         (1 - F.col("discount")), 2))
    .withColumn("price_bucket", F.when(F.col("price") < 50,   "low")
                                 .when(F.col("price") < 300,  "mid")
                                 .otherwise("high"))
)
# Force evaluation (Action)
_ = df.count()
checkpoint("2_feature_engineering", t0, metrics)

# ═══════════════════════════════════════════════════════════════════════════
#  STEP 3 — Filter: non-trivial discount
# ═══════════════════════════════════════════════════════════════════════════
log.info("── STEP 3: Filtering (discount > 0.1)")
t0 = time.perf_counter()

df_filtered = df.filter(F.col("discount") > 0.1)
filtered_count = df_filtered.count()
checkpoint("3_filter", t0, metrics)
log.info(f"  Rows after filter: {filtered_count:,}")

# ═══════════════════════════════════════════════════════════════════════════
#  STEP 4 — GroupBy aggregation per category
# ═══════════════════════════════════════════════════════════════════════════
log.info("── STEP 4: GroupBy category aggregation")
t0 = time.perf_counter()

df_agg = (
    df_filtered
    .groupBy("category", "region")
    .agg(
        F.count("*").alias("tx_count"),
        F.sum("revenue").alias("total_revenue"),
        F.avg("discount").alias("avg_discount"),
        F.avg("quantity").alias("avg_quantity"),
    )
    .orderBy(F.desc("total_revenue"))
)
df_agg.count()   # Action
checkpoint("4_groupby_agg", t0, metrics)

# ═══════════════════════════════════════════════════════════════════════════
#  STEP 5 — Window function: revenue rank within category
# ═══════════════════════════════════════════════════════════════════════════
log.info("── STEP 5: Window rank (revenue per user per category)")
t0 = time.perf_counter()

w = Window.partitionBy("category").orderBy(F.desc("revenue"))
df_ranked = df_filtered.withColumn("rank_in_cat", F.rank().over(w))
# Show top rows per category (Action)
df_ranked.filter(F.col("rank_in_cat") <= 3).count()
checkpoint("5_window_rank", t0, metrics)

# ═══════════════════════════════════════════════════════════════════════════
#  STEP 6 — Write aggregation result to HDFS
# ═══════════════════════════════════════════════════════════════════════════
log.info(f"── STEP 6: Writing results to {HDFS_OUT}")
t0 = time.perf_counter()

df_agg.coalesce(1).write.mode("overwrite").csv(HDFS_OUT + "/aggregation", header=True)
checkpoint("6_write_hdfs", t0, metrics)

# ═══════════════════════════════════════════════════════════════════════════
#  DONE
# ═══════════════════════════════════════════════════════════════════════════
total_s = time.perf_counter() - t_global
log.info(f"═══ Experiment {EXP_LABEL} complete — total {total_s:.2f}s ═══")
save_results(metrics, total_s)

spark.stop()