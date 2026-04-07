"""
spark_app_optimized.py  —  Optimized Spark application
────────────────────────────────────────────────────────
Optimizations applied vs. baseline:
  1. .repartition()    — tune parallelism to cluster size
  2. .cache()          — persist filtered DF in memory (reused in steps 4 & 5)
  3. Broadcast join    — small category-metadata table joined without shuffle
  4. Avoid re-reading  — all downstream ops from cached DF
  5. Kryo serializer   — faster than Java default

Usage:
  spark-submit \
    --master local[*] \
    --driver-memory 1g \
    --conf spark.driver.extraJavaOptions="-Dlog4j.configuration=file:config/log4j.properties" \
    spark/spark_app_optimized.py [hdfs://namenode:9000] [experiment_label]

Experiment labels: "1dn_opt" | "3dn_opt"
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

# ── CLI args ─────────────────────────────────────────────────────────────────
HDFS_URL   = sys.argv[1] if len(sys.argv) > 1 else "hdfs://namenode:9000"
EXP_LABEL  = sys.argv[2] if len(sys.argv) > 2 else "1dn_opt"
HDFS_INPUT = f"{HDFS_URL}/data/transactions.csv"
HDFS_OUT   = f"{HDFS_URL}/output/{EXP_LABEL}"
RESULTS_FILE = Path("results/experiment_results.json")

# Number of partitions — set to 2× physical cores for local mode.
# For a real YARN cluster, set to 2-3× number of executor cores.
import multiprocessing
N_PARTITIONS = max(4, multiprocessing.cpu_count() * 2)

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("SparkLab")

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
log.info(f"Starting OPTIMIZED experiment: {EXP_LABEL}")
log.info(f"Target partitions: {N_PARTITIONS}")

spark = (
    SparkSession.builder
    .appName(f"SparkLab_{EXP_LABEL}")
    .config("spark.driver.memory", "1g")
    .config("spark.executor.memory", "1g")
    # ── Optimization configs ─────────────────────────────────────────────
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.default.parallelism",          str(N_PARTITIONS))
    .config("spark.sql.shuffle.partitions",       str(N_PARTITIONS))
    # Adaptive Query Execution (Spark 3+): auto-coalesces shuffle partitions
    .config("spark.sql.adaptive.enabled",         "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    # Broadcast join threshold — tables < 20 MB broadcast automatically
    .config("spark.sql.autoBroadcastJoinThreshold", str(20 * 1024 * 1024))
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")
spark.sparkContext._jvm.org.apache.log4j.Logger \
    .getLogger("org.apache.spark.scheduler.DAGScheduler") \
    .setLevel(spark.sparkContext._jvm.org.apache.log4j.Level.INFO)

metrics: dict = {}
t_global = time.perf_counter()

# ═══════════════════════════════════════════════════════════════════════════
#  STEP 1 — Load + repartition
# ═══════════════════════════════════════════════════════════════════════════
log.info("── STEP 1: Load CSV from HDFS + repartition")
t0 = time.perf_counter()

df_raw = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(HDFS_INPUT)
    .repartition(N_PARTITIONS)   # ← distribute evenly across partitions
)
row_count = df_raw.count()
checkpoint("1_load_repartition", t0, metrics)
log.info(f"  Rows: {row_count:,}  |  Partitions: {df_raw.rdd.getNumPartitions()}")

# ═══════════════════════════════════════════════════════════════════════════
#  STEP 2 — Feature engineering + CACHE the filtered DF
#           (reused in steps 3, 4, 5 — avoids re-reading HDFS)
# ═══════════════════════════════════════════════════════════════════════════
log.info("── STEP 2: Feature engineering + cache")
t0 = time.perf_counter()

df = (
    df_raw
    .dropna()
    .withColumn("revenue",      F.round(F.col("price") * F.col("quantity") *
                                         (1 - F.col("discount")), 2))
    .withColumn("price_bucket", F.when(F.col("price") < 50,   "low")
                                 .when(F.col("price") < 300,  "mid")
                                 .otherwise("high"))
    .filter(F.col("discount") > 0.1)   # combine filter here to cache smaller DF
    .cache()                            # ← PERSIST in memory
)
cached_count = df.count()   # materialise the cache
checkpoint("2_feature_engineering_cached", t0, metrics)
log.info(f"  Cached rows: {cached_count:,}")

# ═══════════════════════════════════════════════════════════════════════════
#  STEP 3 — Broadcast join with small metadata table
#           (category tier lookup — built in driver, broadcast to executors)
# ═══════════════════════════════════════════════════════════════════════════
log.info("── STEP 3: Broadcast join — category metadata")
t0 = time.perf_counter()

category_meta = spark.createDataFrame([
    ("Electronics",    "tech",       "A"),
    ("Clothing",       "apparel",    "B"),
    ("Books",          "media",      "C"),
    ("Home & Garden",  "home",       "B"),
    ("Sports",         "lifestyle",  "B"),
    ("Toys",           "kids",       "C"),
    ("Food & Beverage","consumable", "A"),
    ("Health",         "wellness",   "A"),
], ["category", "vertical", "tier"])

# Spark will auto-broadcast (< threshold) — explicit hint for clarity
df_enriched = df.join(F.broadcast(category_meta), on="category", how="left")
enriched_count = df_enriched.count()
checkpoint("3_broadcast_join", t0, metrics)
log.info(f"  Enriched rows: {enriched_count:,}")

# ═══════════════════════════════════════════════════════════════════════════
#  STEP 4 — GroupBy aggregation (reads from cache, no HDFS re-read)
# ═══════════════════════════════════════════════════════════════════════════
log.info("── STEP 4: GroupBy aggregation (from cache)")
t0 = time.perf_counter()

df_agg = (
    df_enriched
    .groupBy("category", "region", "tier")
    .agg(
        F.count("*").alias("tx_count"),
        F.sum("revenue").alias("total_revenue"),
        F.avg("discount").alias("avg_discount"),
        F.avg("quantity").alias("avg_quantity"),
    )
    .orderBy(F.desc("total_revenue"))
)
df_agg.count()
checkpoint("4_groupby_agg", t0, metrics)

# ═══════════════════════════════════════════════════════════════════════════
#  STEP 5 — Window function (reads from cache)
# ═══════════════════════════════════════════════════════════════════════════
log.info("── STEP 5: Window rank (from cache)")
t0 = time.perf_counter()

w = Window.partitionBy("category").orderBy(F.desc("revenue"))
df_ranked = df.withColumn("rank_in_cat", F.rank().over(w))
df_ranked.filter(F.col("rank_in_cat") <= 3).count()
checkpoint("5_window_rank", t0, metrics)

# ═══════════════════════════════════════════════════════════════════════════
#  STEP 6 — Write
# ═══════════════════════════════════════════════════════════════════════════
log.info(f"── STEP 6: Write to {HDFS_OUT}")
t0 = time.perf_counter()

df_agg.coalesce(1).write.mode("overwrite").csv(HDFS_OUT + "/aggregation", header=True)
checkpoint("6_write_hdfs", t0, metrics)

# ── Unpersist ────────────────────────────────────────────────────────────────
df.unpersist()

# ── Done ─────────────────────────────────────────────────────────────────────
total_s = time.perf_counter() - t_global
log.info(f"═══ Optimized experiment {EXP_LABEL} complete — total {total_s:.2f}s ═══")
save_results(metrics, total_s)

spark.stop()