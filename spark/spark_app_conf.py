"""
Flags (all off by default):
  --cache            cache filtered DF after filter
  --cache-prefilter  cache before filter (larger DF — order-of-ops contrast)
  --broadcast        broadcast-join metadata table instead of shuffle join
  --repartition N    full shuffle to N partitions after filter
  --coalesce N       merge to N partitions after filter, no shuffle
  --aqe              enable Adaptive Query Execution

--repartition and --aqe are mutually exclusive by design: both control
shuffle partition count, AQE reactively vs repartition upfront.
"""

import time
import json
import logging
import argparse
from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ── Args ──────────────────────────────────────────────────────────────────────
parser = argparse.ArgumentParser()
parser.add_argument("--hdfs",            default="hdfs://namenode:9000")
parser.add_argument("--label",           default="experiment")
parser.add_argument("--cache",           action="store_true")
parser.add_argument("--cache-prefilter", action="store_true")
parser.add_argument("--broadcast",       action="store_true")
parser.add_argument("--repartition",     type=int, default=0)
parser.add_argument("--coalesce",        type=int, default=0)
parser.add_argument("--aqe",             action="store_true")
args = parser.parse_args()

HDFS_URL     = args.hdfs
EXP_LABEL    = args.label
HDFS_INPUT   = f"{HDFS_URL}/data/transactions.csv"
HDFS_OUT     = f"{HDFS_URL}/output/{EXP_LABEL}"
RESULTS_FILE = Path("results/experiment_results.json")

opts = {
    "cache":           args.cache,
    "cache_prefilter": args.cache_prefilter,
    "broadcast":       args.broadcast,
    "repartition":     args.repartition,
    "coalesce":        args.coalesce,
    "aqe":             args.aqe,
}

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("SparkLab")

def get_jvm_heap_mb():
    # Runtime.getRuntime() queries the JVM heap directly.
    # Python-side psutil would only see the Py4J wrapper (~90 MB constant).
    rt = spark._jvm.Runtime.getRuntime()
    return (rt.totalMemory() - rt.freeMemory()) / 1_048_576

def checkpoint(label, t0, metrics):
    elapsed = time.perf_counter() - t0
    ram     = get_jvm_heap_mb()
    log.info(f"[CHECKPOINT] {label:<42}  elapsed={elapsed:7.2f}s  JVM heap={ram:7.1f} MB")
    metrics[label] = {"elapsed_s": round(elapsed, 3), "ram_mb": round(ram, 1)}

def save_results(metrics, total_s):
    RESULTS_FILE.parent.mkdir(parents=True, exist_ok=True)
    entry = {
        "experiment":  EXP_LABEL,
        "timestamp":   datetime.now().isoformat(),
        "total_s":     round(total_s, 3),
        "peak_ram_mb": max(v["ram_mb"] for v in metrics.values()),
        "opts":        opts,
        "checkpoints": metrics,
    }
    existing = []
    if RESULTS_FILE.exists():
        with open(RESULTS_FILE) as f:
            try:
                existing = json.load(f)
            except json.JSONDecodeError:
                existing = []
    existing = [e for e in existing if e["experiment"] != EXP_LABEL]
    existing.append(entry)
    with open(RESULTS_FILE, "w") as f:
        json.dump(existing, f, indent=2)
    log.info(f"Saved → {RESULTS_FILE}  total={total_s:.2f}s")

# Spark session
log.info(f"Experiment : {EXP_LABEL}")
log.info(f"Opts active: { {k: v for k, v in opts.items() if v} }")

builder = (
    SparkSession.builder
    .appName(f"SparkLab_{EXP_LABEL}")
    # Disable auto-broadcast so join strategy is controlled by --broadcast flag
    .config("spark.sql.autoBroadcastJoinThreshold", "-1")
    .config("spark.sql.adaptive.enabled", "true" if args.aqe else "false")
)
if args.aqe:
    builder = builder.config("spark.sql.adaptive.coalescePartitions.enabled", "true")
if args.broadcast:
    builder = builder.config("spark.sql.autoBroadcastJoinThreshold", str(20 * 1024 * 1024))

spark = builder.getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
spark.sparkContext._jvm.org.apache.log4j.Logger \
    .getLogger("org.apache.spark.scheduler.DAGScheduler") \
    .setLevel(spark.sparkContext._jvm.org.apache.log4j.Level.INFO)

metrics  = {}
t_global = time.perf_counter()
df_cached = None

#  STEP 1 — Load
log.info("── STEP 1: Load")
t0 = time.perf_counter()

df_raw    = spark.read.option("header", "true").option("inferSchema", "true").csv(HDFS_INPUT)
row_count = df_raw.count()
checkpoint("1_load", t0, metrics)
log.info(f"  Rows: {row_count:,}")

#  STEP 2 — Feature engineering
log.info("── STEP 2: Feature engineering")
t0 = time.perf_counter()

df_featured = (
    df_raw
    .dropna()
    .withColumn("revenue",      F.round(F.col("price") * F.col("quantity") *
                                        (1 - F.col("discount")), 2))
    .withColumn("price_bucket", F.when(F.col("price") < 50,  "low")
                                 .when(F.col("price") < 300, "mid")
                                 .otherwise("high"))
)
df_featured.count()
checkpoint("2_features", t0, metrics)

# ── Cache pre-filter ────────────────────────────────
if args.cache_prefilter:
    log.info("── STEP 2c: Cache pre-filter")
    t0 = time.perf_counter()
    df_featured = df_featured.cache()
    df_featured.count()
    df_cached = df_featured
    checkpoint("2_cache_prefilter", t0, metrics)

#  STEP 3 — Filter
log.info("── STEP 3: Filter")
t0 = time.perf_counter()

df = df_featured.filter(F.col("discount") > 0.1)

if args.repartition > 0:
    df = df.repartition(args.repartition)
elif args.coalesce > 0:
    df = df.coalesce(args.coalesce)

df.count()
checkpoint("3_filter", t0, metrics)
log.info(f"  Partitions: {df.rdd.getNumPartitions()}")

# Cache post-filter
if args.cache:
    log.info("── STEP 3c: Cache post-filter")
    t0 = time.perf_counter()
    df       = df.cache()
    df.count()
    df_cached = df
    checkpoint("3_cache", t0, metrics)

#  STEP 4 — Metadata join
log.info(f"── STEP 4: Join ({'broadcast' if args.broadcast else 'shuffle'})")
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

if args.broadcast:
    df_enriched = df.join(F.broadcast(category_meta), on="category", how="left")
else:
    df_enriched = df.join(category_meta, on="category", how="left")

df_enriched.count()
checkpoint("4_join", t0, metrics)

#  STEP 5 — GroupBy aggregation
log.info("── STEP 5: GroupBy")
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
checkpoint("5_groupby", t0, metrics)

#  STEP 6 — Window rank
log.info("── STEP 6: Window rank")
t0 = time.perf_counter()

w = Window.partitionBy("category").orderBy(F.desc("revenue"))
df.withColumn("rank_in_cat", F.rank().over(w)).filter(F.col("rank_in_cat") <= 3).count()
checkpoint("6_window_rank", t0, metrics)

#  STEP 7 — Write
log.info("── STEP 7: Write")
t0 = time.perf_counter()

df_agg.coalesce(1).write.mode("overwrite").csv(HDFS_OUT + "/agg", header=True)
checkpoint("7_write", t0, metrics)

if df_cached is not None:
    df_cached.unpersist()

total_s = time.perf_counter() - t_global
log.info(f"═══ {EXP_LABEL} complete — {total_s:.2f}s ═══")
save_results(metrics, total_s)
spark.stop()