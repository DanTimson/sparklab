#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
#  run_all.sh
#  Orchestrates all 4 experiments.
#
#  Prerequisites (WSL2):
#    - Docker Desktop running
#    - pip install pyspark psutil pandas numpy matplotlib
#    - JAVA_HOME set (e.g. export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64)
#
#  Usage:  bash run_all.sh
# ─────────────────────────────────────────────────────────────────────────────

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

HDFS_URL="hdfs://localhost:9000"

log() { echo -e "\n\033[1;36m══ $* ══\033[0m"; }
die() { echo -e "\033[1;31mERROR: $*\033[0m"; exit 1; }

# ── 0. Generate dataset ──────────────────────────────────────────────────────
log "STEP 0: Generate dataset"
python data/generate_dataset.py

SPARK_SUBMIT="spark-submit \
  --master local[*] \
  --driver-memory 1g \
  --conf spark.driver.extraJavaOptions=-Dlog4j.configuration=file:config/log4j.properties"

upload_to_hdfs() {
    log "Uploading data to HDFS"
    docker exec namenode hdfs dfs -mkdir -p /data
    docker exec namenode hdfs dfs -rm -f /data/transactions.csv 2>/dev/null || true
    docker cp data/transactions.csv namenode:/tmp/transactions.csv
    docker exec namenode hdfs dfs -put /tmp/transactions.csv /data/transactions.csv
    docker exec namenode hdfs dfs -ls /data
}

# ════════════════════════════════════════════════════════════════════════════
#  EXPERIMENT 1 & 2  —  1 DataNode
# ════════════════════════════════════════════════════════════════════════════
log "Starting 1-DataNode Hadoop cluster"
docker compose -f docker/1dn/docker-compose.yml down -v 2>/dev/null || true
docker compose -f docker/1dn/docker-compose.yml up -d

echo "Waiting for HDFS to become healthy (60s)…"
sleep 60

upload_to_hdfs

# Experiment 1: 1 DN, baseline
log "EXPERIMENT 1/4: 1 DataNode — Baseline Spark"
$SPARK_SUBMIT spark/spark_app.py "$HDFS_URL" 1dn_base

# Experiment 2: 1 DN, optimized
log "EXPERIMENT 2/4: 1 DataNode — Optimized Spark"
$SPARK_SUBMIT spark/spark_app_optimized.py "$HDFS_URL" 1dn_opt

log "Stopping 1-DataNode cluster"
docker compose -f docker/1dn/docker-compose.yml down -v

# ════════════════════════════════════════════════════════════════════════════
#  EXPERIMENT 3 & 4  —  3 DataNodes
# ════════════════════════════════════════════════════════════════════════════
log "Starting 3-DataNode Hadoop cluster"
docker compose -f docker/3dn/docker-compose.yml up -d

echo "Waiting for HDFS to become healthy (75s)…"
sleep 75

upload_to_hdfs

# Experiment 3: 3 DN, baseline
log "EXPERIMENT 3/4: 3 DataNodes — Baseline Spark"
$SPARK_SUBMIT spark/spark_app.py "$HDFS_URL" 3dn_base

# Experiment 4: 3 DN, optimized
log "EXPERIMENT 4/4: 3 DataNodes — Optimized Spark"
$SPARK_SUBMIT spark/spark_app_optimized.py "$HDFS_URL" 3dn_opt

log "Stopping 3-DataNode cluster"
docker compose -f docker/3dn/docker-compose.yml down -v

# ════════════════════════════════════════════════════════════════════════════
#  COMPARE
# ════════════════════════════════════════════════════════════════════════════
log "Generating comparison plots"
python results/compare_results.py

echo -e "\n\033[1;32m✓ All experiments complete. Results in results/\033[0m"