#!/usr/bin/env bash
# run_all.sh — orchestrates all 4 experiments
# spark-submit runs inside the spark-client container (same Docker network
# as the DataNodes), so HDFS block reads work without any host routing tricks.

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

HDFS_URL="hdfs://namenode:9000"

log()  { echo -e "\n\033[1;36m══ $* ══\033[0m"; }
info() { echo -e "  \033[0;33m→\033[0m $*"; }
die()  { echo -e "\033[1;31mERROR: $*\033[0m"; exit 1; }

# spark-submit inside the container; /app is the mounted project root
spark_run() {
    local script=$1
    local label=$2
    info "Running: $script  label=$label"
    docker exec spark-client spark-submit \
        --master "local[*]" \
        --driver-memory 1g \
        --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:/app/config/log4j.properties" \
        "/app/$script" "$HDFS_URL" "$label"
}

upload_to_hdfs() {
    log "Uploading data to HDFS"
    info "Waiting for NameNode to leave safe mode..."
    docker exec namenode hdfs dfsadmin -safemode wait
    docker exec namenode hdfs dfs -mkdir -p /data
    docker exec namenode hdfs dfs -rm -f /data/transactions.csv 2>/dev/null || true
    docker cp data/transactions.csv namenode:/tmp/transactions.csv
    docker exec namenode hdfs dfs -put /tmp/transactions.csv /data/transactions.csv
    docker exec namenode hdfs dfs -ls /data
}

wait_healthy() {
    local expected=$1
    local seconds=$2
    info "Waiting ${seconds}s for cluster..."
    sleep "$seconds"
    info "DataNode report:"
    docker exec namenode hdfs dfsadmin -report | grep -E "^(Live|Dead) datanodes" || true
}

# ── 0. Generate dataset ───────────────────────────────────────────────────────
log "STEP 0: Generate dataset"
python data/generate_dataset.py
mkdir -p results

# ════════════════════════════════════════════════════════════════════════════
#  EXPERIMENTS 1 & 2  —  1 DataNode
# ════════════════════════════════════════════════════════════════════════════
log "Starting 1-DataNode cluster (building spark-client if needed)"
docker compose -f docker/1dn/docker-compose.yml down -v 2>/dev/null || true
docker compose -f docker/1dn/docker-compose.yml up -d --build
wait_healthy 1 60

upload_to_hdfs

log "EXPERIMENT 1/4: 1 DataNode — Baseline"
spark_run spark/spark_app.py 1dn_base

log "EXPERIMENT 2/4: 1 DataNode — Optimized"
spark_run spark/spark_app_opti.py 1dn_opt

log "Stopping 1-DataNode cluster"
docker compose -f docker/1dn/docker-compose.yml down -v

# ════════════════════════════════════════════════════════════════════════════
#  EXPERIMENTS 3 & 4  —  3 DataNodes
# ════════════════════════════════════════════════════════════════════════════
log "Starting 3-DataNode cluster"
docker compose -f docker/3dn/docker-compose.yml up -d --build
wait_healthy 3 75

upload_to_hdfs

log "EXPERIMENT 3/4: 3 DataNodes — Baseline"
spark_run spark/spark_app.py 3dn_base

log "EXPERIMENT 4/4: 3 DataNodes — Optimized"
spark_run spark/spark_app_opti.py 3dn_opt

log "Stopping 3-DataNode cluster"
docker compose -f docker/3dn/docker-compose.yml down -v

# ════════════════════════════════════════════════════════════════════════════
#  COMPARE
# ════════════════════════════════════════════════════════════════════════════
log "Generating comparison plots"
python results/compare_results.py

echo -e "\n\033[1;32m✓ All experiments complete. Results in results/\033[0m"