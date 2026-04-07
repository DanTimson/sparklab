#!/usr/bin/env bash
# run_all.sh — full experiment matrix
#
# sizes    × 100k | 1M | 3M
# clusters × 1dn  | 3dn
# variants × 12
# = 72 experiments
#
# Variants:
#   baseline        no opts
#   cache           cache after filter
#   cache_prefilter cache before filter  (order-of-ops contrast)
#   broadcast       broadcast join only
#   repartition     full shuffle to REPART_N partitions
#   coalesce        merge to COALESCE_N partitions, no shuffle
#   aqe             adaptive partition coalescing
#   cache_broadcast cache + broadcast
#   cache_coalesce  cache + coalesce
#   all_repart      cache + broadcast + repartition
#   all_coalesce    cache + broadcast + coalesce
#   all_aqe         cache + broadcast + aqe

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

HDFS_URL="hdfs://namenode:9000"
REPART_N=8
COALESCE_N=4

log()  { echo -e "\n\033[1;36m══ $* ══\033[0m"; }
info() { echo -e "  \033[0;33m→\033[0m $*"; }

SPARK="docker exec spark-client spark-submit \
  --master local[*] \
  --driver-memory 1g \
  --conf spark.driver.extraJavaOptions=-Dlog4j.configuration=file:/app/config/log4j.properties \
  /app/spark/spark_app_conf.py --hdfs $HDFS_URL"

run_exp() {
    local label=$1; shift
    info "── $label  $*"
    $SPARK --label "$label" "$@"
}

upload_to_hdfs() {
    docker exec namenode hdfs dfsadmin -safemode wait
    docker exec namenode hdfs dfs -mkdir -p /data
    docker exec namenode hdfs dfs -rm -f /data/transactions.csv 2>/dev/null || true
    docker cp data/transactions.csv namenode:/tmp/transactions.csv
    docker exec namenode hdfs dfs -put /tmp/transactions.csv /data/transactions.csv
    info "$(docker exec namenode hdfs dfs -du -h /data)"
}

wait_cluster() {
    info "Waiting $1s for cluster..."
    sleep "$1"
    docker exec namenode hdfs dfsadmin -report | grep -E "^(Live|Dead) datanodes" || true
}

mkdir -p results

declare -a SIZE_ORDER=( "100k" "1M" "3M" )
declare -A SIZE_ROWS=( ["100k"]=100000 ["1M"]=1000000 ["3M"]=3000000 )

for size in "${SIZE_ORDER[@]}"; do
    log "Generating dataset: $size (${SIZE_ROWS[$size]} rows)"
    python data/generate_dataset.py "${SIZE_ROWS[$size]}"

    for dn_tag in "1dn" "3dn"; do
        wait_s=60; [[ "$dn_tag" == "3dn" ]] && wait_s=75

        log "$size — ${dn_tag} cluster"
        docker compose -f "docker/${dn_tag}/docker-compose.yml" down -v 2>/dev/null || true
        docker compose -f "docker/${dn_tag}/docker-compose.yml" up -d --build
        wait_cluster $wait_s
        upload_to_hdfs

        p="${size}_${dn_tag}"

        run_exp "${p}_baseline"
        run_exp "${p}_cache"            --cache
        run_exp "${p}_cache_prefilter"  --cache-prefilter
        run_exp "${p}_broadcast"        --broadcast
        run_exp "${p}_repartition"      --repartition "$REPART_N"
        run_exp "${p}_coalesce"         --coalesce "$COALESCE_N"
        run_exp "${p}_aqe"              --aqe
        run_exp "${p}_cache_broadcast"  --cache --broadcast
        run_exp "${p}_cache_coalesce"   --cache --coalesce "$COALESCE_N"
        run_exp "${p}_all_repart"       --cache --broadcast --repartition "$REPART_N"
        run_exp "${p}_all_coalesce"     --cache --broadcast --coalesce "$COALESCE_N"
        run_exp "${p}_all_aqe"          --cache --broadcast --aqe

        docker compose -f "docker/${dn_tag}/docker-compose.yml" down -v
    done
done

log "Generating plots"
python results/compare_results.py

echo -e "\n\033[1;32m✓ All 72 experiments complete.\033[0m"