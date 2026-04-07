# Hadoop + Spark Lab

4 experiments: 1 DataNode vs 3 DataNodes × baseline vs optimized Spark.

## Prerequisites

```bash
# Java (required by PySpark)
sudo apt update && sudo apt install -y openjdk-11-jdk
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# Python deps
pip install pyspark psutil pandas numpy matplotlib

# Verify Docker Desktop is running in WSL integration mode
docker info
```

## Project layout

```
hadoop-spark-lab/
├── docker/
│   ├── 1dn/docker-compose.yml    # 1 NameNode + 1 DataNode
│   └── 3dn/docker-compose.yml    # 1 NameNode + 3 DataNodes
├── config/
│   └── log4j.properties          # Suppress WARN/FATAL, keep job/stage INFO
├── data/
│   └── generate_dataset.py       # Creates data/transactions.csv (120k rows)
├── spark/
│   ├── spark_app.py              # Baseline Spark pipeline
│   └── spark_app_optimized.py   # Optimized: cache + repartition + broadcast
├── results/
│   ├── experiment_results.json   # Written by each spark-submit run
│   └── compare_results.py        # Produces 4 comparison PNG charts
└── run_all.sh                    # Runs all 4 experiments end-to-end
```

## Run everything

```bash
cd hadoop-spark-lab
bash run_all.sh
```

To run experiments individually:

```bash
# 1. Generate data
python data/generate_dataset.py

# 2. Start 1-DN cluster
docker compose -f docker/1dn/docker-compose.yml up -d
sleep 60

# 3. Upload to HDFS
docker cp data/transactions.csv namenode:/tmp/transactions.csv
docker exec namenode hdfs dfs -mkdir -p /data
docker exec namenode hdfs dfs -put /tmp/transactions.csv /data/transactions.csv

# 4. Run experiments
spark-submit --master local[*] --driver-memory 1g \
  --conf spark.driver.extraJavaOptions="-Dlog4j.configuration=file:config/log4j.properties" \
  spark/spark_app.py hdfs://localhost:9000 1dn_base

spark-submit --master local[*] --driver-memory 1g \
  --conf spark.driver.extraJavaOptions="-Dlog4j.configuration=file:config/log4j.properties" \
  spark/spark_app_optimized.py hdfs://localhost:9000 1dn_opt

# 5. Stop cluster, start 3-DN cluster, repeat
docker compose -f docker/1dn/docker-compose.yml down -v
docker compose -f docker/3dn/docker-compose.yml up -d
sleep 75
# ... upload + run 3dn_base + 3dn_opt

# 6. Compare
python results/compare_results.py
```

## Dataset schema

| Column          | Type    | Description                       |
|-----------------|---------|-----------------------------------|
| user_id         | int     | Customer ID (1–50000)             |
| product_id      | int     | Product SKU (1–5000)              |
| quantity        | int     | Units ordered (1–20)              |
| price           | float   | Unit price (USD)                  |
| discount        | float   | Discount fraction (0.0–0.5)       |
| category        | string  | Product category (8 values)       |
| payment_method  | string  | Payment type (5 values)           |
| region          | string  | Sales region (6 values)           |

120,000 rows · ~12 MB CSV

## Hadoop block size

Set to **128 MB** via `HDFS_CONF_dfs_blocksize=134217728` in docker-compose.  
With a 12 MB file this means 1 block — to test multi-block behavior, increase
the dataset to 1M+ rows or reduce block size to 1 MB
(`HDFS_CONF_dfs_blocksize=1048576`).

## Memory limits

- Namenode: 1 GB (`deploy.resources.limits.memory`)
- Datanode (1-DN setup): 1 GB
- Datanode (3-DN setup): 768 MB each (3 × 768 MB = 2.25 GB total)
- Spark driver + executor: 1 GB each (set in SparkSession config)

## Optimizations in spark_app_optimized.py

| Technique              | Where applied                     | Effect                                |
|------------------------|-----------------------------------|---------------------------------------|
| `.repartition(N)`      | After CSV load                    | Even parallelism, avoids skew         |
| `.cache()`             | After filter + feature eng.       | Steps 4 & 5 skip HDFS re-read        |
| Broadcast join         | Small metadata table              | No shuffle for small-large join       |
| Kryo serializer        | SparkSession config               | Faster serialization                  |
| AQE (adaptive)         | `spark.sql.adaptive.enabled=true` | Auto-coalesces shuffle partitions     |
| Combined filter        | Merged into cached DF             | Single HDFS scan instead of two       |

## Logs

- Console: job/stage events from DAGScheduler (INFO level)
- File: `results/spark_run.log` (appended per run — clear between experiments)

## Troubleshooting

**NameNode stays in safe mode:**
```bash
docker exec namenode hdfs dfsadmin -safemode leave
```

**`JAVA_HOME` not found by PySpark:**
```bash
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH
```

**Port 9000 already in use:**
Check `ss -tlnp | grep 9000` — another Hadoop or service may be running.

**DataNodes don't register:**
Wait longer (90s) after `docker compose up`. Check with:
```bash
docker exec namenode hdfs dfsadmin -report
```