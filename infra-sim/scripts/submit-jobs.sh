#!/usr/bin/env bash
# 把两个 Flink 作业 Jar 复制进 JobManager 挂载目录并提交。
# 前置：仓库根目录已执行 mvn clean package；infra-sim 下已执行 docker compose up -d
set -euo pipefail

INFRA_DIR="$(cd "$(dirname "$0")/.." && pwd)"
ROOT_DIR="$(cd "$INFRA_DIR/.." && pwd)"
JOBS_DIR="$INFRA_DIR/jobs"
VERSION="1.0.0-SNAPSHOT"
CDC_JAR="cdc-ingestion-$VERSION.jar"
ETL_JAR="stream-etl-$VERSION.jar"

for jar in "$ROOT_DIR/cdc-ingestion/target/$CDC_JAR" "$ROOT_DIR/stream-etl/target/$ETL_JAR"; do
  if [[ ! -f "$jar" ]]; then
    echo "找不到 $jar，请先在仓库根目录执行 mvn clean package" >&2
    exit 1
  fi
done

mkdir -p "$JOBS_DIR"
cp "$ROOT_DIR/cdc-ingestion/target/$CDC_JAR" "$JOBS_DIR/"
cp "$ROOT_DIR/stream-etl/target/$ETL_JAR" "$JOBS_DIR/"
echo "Jar 已就绪: $JOBS_DIR"

FLINK_RUN="/opt/flink/bin/flink run -d"

echo ">>> 提交 cdc-ingestion"
docker compose -f "$INFRA_DIR/docker-compose.yml" exec -T flink-jobmanager \
  $FLINK_RUN "/opt/flink/jobs/$CDC_JAR" \
  --mysql-host mysql \
  --mysql-port 3306 \
  --mysql-user cdc \
  --mysql-password cdc123456 \
  --mysql-database bank \
  --mysql-table bank.transaction \
  --mysql-server-id 5401-5404 \
  --cdc-startup-mode initial \
  --kafka-bootstrap kafka:9092 \
  --kafka-topic bank-cdc-events \
  --parallelism 1 \
  --job-name cdc-ingestion

echo ">>> 提交 stream-etl"
docker compose -f "$INFRA_DIR/docker-compose.yml" exec -T flink-jobmanager \
  $FLINK_RUN "/opt/flink/jobs/$ETL_JAR" \
  --kafka-bootstrap kafka:9092 \
  --kafka-topic bank-cdc-events \
  --dlq-topic bank-cdc-dlq \
  --kafka-startup-mode earliest \
  --doris-fe-host doris-fe \
  --doris-http-port 8030 \
  --doris-database risk \
  --doris-table risk_event_detail \
  --doris-user root \
  --doris-password "" \
  --hudi-base-path file:///data/hudi/bank_transaction_changelog \
  --parallelism 2 \
  --job-name stream-etl

echo ">>> 提交完成，打开 http://localhost:8081 查看作业状态"
