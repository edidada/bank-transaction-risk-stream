#!/usr/bin/env bash
# 端到端冒烟检查：MySQL 源表 -> Kafka -> Doris -> Hudi 变更日志
set -uo pipefail

INFRA_DIR="$(cd "$(dirname "$0")/.." && pwd)"
COMPOSE="docker compose -f $INFRA_DIR/docker-compose.yml"
NETWORK="bank-risk-sim_bank-net"
FAILED=0

check() {
  local name="$1"; shift
  echo "----- $name -----"
  if "$@"; then
    echo "[OK] $name"
  else
    echo "[FAIL] $name" >&2
    FAILED=1
  fi
}

check "MySQL 源表交易数" \
  $COMPOSE exec -T mysql mysql -uroot -proot -N -e "SELECT COUNT(*) FROM bank.\`transaction\`"

check "Kafka CDC Topic 最新 3 条消息" \
  $COMPOSE exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 --topic bank-cdc-events \
    --max-messages 3 --timeout-ms 10000 --from-beginning

check "Kafka DLQ Topic 积压量" \
  $COMPOSE exec -T kafka /opt/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
    --bootstrap-server localhost:9092 --topic bank-cdc-dlq

check "Doris 风控结果分布" \
  docker run --rm --network "$NETWORK" mysql:8.0.39 \
    mysql -h doris-fe -P 9030 -uroot -e \
    "SELECT level, COUNT(*) AS cnt FROM risk.risk_event_detail GROUP BY level;"

check "Doris 高风险样例" \
  docker run --rm --network "$NETWORK" mysql:8.0.39 \
    mysql -h doris-fe -P 9030 -uroot -e \
    "SELECT transaction_id, user_id, score, reasons FROM risk.risk_event_detail WHERE level='HIGH' ORDER BY event_time DESC LIMIT 5;"

echo "----- Hudi 等价变更日志文件 -----"
find "$INFRA_DIR/data/hudi" -name '*.ndjson' -type f 2>/dev/null | head -10
if find "$INFRA_DIR/data/hudi" -name '*.ndjson' -type f 2>/dev/null | grep -q .; then
  echo "[OK] 变更日志已落盘"
else
  echo "[FAIL] 未找到变更日志文件" >&2
  FAILED=1
fi

exit "$FAILED"
