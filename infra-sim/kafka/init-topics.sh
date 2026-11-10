#!/usr/bin/env bash
# 预创建 Topic：显式指定分区数，保证同一 transaction_id 的变更始终落在同一分区。
set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:9092}"
KAFKA_TOPICS=/opt/kafka/bin/kafka-topics.sh

create_topic() {
  local name="$1" partitions="$2"
  if "$KAFKA_TOPICS" --bootstrap-server "$BOOTSTRAP" --list | grep -qx "$name"; then
    echo "Topic 已存在: $name"
  else
    "$KAFKA_TOPICS" --bootstrap-server "$BOOTSTRAP" --create \
      --topic "$name" --partitions "$partitions" --replication-factor 1 \
      --config retention.ms=604800000
    echo "Topic 已创建: $name (partitions=$partitions)"
  fi
}

create_topic "bank-cdc-events" 6
create_topic "bank-cdc-dlq"    3

"$KAFKA_TOPICS" --bootstrap-server "$BOOTSTRAP" --list
