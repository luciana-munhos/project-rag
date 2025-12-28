#!/usr/bin/env bash
set -euo pipefail

KAFKA_DIR="${KAFKA_DIR:-$HOME/kafka_2.13-3.8.0}"
BS="${BS:-localhost:9092}"

TOPICS=(
  raw-events
  verified-events
  dlq-events
  indexed-events
)

for t in "${TOPICS[@]}"; do
  echo "== Topic: $t =="

  # delete if exists
  if "$KAFKA_DIR/bin/kafka-topics.sh" --bootstrap-server "$BS" --list | grep -qx "$t"; then
    echo "Deleting $t"
    "$KAFKA_DIR/bin/kafka-topics.sh" --bootstrap-server "$BS" --delete --topic "$t" || true
  fi
done

# Give Kafka a moment to actually delete (sometimes async)
sleep 2

for t in "${TOPICS[@]}"; do
  echo "Creating $t"
  "$KAFKA_DIR/bin/kafka-topics.sh" \
    --bootstrap-server "$BS" \
    --create \
    --topic "$t" \
    --partitions 1 \
    --replication-factor 1 || true
done

echo "Done."
