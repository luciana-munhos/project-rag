#!/usr/bin/env bash
set -euo pipefail

KAFKA_CONTAINER="${KAFKA_CONTAINER:-kafka}"
BS="${BS:-localhost:9092}"

TOPICS=(
  raw-events
  verified-events
  dlq-events
  indexed-events
)

echo "Using Kafka container: $KAFKA_CONTAINER"
echo "Bootstrap server: $BS"
echo

for t in "${TOPICS[@]}"; do
  echo "== Topic: $t =="

  if docker exec "$KAFKA_CONTAINER" kafka-topics \
       --bootstrap-server "$BS" \
       --list | grep -qx "$t"; then
    echo "Deleting $t"
    docker exec "$KAFKA_CONTAINER" kafka-topics \
      --bootstrap-server "$BS" \
      --delete \
      --topic "$t" || true
  else
    echo "Topic $t does not exist"
  fi
done

# Kafka deletes topics asynchronously
sleep 2

for t in "${TOPICS[@]}"; do
  echo "Creating $t"
  docker exec "$KAFKA_CONTAINER" kafka-topics \
    --bootstrap-server "$BS" \
    --create \
    --topic "$t" \
    --partitions 1 \
    --replication-factor 1 || true
done

echo
echo "Done."
