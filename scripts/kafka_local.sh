#!/usr/bin/env bash
# Single-node Kafka (KRaft mode, no ZooKeeper) for the streaming part. Needs Java 17.
#
#   bash scripts/kafka_local.sh start    # downloads Kafka on first run, starts the broker, creates the topic
#   bash scripts/kafka_local.sh stop
#
# Kafka is installed outside the repo (KAFKA_DIR) because its shell scripts
# break when the path contains spaces.
set -euo pipefail

VERSION=3.9.0
SCALA=2.13
TOPIC=wiki-recentchange
home="${KAFKA_DIR:-$HOME/.local/share/imdb-kafka}"
dist="$home/kafka_${SCALA}-${VERSION}"
conf="$home/server.properties"

case "${1:-start}" in
start)
  mkdir -p "$home"
  if [ ! -d "$dist" ]; then
    echo "download kafka $VERSION"
    curl -fL "https://archive.apache.org/dist/kafka/${VERSION}/kafka_${SCALA}-${VERSION}.tgz" | tar -xz -C "$home"
  fi
  if [ ! -f "$conf" ]; then
    sed "s#^log.dirs=.*#log.dirs=$home/logs#" "$dist/config/kraft/server.properties" > "$conf"
    "$dist/bin/kafka-storage.sh" format -t "$("$dist/bin/kafka-storage.sh" random-uuid)" -c "$conf"
  fi
  "$dist/bin/kafka-server-start.sh" -daemon "$conf"
  for _ in $(seq 1 30); do
    if "$dist/bin/kafka-topics.sh" --bootstrap-server localhost:9092 --list >/dev/null 2>&1; then
      break
    fi
    sleep 2
  done
  "$dist/bin/kafka-topics.sh" --bootstrap-server localhost:9092 --create --if-not-exists \
    --topic "$TOPIC" --partitions 3 --replication-factor 1
  echo "kafka up on localhost:9092, topic $TOPIC"
  ;;
stop)
  "$dist/bin/kafka-server-stop.sh" || true
  ;;
*)
  echo "usage: $0 start|stop" >&2
  exit 1
  ;;
esac
