#!/usr/bin/env bash
#
# End-to-end demo for CloudEvents webhook delivery in Apicurio Registry.
#
# Proves, in order:
#   1. a subscription can be registered over REST
#   2. creating an artifact version emits a CloudEvent to that endpoint
#   3. a rejecting endpoint causes a persisted, backing-off retry
#   4. killing the registry mid-backoff does not lose the delivery
#   5. after restart the delivery resumes and eventually succeeds
#
# Step 4 is the point of the whole exercise. In-thread retry (MicroProfile @Retry) cannot survive it.
#
set -uo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -W 2>/dev/null || cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DEMO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_DIR="${DEMO_DIR}/data"
RECV_LOG="${DEMO_DIR}/receiver.log"
SECRET="demo-secret"
PORT=9099
IMG=maven:3.9-eclipse-temurin-21
CONTAINER=registry-webhook-demo

say() { printf '\n\033[1m== %s\033[0m\n' "$*"; }
stamp() { date -u +%H:%M:%S.%3N; }

cleanup() {
  docker rm -f "$CONTAINER" >/dev/null 2>&1
  [[ -n "${RECV_PID:-}" ]] && kill "$RECV_PID" >/dev/null 2>&1
}
trap cleanup EXIT

rm -rf "$DATA_DIR" "$RECV_LOG"; mkdir -p "$DATA_DIR"
docker rm -f "$CONTAINER" >/dev/null 2>&1

say "1. start the mock receiver (rejects the first 4 attempts with HTTP 500)"
python "${DEMO_DIR}/receiver.py" --port "$PORT" --secret "$SECRET" --fail-first 4 > "$RECV_LOG" 2>&1 &
RECV_PID=$!
sleep 2
cat "$RECV_LOG"

start_registry() {
  MSYS_NO_PATHCONV=1 docker run -d --name "$CONTAINER" \
    -v "${REPO}:/w" -v "${DATA_DIR}:/data" \
    -p 8080:8080 -w /w \
    --add-host host.docker.internal:host-gateway \
    "$IMG" \
    java -Dapicurio.datasource.url='jdbc:h2:file:/data/registry;DB_CLOSE_DELAY=-1;AUTO_SERVER=TRUE' \
         -Dapicurio.webhooks.dispatcher.every=1s \
         -jar app/target/quarkus-app/quarkus-run.jar >/dev/null
}

wait_ready() {
  for _ in $(seq 1 90); do
    if curl -sf http://localhost:8080/health/ready >/dev/null 2>&1 \
       || curl -sf http://localhost:8080/q/health/ready >/dev/null 2>&1; then
      echo "$(stamp)  registry ready"; return 0
    fi
    sleep 2
  done
  echo "registry did not become ready"; docker logs "$CONTAINER" | tail -40; return 1
}

say "2. start the registry on a file-backed H2 database"
start_registry
wait_ready || exit 1

say "3. register a webhook subscription"
SUB=$(curl -sf -X POST http://localhost:8080/apis/registry/v3/admin/webhooks \
  -H 'Content-Type: application/json' \
  -d "{\"endpointUrl\":\"http://host.docker.internal:${PORT}/\",\"eventTypes\":[\"io.apicurio.registry.artifact.version.created\"],\"secret\":\"${SECRET}\"}")
echo "$SUB"
SUB_ID=$(echo "$SUB" | python -c 'import sys,json; print(json.load(sys.stdin)["subscriptionId"])')

say "4. create an artifact version, which should emit a CloudEvent"
curl -sf -X POST "http://localhost:8080/apis/registry/v3/groups/default/artifacts" \
  -H 'Content-Type: application/json' \
  -d '{"artifactId":"orders","artifactType":"JSON","firstVersion":{"version":"1.0","content":{"content":"{\"type\":\"object\"}","contentType":"application/json"}}}' \
  >/dev/null && echo "$(stamp)  artifact orders:1.0 created"

say "5. let the endpoint reject a few attempts (watch the backoff grow)"
sleep 12
cat "$RECV_LOG"

say "6. KILL the registry mid-backoff"
docker rm -f "$CONTAINER" >/dev/null
echo "$(stamp)  registry killed"
ATTEMPTS_BEFORE=$(grep -c REJECT "$RECV_LOG")
echo "attempts seen before the kill: ${ATTEMPTS_BEFORE}"

say "7. restart against the same database"
start_registry
wait_ready || exit 1

say "8. delivery resumes from the database and eventually succeeds"
for _ in $(seq 1 40); do
  grep -q ACCEPT "$RECV_LOG" && break
  sleep 2
done
cat "$RECV_LOG"

say "9. delivery log as recorded by the registry"
curl -sf "http://localhost:8080/apis/registry/v3/admin/webhooks/${SUB_ID}/deliveries" \
  | python -m json.tool

say "done"
