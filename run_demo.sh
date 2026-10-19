#!/usr/bin/env bash
set -euo pipefail

# ==============================================================================
# SRE AUTOMATED LOCAL ACCELERATED TEST HARNESS
# Tests zero-downtime cutover, token collision prevention, and failover RTO.
# ==============================================================================

PORT="${PORT:-3000}"
REDIS_URL="${REDIS_URL:-redis://127.0.0.1:6379}"
HEARTBEAT_KEY="${HEARTBEAT_KEY:-relay:heartbeat}"

echo "=========================================================="
echo " 🚀 SRE ACCELERATED HOT-STANDBY RELAY TEST HARNESS"
echo " Target Port: $PORT | Redis: $REDIS_URL"
echo "=========================================================="

# 1. Ensure Redis is running and clean heartbeat key
echo "[Setup] Checking Redis connectivity..."
if ! redis-cli ping >/dev/null 2>&1; then
  echo "[Setup] Starting local redis-server..."
  sudo service redis-server start >/dev/null 2>&1 || sudo systemctl start redis-server >/dev/null 2>&1 || redis-server --daemonize yes >/dev/null 2>&1 || true
fi
redis-cli del "$HEARTBEAT_KEY" >/dev/null 2>&1 || true

# Helper to kill background processes on exit
SERVER_PID=""
WORKER1_PID=""
WORKER2_PID=""

cleanup() {
  echo ""
  echo "[Teardown] Cleaning up background processes..."
  [ -n "$WORKER1_PID" ] && kill -9 "$WORKER1_PID" 2>/dev/null || true
  [ -n "$WORKER2_PID" ] && kill -9 "$WORKER2_PID" 2>/dev/null || true
  [ -n "$SERVER_PID" ] && kill -9 "$SERVER_PID" 2>/dev/null || true
  redis-cli del "$HEARTBEAT_KEY" >/dev/null 2>&1 || true
  echo "[Teardown] All test services terminated."
}
trap cleanup EXIT INT TERM

# 2. Launch Mock Chat Server
echo "[1/4] Starting Mock Chat Server on port $PORT..."
PORT="$PORT" node server.js > server.log 2>&1 &
SERVER_PID=$!

# Wait for server health
echo "[1/4] Waiting for server health check..."
for i in {1..20}; do
  if curl -s "http://127.0.0.1:$PORT/health" | grep -q '"status":"ok"'; then
    echo "[1/4] Server online!"
    break
  fi
  sleep 0.2
done

# 3. Launch Worker 1 (Active Leader)
echo "[2/4] Starting Worker 1 (Leader: worker-alpha) in ACTIVE mode..."
WORKER_ID="worker-alpha" \
INITIAL_MODE="ACTIVE" \
SERVER_URL="http://127.0.0.1:$PORT" \
HEARTBEAT_KEY="$HEARTBEAT_KEY" \
HEARTBEAT_INTERVAL_MS=500 \
HEARTBEAT_TTL_SEC=2 \
MAX_RUN_DURATION_SEC=12 \
PRE_WARM_BUFFER_SEC=4 \
node worker.js > worker1.log 2>&1 &
WORKER1_PID=$!

sleep 1

# 4. Launch Worker 2 (Hot-Standby)
echo "[3/4] Starting Worker 2 (Standby: worker-bravo) in HOT-STANDBY mode..."
WORKER_ID="worker-bravo" \
INITIAL_MODE="STANDBY" \
SERVER_URL="http://127.0.0.1:$PORT" \
HEARTBEAT_KEY="$HEARTBEAT_KEY" \
STANDBY_POLL_INTERVAL_MS=300 \
HEARTBEAT_TIMEOUT_MS=1200 \
MAX_RUN_DURATION_SEC=60 \
node worker.js > worker2.log 2>&1 &
WORKER2_PID=$!

sleep 1

# 5. Launch SRE Chaos Benchmark
echo "[4/4] Executing Sustained Traffic & Mid-Stream Chaos Failover..."
echo "      Worker 1 PID: $WORKER1_PID | Worker 2 PID: $WORKER2_PID"

TARGET_URL="http://127.0.0.1:$PORT" \
RATE_PER_SECOND=40 \
DURATION_SEC=16 \
CHAOS_INJECTION_DELAY_SEC=6 \
CHAOS_TARGET_PID="$WORKER1_PID" \
CHAOS_TYPE="SIGTERM" \
HEARTBEAT_KEY="$HEARTBEAT_KEY" \
node benchmark.js

echo "=========================================================="
echo " ✅ Test run finished. Logs saved to server.log, worker1.log, worker2.log"
echo "=========================================================="
