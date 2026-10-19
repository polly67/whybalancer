/**
 * Dual-Mode Bot Worker (Active / Hot-Standby Autonomous Relay)
 * 100% dynamic: driven by environment variables with zero hardcoding.
 */

require('dotenv').config();
const { WebSocket } = require('ws');
const { exec } = require('child_process');
const { DynamicStoreClient } = require('./redis_client');
const crypto = require('crypto');

// Dynamic configuration with sensible fallbacks
const WORKER_ID = process.env.WORKER_ID || `worker-${process.pid}-${crypto.randomBytes(3).toString('hex')}`;
let CURRENT_MODE = (process.env.INITIAL_MODE || 'STANDBY').toUpperCase();
const SERVER_URL = process.env.SERVER_URL || 'http://127.0.0.1:3000';
const WS_PATH = process.env.WS_PATH || '/chat';
const WS_URL = process.env.WS_URL || SERVER_URL.replace(/^http/, 'ws') + WS_PATH;
const HEARTBEAT_KEY = process.env.HEARTBEAT_KEY || 'relay:heartbeat';
const HEARTBEAT_INTERVAL_MS = parseInt(process.env.HEARTBEAT_INTERVAL_MS || '1000', 10);
const HEARTBEAT_TTL_SEC = parseInt(process.env.HEARTBEAT_TTL_SEC || '3', 10);
const STANDBY_POLL_INTERVAL_MS = parseInt(process.env.STANDBY_POLL_INTERVAL_MS || '500', 10);
const HEARTBEAT_TIMEOUT_MS = parseInt(process.env.HEARTBEAT_TIMEOUT_MS || '2500', 10);
const MAX_RUN_DURATION_SEC = parseInt(process.env.MAX_RUN_DURATION_SEC || '21600', 10); // 6 hours default
const PRE_WARM_BUFFER_SEC = parseInt(process.env.PRE_WARM_BUFFER_SEC || '1200', 10); // 20 mins default
const PRE_WARM_COMMAND = process.env.PRE_WARM_COMMAND || null;

// Observability and state tracking
const startTime = Date.now();
let wsClient = null;
let heartbeatTimer = null;
let standbyTimer = null;
let runMonitorTimer = null;
let isShuttingDown = false;
let hasTriggeredPrewarm = false;
let processedMessagesCount = 0;

const store = new DynamicStoreClient({
  redisUrl: process.env.REDIS_URL,
  upstashUrl: process.env.UPSTASH_REDIS_REST_URL,
  upstashToken: process.env.UPSTASH_REDIS_REST_TOKEN,
  keyPrefix: process.env.REDIS_KEY_PREFIX || '',
});

console.log(`[Worker:${WORKER_ID}] Initializing in mode: ${CURRENT_MODE}`);
console.log(`[Worker:${WORKER_ID}] Target Server: ${SERVER_URL} (WS: ${WS_URL})`);
console.log(`[Worker:${WORKER_ID}] Heartbeat Key: ${HEARTBEAT_KEY} | Interval: ${HEARTBEAT_INTERVAL_MS}ms | Timeout: ${HEARTBEAT_TIMEOUT_MS}ms`);
console.log(`[Worker:${WORKER_ID}] Max Duration: ${MAX_RUN_DURATION_SEC}s | Pre-warm Buffer: ${PRE_WARM_BUFFER_SEC}s`);

// --- Active Mode Logic ---

async function emitHeartbeat() {
  if (CURRENT_MODE !== 'ACTIVE' || isShuttingDown) return;

  const heartbeatData = {
    workerId: WORKER_ID,
    timestamp: Date.now(),
    uptimeSec: Math.floor((Date.now() - startTime) / 1000),
    processedCount: processedMessagesCount,
    mode: 'ACTIVE',
  };

  try {
    await store.set(HEARTBEAT_KEY, heartbeatData, { ttlSec: HEARTBEAT_TTL_SEC });
    if (process.env.DEBUG_HEARTBEAT === 'true') {
      console.log(`[Worker:${WORKER_ID}] Heartbeat emitted at ${new Date().toISOString()}`);
    }
  } catch (err) {
    console.error(`[Worker:${WORKER_ID}] Failed to emit heartbeat:`, err.message);
  }
}

function connectToChatServer() {
  if (isShuttingDown || CURRENT_MODE !== 'ACTIVE') return;

  console.log(`[Worker:${WORKER_ID}] Connecting to Chat WebSocket: ${WS_URL}`);
  wsClient = new WebSocket(WS_URL);

  wsClient.on('open', () => {
    console.log(`[Worker:${WORKER_ID}] Connected to chat stream! Identifying...`);
    wsClient.send(JSON.stringify({
      type: 'IDENTIFY',
      workerId: WORKER_ID,
    }));
  });

  wsClient.on('message', (raw) => {
    try {
      const data = JSON.parse(raw.toString());

      if (data.type === 'CHAT_MESSAGE') {
        const msg = data.message;
        processedMessagesCount += 1;

        if (process.env.DEBUG_MESSAGES === 'true') {
          console.log(`[Worker:${WORKER_ID}] Handling msg ${msg.id}: "${msg.text}"`);
        }

        // Respond immediately with worker ID and timestamp
        const responsePayload = {
          type: 'CHAT_RESPONSE',
          messageId: msg.id,
          workerId: WORKER_ID,
          timestamp: Date.now(),
          text: `Ack [${msg.id}] processed by ${WORKER_ID}`,
        };

        if (wsClient && wsClient.readyState === WebSocket.OPEN) {
          wsClient.send(JSON.stringify(responsePayload));
        } else {
          // Fallback to HTTP response if WS socket is buffer-choked
          fetch(`${SERVER_URL}/messages/${msg.id}/respond`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(responsePayload),
          }).catch(() => {});
        }
      }
    } catch (err) {
      console.error(`[Worker:${WORKER_ID}] Error handling chat message:`, err.message);
    }
  });

  wsClient.on('close', (code, reason) => {
    console.log(`[Worker:${WORKER_ID}] Chat WS disconnected (code: ${code}).`);
    wsClient = null;
    if (!isShuttingDown && CURRENT_MODE === 'ACTIVE') {
      console.log(`[Worker:${WORKER_ID}] Reconnecting in 1000ms...`);
      setTimeout(connectToChatServer, 1000);
    }
  });

  wsClient.on('error', (err) => {
    console.error(`[Worker:${WORKER_ID}] Chat WS error:`, err.message);
  });
}

function startActiveMode() {
  CURRENT_MODE = 'ACTIVE';
  console.log(`\n======================================================`);
  console.log(`[Worker:${WORKER_ID}] 🚀 PROMOTED TO ACTIVE LEADER AT ${new Date().toISOString()}`);
  console.log(`======================================================\n`);

  // Clear standby timer if running
  if (standbyTimer) {
    clearInterval(standbyTimer);
    standbyTimer = null;
  }

  // 1. Immediately emit first heartbeat
  emitHeartbeat();
  heartbeatTimer = setInterval(emitHeartbeat, HEARTBEAT_INTERVAL_MS);

  // 2. Connect to real-time chat stream
  connectToChatServer();
}

// --- Hot-Standby Mode Logic ---

async function checkHeartbeatAndEvaluate() {
  if (CURRENT_MODE !== 'STANDBY' || isShuttingDown) return;

  try {
    const raw = await store.get(HEARTBEAT_KEY);
    const now = Date.now();

    if (!raw) {
      console.log(`[Worker:${WORKER_ID}] Heartbeat key missing in store. Initiating failover promotion!`);
      startActiveMode();
      return;
    }

    let heartbeat = null;
    try {
      heartbeat = typeof raw === 'string' ? JSON.parse(raw) : raw;
    } catch {
      heartbeat = null;
    }

    if (!heartbeat || !heartbeat.timestamp) {
      console.log(`[Worker:${WORKER_ID}] Heartbeat payload corrupted. Promoting to ACTIVE!`);
      startActiveMode();
      return;
    }

    const ageMs = now - heartbeat.timestamp;
    if (ageMs > HEARTBEAT_TIMEOUT_MS) {
      console.log(`[Worker:${WORKER_ID}] ⚡ Leader heartbeat expired! Age: ${ageMs}ms > Timeout: ${HEARTBEAT_TIMEOUT_MS}ms. PROMOTING!`);
      startActiveMode();
    } else {
      if (process.env.DEBUG_STANDBY === 'true') {
        console.log(`[Worker:${WORKER_ID}] Leader ${heartbeat.workerId} alive (heartbeat age: ${ageMs}ms)`);
      }
    }
  } catch (err) {
    console.error(`[Worker:${WORKER_ID}] Standby heartbeat check error:`, err.message);
  }
}

function startStandbyMode() {
  CURRENT_MODE = 'STANDBY';
  console.log(`[Worker:${WORKER_ID}] 🛡️ Standing by. Polling heartbeat every ${STANDBY_POLL_INTERVAL_MS}ms...`);
  standbyTimer = setInterval(checkHeartbeatAndEvaluate, STANDBY_POLL_INTERVAL_MS);
}

// --- Execution Duration & Self-Replication Monitor ---

function startLifecycleMonitor() {
  runMonitorTimer = setInterval(async () => {
    if (isShuttingDown) return;

    const elapsedSec = (Date.now() - startTime) / 1000;

    // 1. Pre-warm threshold check
    const prewarmThresholdSec = Math.max(MAX_RUN_DURATION_SEC - PRE_WARM_BUFFER_SEC, 5);
    if (CURRENT_MODE === 'ACTIVE' && elapsedSec >= prewarmThresholdSec && !hasTriggeredPrewarm) {
      hasTriggeredPrewarm = true;
      console.log(`\n[Worker:${WORKER_ID}] 🔔 PRE-WARM THRESHOLD REACHED (${elapsedSec.toFixed(1)}s / ${MAX_RUN_DURATION_SEC}s).`);
      console.log(`[Worker:${WORKER_ID}] Spawning/dispatching hot-standby worker now...`);

      if (PRE_WARM_COMMAND) {
        console.log(`[Worker:${WORKER_ID}] Executing PRE_WARM_COMMAND: ${PRE_WARM_COMMAND}`);
        exec(PRE_WARM_COMMAND, (err, stdout, stderr) => {
          if (err) {
            console.error(`[Worker:${WORKER_ID}] Pre-warm command error:`, err.message);
          } else {
            console.log(`[Worker:${WORKER_ID}] Pre-warm command executed: ${stdout || 'OK'}`);
          }
        });
      }
    }

    // 2. Maximum execution duration reached (Ephemeral runner expiration)
    if (elapsedSec >= MAX_RUN_DURATION_SEC) {
      console.log(`\n[Worker:${WORKER_ID}] 🛑 MAX RUN DURATION (${MAX_RUN_DURATION_SEC}s) REACHED.`);
      console.log(`[Worker:${WORKER_ID}] Initiating graceful cutover & handoff...`);
      await gracefulShutdown('DURATION_LIMIT');
    }
  }, 1000);
}

// --- Graceful Shutdown Handler ---

async function gracefulShutdown(reason = 'SIGNAL') {
  if (isShuttingDown) return;
  isShuttingDown = true;
  console.log(`\n[Worker:${WORKER_ID}] Graceful shutdown initiated (Reason: ${reason})...`);

  // Clear all timers
  if (heartbeatTimer) clearInterval(heartbeatTimer);
  if (standbyTimer) clearInterval(standbyTimer);
  if (runMonitorTimer) clearInterval(runMonitorTimer);

  // Disconnect from chat server
  if (wsClient) {
    try {
      wsClient.close(1000, 'Worker shutdown');
    } catch {}
    wsClient = null;
  }

  // Delete heartbeat key immediately to allow instant standby failover
  if (CURRENT_MODE === 'ACTIVE') {
    try {
      console.log(`[Worker:${WORKER_ID}] Deleting heartbeat key: ${HEARTBEAT_KEY} for zero-latency cutover.`);
      await store.del(HEARTBEAT_KEY);
    } catch (err) {
      console.error(`[Worker:${WORKER_ID}] Failed to delete heartbeat key:`, err.message);
    }
  }

  await store.close();
  console.log(`[Worker:${WORKER_ID}] Teardown complete. Process exiting cleanly (Handled: ${processedMessagesCount} msgs).`);
  process.exit(0);
}

process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGINT', () => gracefulShutdown('SIGINT'));

// Boot based on INITIAL_MODE
startLifecycleMonitor();
if (CURRENT_MODE === 'ACTIVE') {
  startActiveMode();
} else {
  startStandbyMode();
}
