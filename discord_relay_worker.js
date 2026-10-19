/**
 * Production-Ready Discord Bot Autonomous Hot-Standby Relay Worker
 * Maps local proto-lab validated logic directly to ephemeral runners (GitHub Actions).
 * 100% dynamic: driven entirely by environment variables and GitHub secrets/vars.
 */

require('dotenv').config();
const { DynamicStoreClient } = require('./redis_client');
const crypto = require('crypto');

// Dynamic configuration with sensible fallbacks
const WORKER_ID = process.env.WORKER_ID || `gh-runner-${process.env.GITHUB_RUN_ID || 'local'}-${crypto.randomBytes(3).toString('hex')}`;
const DISCORD_BOT_TOKEN = process.env.DISCORD_BOT_TOKEN || null;
const GITHUB_API_URL = (process.env.GITHUB_API_URL || 'https://api.github.com').replace(/\/+$/, '');
const GITHUB_REPOSITORY = process.env.GITHUB_REPOSITORY || null;
const GH_DISPATCH_TOKEN = process.env.GH_DISPATCH_TOKEN || null;
const DISPATCH_EVENT_TYPE = process.env.DISPATCH_EVENT_TYPE || 'spawn-standby';

// Heartbeat & Handoff Timings (Dynamic)
const HEARTBEAT_KEY = process.env.HEARTBEAT_KEY || 'relay:discord:heartbeat';
const HEARTBEAT_INTERVAL_MS = parseInt(process.env.HEARTBEAT_INTERVAL_MS || '1000', 10);
const HEARTBEAT_TTL_SEC = parseInt(process.env.HEARTBEAT_TTL_SEC || '5', 10);
const STANDBY_POLL_INTERVAL_MS = parseInt(process.env.STANDBY_POLL_INTERVAL_MS || '1000', 10);
const HEARTBEAT_TIMEOUT_MS = parseInt(process.env.HEARTBEAT_TIMEOUT_MS || '3500', 10);

// Ephemeral Runner Ceiling Timings
const PRE_WARM_MINUTES = parseFloat(process.env.PRE_WARM_MINUTES || '340'); // Default: 5h 40m
const RUN_LIMIT_MINUTES = parseFloat(process.env.RUN_LIMIT_MINUTES || '359'); // Default: 5h 59m

// Fast local testing overrides (if SECONDS mode is enabled)
const IS_ACCELERATED_MODE = process.env.ACCELERATED_MODE === 'true';
const PRE_WARM_SECONDS = IS_ACCELERATED_MODE 
  ? parseFloat(process.env.PRE_WARM_SECONDS || '15') 
  : PRE_WARM_MINUTES * 60;
const RUN_LIMIT_SECONDS = IS_ACCELERATED_MODE 
  ? parseFloat(process.env.RUN_LIMIT_SECONDS || '30') 
  : RUN_LIMIT_MINUTES * 60;

// State
let CURRENT_MODE = (process.env.INITIAL_MODE || 'STANDBY').toUpperCase();
const startTime = Date.now();
let heartbeatTimer = null;
let standbyTimer = null;
let monitorTimer = null;
let hasDispatchedNextRunner = false;
let isShuttingDown = false;
let discordClient = null;

const store = new DynamicStoreClient({
  redisUrl: process.env.REDIS_URL,
  upstashUrl: process.env.UPSTASH_REDIS_REST_URL,
  upstashToken: process.env.UPSTASH_REDIS_REST_TOKEN,
  keyPrefix: process.env.REDIS_KEY_PREFIX || '',
});

console.log(`\n==========================================================`);
console.log(` 🤖 DISCORD AUTONOMOUS HOT-STANDBY RELAY WORKER`);
console.log(`==========================================================`);
console.log(`Worker ID:          ${WORKER_ID}`);
console.log(`Repository:         ${GITHUB_REPOSITORY || 'Local / Unspecified'}`);
console.log(`Pre-Warm Target:    ${PRE_WARM_SECONDS}s (${(PRE_WARM_SECONDS / 60).toFixed(1)} mins)`);
console.log(`Run Limit:          ${RUN_LIMIT_SECONDS}s (${(RUN_LIMIT_SECONDS / 60).toFixed(1)} mins)`);
console.log(`Heartbeat Key:      ${HEARTBEAT_KEY} (TTL: ${HEARTBEAT_TTL_SEC}s, Poll: ${STANDBY_POLL_INTERVAL_MS}ms)`);
console.log(`==========================================================\n`);

// --- GitHub Actions Dispatch with Exponential Backoff ---

async function dispatchNextRunner(attempt = 1, maxAttempts = 4) {
  if (!GH_DISPATCH_TOKEN || !GITHUB_REPOSITORY) {
    console.warn(`[Worker:${WORKER_ID}] GH_DISPATCH_TOKEN or GITHUB_REPOSITORY not set. Skipping GitHub Actions dispatch.`);
    return false;
  }

  const endpoint = `${GITHUB_API_URL}/repos/${GITHUB_REPOSITORY}/dispatches`;
  const body = JSON.stringify({
    event_type: DISPATCH_EVENT_TYPE,
    client_payload: {
      spawned_by: WORKER_ID,
      timestamp: Date.now(),
      reason: 'Ephemeral runner pre-warm rotation',
    },
  });

  console.log(`[Worker:${WORKER_ID}] Dispatching standby runner (Attempt ${attempt}/${maxAttempts}) via ${endpoint}...`);

  try {
    const response = await fetch(endpoint, {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${GH_DISPATCH_TOKEN}`,
        'Accept': 'application/vnd.github.v3+json',
        'User-Agent': `RelayWorker/${WORKER_ID}`,
        'Content-Type': 'application/json',
      },
      body,
    });

    if (response.status === 204 || response.ok) {
      console.log(`[Worker:${WORKER_ID}] ✅ Standby runner workflow successfully dispatched!`);
      return true;
    }

    const errText = await response.text();
    console.error(`[Worker:${WORKER_ID}] GitHub dispatch failed [${response.status}]: ${errText}`);
  } catch (err) {
    console.error(`[Worker:${WORKER_ID}] Network error during GitHub dispatch:`, err.message);
  }

  if (attempt < maxAttempts) {
    const backoffMs = Math.min(1000 * Math.pow(2, attempt), 10000);
    console.log(`[Worker:${WORKER_ID}] Retrying dispatch in ${backoffMs}ms...`);
    await new Promise(r => setTimeout(r, backoffMs));
    return await dispatchNextRunner(attempt + 1, maxAttempts);
  }

  return false;
}

// --- Active Mode / Gateway Login ---

async function emitHeartbeat() {
  if (CURRENT_MODE !== 'ACTIVE' || isShuttingDown) return;

  const payload = {
    workerId: WORKER_ID,
    timestamp: Date.now(),
    uptimeSec: Math.floor((Date.now() - startTime) / 1000),
    mode: 'ACTIVE',
  };

  try {
    await store.set(HEARTBEAT_KEY, payload, { ttlSec: HEARTBEAT_TTL_SEC });
  } catch (err) {
    console.error(`[Worker:${WORKER_ID}] Failed to emit heartbeat:`, err.message);
  }
}

async function startActiveMode() {
  CURRENT_MODE = 'ACTIVE';
  console.log(`\n[Worker:${WORKER_ID}] 🚀 PROMOTED TO ACTIVE. Acquiring Discord Gateway session...`);

  if (standbyTimer) {
    clearInterval(standbyTimer);
    standbyTimer = null;
  }

  // 1. Immediately emit first heartbeat
  await emitHeartbeat();
  heartbeatTimer = setInterval(emitHeartbeat, HEARTBEAT_INTERVAL_MS);

  // 2. Initialize and login to Discord Gateway
  // In production, require('discord.js').Client is used:
  if (DISCORD_BOT_TOKEN) {
    try {
      console.log(`[Worker:${WORKER_ID}] Connecting to Discord Gateway with token...`);
      // Lazy-load discord.js if available
      let Client, GatewayIntentBits;
      try {
        const djs = require('discord.js');
        Client = djs.Client;
        GatewayIntentBits = djs.GatewayIntentBits;
      } catch {
        console.log(`[Worker:${WORKER_ID}] (discord.js not in bundle - running in simulated gateway mode)`);
      }

      if (Client) {
        discordClient = new Client({
          intents: [GatewayIntentBits.Guilds, GatewayIntentBits.GuildMessages, GatewayIntentBits.MessageContent],
        });

        discordClient.once('ready', () => {
          console.log(`[Worker:${WORKER_ID}] ✅ Discord Gateway Ready! Logged in as ${discordClient.user.tag}`);
        });

        discordClient.on('messageCreate', (msg) => {
          if (msg.author.bot) return;
          if (msg.content === '!ping') {
            msg.reply(`Pong from active relay worker: \`${WORKER_ID}\`!`);
          }
        });

        await discordClient.login(DISCORD_BOT_TOKEN);
      }
    } catch (err) {
      console.error(`[Worker:${WORKER_ID}] Discord Gateway login failed:`, err.message);
    }
  } else {
    console.log(`[Worker:${WORKER_ID}] ℹ️ No DISCORD_BOT_TOKEN provided. Operating in dry-run simulated relay mode.`);
  }
}

// --- Hot-Standby Mode ---

async function checkStandbyHeartbeat() {
  if (CURRENT_MODE !== 'STANDBY' || isShuttingDown) return;

  try {
    const raw = await store.get(HEARTBEAT_KEY);
    const now = Date.now();

    if (!raw) {
      console.log(`[Worker:${WORKER_ID}] No active heartbeat detected in store. Triggering immediate failover promotion!`);
      await startActiveMode();
      return;
    }

    let heartbeat = null;
    try {
      heartbeat = typeof raw === 'string' ? JSON.parse(raw) : raw;
    } catch {
      heartbeat = null;
    }

    if (!heartbeat || !heartbeat.timestamp) {
      console.log(`[Worker:${WORKER_ID}] Heartbeat malformed. Triggering promotion!`);
      await startActiveMode();
      return;
    }

    const ageMs = now - heartbeat.timestamp;
    if (ageMs > HEARTBEAT_TIMEOUT_MS) {
      console.log(`[Worker:${WORKER_ID}] ⚡ Primary heartbeat expired (Age: ${ageMs}ms > Timeout: ${HEARTBEAT_TIMEOUT_MS}ms). PROMOTING!`);
      await startActiveMode();
    }
  } catch (err) {
    console.error(`[Worker:${WORKER_ID}] Standby check error:`, err.message);
  }
}

function startStandbyMode() {
  CURRENT_MODE = 'STANDBY';
  console.log(`[Worker:${WORKER_ID}] 🛡️ Initialized in HOT-STANDBY mode.`);
  console.log(`[Worker:${WORKER_ID}] Zero token collision guarantee: Client login deferred until leader loss.`);
  standbyTimer = setInterval(checkStandbyHeartbeat, STANDBY_POLL_INTERVAL_MS);
}

// --- Lifecycle & Ephemeral Timer ---

function startLifecycleMonitor() {
  monitorTimer = setInterval(async () => {
    if (isShuttingDown) return;

    const elapsedSec = (Date.now() - startTime) / 1000;

    // 1. Pre-warm Dispatch Trigger
    if (CURRENT_MODE === 'ACTIVE' && elapsedSec >= PRE_WARM_SECONDS && !hasDispatchedNextRunner) {
      hasDispatchedNextRunner = true;
      console.log(`\n[Worker:${WORKER_ID}] 🔔 PRE-WARM WINDOW REACHED (${elapsedSec.toFixed(1)}s elapsed).`);
      console.log(`[Worker:${WORKER_ID}] Initiating GitHub Actions workflow self-replication...`);
      await dispatchNextRunner();
    }

    // 2. Execution Ceiling Reached (Runner Handoff)
    if (elapsedSec >= RUN_LIMIT_SECONDS) {
      console.log(`\n[Worker:${WORKER_ID}] 🛑 RUN LIMIT REACHED (${elapsedSec.toFixed(1)}s). Commencing graceful relay handoff...`);
      await gracefulShutdown('RUN_LIMIT_EXPIRED');
    }
  }, 1000);
}

// --- Graceful Teardown ---

async function gracefulShutdown(reason = 'SIGNAL') {
  if (isShuttingDown) return;
  isShuttingDown = true;
  console.log(`[Worker:${WORKER_ID}] Shutting down cleanly (Reason: ${reason})...`);

  if (heartbeatTimer) clearInterval(heartbeatTimer);
  if (standbyTimer) clearInterval(standbyTimer);
  if (monitorTimer) clearInterval(monitorTimer);

  if (discordClient) {
    try {
      console.log(`[Worker:${WORKER_ID}] Destroying Discord client connection...`);
      await discordClient.destroy();
    } catch {}
    discordClient = null;
  }

  // Delete heartbeat key so the standby worker promotes immediately with zero wait
  if (CURRENT_MODE === 'ACTIVE') {
    try {
      console.log(`[Worker:${WORKER_ID}] Deleting heartbeat key: ${HEARTBEAT_KEY}`);
      await store.del(HEARTBEAT_KEY);
    } catch (err) {
      console.error(`[Worker:${WORKER_ID}] Failed to delete heartbeat:`, err.message);
    }
  }

  await store.close();
  console.log(`[Worker:${WORKER_ID}] Clean handoff completed. Exiting.`);
  process.exit(0);
}

process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGINT', () => gracefulShutdown('SIGINT'));

// Boot sequence: evaluate existing heartbeat in store to determine initial state
async function boot() {
  startLifecycleMonitor();

  // If INITIAL_MODE was explicitly passed, respect it; otherwise auto-detect:
  if (process.env.INITIAL_MODE) {
    if (CURRENT_MODE === 'ACTIVE') {
      await startActiveMode();
    } else {
      startStandbyMode();
    }
    return;
  }

  try {
    const existingHeartbeat = await store.get(HEARTBEAT_KEY);
    if (existingHeartbeat) {
      console.log(`[Worker:${WORKER_ID}] Active leader already detected in store. Booting into HOT-STANDBY.`);
      startStandbyMode();
    } else {
      console.log(`[Worker:${WORKER_ID}] No leader detected in store. Booting into ACTIVE leader.`);
      await startActiveMode();
    }
  } catch {
    await startActiveMode();
  }
}

boot();
