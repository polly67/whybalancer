/**
 * Mock Chat Server (Real-Time Streaming & Observability Engine)
 * 100% dynamic configuration via environment variables.
 * Tracks message latencies, duplicate responses, worker distribution, and cutover windows.
 */

require('dotenv').config();
const http = require('http');
const { WebSocketServer, WebSocket } = require('ws');
const { URL } = require('url');

// Dynamic configuration with sensible fallbacks
const PORT = parseInt(process.env.PORT || '3000', 10);
const HOST = process.env.HOST || '0.0.0.0';
const WS_PATH = process.env.WS_PATH || '/chat';
const HEALTH_PATH = process.env.HEALTH_PATH || '/health';
const METRICS_PATH = process.env.METRICS_PATH || '/metrics';
const MESSAGES_PATH = process.env.MESSAGES_PATH || '/messages';
const EVENTS_PATH = process.env.EVENTS_PATH || '/events';
const RESET_PATH = process.env.RESET_PATH || '/reset';
const MAX_HISTORY_SIZE = parseInt(process.env.MAX_HISTORY_SIZE || '5000', 10);

// Observability state
const state = {
  startTime: Date.now(),
  messages: new Map(), // messageId -> { id, user, text, timestampHr, timestampMs, responses: [] }
  messageOrder: [],
  latenciesMs: [],
  duplicates: [],
  workerStats: {}, // workerId -> count
  sseClients: new Set(),
  activeWorkers: new Map(), // ws -> { workerId, connectedAt }
};

function calculatePercentiles(values) {
  if (values.length === 0) {
    return { min: 0, max: 0, avg: 0, p50: 0, p95: 0, p99: 0 };
  }
  const sorted = [...values].sort((a, b) => a - b);
  const sum = sorted.reduce((acc, val) => acc + val, 0);
  const avg = Number((sum / sorted.length).toFixed(2));
  const min = Number(sorted[0].toFixed(2));
  const max = Number(sorted[sorted.length - 1].toFixed(2));
  const p50 = Number(sorted[Math.floor(sorted.length * 0.5)].toFixed(2));
  const p95 = Number(sorted[Math.floor(sorted.length * 0.95)].toFixed(2));
  const p99 = Number(sorted[Math.floor(sorted.length * 0.99)].toFixed(2));
  return { min, max, avg, p50, p95, p99 };
}

function recordResponse(messageId, workerId, clientSentAt = null) {
  const nowMs = Date.now();
  const msg = state.messages.get(messageId);

  if (!msg) {
    return { error: 'Message not found', status: 404 };
  }

  const responsePayload = {
    workerId,
    receivedAtMs: nowMs,
    clientSentAtMs: clientSentAt || nowMs,
    latencyMs: nowMs - msg.timestampMs,
  };

  msg.responses.push(responsePayload);

  // Check for DUPLICATE responses (Token collision / Double-posting bug)
  if (msg.responses.length > 1) {
    const dupRecord = {
      messageId,
      firstWorker: msg.responses[0].workerId,
      duplicateWorker: workerId,
      firstResponseMs: msg.responses[0].receivedAtMs,
      secondResponseMs: nowMs,
      timeDifferenceMs: nowMs - msg.responses[0].receivedAtMs,
    };
    state.duplicates.push(dupRecord);
    console.error(`🚨 [DUPLICATE DETECTED] Msg ${messageId} answered by ${msg.responses[0].workerId} AND ${workerId}!`);
  }

  // Update latency & worker stats
  state.latenciesMs.push(responsePayload.latencyMs);
  state.workerStats[workerId] = (state.workerStats[workerId] || 0) + 1;

  return { success: true, latencyMs: responsePayload.latencyMs, duplicates: msg.responses.length - 1 };
}

// HTTP Server
const server = http.createServer((req, res) => {
  const parsedUrl = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
  const pathname = parsedUrl.pathname;

  // Set CORS headers
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');

  if (req.method === 'OPTIONS') {
    res.writeHead(204);
    res.end();
    return;
  }

  // 1. Health Endpoint
  if (pathname === HEALTH_PATH && req.method === 'GET') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      status: 'ok',
      uptimeSec: Math.floor((Date.now() - state.startTime) / 1000),
      connectedWorkers: state.activeWorkers.size,
      totalMessages: state.messages.size,
      timestamp: Date.now(),
    }));
    return;
  }

  // 2. Metrics Endpoint
  if (pathname === METRICS_PATH && req.method === 'GET') {
    const totalMessages = state.messages.size;
    let answeredMessages = 0;
    let unansweredMessages = 0;

    for (const msg of state.messages.values()) {
      if (msg.responses.length > 0) {
        answeredMessages += 1;
      } else {
        unansweredMessages += 1;
      }
    }

    const latencyStats = calculatePercentiles(state.latenciesMs);

    // Compute exact worker cutover timeline
    const workerTimeline = {};
    for (const msg of state.messages.values()) {
      for (const resp of msg.responses) {
        if (!workerTimeline[resp.workerId]) {
          workerTimeline[resp.workerId] = {
            firstSeenMs: resp.receivedAtMs,
            lastSeenMs: resp.receivedAtMs,
            count: 0,
          };
        }
        workerTimeline[resp.workerId].lastSeenMs = Math.max(workerTimeline[resp.workerId].lastSeenMs, resp.receivedAtMs);
        workerTimeline[resp.workerId].count += 1;
      }
    }

    const workerNames = Object.keys(workerTimeline);
    let rtoMs = 0;
    if (workerNames.length >= 2) {
      const w1 = workerTimeline[workerNames[0]];
      const w2 = workerTimeline[workerNames[1]];
      rtoMs = Math.max(0, w2.firstSeenMs - w1.lastSeenMs);
    }

    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      totalMessagesSent: totalMessages,
      totalResponsesReceived: state.latenciesMs.length,
      answeredMessages,
      unansweredMessages,
      duplicatesCount: state.duplicates.length,
      duplicateDetails: state.duplicates,
      workerBreakdown: state.workerStats,
      workerTimeline,
      calculatedRtoMs: rtoMs,
      latency: latencyStats,
      connectedWorkersCount: state.activeWorkers.size,
      uptimeSeconds: Number(((Date.now() - state.startTime) / 1000).toFixed(1)),
    }, null, 2));
    return;
  }

  // 3. Reset Metrics Endpoint
  if (pathname === RESET_PATH && req.method === 'POST') {
    state.messages.clear();
    state.messageOrder = [];
    state.latenciesMs = [];
    state.duplicates = [];
    state.workerStats = {};
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ success: true, message: 'Metrics reset successfully' }));
    return;
  }

  // 4. Publish User Message Endpoint (POST /messages)
  if (pathname === MESSAGES_PATH && req.method === 'POST') {
    let body = '';
    req.on('data', chunk => { body += chunk; });
    req.on('end', () => {
      try {
        const payload = body ? JSON.parse(body) : {};
        const id = payload.id || `msg-${Date.now()}-${Math.random().toString(36).slice(2, 7)}`;
        const user = payload.user || 'synthetic-user';
        const text = payload.text || 'Hello relay cluster!';
        const nowMs = Date.now();

        const msgObj = {
          id,
          user,
          text,
          timestampMs: nowMs,
          responses: [],
        };

        state.messages.set(id, msgObj);
        state.messageOrder.push(id);

        if (state.messageOrder.length > MAX_HISTORY_SIZE) {
          const evictId = state.messageOrder.shift();
          state.messages.delete(evictId);
        }

        // Broadcast to WebSocket workers
        const broadcastPayload = JSON.stringify({
          type: 'CHAT_MESSAGE',
          message: msgObj,
        });

        for (const [wsClient] of state.activeWorkers) {
          if (wsClient.readyState === WebSocket.OPEN) {
            wsClient.send(broadcastPayload);
          }
        }

        // Broadcast to SSE clients
        for (const sseRes of state.sseClients) {
          sseRes.write(`data: ${broadcastPayload}\n\n`);
        }

        res.writeHead(201, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ success: true, messageId: id, timestamp: nowMs }));
      } catch (err) {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'Invalid JSON payload', details: err.message }));
      }
    });
    return;
  }

  // 5. Worker Response Endpoint (POST /messages/:id/respond)
  if (pathname.startsWith(MESSAGES_PATH) && pathname.endsWith('/respond') && req.method === 'POST') {
    const parts = pathname.split('/');
    // e.g. /messages/<messageId>/respond -> parts: ['', 'messages', '<id>', 'respond']
    const messageId = parts[2];

    let body = '';
    req.on('data', chunk => { body += chunk; });
    req.on('end', () => {
      try {
        const payload = body ? JSON.parse(body) : {};
        const workerId = payload.workerId || 'anonymous-worker';
        const clientSentAt = payload.timestamp || Date.now();

        const result = recordResponse(messageId, workerId, clientSentAt);
        if (result.error) {
          res.writeHead(result.status || 400, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify(result));
          return;
        }

        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify(result));
      } catch (err) {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'Invalid JSON', details: err.message }));
      }
    });
    return;
  }

  // 6. SSE Events Stream (GET /events)
  if (pathname === EVENTS_PATH && req.method === 'GET') {
    res.writeHead(200, {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache',
      'Connection': 'keep-alive',
    });
    res.write(': connected\n\n');
    state.sseClients.add(res);

    req.on('close', () => {
      state.sseClients.delete(res);
    });
    return;
  }

  // Fallback 404
  res.writeHead(404, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify({ error: 'Endpoint not found', path: pathname }));
});

// WebSocket Server attached to HTTP server
const wss = new WebSocketServer({ noServer: true });

server.on('upgrade', (request, socket, head) => {
  const parsedUrl = new URL(request.url, `http://${request.headers.host || 'localhost'}`);
  if (parsedUrl.pathname === WS_PATH) {
    wss.handleUpgrade(request, socket, head, (ws) => {
      wss.emit('connection', ws, request);
    });
  } else {
    socket.destroy();
  }
});

wss.on('connection', (ws, request) => {
  const workerMeta = {
    workerId: 'unregistered',
    connectedAt: Date.now(),
  };
  state.activeWorkers.set(ws, workerMeta);

  ws.on('message', (raw) => {
    try {
      const data = JSON.parse(raw.toString());

      if (data.type === 'IDENTIFY') {
        workerMeta.workerId = data.workerId || `worker-${Math.random().toString(36).slice(2, 6)}`;
        ws.send(JSON.stringify({ type: 'IDENTIFIED', workerId: workerMeta.workerId }));

        // Replay any pending unanswered messages that arrived during cutover
        let replayed = 0;
        for (const msg of state.messages.values()) {
          if (msg.responses.length === 0) {
            ws.send(JSON.stringify({
              type: 'CHAT_MESSAGE',
              message: msg,
            }));
            replayed += 1;
          }
        }
        if (replayed > 0) {
          console.log(`[ChatServer] Replayed ${replayed} unhandled cutover messages to ${workerMeta.workerId}`);
        }
        return;
      }

      if (data.type === 'CHAT_RESPONSE') {
        const { messageId, workerId, timestamp } = data;
        const result = recordResponse(messageId, workerId || workerMeta.workerId, timestamp);
        ws.send(JSON.stringify({
          type: 'RESPONSE_ACK',
          messageId,
          ...result,
        }));
      }
    } catch (err) {
      ws.send(JSON.stringify({ type: 'ERROR', message: err.message }));
    }
  });

  ws.on('close', () => {
    state.activeWorkers.delete(ws);
  });
});

server.listen(PORT, HOST, () => {
  console.log(`[ChatServer] Listening on http://${HOST}:${PORT}`);
  console.log(`[ChatServer] WebSocket available at ws://${HOST}:${PORT}${WS_PATH}`);
  console.log(`[ChatServer] Health endpoint: ${HEALTH_PATH} | Metrics: ${METRICS_PATH}`);
});

module.exports = { server, state };
