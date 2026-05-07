/**
 * telegram-bridge.js
 * ─────────────────────────────────────────────────────────────
 * Per-agent Telegram bot bridge for OpenClaw / Nexus.AI portal
 *
 * Each agent in your dashboard gets its OWN Telegram bot.
 * Sending a message to @CodeGuardBot → only Code-Guard agent responds.
 * Sending a message to @QAAgentBot  → only QA-Agent responds.
 *
 * HOW IT WORKS:
 *   1. Reads agent list from your existing ~/.openclaw/openclaw.json
 *   2. Reads Telegram tokens from ~/.openclaw/telegram-config.json
 *   3. Starts one polling loop per agent (all concurrent via setInterval)
 *   4. On incoming message → calls your existing sendToAgent() logic
 *      via the same OpenClaw WebSocket gateway your server.js uses
 *   5. Sends the agent's reply back to that specific Telegram chat
 *
 * SETUP:
 *   1. npm install node-fetch          (already in your project most likely)
 *   2. Create ~/.openclaw/telegram-config.json  (see template below)
 *   3. node telegram-bridge.js
 *      — OR —
 *      Add require('./telegram-bridge') to the bottom of server.js
 *
 * TELEGRAM CONFIG FILE (~/.openclaw/telegram-config.json):
 * {
 *   "agents": {
 *     "Code-Guard":       { "botToken": "7100000001:AAF...", "enabled": true },
 *     "CodeShield-Agent": { "botToken": "7100000002:AAG...", "enabled": true },
 *     "PlanForge-PM":     { "botToken": "7100000003:AAH...", "enabled": true },
 *     "QA-Agent":         { "botToken": "7100000004:AAI...", "enabled": true },
 *     "RevenuePilot":     { "botToken": "7100000005:AAJ...", "enabled": true },
 *     "main":             { "botToken": "7100000006:AAK...", "enabled": true }
 *   },
 *   "settings": {
 *     "pollIntervalMs":   1500,
 *     "typingIndicator":  true,
 *     "maxMessageLength": 4000,
 *     "replyOnError":     true
 *   }
 * }
 *
 * GET YOUR BOT TOKEN:
 *   Telegram → @BotFather → /newbot → copy token → paste above
 *
 * GET CHAT ID (after creating bot):
 *   Send any message to your bot, then visit:
 *   https://api.telegram.org/bot<TOKEN>/getUpdates
 *   Look for: "chat": { "id": 123456789 }
 */

'use strict';

const fs        = require('fs');
const path      = require('path');
const os        = require('os');
const crypto    = require('crypto');
const http      = require('http');
const WebSocket = require('ws');

// ── Paths (mirrors your server.js) ───────────────────────────
const HOME              = os.homedir();
const OC                = path.join(HOME, '.openclaw');
const CONFIG_PATH       = path.join(OC, 'openclaw.json');
const TG_CONFIG_PATH    = path.join(OC, 'telegram-config.json');
const TG_STATE_PATH     = path.join(OC, 'telegram-state.json');   // stores last update_id per bot
const OC_WS_URL         = 'ws://127.0.0.1:18789';
const FALLBACK_GW_TOKEN = '89bb4a09636d7b7e54a09639c8f3273c4936d150347132a4';

// ── Known working client ID file (shared with server.js) ─────
const WORKING_CLIENT_ID_FILE = path.join(OC, 'portal-working-client-id.json');

// ── Client ID candidates (same list as server.js) ────────────
const CLIENT_ID_CANDIDATES = [
  'openclaw-control-ui',
  'nexus-portal',
  'openclaw-portal',
  'openclaw-service',
  'openclaw-api',
  'portal',
  'service',
];

// ── Telegram API base ─────────────────────────────────────────
const TG_API = 'https://api.telegram.org/bot';

// ─────────────────────────────────────────────────────────────
// HELPERS
// ─────────────────────────────────────────────────────────────
const readJson  = (p, def = {}) => { try { return JSON.parse(fs.readFileSync(p, 'utf8')); } catch { return def; } };
const writeJson = (p, d) => { fs.mkdirSync(path.dirname(p), { recursive: true }); fs.writeFileSync(p, JSON.stringify(d, null, 2), 'utf8'); };
const log       = (tag, msg) => console.log(`[${new Date().toISOString().slice(11,19)}] [${tag}] ${msg}`);
const sleep     = ms => new Promise(r => setTimeout(r, ms));

function getGatewayToken() {
  const cfg = readJson(CONFIG_PATH);
  return cfg?.gateway?.auth?.token
      || cfg?.gateway?.auth?.tokens?.[0]
      || FALLBACK_GW_TOKEN
      || '';
}

function getWorkingClientId() {
  try {
    const d = readJson(WORKING_CLIENT_ID_FILE);
    if (d.clientId) return d.clientId;
  } catch {}
  return CLIENT_ID_CANDIDATES[0];
}

// ── Telegram state (stores last update_id per agent) ─────────
function loadTgState() { return readJson(TG_STATE_PATH, {}); }
function saveTgState(state) { writeJson(TG_STATE_PATH, state); }

// ── Split long messages (Telegram max 4096 chars) ─────────────
function splitMessage(text, maxLen = 4000) {
  if (text.length <= maxLen) return [text];
  const parts = [];
  let i = 0;
  while (i < text.length) {
    let end = i + maxLen;
    if (end < text.length) {
      const nl = text.lastIndexOf('\n', end);
      if (nl > i) end = nl;
    }
    parts.push(text.slice(i, end));
    i = end;
  }
  return parts;
}

// ─────────────────────────────────────────────────────────────
// TELEGRAM API CALLS
// ─────────────────────────────────────────────────────────────
async function tgCall(token, method, body = {}) {
  const url = `${TG_API}${token}/${method}`;
  const res = await fetch(url, {
    method:  'POST',
    headers: { 'Content-Type': 'application/json' },
    body:    JSON.stringify(body),
    signal:  AbortSignal.timeout(15000),
  });
  const data = await res.json();
  if (!data.ok) throw new Error(`Telegram ${method} error: ${data.description || JSON.stringify(data)}`);
  return data.result;
}

async function tgGetUpdates(token, offset = 0, timeout = 30) {
  const url = `${TG_API}${token}/getUpdates?offset=${offset}&timeout=${timeout}&allowed_updates=["message"]`;
  const res = await fetch(url, { signal: AbortSignal.timeout((timeout + 5) * 1000) });
  const data = await res.json();
  if (!data.ok) throw new Error(`getUpdates error: ${data.description}`);
  return data.result || [];
}

async function tgSendMessage(token, chatId, text, replyToId = null) {
  const parts = splitMessage(text);
  for (let i = 0; i < parts.length; i++) {
    await tgCall(token, 'sendMessage', {
      chat_id:    chatId,
      text:       parts[i],
      parse_mode: 'Markdown',
      ...(i === 0 && replyToId ? { reply_to_message_id: replyToId } : {}),
    });
    if (parts.length > 1) await sleep(300);
  }
}

async function tgSendTyping(token, chatId) {
  try { await tgCall(token, 'sendChatAction', { chat_id: chatId, action: 'typing' }); } catch {}
}

async function tgGetMe(token) {
  return tgCall(token, 'getMe');
}

// ─────────────────────────────────────────────────────────────
// OPENCLAW WEBSOCKET — send task to agent, collect response
// (Mirrors the sendToAgent() function in your server.js exactly)
// ─────────────────────────────────────────────────────────────
function sendToAgentWS(agentId, message) {
  return new Promise((resolve, reject) => {
    const token    = getGatewayToken();
    const clientId = getWorkingClientId();
    const reqId    = `tg-${Date.now()}-${crypto.randomBytes(3).toString('hex')}`;

    if (!token) return reject(new Error('No gateway token found in openclaw.json'));

    let fullText   = '';
    let finished   = false;
    let connected  = false;
    let msgSent    = false;
    let gotChunk   = false;
    let errorCount = 0;
    let sessionKey = `agent:${agentId}:main`;

    const ws = new WebSocket(OC_WS_URL, {
      headers:          { Origin: 'http://127.0.0.1:18789' },
      handshakeTimeout: 10000,
    });

    const finish = (err) => {
      if (finished) return;
      finished = true;
      clearTimeout(gto);
      clearInterval(hbi);
      try { ws.terminate(); } catch {}
      if (err) reject(new Error(String(err)));
      else     resolve(fullText || '(Agent returned empty response)');
    };

    // 3-minute timeout
    const gto = setTimeout(() => finish('No response after 3 minutes — is the model running?'), 180_000);
    const hbi = setInterval(() => log(agentId, '... waiting for agent response'), 20_000);

    ws.on('open', () => log(agentId, `WS connected → sending task`));

    ws.on('message', raw => {
      let f;
      try { f = JSON.parse(raw.toString()); } catch { return; }

      const { type, event, id: fid, ok, payload, error } = f;

      // ── Auth challenge ──────────────────────────────────────
      if (type === 'event' && event === 'connect.challenge') {
        ws.send(JSON.stringify({
          type: 'req', id: `${reqId}-c`, method: 'connect',
          params: {
            minProtocol: 3, maxProtocol: 3,
            role: 'operator',
            scopes: ['operator.read', 'operator.write'],
            caps: [], commands: [], permissions: {},
            auth: { token },
            locale: 'en-US',
            userAgent: `nexus-portal-tg/1.0 (${clientId})`,
            client: { id: clientId, version: '2026.4', platform: 'web', mode: 'webchat' },
          },
        }));
        return;
      }

      // ── Auth response ───────────────────────────────────────
      if (type === 'res' && fid === `${reqId}-c`) {
        if (!ok) {
          const msg = JSON.stringify(error || payload || '');
          return finish(`Auth failed (client.id="${clientId}"): ${msg}`);
        }
        connected = true;
        ws.send(JSON.stringify({
          type: 'req', id: `${reqId}-m`, method: 'chat.send',
          params: { sessionKey, message, idempotencyKey: reqId },
        }));
        return;
      }

      // ── chat.send response ──────────────────────────────────
      if (type === 'res' && fid === `${reqId}-m`) {
        if (!ok) {
          const errDetail = JSON.stringify(error || payload || '');
          // Try alternate session key formats
          if (errDetail.includes('session') || errDetail.includes('not found')) {
            const alt = agentId;
            if (alt !== sessionKey) {
              sessionKey = alt;
              ws.send(JSON.stringify({
                type: 'req', id: `${reqId}-m2`, method: 'chat.send',
                params: { sessionKey, message, idempotencyKey: `${reqId}-r` },
              }));
              return;
            }
          }
          return finish(`Message rejected: ${errDetail}`);
        }
        msgSent = true;
        return;
      }

      // ── chat.send retry response ────────────────────────────
      if (type === 'res' && fid === `${reqId}-m2`) {
        if (!ok) return finish(`Message rejected on retry: ${JSON.stringify(error || payload || '')}`);
        msgSent = true;
        return;
      }

      // ── Agent streaming ─────────────────────────────────────
      if (type === 'event' && event === 'agent') {
        const { stream, data } = payload || {};

        if (stream === 'assistant' || stream === 'delta' || stream === 'text') {
          let chunk = '';
          if (typeof data === 'string')   chunk = data;
          else if (data?.delta != null)   chunk = String(data.delta);
          else if (data?.content != null) chunk = String(data.content);
          if (!chunk && data?.text) {
            const np = String(data.text).slice(fullText.length);
            if (np) chunk = np;
          }
          if (chunk) { fullText += chunk; gotChunk = true; }
          return;
        }

        if (stream === 'lifecycle') {
          const ph = data?.phase;
          if (ph === 'end' || ph === 'done' || ph === 'complete') return finish(null);
          if (ph === 'error') {
            errorCount++;
            const errMsg = data?.error || data?.message || JSON.stringify(data);
            if (errorCount >= 2 || errMsg.includes('500')) return finish(`Agent error: ${errMsg}`);
            if (gotChunk) return finish(null);
          }
          return;
        }

        if (stream === 'done' || stream === 'end' || stream === 'complete') {
          if (!gotChunk && data) {
            const t = typeof data === 'string' ? data : (data?.text || data?.content || '');
            if (t) { fullText = t; gotChunk = true; }
          }
          return finish(null);
        }

        if (stream === 'error') {
          const msg = typeof data === 'string' ? data : (data?.message || data?.error || JSON.stringify(data));
          return finish(`Agent stream error: ${msg}`);
        }
      }
    });

    ws.on('error', e => finish(`WS error: ${e.message}`));
    ws.on('close', code => {
      if (!finished) {
        if (gotChunk)                   finish(null);
        else if (connected && msgSent)  finish('Agent disconnected without responding');
        else                            finish(`WS closed unexpectedly (code=${code})`);
      }
    });
  });
}

// ─────────────────────────────────────────────────────────────
// COMMAND HANDLERS
// ─────────────────────────────────────────────────────────────
const COMMANDS = {
  '/start': (agentId, agentName) =>
    `👋 *${agentName}* is ready.\n\nSend any message and I'll get to work on it immediately.\n\n*Commands:*\n/start — show this message\n/status — check agent status\n/help — usage guide`,

  '/status': (agentId, agentName) =>
    `✅ *${agentName}* is online and connected to OpenClaw.\n\nAgent ID: \`${agentId}\`\nSession: \`agent:${agentId}:main\``,

  '/help': (agentId, agentName) =>
    `*${agentName} — Help*\n\nJust send a plain message with your task.\n\n*Examples:*\n• Review this code for bugs\n• Write test cases for the login flow\n• Summarise the Q2 revenue data\n\nI'll start working on it right away.`,
};

// ─────────────────────────────────────────────────────────────
// PROCESS ONE INCOMING TELEGRAM MESSAGE
// ─────────────────────────────────────────────────────────────
async function handleMessage(agentId, agentName, token, message, settings) {
  const chatId    = message.chat.id;
  const msgId     = message.message_id;
  const text      = (message.text || '').trim();
  const senderName = message.from?.first_name || 'User';

  if (!text) return;  // ignore non-text (photos, stickers etc)

  log(agentId, `← [${senderName}] "${text.slice(0, 60)}${text.length > 60 ? '…' : ''}"`);

  // Handle bot commands
  const cmdBase = text.split(' ')[0].split('@')[0].toLowerCase();
  if (COMMANDS[cmdBase]) {
    await tgSendMessage(token, chatId, COMMANDS[cmdBase](agentId, agentName), msgId);
    return;
  }

  // Show typing indicator
  if (settings.typingIndicator !== false) {
    await tgSendTyping(token, chatId);
    // Keep refreshing typing indicator while agent thinks
    const typingInterval = setInterval(() => tgSendTyping(token, chatId), 4000);
    try {
      const reply = await sendToAgentWS(agentId, text);
      clearInterval(typingInterval);
      log(agentId, `→ [${senderName}] reply ${reply.length} chars`);
      await tgSendMessage(token, chatId, reply, msgId);
    } catch (err) {
      clearInterval(typingInterval);
      log(agentId, `✗ Error: ${err.message}`);
      if (settings.replyOnError !== false) {
        await tgSendMessage(token, chatId,
          `⚠️ *${agentName}* encountered an error:\n\`${err.message}\`\n\nPlease try again.`,
          msgId
        );
      }
    }
  } else {
    // No typing indicator
    try {
      const reply = await sendToAgentWS(agentId, text);
      await tgSendMessage(token, chatId, reply, msgId);
    } catch (err) {
      log(agentId, `✗ Error: ${err.message}`);
      if (settings.replyOnError !== false) {
        await tgSendMessage(token, chatId,
          `⚠️ Error from ${agentName}: ${err.message}`, msgId
        );
      }
    }
  }
}

// ─────────────────────────────────────────────────────────────
// POLLING LOOP — one per agent
// ─────────────────────────────────────────────────────────────
async function startAgentBot(agentId, agentName, token, settings) {
  // Verify the token works and get bot info
  let botInfo;
  try {
    botInfo = await tgGetMe(token);
    log(agentId, `✓ Bot @${botInfo.username} (${botInfo.first_name}) connected`);
  } catch (err) {
    log(agentId, `✗ Invalid token — bot skipped: ${err.message}`);
    return;
  }

  // Load last processed update_id for this agent
  const state       = loadTgState();
  let   lastUpdateId = state[agentId] || 0;

  log(agentId, `Polling started (offset=${lastUpdateId})`);

  // Active message queue — ensures messages processed one at a time per agent
  let processing = false;
  const queue    = [];

  async function processQueue() {
    if (processing || !queue.length) return;
    processing = true;
    const { message } = queue.shift();
    try {
      await handleMessage(agentId, agentName, token, message, settings);
    } catch (e) {
      log(agentId, `Queue handler error: ${e.message}`);
    }
    processing = false;
    if (queue.length) processQueue();
  }

  // Long-polling loop
  while (true) {
    try {
      const updates = await tgGetUpdates(token, lastUpdateId + 1, 25);

      for (const update of updates) {
        lastUpdateId = update.update_id;
        if (update.message) {
          queue.push(update);
          processQueue();  // non-blocking enqueue
        }
      }

      // Persist offset so we don't reprocess on restart
      if (updates.length) {
        const s     = loadTgState();
        s[agentId]  = lastUpdateId;
        saveTgState(s);
      }
    } catch (err) {
      if (!err.message.includes('timeout')) {
        log(agentId, `Polling error: ${err.message} — retrying in 5s`);
        await sleep(5000);
      }
    }

    await sleep(settings.pollIntervalMs || 1500);
  }
}

// ─────────────────────────────────────────────────────────────
// ENSURE TEMPLATE CONFIG EXISTS
// ─────────────────────────────────────────────────────────────
function ensureTgConfigTemplate(agentIds) {
  if (fs.existsSync(TG_CONFIG_PATH)) return;

  const agents = {};
  for (const id of agentIds) {
    agents[id] = { botToken: '', enabled: false };
  }

  const template = {
    _readme: [
      'Telegram bridge config for Nexus.AI / OpenClaw agents.',
      'Create one bot per agent via @BotFather on Telegram.',
      'Paste each bot token into the matching agentId below.',
      'Set enabled:true to activate that bot.',
    ],
    agents,
    settings: {
      pollIntervalMs:  1500,
      typingIndicator: true,
      maxMessageLength: 4000,
      replyOnError:    true,
    },
  };

  writeJson(TG_CONFIG_PATH, template);
  console.log(`\n[TG BRIDGE] Created template config: ${TG_CONFIG_PATH}`);
  console.log('[TG BRIDGE] Fill in your bot tokens to activate each agent bot.\n');
}

// ─────────────────────────────────────────────────────────────
// MAIN — start all agent bots
// ─────────────────────────────────────────────────────────────
async function startTelegramBridge() {
  console.log('\n══════════════════════════════════════════════════');
  console.log('  Nexus.AI — Telegram Bridge');
  console.log('══════════════════════════════════════════════════');

  // Load OpenClaw agent list
  const cfg      = readJson(CONFIG_PATH);
  const agentList = cfg?.agents?.list || [];

  if (!agentList.length) {
    console.warn('[TG BRIDGE] No agents found in openclaw.json — bridge idle.');
    return;
  }

  // Create template config if missing
  ensureTgConfigTemplate(agentList.map(a => a.id));

  // Load Telegram config
  const tgCfg   = readJson(TG_CONFIG_PATH);
  const settings = tgCfg.settings || {};
  const agentTg  = tgCfg.agents   || {};

  // Find enabled agents with tokens
// Read directly from telegram-config.json — no cross-reference with openclaw.json
const toStart = Object.entries(agentTg)
    .filter(([id, cfg]) => cfg.enabled === true && (cfg.botToken || '').length > 10)
    .map(([id, cfg]) => ({
      id,
      name: id,
      token: cfg.botToken,
    }));

  if (!toStart.length) {
    console.warn(`[TG BRIDGE] No enabled bots found in ${TG_CONFIG_PATH}`);
    console.warn('[TG BRIDGE] Set enabled:true and fill botToken for each agent to activate.');
    return;
  }

  console.log(`[TG BRIDGE] Starting ${toStart.length} bot(s):\n`);
  toStart.forEach(a => console.log(`  • ${a.name.padEnd(22)} → token ${a.token.slice(0, 10)}...`));
  console.log('');

  // Start all bots concurrently — each runs its own infinite loop
  const promises = toStart.map(a =>
    startAgentBot(a.id, a.name, a.token, settings)
      .catch(err => log(a.id, `Fatal bot error: ${err.message}`))
  );

  await Promise.all(promises);
}

// ─────────────────────────────────────────────────────────────
// EXPORT for use inside server.js  OR  run standalone
// ─────────────────────────────────────────────────────────────
module.exports = { startTelegramBridge };

// If run directly: node telegram-bridge.js
if (require.main === module) {
  startTelegramBridge().catch(err => {
    console.error('[TG BRIDGE] Fatal:', err.message);
    process.exit(1);
  });
}