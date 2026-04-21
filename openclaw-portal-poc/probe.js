/**
 * probe.js — find which client.id the local OpenClaw gateway accepts.
 *
 * Reads the gateway token from ~/.openclaw/openclaw.json and opens a
 * WebSocket to 127.0.0.1:18789. For each candidate id/mode pair, it
 * waits for the connect.challenge, sends a connect request, and reports
 * whether the gateway accepted or rejected it.
 *
 * Usage:  node probe.js
 */
'use strict';

const fs        = require('fs');
const os        = require('os');
const path      = require('path');
const WebSocket = require('ws');

const CONFIG_PATH = path.join(os.homedir(), '.openclaw', 'openclaw.json');
const WS_URL      = 'ws://127.0.0.1:18789';

function getToken() {
  try {
    const cfg = JSON.parse(fs.readFileSync(CONFIG_PATH, 'utf8'));
    return cfg?.gateway?.auth?.token || cfg?.gateway?.auth?.tokens?.[0] || '';
  } catch { return ''; }
}

const candidates = [
  // [client.id, client.mode, role]
  ['openclaw-cli',       'cli',     'operator'],
  ['openclaw',           'cli',     'operator'],
  ['openclaw-agent-sdk', 'sdk',     'operator'],
  ['openclaw-sdk',       'sdk',     'operator'],
  ['openclaw-agent',     'agent',   'operator'],
  ['openclaw-api',       'api',     'operator'],
  ['openclaw-service',   'service', 'service' ],
  ['openclaw-mcp',       'mcp',     'operator'],
  ['openclaw-plugin',    'plugin',  'operator'],
  ['openclaw-control-ui','webchat', 'operator'],
  ['oc-cli',             'cli',     'operator'],
  ['claw-cli',           'cli',     'operator'],
];

function tryOne({ id, mode, role }) {
  const token = getToken();
  return new Promise(resolve => {
    const ws = new WebSocket(WS_URL, {
      headers: { Origin: 'http://127.0.0.1:18789' },
      handshakeTimeout: 5000,
    });
    let settled = false;
    const done = (result) => { if (settled) return; settled = true; try { ws.terminate(); } catch {} resolve(result); };
    const reqId = `probe-${Date.now()}`;
    setTimeout(() => done({ id, mode, status: 'timeout' }), 6000);

    ws.on('error', e => done({ id, mode, status: 'socket_error', detail: e.message }));

    ws.on('message', raw => {
      let f; try { f = JSON.parse(raw.toString()); } catch { return; }
      if (f.type === 'event' && f.event === 'connect.challenge') {
        ws.send(JSON.stringify({
          type: 'req', id: reqId, method: 'connect',
          params: {
            minProtocol: 3, maxProtocol: 3,
            role,
            scopes: ['operator.read','operator.write','agent.read','agent.write','chat.send','chat.stream'],
            caps: [], commands: [], permissions: {},
            auth: { token },
            locale: 'en-US',
            userAgent: `probe/${id}`,
            client: { id, version: '2026.4', platform: 'node', mode },
          },
        }));
        return;
      }
      if (f.type === 'res' && f.id === reqId) {
        if (f.ok) return done({ id, mode, status: 'ACCEPTED', protocol: f.payload?.protocol });
        const msg = f.error?.message || JSON.stringify(f.error || f.payload);
        const which = [];
        if (/\/client\/id/.test(msg))   which.push('id');
        if (/\/client\/mode/.test(msg)) which.push('mode');
        if (/\/role/.test(msg))         which.push('role');
        if (/\/scopes/.test(msg))       which.push('scopes');
        if (/DEVICE_IDENTITY/i.test(msg)) which.push('device-identity');
        return done({ id, mode, status: 'rejected', bad: which.join(',') || 'other', detail: msg.slice(0, 200) });
      }
    });
  });
}

(async () => {
  const token = getToken();
  console.log('Probing', WS_URL, 'with token', token ? token.slice(0, 8)+'...' : '(none)');
  console.log('─'.repeat(90));
  for (const [id, mode, role] of candidates) {
    const r = await tryOne({ id, mode, role });
    const mark = r.status === 'ACCEPTED' ? '✅' : r.status === 'timeout' ? '⏱ ' : r.status === 'socket_error' ? '💥' : '❌';
    const info = r.status === 'ACCEPTED'
      ? `proto=${r.protocol}`
      : r.bad
        ? `bad=${r.bad}  ${r.detail || ''}`
        : (r.detail || '');
    console.log(`${mark}  id=${id.padEnd(22)} mode=${mode.padEnd(8)} role=${role.padEnd(10)} → ${r.status.padEnd(12)} ${info}`);
  }
  console.log('─'.repeat(90));
  console.log('Look for the ✅ line — that is the client.id/mode pair to use in server.js.');
  process.exit(0);
})();
