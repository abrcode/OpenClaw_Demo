/**
 * Nexus.AI Portal — server.js
 * ─────────────────────────────────────────────────────────────
 * Full per-agent OAuth2 provider system
 *
 * FIXES APPLIED:
 *   1. EADDRINUSE — auto-kills old process on port and retries
 *   2. Auth client.id — tries multiple known valid identities
 *      with fallback probe so you can find the right one
 *
 * Storage layout (~/.openclaw/):
 *   portal-credentials.json  ← AES-256-GCM encrypted tokens, per agent
 *   portal-oauth-state.json  ← in-flight OAuth CSRF state (30-min TTL)
 *   portal-tasks.json        ← task history
 *
 * OAuth config → ~/.openclaw/portal-config.json (NOT openclaw.json):
 *   "portal": {
 *     "baseUrl":  "http://localhost:3001",
 *     "github":   { "clientId": "...", "clientSecret": "..." },
 *     "gmail":    { "clientId": "...", "clientSecret": "..." }
 *   }
 *
 * GitHub App:   https://github.com/settings/developers
 *   Callback:   http://localhost:3001/api/oauth/github/callback
 *
 * Google App:   https://console.cloud.google.com/apis/credentials
 *   Callback:   http://localhost:3001/api/email/oauth/callback
 *   Scopes:     gmail.readonly, gmail.send, userinfo.email, userinfo.profile
 */

'use strict';

const express    = require('express');
const bodyParser = require('body-parser');
const fs         = require('fs');
const path       = require('path');
const os         = require('os');
const crypto     = require('crypto');
const http       = require('http');
const { exec, execSync } = require('child_process');
const WebSocket  = require('ws');

const app    = express();
const server = http.createServer(app);
const PORT   = parseInt(process.env.PORT || '3001', 10);
const BIND   = '0.0.0.0';

// ── Default model — workspace is locked to this one ──────────
const DEFAULT_MODEL = 'ollama/gemma3:4b-cloud';

// ── Gateway token fallback (if openclaw.json doesn't expose it) ──
const FALLBACK_GATEWAY_TOKEN = '079d3ac8543c5de7a0030f2024659b97de49551df8ac8714';

// ── Known client identities to try in order ──────────────────
// The gateway schema validates client.id against a strict constant/anyOf.
// We try candidates in sequence until one succeeds.
const CLIENT_ID_CANDIDATES = [
    'openclaw-control-ui',
    'nexus-portal',
    'openclaw-portal',
    'openclaw-service',
    'openclaw-api',
    'portal',
    'service',
];

app.use(bodyParser.json({ limit: '4mb' }));
app.use(express.static('public'));
app.use((req, res, next) => {
    res.header('Access-Control-Allow-Origin', '*');
    res.header('Access-Control-Allow-Headers', 'Content-Type,Authorization');
    res.header('Access-Control-Allow-Methods', 'GET,POST,PUT,DELETE,OPTIONS');
    if (req.method === 'OPTIONS') return res.sendStatus(200);
    next();
});

// ════════════════════════════════════════════════════════════════
// PATHS
// ════════════════════════════════════════════════════════════════
const HOME        = os.homedir();
const OC          = path.join(HOME, '.openclaw');
const AGENTS_ROOT = path.join(OC, 'agents');
const CONFIG_PATH = path.join(OC, 'openclaw.json');
const TASKS_FILE  = path.join(OC, 'portal-tasks.json');
const CREDS_FILE  = path.join(OC, 'portal-credentials.json');
const STATE_FILE         = path.join(OC, 'portal-oauth-state.json');
const PORTAL_CONFIG_PATH = path.join(OC, 'portal-config.json');
const OC_WS_URL   = 'ws://127.0.0.1:18789';
const WS_FILES    = ['SOUL.md','AGENTS.md','IDENTITY.md','USER.md','TOOLS.md','HEARTBEAT.md','MEMORY.md'];

// ── Persisted working client.id (discovered at runtime) ──────
const WORKING_CLIENT_ID_FILE = path.join(OC, 'portal-working-client-id.json');
let _workingClientId = null;

function loadWorkingClientId() {
    try {
        const d = JSON.parse(fs.readFileSync(WORKING_CLIENT_ID_FILE, 'utf8'));
        if (d.clientId) { _workingClientId = d.clientId; return d.clientId; }
    } catch {}
    return null;
}

function saveWorkingClientId(id) {
    _workingClientId = id;
    try { fs.writeFileSync(WORKING_CLIENT_ID_FILE, JSON.stringify({ clientId: id, discoveredAt: new Date().toISOString() }, null, 2), 'utf8'); } catch {}
}

// Ensure OC dir exists
if (!fs.existsSync(OC)) fs.mkdirSync(OC, { recursive: true });

// ════════════════════════════════════════════════════════════════
// ENCRYPTION  — AES-256-GCM, machine-derived key
// ════════════════════════════════════════════════════════════════
const ENC_KEY = crypto.scryptSync(
    `nexus-portal-v2-${os.hostname()}-${os.userInfo().username}`,
    'nexus-salt-2026',
    32
);

function enc(plain) {
    if (!plain) return '';
    const iv     = crypto.randomBytes(12);
    const cipher = crypto.createCipheriv('aes-256-gcm', ENC_KEY, iv);
    const data   = Buffer.concat([cipher.update(String(plain), 'utf8'), cipher.final()]);
    const tag    = cipher.getAuthTag();
    return `${iv.toString('hex')}.${data.toString('hex')}.${tag.toString('hex')}`;
}

function dec(cipher) {
    if (!cipher) return null;
    try {
        const [ivH, dataH, tagH] = cipher.split('.');
        const iv  = Buffer.from(ivH, 'hex');
        const dat = Buffer.from(dataH, 'hex');
        const tag = Buffer.from(tagH, 'hex');
        const d   = crypto.createDecipheriv('aes-256-gcm', ENC_KEY, iv);
        d.setAuthTag(tag);
        return Buffer.concat([d.update(dat), d.final()]).toString('utf8');
    } catch { return null; }
}

// ════════════════════════════════════════════════════════════════
// CREDENTIAL STORE  — per agent, encrypted
// ════════════════════════════════════════════════════════════════
const readCreds  = () => { try { return JSON.parse(fs.readFileSync(CREDS_FILE, 'utf8')); } catch { return {}; } };
const writeCreds = (d) => fs.writeFileSync(CREDS_FILE, JSON.stringify(d, null, 2), 'utf8');

function getAgentCreds(agentId) { return readCreds()[agentId] || {}; }

function setAgentService(agentId, service, payload) {
    const all = readCreds();
    if (!all[agentId]) all[agentId] = {};
    all[agentId][service] = payload;
    writeCreds(all);
}

function removeAgentService(agentId, service) {
    const all = readCreds();
    if (all[agentId]?.[service]) {
        delete all[agentId][service];
        writeCreds(all);
    }
}

/** Safe view — never exposes tokens */
function safeCredView(agentId) {
    const c = getAgentCreds(agentId);
    const out = {};
    if (c.github) out.github = {
        connected:    true,
        login:        c.github.login,
        name:         c.github.name,
        avatar_url:   c.github.avatar_url,
        scopes:       c.github.scopes || 'repo,user:email',
        connected_at: c.github.connected_at,
    };
    if (c.gmail) out.gmail = {
        connected:    true,
        email:        c.gmail.email,
        name:         c.gmail.name,
        picture:      c.gmail.picture,
        connected_at: c.gmail.connected_at,
        expires_at:   c.gmail.expires_at,
    };
    return out;
}

// ════════════════════════════════════════════════════════════════
// OAUTH STATE STORE  — CSRF protection, 30-min TTL
// ════════════════════════════════════════════════════════════════
const readStates  = () => { try { return JSON.parse(fs.readFileSync(STATE_FILE, 'utf8')); } catch { return {}; } };
const writeStates = (d) => fs.writeFileSync(STATE_FILE, JSON.stringify(d, null, 2), 'utf8');

function createState(agentId, service) {
    const token  = crypto.randomBytes(32).toString('hex');
    const states = readStates();
    // prune expired
    const cutoff = Date.now() - 30 * 60_000;
    for (const k of Object.keys(states)) if (states[k].ts < cutoff) delete states[k];
    states[token] = { agentId, service, ts: Date.now() };
    writeStates(states);
    return token;
}

function consumeState(token) {
    const states = readStates();
    const entry  = states[token];
    if (!entry || Date.now() - entry.ts > 30 * 60_000) {
        if (entry) { delete states[token]; writeStates(states); }
        return null;
    }
    delete states[token];
    writeStates(states);
    return entry;
}

// ════════════════════════════════════════════════════════════════
// HELPERS
// ════════════════════════════════════════════════════════════════
const isDir  = p => { try { return fs.statSync(p).isDirectory(); } catch { return false; } };
const readF  = p => { try { return fs.readFileSync(p, 'utf8'); } catch { return ''; } };
const writeF = (p, c) => { fs.mkdirSync(path.dirname(p), { recursive: true }); fs.writeFileSync(p, c, 'utf8'); };

function getAgentDirs() {
    if (!fs.existsSync(AGENTS_ROOT)) return [];
    return fs.readdirSync(AGENTS_ROOT).filter(e => !e.startsWith('.') && isDir(path.join(AGENTS_ROOT, e)));
}

function readConfig() { try { return JSON.parse(fs.readFileSync(CONFIG_PATH, 'utf8')); } catch { return {}; } }

function writeConfig(cfg) {
    if (fs.existsSync(CONFIG_PATH)) {
        const bak = `${CONFIG_PATH}.bak.${Date.now()}`;
        fs.copyFileSync(CONFIG_PATH, bak);
        // keep last 5 backups
        fs.readdirSync(path.dirname(CONFIG_PATH))
            .filter(f => f.startsWith('openclaw.json.bak.')).sort()
            .slice(0, -5).forEach(b => { try { fs.unlinkSync(path.join(path.dirname(CONFIG_PATH), b)); } catch {} });
    }
    const { _path, ...clean } = cfg;
    fs.writeFileSync(CONFIG_PATH, JSON.stringify(clean, null, 2), 'utf8');
}

function getGatewayToken() {
    const cfg = readConfig();
    return cfg?.gateway?.auth?.token
        || cfg?.gateway?.auth?.tokens?.[0]
        || FALLBACK_GATEWAY_TOKEN
        || '';
}

function getPortalConfig() {
    try { return JSON.parse(fs.readFileSync(PORTAL_CONFIG_PATH, 'utf8')); } catch { return {}; }
}

function getLocalIPs() {
    const ips = [];
    for (const [, nets] of Object.entries(os.networkInterfaces()))
        for (const n of nets) if (n.family === 'IPv4' && !n.internal) ips.push(n.address);
    return ips;
}

function runCmd(cmd) {
    return new Promise(r => exec(cmd, { timeout: 25000 }, (e, o, s) =>
        r({ ok: !e, stdout: o?.trim() || '', stderr: s?.trim() || '', error: e?.message })));
}

function getAgentWorkspace(id) {
    const e = readConfig()?.agents?.list?.find(a => a.id === id);
    if (e?.workspace) return e.workspace.replace(/^~/, HOME);
    return path.join(OC, `workspace-${id}`);
}

function baseUrl() {
    return getPortalConfig().baseUrl || `http://localhost:${PORT}`;
}

// ════════════════════════════════════════════════════════════════
// SSE BROADCAST
// ════════════════════════════════════════════════════════════════
const sseClients = new Set();
function broadcast(data) {
    const msg = `data: ${JSON.stringify(data)}\n\n`;
    for (const c of sseClients) { try { c.write(msg); } catch { sseClients.delete(c); } }
}

app.get('/api/events', (req, res) => {
    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');
    res.setHeader('X-Accel-Buffering', 'no');
    res.flushHeaders();
    res.write('data: {"type":"hello"}\n\n');
    sseClients.add(res);
    const hb = setInterval(() => { try { res.write(': ping\n\n'); } catch { sseClients.delete(res); clearInterval(hb); } }, 20000);
    req.on('close', () => { sseClients.delete(res); clearInterval(hb); });
});

// ════════════════════════════════════════════════════════════════
// POPUP HTML HELPERS
// ════════════════════════════════════════════════════════════════
function popupHtml(err, data) {
    const payload = err
        ? `{type:'oauth_error',error:${JSON.stringify(String(err))}}`
        : `{type:'oauth_success',data:${JSON.stringify(data)}}`;
    const msg = err
        ? `<p style="color:#ef4444;font-family:sans-serif;padding:24px">✗ ${String(err)}</p>`
        : `<p style="color:#10b981;font-family:sans-serif;padding:24px">✓ Connected! Closing window...</p>`;
    return `<!DOCTYPE html><html><head><title>OAuth</title></head><body>${msg}<script>
try{window.opener.postMessage(${payload},'*');}catch(e){}
setTimeout(()=>window.close(),${err ? 2500 : 900});
</script></body></html>`;
}

// ════════════════════════════════════════════════════════════════
// TOKEN REFRESH HELPERS
// ════════════════════════════════════════════════════════════════
async function refreshGoogleToken(agentId) {
    const creds = getAgentCreds(agentId);
    if (!creds.gmail?.refresh_token_enc) throw new Error('No refresh token stored');
    const rt  = dec(creds.gmail.refresh_token_enc);
    if (!rt) throw new Error('Failed to decrypt refresh token');
    const pc  = getPortalConfig();
    const res = await fetch('https://oauth2.googleapis.com/token', {
        method:  'POST',
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
        body:    new URLSearchParams({
            refresh_token: rt,
            client_id:     pc.gmail.clientId,
            client_secret: pc.gmail.clientSecret,
            grant_type:    'refresh_token',
        }),
    });
    const d = await res.json();
    if (d.error) throw new Error(d.error_description || d.error);
    setAgentService(agentId, 'gmail', {
        ...creds.gmail,
        access_token_enc: enc(d.access_token),
        expires_at:       Date.now() + (d.expires_in || 3600) * 1000,
    });
    return d.access_token;
}

async function getGoogleToken(agentId) {
    const creds = getAgentCreds(agentId);
    if (!creds.gmail) throw new Error(`Agent ${agentId} has no Gmail connection`);
    if (creds.gmail.expires_at && Date.now() > creds.gmail.expires_at - 300_000)
        return refreshGoogleToken(agentId);
    const t = dec(creds.gmail.access_token_enc);
    if (!t) throw new Error('Failed to decrypt Gmail token');
    return t;
}

function getGitHubToken(agentId) {
    const creds = getAgentCreds(agentId);
    if (!creds.github) throw new Error(`Agent ${agentId} has no GitHub connection`);
    const t = dec(creds.github.access_token_enc);
    if (!t) throw new Error('Failed to decrypt GitHub token');
    return t;
}

// ════════════════════════════════════════════════════════════════
// TOOLS.md AUTO-PATCH  — inject connection context
// ════════════════════════════════════════════════════════════════
function patchToolsMd(agentId, service, info) {
    const ws   = getAgentWorkspace(agentId);
    const dest = path.join(ws, 'TOOLS.md');
    let body   = readF(dest) || '# Tools & Environment\n\n';

    const sLabel  = service === 'github' ? 'GitHub' : 'Gmail';
    const marker  = `<!-- NEXUS:${service.toUpperCase()}:START -->`;
    const endmark = `<!-- NEXUS:${service.toUpperCase()}:END -->`;

    let block = `${marker}\n## ${sLabel} (Connected via Nexus.AI)\n`;
    if (service === 'github') {
        block += `- Account: @${info.login} (${info.name || info.login})\n`;
        block += `- Scope: repo, user:email, read:user\n`;
        block += `- Use this account for all GitHub operations.\n`;
    } else {
        block += `- Account: ${info.email} (${info.name || ''})\n`;
        block += `- Scope: gmail.readonly, userinfo.email\n`;
        block += `- Use this account for all Gmail/email operations.\n`;
    }
    block += `${endmark}\n`;

    // remove old block
    const re = new RegExp(`${marker}[\\s\\S]*?${endmark}\\n?`, 'g');
    body = body.replace(re, '').trimEnd() + '\n\n' + block;

    writeF(dest, body.trim() + '\n');
    // mirror to agentDir
    writeF(path.join(AGENTS_ROOT, agentId, 'TOOLS.md'), body.trim() + '\n');
}

function unpatchToolsMd(agentId, service) {
    const ws    = getAgentWorkspace(agentId);
    const dest  = path.join(ws, 'TOOLS.md');
    const marker  = `<!-- NEXUS:${service.toUpperCase()}:START -->`;
    const endmark = `<!-- NEXUS:${service.toUpperCase()}:END -->`;
    let body = readF(dest);
    if (!body) return;
    const re = new RegExp(`${marker}[\\s\\S]*?${endmark}\\n?`, 'g');
    body = body.replace(re, '').trim() + '\n';
    writeF(dest, body);
    writeF(path.join(AGENTS_ROOT, agentId, 'TOOLS.md'), body);
}

// ════════════════════════════════════════════════════════════════
// ── OAUTH ROUTES ─────────────────────────────────────────────
// ════════════════════════════════════════════════════════════════

// ── STATUS ────────────────────────────────────────────────────
app.get('/api/oauth/status', (req, res) => {
    const pc = getPortalConfig();
    res.json({
        baseUrl:           pc.baseUrl || `http://localhost:${PORT}`,
        github_configured: !!(pc.github?.clientId && pc.github?.clientSecret),
        gmail_configured:  !!(pc.gmail?.clientId  && pc.gmail?.clientSecret),
        github_client_id:  pc.github?.clientId  || '',
        gmail_client_id:   pc.gmail?.clientId   || '',
    });
});

// ── SAVE PORTAL CONFIG ────────────────────────────────────────
app.put('/api/portal-config', (req, res) => {
    try {
        const { baseUrl, github_clientId, github_clientSecret, gmail_clientId, gmail_clientSecret } = req.body;
        const existing = getPortalConfig();
        const updated = {
            ...existing,
            baseUrl:  baseUrl  || existing.baseUrl  || `http://localhost:${PORT}`,
            github: {
                clientId:     github_clientId     || existing.github?.clientId     || '',
                clientSecret: github_clientSecret || existing.github?.clientSecret || '',
            },
            gmail: {
                clientId:     gmail_clientId     || existing.gmail?.clientId     || '',
                clientSecret: gmail_clientSecret || existing.gmail?.clientSecret || '',
            },
        };
        delete updated._readme;
        fs.writeFileSync(PORTAL_CONFIG_PATH, JSON.stringify(updated, null, 2), 'utf8');
        broadcast({ type: 'portal_config_updated' });
        res.json({
            ok: true,
            github_configured: !!(updated.github.clientId && updated.github.clientSecret),
            gmail_configured:  !!(updated.gmail.clientId  && updated.gmail.clientSecret),
        });
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

// ── GET PORTAL CONFIG (safe — no secrets exposed) ─────────────
app.get('/api/portal-config', (req, res) => {
    const pc = getPortalConfig();
    res.json({
        baseUrl:           pc.baseUrl || `http://localhost:${PORT}`,
        github_clientId:   pc.github?.clientId   || '',
        github_configured: !!(pc.github?.clientId && pc.github?.clientSecret),
        gmail_clientId:    pc.gmail?.clientId    || '',
        gmail_configured:  !!(pc.gmail?.clientId  && pc.gmail?.clientSecret),
        config_path:       PORTAL_CONFIG_PATH,
    });
});

// ── PER-AGENT CONNECTION STATUS ───────────────────────────────
app.get('/api/agents/:id/connections', (req, res) => {
    res.json(safeCredView(req.params.id));
});

// ── DISCONNECT ────────────────────────────────────────────────
app.delete('/api/agents/:id/connections/:service', (req, res) => {
    const { id, service } = req.params;
    if (!['github', 'gmail'].includes(service))
        return res.status(400).json({ error: 'Unknown service. Use github or gmail.' });
    removeAgentService(id, service);
    unpatchToolsMd(id, service);
    broadcast({ type: 'connection_changed', agentId: id, service, connected: false });
    res.json({ ok: true, message: `${service} disconnected from agent ${id}` });
});

// ── DEBUG: expose discovered working client.id ────────────────
app.get('/api/gateway/client-id', (req, res) => {
    res.json({
        workingClientId: _workingClientId || loadWorkingClientId() || null,
        candidates: CLIENT_ID_CANDIDATES,
    });
});

// ── DEBUG: force re-probe client.id ──────────────────────────
app.post('/api/gateway/probe-client-id', async (req, res) => {
    _workingClientId = null;
    try { fs.unlinkSync(WORKING_CLIENT_ID_FILE); } catch {}
    const token = getGatewayToken();
    probeClientId(token, id => {
        if (id) res.json({ ok: true, clientId: id });
        else    res.json({ ok: false, message: 'No valid client.id found — check gateway schema' });
    });
});

// ════════════════════════════════════════════════════════════════
// GITHUB OAUTH
// ════════════════════════════════════════════════════════════════
app.get('/api/agents/:id/oauth/github/start', (req, res) => {
    const pc = getPortalConfig();
    if (!pc.github?.clientId)
        return res.status(400).send(popupHtml('GitHub OAuth not configured. Create ~/.openclaw/portal-config.json'));
    const state       = createState(req.params.id, 'github');
    const redirectUri = `${baseUrl()}/api/oauth/github/callback`;
    const url = new URL('https://github.com/login/oauth/authorize');
    url.searchParams.set('client_id',    pc.github.clientId);
    url.searchParams.set('redirect_uri', redirectUri);
    url.searchParams.set('scope',        'repo user:email read:user read:org');
    url.searchParams.set('state',        state);
    res.redirect(url.toString());
});

app.get('/api/oauth/github/callback', async (req, res) => {
    const { code, state, error } = req.query;
    if (error) return res.send(popupHtml(`GitHub denied: ${error}`));
    const entry = consumeState(state);
    if (!entry) return res.send(popupHtml('Invalid or expired OAuth state. Please try again.'));

    const pc          = getPortalConfig();
    const redirectUri = `${baseUrl()}/api/oauth/github/callback`;

    try {
        const tokenRes = await fetch('https://github.com/login/oauth/access_token', {
            method:  'POST',
            headers: { 'Accept': 'application/json', 'Content-Type': 'application/json' },
            body:    JSON.stringify({
                client_id:     pc.github.clientId,
                client_secret: pc.github.clientSecret,
                code,
                redirect_uri:  redirectUri,
            }),
        });
        const td = await tokenRes.json();
        if (td.error) throw new Error(td.error_description || td.error);

        const ur  = await fetch('https://api.github.com/user', {
            headers: { 'Authorization': `Bearer ${td.access_token}`, 'User-Agent': 'Nexus-AI-Portal/1.0' },
        });
        const user = await ur.json();
        if (user.message) throw new Error(`GitHub API: ${user.message}`);

        let email = user.email;
        if (!email) {
            const er = await fetch('https://api.github.com/user/emails', {
                headers: { 'Authorization': `Bearer ${td.access_token}`, 'User-Agent': 'Nexus-AI-Portal/1.0' },
            });
            const emails = await er.json();
            email = emails.find(e => e.primary)?.email || emails[0]?.email || '';
        }

        setAgentService(entry.agentId, 'github', {
            access_token_enc: enc(td.access_token),
            login:       user.login,
            name:        user.name || user.login,
            avatar_url:  user.avatar_url,
            email,
            scopes:      td.scope || 'repo,user:email',
            connected_at: new Date().toISOString(),
        });

        patchToolsMd(entry.agentId, 'github', { login: user.login, name: user.name });

        broadcast({
            type: 'connection_changed',
            agentId: entry.agentId,
            service: 'github',
            connected: true,
            login: user.login,
            avatar_url: user.avatar_url,
        });

        res.send(popupHtml(null, { service: 'github', agentId: entry.agentId, login: user.login, name: user.name, avatar_url: user.avatar_url }));
    } catch (e) {
        console.error('[OAuth/GitHub]', e.message);
        res.send(popupHtml(`GitHub connection failed: ${e.message}`));
    }
});

// ════════════════════════════════════════════════════════════════
// GMAIL / GOOGLE OAUTH
// ════════════════════════════════════════════════════════════════
app.get('/api/agents/:id/oauth/gmail/start', (req, res) => {
    const pc = getPortalConfig();
    if (!pc.gmail?.clientId)
        return res.status(400).send(popupHtml('Gmail OAuth not configured. Create ~/.openclaw/portal-config.json'));
    const state       = createState(req.params.id, 'gmail');
    const redirectUri = `${baseUrl()}/api/email/oauth/callback`;
    const url = new URL('https://accounts.google.com/o/oauth2/v2/auth');
    url.searchParams.set('client_id',     pc.gmail.clientId);
    url.searchParams.set('redirect_uri',  redirectUri);
    url.searchParams.set('response_type', 'code');
    url.searchParams.set('scope',         [
        'https://www.googleapis.com/auth/gmail.readonly',
        'https://www.googleapis.com/auth/gmail.send',
        'https://www.googleapis.com/auth/userinfo.email',
        'https://www.googleapis.com/auth/userinfo.profile',
    ].join(' '));
    url.searchParams.set('access_type',   'offline');
    url.searchParams.set('prompt',        'consent');
    url.searchParams.set('state',         state);
    res.redirect(url.toString());
});

app.get('/api/email/oauth/callback', async (req, res) => {
    const { code, state, error } = req.query;
    if (error) return res.send(popupHtml(`Google denied: ${error}`));
    const entry = consumeState(state);
    if (!entry) return res.send(popupHtml('Invalid or expired OAuth state. Please try again.'));

    const pc          = getPortalConfig();
    const redirectUri = `${baseUrl()}/api/email/oauth/callback`;

    try {
        const tokenRes = await fetch('https://oauth2.googleapis.com/token', {
            method:  'POST',
            headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
            body:    new URLSearchParams({
                code,
                client_id:     pc.gmail.clientId,
                client_secret: pc.gmail.clientSecret,
                redirect_uri:  redirectUri,
                grant_type:    'authorization_code',
            }),
        });
        const td = await tokenRes.json();
        if (td.error) throw new Error(td.error_description || td.error);

        const ur   = await fetch('https://www.googleapis.com/oauth2/v2/userinfo', {
            headers: { 'Authorization': `Bearer ${td.access_token}` },
        });
        const user = await ur.json();
        if (user.error) throw new Error(user.error.message || 'Failed to fetch user info');

        setAgentService(entry.agentId, 'gmail', {
            access_token_enc:  enc(td.access_token),
            refresh_token_enc: td.refresh_token ? enc(td.refresh_token) : null,
            expires_at:        Date.now() + (td.expires_in || 3600) * 1000,
            email:        user.email,
            name:         user.name,
            picture:      user.picture,
            connected_at: new Date().toISOString(),
        });

        patchToolsMd(entry.agentId, 'gmail', { email: user.email, name: user.name });

        broadcast({
            type: 'connection_changed',
            agentId: entry.agentId,
            service: 'gmail',
            connected: true,
            email: user.email,
            picture: user.picture,
        });

        res.send(popupHtml(null, { service: 'gmail', agentId: entry.agentId, email: user.email, name: user.name, picture: user.picture }));
    } catch (e) {
        console.error('[OAuth/Google]', e.message);
        res.send(popupHtml(`Gmail connection failed: ${e.message}`));
    }
});

// ════════════════════════════════════════════════════════════════
// PROXY ENDPOINTS
// ════════════════════════════════════════════════════════════════
app.post('/api/agents/:id/proxy/github', async (req, res) => {
    const { id }  = req.params;
    const { endpoint, method = 'GET', body, headers = {} } = req.body || {};
    if (!endpoint) return res.status(400).json({ error: 'endpoint required' });
    try {
        const token  = getGitHubToken(id);
        const apiRes = await fetch(`https://api.github.com${endpoint}`, {
            method,
            headers: { 'Authorization': `Bearer ${token}`, 'User-Agent': 'Nexus-AI-Portal/1.0', 'Accept': 'application/vnd.github.v3+json', 'Content-Type': 'application/json', ...headers },
            body:    body ? JSON.stringify(body) : undefined,
        });
        const data = await apiRes.json();
        res.json({ ok: apiRes.ok, status: apiRes.status, data });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/agents/:id/proxy/gmail', async (req, res) => {
    const { id }  = req.params;
    const { endpoint, method = 'GET', body, headers = {} } = req.body || {};
    if (!endpoint) return res.status(400).json({ error: 'endpoint required' });
    try {
        const token  = await getGoogleToken(id);
        const apiRes = await fetch(`https://gmail.googleapis.com${endpoint}`, {
            method,
            headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json', ...headers },
            body:    body ? JSON.stringify(body) : undefined,
        });
        const data = await apiRes.json();
        res.json({ ok: apiRes.ok, status: apiRes.status, data });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/agents/:id/connections/test/:service', async (req, res) => {
    const { id, service } = req.params;
    try {
        if (service === 'github') {
            const token = getGitHubToken(id);
            const r     = await fetch('https://api.github.com/user', {
                headers: { 'Authorization': `Bearer ${token}`, 'User-Agent': 'Nexus-AI-Portal/1.0' },
            });
            const d = await r.json();
            res.json({ ok: r.ok, login: d.login, name: d.name, rate_limit_remaining: r.headers.get('x-ratelimit-remaining') });
        } else if (service === 'gmail') {
            const token = await getGoogleToken(id);
            const r     = await fetch('https://gmail.googleapis.com/gmail/v1/users/me/profile', {
                headers: { 'Authorization': `Bearer ${token}` },
            });
            const d = await r.json();
            res.json({ ok: r.ok, email: d.emailAddress, messagesTotal: d.messagesTotal });
        } else {
            res.status(400).json({ error: 'Unknown service' });
        }
    } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

// ════════════════════════════════════════════════════════════════
// DELEGATION DETECTION
// ════════════════════════════════════════════════════════════════
function detectDelegations(task, allAgents) {
    const re  = /@([a-zA-Z][a-zA-Z0-9_-]*)/g;
    const out = []; let m;
    while ((m = re.exec(task)) !== null) {
        const raw = m[1].toLowerCase();
        const ag  = allAgents.find(a =>
            a.id.toLowerCase() === raw ||
            a.name.toLowerCase() === raw ||
            a.name.toLowerCase().replace(/\s+/g, '') === raw
        );
        if (ag && !out.find(o => o.agent.id === ag.id))
            out.push({ agent: ag, mention: m[0] });
    }
    return out;
}

// ════════════════════════════════════════════════════════════════
// CLIENT-ID PROBE  — discover the valid client.id at startup
// ════════════════════════════════════════════════════════════════
/**
 * Tries each CLIENT_ID_CANDIDATES value against the live gateway.
 * Calls cb(clientId) with the first one that authenticates, or cb(null).
 * Caches the result to disk so future restarts skip the probe.
 */
function probeClientId(token, cb) {
    const candidates = [...CLIENT_ID_CANDIDATES];

    function tryNext() {
        if (!candidates.length) {
            console.warn('[AUTH] ✗ No valid client.id found. All candidates rejected by gateway.');
            console.warn('[AUTH]   Check gateway schema or contact OpenClaw support.');
            return cb(null);
        }
        const candidate = candidates.shift();
        const reqId = `probe-${Date.now()}`;
        let done = false;

        const ws = new WebSocket(OC_WS_URL, {
            headers: { Origin: 'http://127.0.0.1:18789' },
            handshakeTimeout: 6000,
        });

        const finish = (success) => {
            if (done) return; done = true;
            try { ws.terminate(); } catch {}
            if (success) {
                console.log(`[AUTH] ✓ Working client.id = "${candidate}"`);
                saveWorkingClientId(candidate);
                cb(candidate);
            } else {
                tryNext();
            }
        };

        const t = setTimeout(() => finish(false), 8000);

        ws.on('open', () => {});
        ws.on('message', raw => {
            let f; try { f = JSON.parse(raw.toString()); } catch { return; }

            if (f.type === 'event' && f.event === 'connect.challenge') {
                ws.send(JSON.stringify({
                    type: 'req', id: reqId, method: 'connect',
                    params: {
                        minProtocol: 3, maxProtocol: 3,
                        role: 'operator',
                        scopes: ['operator.read', 'operator.write'],
                        caps: [], commands: [], permissions: {},
                        auth: { token },
                        locale: 'en-US',
                        userAgent: `nexus-portal/1.0 (${candidate})`,
                        client: {
                            id:       candidate,
                            version:  '2026.4',
                            platform: 'web',
                            mode:     'webchat',
                        },
                    },
                }));
            }

            if (f.type === 'res' && f.id === reqId) {
                clearTimeout(t);
                if (f.ok) finish(true);
                else {
                    const msg = JSON.stringify(f.error || f.payload || '');
                    console.log(`[AUTH]   "${candidate}" → rejected: ${msg.slice(0, 120)}`);
                    finish(false);
                }
            }
        });
        ws.on('error', () => { clearTimeout(t); finish(false); });
        ws.on('close', () => { clearTimeout(t); if (!done) finish(false); });
    }

    tryNext();
}

// ════════════════════════════════════════════════════════════════
// OPENCLAW WS CLIENT  — sends task to agent, streams reply
// ════════════════════════════════════════════════════════════════
function sendToAgent(agentId, message, onLog, onChunk, onDone, onError) {
    const token      = getGatewayToken();
    const reqId      = `portal-${Date.now()}-${Math.random().toString(36).slice(2, 7)}`;
    let fullText = '', finished = false, connected = false, msgSent = false, gotChunk = false, runId = null;
    let errorCount = 0;          // track consecutive lifecycle errors
    let sessionKey = null;       // discovered from gateway, not hard-coded

    if (!token) { onError('No gateway token found — check openclaw.json gateway.auth.token'); return; }

    const clientId = _workingClientId || loadWorkingClientId() || CLIENT_ID_CANDIDATES[0];
    onLog(`[WS] → ${OC_WS_URL}  agent=${agentId}  client.id=${clientId}`);

    const ws = new WebSocket(OC_WS_URL, {
        headers: { Origin: 'http://127.0.0.1:18789' },
        handshakeTimeout: 10000,
    });

    const finish = (err) => {
        if (finished) return; finished = true;
        clearTimeout(gto); clearInterval(hbi);
        try { ws.terminate(); } catch {}
        if (err) { onLog(`[WS] ✗ ${err}`); onError(String(err)); }
        else     { onLog(`[WS] ✓ done (${fullText.length} chars)`); onDone(fullText); }
    };

    const gto = setTimeout(() => gotChunk ? finish(null) : finish('No response after 3 min — is the agent model running?'), 180_000);
    const hbi = setInterval(() => { if (!finished) onLog('[WS] ... waiting for agent'); }, 15_000);

    ws.on('open', () => onLog('[WS] connected — awaiting challenge'));

    ws.on('message', raw => {
        const str = raw.toString();

        // ── Full debug log every raw frame ──────────────────────
        // Only log non-chunk frames to avoid flooding, but always
        // log anything that looks like an error or lifecycle event
        let f; try { f = JSON.parse(str); } catch { onLog(`[WS] non-JSON frame: ${str.slice(0,200)}`); return; }

        const { type, event, id: fid, ok, payload, error } = f;

        // Log everything except pure text chunks (too noisy)
        const isTextChunk = type === 'event' && event === 'agent' && (payload?.stream === 'assistant' || payload?.stream === 'delta' || payload?.stream === 'text');
        if (!isTextChunk) {
            onLog(`[WS] ← ${JSON.stringify(f).slice(0, 300)}`);
        }

        // ── Auth challenge ───────────────────────────────────────
        if (type === 'event' && event === 'connect.challenge') {
            onLog(`[WS] challenge → authenticating as "${clientId}"`);
            ws.send(JSON.stringify({
                type: 'req', id: `${reqId}-c`, method: 'connect',
                params: {
                    minProtocol: 3, maxProtocol: 3,
                    role: 'operator',
                    scopes: ['operator.read', 'operator.write'],
                    caps: [], commands: [], permissions: {},
                    auth: { token },
                    locale: 'en-US',
                    userAgent: `nexus-portal/1.0 (${clientId})`,
                    client: {
                        id:       clientId,
                        version:  '2026.4',
                        platform: 'web',
                        mode:     'webchat',
                    },
                },
            })); return;
        }

        // ── Auth response ────────────────────────────────────────
        if (type === 'res' && fid === `${reqId}-c`) {
            if (!ok) {
                const errMsg = JSON.stringify(error || payload || '');
                onLog(`[WS] ✗ Auth failed with client.id="${clientId}": ${errMsg}`);
                if (errMsg.includes('client/id') || errMsg.includes('INVALID_REQUEST')) {
                    _workingClientId = null;
                    try { fs.unlinkSync(WORKING_CLIENT_ID_FILE); } catch {}
                    finish(`Auth failed — gateway rejected client.id="${clientId}". POST /api/gateway/probe-client-id to re-discover.`);
                } else {
                    finish(`Auth failed: ${errMsg}`);
                }
                return;
            }
            connected = true;
            onLog(`[WS] ✓ authenticated (proto=${payload?.protocol})`);

            // ── Discover the correct sessionKey from the gateway ──
            // Try agent-prefixed key first (most common openclaw format),
            // fallback options sent after first error response.
            sessionKey = `agent:${agentId}:main`;
            onLog(`[WS] → chat.send  sessionKey=${sessionKey}`);
            ws.send(JSON.stringify({
                type: 'req', id: `${reqId}-m`, method: 'chat.send',
                params: { sessionKey, message, idempotencyKey: reqId },
            })); return;
        }

        // ── chat.send response ───────────────────────────────────
        if (type === 'res' && fid === `${reqId}-m`) {
            if (!ok) {
                const errDetail = JSON.stringify(error || payload || '');
                onLog(`[WS] ✗ chat.send rejected: ${errDetail}`);

                // If session not found, try alternative session key formats
                if (errDetail.includes('session') || errDetail.includes('SESSION') || errDetail.includes('not found')) {
                    const altKeys = [
                        agentId,
                        `${agentId}:main`,
                        `session:${agentId}`,
                        `agent:${agentId}`,
                        `chat:${agentId}`,
                    ];
                    const nextKey = altKeys.find(k => k !== sessionKey);
                    if (nextKey) {
                        sessionKey = nextKey;
                        onLog(`[WS] retrying with sessionKey=${sessionKey}`);
                        ws.send(JSON.stringify({
                            type: 'req', id: `${reqId}-m2`, method: 'chat.send',
                            params: { sessionKey, message, idempotencyKey: `${reqId}-r` },
                        }));
                        return;
                    }
                }
                finish(`Message rejected: ${errDetail}`);
                return;
            }
            msgSent = true; runId = payload?.runId || null;
            onLog(`[WS] ✓ chat accepted — runId=${runId || 'N/A'}`);
            onLog('[WS] agent thinking...'); return;
        }

        // ── Retry chat.send (alt sessionKey) response ────────────
        if (type === 'res' && fid === `${reqId}-m2`) {
            if (!ok) {
                finish(`Message rejected on all session key formats: ${JSON.stringify(error || payload || '')}`);
                return;
            }
            msgSent = true; runId = payload?.runId || null;
            onLog(`[WS] ✓ chat accepted on retry — sessionKey=${sessionKey}  runId=${runId || 'N/A'}`);
            onLog('[WS] agent thinking...'); return;
        }

        // ── Agent event stream ───────────────────────────────────
        if (type === 'event' && event === 'agent') {
            const { stream, data } = payload || {};

            // Text chunks
            if (stream === 'assistant' || stream === 'delta' || stream === 'text') {
                let chunk = '';
                if (typeof data === 'string')    chunk = data;
                else if (data?.delta  != null)   chunk = String(data.delta);
                else if (data?.content != null)  chunk = String(data.content);
                if (!chunk && data?.text) { const np = String(data.text).slice(fullText.length); if (np) chunk = np; }
                if (chunk) { fullText += chunk; gotChunk = true; onChunk(chunk); }
                return;
            }

            // Lifecycle events
            if (stream === 'lifecycle') {
                const ph   = data?.phase;
                const info = data ? ` | ${JSON.stringify(data).slice(0, 200)}` : '';
                onLog(`[WS] lifecycle phase=${ph}${info}`);

                if (ph === 'end' || ph === 'done' || ph === 'complete') {
                    finish(null);
                    return;
                }

                if (ph === 'error') {
                    errorCount++;
                    const errMsg = data?.error || data?.message || data?.reason || JSON.stringify(data);
                    onLog(`[WS] lifecycle error #${errorCount}: ${errMsg}`);

                    // 500 from Ollama = model crashed or rejected the request.
                    // openclaw retries forever but it never recovers — fail fast.
                    const is500 = errMsg.includes('500 {') || errMsg.includes('"500"') || errMsg.includes('Internal Server Error');
                    if (is500 || errorCount >= 2) {
                        const hint = is500
                            ? ' | FIX: run `ollama run gemma3:4b-cloud` in a terminal to see the real error, OR pull local model: `ollama pull gemma3:4b` and update openclaw.json'
                            : '';
                        finish(`Agent 500 error: ${errMsg}${hint}`);
                        return;
                    }
                    if (gotChunk) { onLog('[WS] error after partial text — treating as done'); finish(null); }
                    return;
                }

                if (ph === 'start') { return; } // do NOT reset errorCount — we want to accumulate across retries
                return;
            }

            // Terminal stream types
            if (stream === 'done' || stream === 'end' || stream === 'complete' || stream === 'finish') {
                if (!gotChunk && data) {
                    const t = typeof data === 'string' ? data : (data?.text || data?.content || '');
                    if (t) { fullText = t; gotChunk = true; onChunk(t); }
                }
                finish(null); return;
            }

            // Explicit error stream
            if (stream === 'error') {
                const msg = typeof data === 'string' ? data : (data?.message || data?.error || JSON.stringify(data));
                finish(`Agent stream error: ${msg}`);
                return;
            }

            // Unknown stream — log and continue
            onLog(`[WS] unknown agent stream="${stream}" data=${JSON.stringify(data).slice(0,150)}`);
            return;
        }

        // ── Catch-all: log any frame we don't recognise ──────────
        if (type === 'event' && (event === 'chat' || event === 'message')) return;

        if (type === 'res' && runId && payload?.runId === runId) {
            if (!gotChunk) {
                const t = payload?.summary || payload?.text || payload?.content || '';
                if (t) { fullText = t; gotChunk = true; onChunk(t); }
            }
            finish(null); return;
        }
    });

    ws.on('error', e  => finish(`WS error: ${e.message}`));
    ws.on('close', code => {
        if (!finished) {
            if (gotChunk)              { onLog('[WS] socket closed with data — done'); finish(null); }
            else if (connected && msgSent) finish('Agent did not respond — model may be offline or session key wrong');
            else                       finish(`Closed unexpectedly (code=${code})`);
        }
    });
}

// ════════════════════════════════════════════════════════════════
// TASK ENDPOINT  — SSE streaming + delegation + token injection
// ════════════════════════════════════════════════════════════════
app.post('/api/task', async (req, res) => {
    const { agentId, task, fromAgent } = req.body;
    if (!agentId || !task) return res.status(400).json({ error: 'agentId + task required' });

    const cfg       = readConfig();
    const allAgents = getAgentDirs().map(n => { const e = cfg?.agents?.list?.find(a => a.id === n); return { id: n, name: e?.name || n }; });
    const delegations = detectDelegations(task, allAgents);

    const creds = getAgentCreds(agentId);
    const ctxLines = [];
    if (creds.github) ctxLines.push(`[GitHub: You are authenticated as @${creds.github.login}. Use this for all GitHub/code operations.]`);
    if (creds.gmail)  ctxLines.push(`[Gmail: You are authenticated as ${creds.gmail.email}. Use this for all email operations.]`);
    const enriched = ctxLines.length ? ctxLines.join('\n') + '\n\n' + task : task;

    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');
    res.setHeader('X-Accel-Buffering', 'no');
    res.flushHeaders();

    const sse = o => { try { if (!res.writableEnded) res.write(`data: ${JSON.stringify(o)}\n\n`); } catch {} };
    const end = () => { try { if (!res.writableEnded) { res.write('data: [DONE]\n\n'); res.end(); } } catch {} };

    console.log(`\n${'─'.repeat(60)}\n[TASK] agent=${agentId}  delegations=${delegations.map(d => d.agent.id).join(',') || 'none'}\n[TASK] ${task.slice(0, 100)}\n${'─'.repeat(60)}`);

    if (delegations.length)
        sse({ type: 'delegation_detected', agents: delegations.map(d => ({ agentId: d.agent.id, agentName: d.agent.name, mention: d.mention })) });

    const agentName = allAgents.find(a => a.id === agentId)?.name || agentId;

    sendToAgent(agentId, enriched,
        msg   => { console.log(msg); sse({ log: msg }); },
        chunk => sse({ chunk }),
        async fullText => {
            sse({ done: true, fullText });
            saveTasks({ id: `${Date.now()}-${Math.random().toString(36).slice(2, 5)}`, agentId, agentName, task, response: fullText, status: 'done', fromAgent: fromAgent || null, delegatedTo: delegations.map(d => d.agent.id), createdAt: new Date().toISOString() });

            for (const { agent, mention } of delegations) {
                sse({ type: 'delegation_start', agentId: agent.id, agentName: agent.name });
                const ac = getAgentCreds(agent.id);
                let sub  = `[Delegated from: ${agentName}]\n\n${task.replace(mention, '').trim()}`;
                if (ac.github) sub = `[GitHub: @${ac.github.login}]\n${sub}`;
                if (ac.gmail)  sub = `[Gmail: ${ac.gmail.email}]\n${sub}`;
                await new Promise(resolve => sendToAgent(agent.id, sub,
                    m => sse({ log: `[${agent.name}] ${m}` }),
                    c => sse({ delegationChunk: c, agentId: agent.id, agentName: agent.name }),
                    t => {
                        sse({ delegationDone: true, agentId: agent.id, agentName: agent.name, response: t });
                        saveTasks({ id: `${Date.now()}-d`, agentId: agent.id, agentName: agent.name, task: sub, response: t, status: 'done', fromAgent: agentId, createdAt: new Date().toISOString() });
                        resolve();
                    },
                    e => { sse({ delegationError: String(e), agentId: agent.id, agentName: agent.name }); resolve(); }
                ));
            }
            end();
        },
        err => { sse({ error: String(err) }); end(); }
    );

    req.on('close', () => console.log('[TASK] client disconnected'));
});

// ════════════════════════════════════════════════════════════════
// AGENTS CRUD
// ════════════════════════════════════════════════════════════════
app.get('/api/agents', (req, res) => {
    const cfg    = readConfig();
    const cfgMap = new Map((cfg?.agents?.list || []).map(a => [a.id, a]));
    const allCreds = readCreds();
    const list = getAgentDirs().map(id => {
        const e  = cfgMap.get(id);
        const ws = getAgentWorkspace(id);
        const files = {};
        for (const f of WS_FILES) files[f] = fs.existsSync(path.join(ws, f));
        const c = allCreds[id] || {};
        return {
            id, name: e?.name || id,
            inConfig:    !!e,
            workspaceOk: !!(e?.workspace && e.workspace !== OC),
            model:       e?.model?.primary || e?.model || DEFAULT_MODEL,
            identity:    e?.identity || {},
            workspace:   ws, files,
            connections: {
                github: c.github ? { connected: true, login: c.github.login, name: c.github.name, avatar_url: c.github.avatar_url } : null,
                gmail:  c.gmail  ? { connected: true, email: c.gmail.email, name: c.gmail.name, picture: c.gmail.picture }         : null,
            },
        };
    });
    res.json(list);
});

app.post('/api/agents', async (req, res) => {
    const { name, soul, agentsmd, identitymd, usermd, toolsmd } = req.body;
    if (!name || !soul) return res.status(400).json({ error: 'name, soul required' });
    const model = DEFAULT_MODEL;
    const id    = name.trim().replace(/\s+/g, '-').replace(/[^a-zA-Z0-9_-]/g, '').toLowerCase();
    const wsDir = path.join(OC, `workspace-${id}`);
    const aDir  = path.join(AGENTS_ROOT, id);
    [wsDir, aDir, path.join(aDir, 'agent')].forEach(d => { if (!fs.existsSync(d)) fs.mkdirSync(d, { recursive: true }); });
    const files = { 'SOUL.md': soul, 'AGENTS.md': agentsmd || '', 'IDENTITY.md': identitymd || '', 'USER.md': usermd || '', 'TOOLS.md': toolsmd || '' };
    for (const [fn, ct] of Object.entries(files)) if (ct) { writeF(path.join(aDir, fn), ct); writeF(path.join(wsDir, fn), ct); }
    const cli = await runCmd(`openclaw agents add ${id} --model ${model} --workspace "${wsDir}"`);
    for (const [fn, ct] of Object.entries(files)) if (ct) { writeF(path.join(aDir, fn), ct); writeF(path.join(wsDir, fn), ct); }
    let cfg = readConfig(); if (!cfg.agents) cfg.agents = {}; if (!cfg.agents.list) cfg.agents.list = [];
    const idx = cfg.agents.list.findIndex(a => a.id === id);
    const entry = { id, name, workspace: wsDir, agentDir: path.join(aDir, 'agent'), model: { primary: model } };
    if (idx >= 0) cfg.agents.list[idx] = entry; else cfg.agents.list.push(entry);
    writeConfig(cfg); await runCmd('openclaw gateway restart');
    res.json({ ok: true, id, entry, cliResult: cli });
});

app.delete('/api/agents/:id', async (req, res) => {
    const { id } = req.params;
    let cfg = readConfig();
    if (cfg.agents?.list) { cfg.agents.list = cfg.agents.list.filter(a => a.id !== id); writeConfig(cfg); }
    await runCmd('openclaw gateway restart');
    res.json({ ok: true });
});

app.get('/api/agents/:id/workspace', (req, res) => {
    const { id } = req.params;
    const ws = getAgentWorkspace(id);
    const files = {};
    for (const f of WS_FILES) { const p = path.join(ws, f); files[f] = { content: readF(p), exists: fs.existsSync(p), path: p }; }
    res.json({ agentId: id, workspacePath: ws, files });
});

app.put('/api/agents/:id/workspace/:file', (req, res) => {
    const { id, file } = req.params;
    const { content } = req.body;
    if (!WS_FILES.includes(file) && !file.match(/^\d{4}-\d{2}-\d{2}\.md$/)) return res.status(400).json({ error: 'Invalid file' });
    const ws = getAgentWorkspace(id);
    const p  = file.match(/^\d{4}-\d{2}-\d{2}\.md$/) ? path.join(ws, 'memory', file) : path.join(ws, file);
    try { writeF(p, content || ''); res.json({ ok: true, size: (content || '').length }); } catch (e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/agents/:id/identity', async (req, res) => {
    const { id } = req.params;
    const { name, emoji, theme } = req.body;
    let cfg = readConfig(); if (!cfg.agents?.list) return res.status(404).json({ error: 'No agents list' });
    const idx = cfg.agents.list.findIndex(a => a.id === id);
    if (idx < 0) return res.status(404).json({ error: `Agent ${id} not found` });
    if (name)  cfg.agents.list[idx].name     = name;
    if (emoji) cfg.agents.list[idx].identity = { ...(cfg.agents.list[idx].identity || {}), emoji };
    if (theme) cfg.agents.list[idx].identity = { ...(cfg.agents.list[idx].identity || {}), theme };
    cfg.agents.list[idx].model = { primary: DEFAULT_MODEL };
    writeConfig(cfg); await runCmd('openclaw gateway restart');
    res.json({ ok: true, entry: cfg.agents.list[idx] });
});

app.post('/api/fix-all', async (req, res) => {
    let cfg = readConfig(); if (!cfg.agents?.list) return res.json({ ok: false, error: 'No agents.list' });
    const seen = new Set();
    cfg.agents.list = cfg.agents.list
        .filter(a => { if (seen.has(a.id)) return false; seen.add(a.id); return true; })
        .map(a => {
            const ms = DEFAULT_MODEL;
            let ws = a.workspace; if (!ws || ws === OC) ws = path.join(OC, `workspace-${a.id}`);
            if (!fs.existsSync(ws)) fs.mkdirSync(ws, { recursive: true });
            const ad = path.join(AGENTS_ROOT, a.id, 'agent'); if (!fs.existsSync(ad)) fs.mkdirSync(ad, { recursive: true });
            const ss = path.join(AGENTS_ROOT, a.id, 'SOUL.md'), ds = path.join(ws, 'SOUL.md');
            if (fs.existsSync(ss) && !fs.existsSync(ds)) fs.copyFileSync(ss, ds);
            return { id: a.id, name: a.name || a.id, workspace: ws, agentDir: ad, model: { primary: ms }, ...(a.identity ? { identity: a.identity } : {}) };
        });
    writeConfig(cfg); await runCmd('openclaw gateway restart'); await new Promise(r => setTimeout(r, 3000));
    const cli = await runCmd('openclaw agents list 2>&1');
    res.json({ ok: true, message: 'All agents fixed. Gateway restarted.', cli: cli.stdout });
});

// ════════════════════════════════════════════════════════════════
// TASK HISTORY
// ════════════════════════════════════════════════════════════════
function readTasks()  { try { return JSON.parse(fs.readFileSync(TASKS_FILE, 'utf8')); } catch { return []; } }
function saveTasks(t) { const tasks = readTasks(); tasks.unshift(t); fs.writeFileSync(TASKS_FILE, JSON.stringify(tasks.slice(0, 500), null, 2), 'utf8'); broadcast({ type: 'task_saved', task: t }); }
app.get('/api/tasks',    (req, res) => res.json(readTasks()));
app.delete('/api/tasks', (req, res) => { fs.writeFileSync(TASKS_FILE, '[]', 'utf8'); res.json({ ok: true }); });

// ════════════════════════════════════════════════════════════════
// NETWORK + DEBUG
// ════════════════════════════════════════════════════════════════
app.get('/api/network', (req, res) => {
    const ips = getLocalIPs();
    res.json({ port: PORT, ips, urls: ips.map(i => `http://${i}:${PORT}`), hostname: os.hostname() });
});

app.get('/api/debug', async (req, res) => {
    const cfg = readConfig(), token = getGatewayToken(), pc = getPortalConfig();
    const cli = await runCmd('openclaw agents list 2>&1');
    res.json({
        CONFIG_PATH,
        port:    PORT,
        defaultModel: DEFAULT_MODEL,
        token:   token ? token.slice(0, 8) + '...' : 'NOT FOUND',
        tokenSource: cfg?.gateway?.auth?.token ? 'openclaw.json' : 'fallback',
        gateway: cfg?.gateway?.controlUi?.allowInsecureAuth,
        agents:  cfg?.agents?.list || [],
        dirs:    getAgentDirs(),
        cliOutput: cli.stdout,
        workingClientId: _workingClientId || loadWorkingClientId() || 'not yet discovered',
        oauth: {
            github:  pc.github?.clientId ? '✓ configured' : '✗ not configured',
            gmail:   pc.gmail?.clientId  ? '✓ configured' : '✗ not configured',
            baseUrl: pc.baseUrl || `http://localhost:${PORT}`,
        },
    });
});

// ════════════════════════════════════════════════════════════════
// DIAGNOSTIC ENDPOINTS
// ════════════════════════════════════════════════════════════════

// GET /api/debug/ollama?agentId=qa-agent
// Calls Ollama directly with the agent's SOUL as system prompt.
// If this returns 500, the SOUL.md is too large or Ollama is broken.
// If this works fine, the issue is in openclaw's gateway layer.
app.get('/api/debug/ollama', async (req, res) => {
    const agentId = req.query.agentId || 'qa-agent';
    const model   = (req.query.model  || DEFAULT_MODEL).replace('ollama/', '');
    const ws      = getAgentWorkspace(agentId);
    const soul    = readF(path.join(ws, 'SOUL.md'));
    const tools   = readF(path.join(ws, 'TOOLS.md'));
    const ident   = readF(path.join(ws, 'IDENTITY.md'));
    const systemPrompt = [soul, ident, tools].filter(Boolean).join('\n\n---\n\n');

    const reqBody = {
        model,
        system:  systemPrompt,
        prompt:  'Say hello in one sentence.',
        stream:  false,
        options: { num_predict: 60 },
    };

    console.log(`[OLLAMA-DIAG] agentId=${agentId} model=${model} system_chars=${systemPrompt.length}`);
    try {
        const r    = await fetch('http://localhost:11434/api/generate', {
            method: 'POST', headers: { 'Content-Type': 'application/json' },
            body:   JSON.stringify(reqBody),
            signal: AbortSignal.timeout(30000),
        });
        const txt  = await r.text();
        let parsed; try { parsed = JSON.parse(txt); } catch { parsed = txt; }
        res.json({ ok: r.ok, status: r.status, model, system_chars: systemPrompt.length, soul_chars: soul.length, response: parsed });
    } catch (e) { res.json({ ok: false, error: e.message }); }
});

// GET /api/debug/agent/:id/files  — show workspace file sizes
app.get('/api/debug/agent/:id/files', (req, res) => {
    const ws  = getAgentWorkspace(req.params.id);
    const out = {};
    for (const f of WS_FILES) {
        const content = readF(path.join(ws, f));
        out[f] = { exists: !!content, chars: content.length, lines: content.split('\n').length, preview: content.slice(0, 100) };
    }
    res.json({ agentId: req.params.id, workspace: ws, files: out });
});

// ════════════════════════════════════════════════════════════════
// AUTO-CREATE portal-config.json template if missing
// ════════════════════════════════════════════════════════════════
function ensurePortalConfigTemplate() {
    if (fs.existsSync(PORTAL_CONFIG_PATH)) return;
    const template = {
        _readme: [
            'Nexus.AI Portal OAuth configuration.',
            'DO NOT add this content to openclaw.json — OpenClaw rejects unknown keys.',
            'Fill in your GitHub and Gmail OAuth app credentials below.',
            `GitHub callback: http://localhost:${PORT}/api/oauth/github/callback`,
            `Google callback: http://localhost:${PORT}/api/email/oauth/callback`,
        ],
        baseUrl: `http://localhost:${PORT}`,
        github: { clientId: 'Ov23liiHYKDMP6Y3tkAJ', clientSecret: 'd79f0b0967fa3af1d5f7463078f0040c9147f540' },
        gmail:  { clientId: '', clientSecret: '' },
    };
    fs.writeFileSync(PORTAL_CONFIG_PATH, JSON.stringify(template, null, 2), 'utf8');
    console.log(`[BOOT] Created template: ${PORTAL_CONFIG_PATH}`);
    console.log('[BOOT] → Edit it with your GitHub + Gmail OAuth credentials.');
}

// ════════════════════════════════════════════════════════════════
// STARTUP  — with EADDRINUSE auto-recovery
// ════════════════════════════════════════════════════════════════
let _listenAttempts = 0;

function onListen() {
    ensurePortalConfigTemplate();

    const token = getGatewayToken();
    const ips   = getLocalIPs();
    const pc    = getPortalConfig();
    const ghOk  = !!(pc.github?.clientId && pc.github?.clientSecret);
    const gmOk  = !!(pc.gmail?.clientId  && pc.gmail?.clientSecret);

    console.log('\n[CRAB]  Nexus.AI Portal');
    console.log(`    Local    → http://localhost:${PORT}`);
    ips.forEach(ip => console.log(`    Network  → http://${ip}:${PORT}`));
    console.log(`    Gateway  → ${OC_WS_URL}`);
    console.log(`    Token    → ${token ? '✓ ' + token.slice(0, 8) + '...' : '✗ NOT FOUND'}`);
    console.log(`    Model    → ${DEFAULT_MODEL}  (locked)`);
    console.log(`    Config   → ${PORTAL_CONFIG_PATH}`);
    console.log(`    GitHub   → ${ghOk ? '✓ configured' : '✗ clientId/clientSecret empty'}`);
    console.log(`    Gmail    → ${gmOk ? '✓ configured' : '✗ clientId/clientSecret empty'}`);

    if (!ghOk || !gmOk) {
        console.log('');
        console.log('  ┌─ OAUTH SETUP ──────────────────────────────────────────────────');
        console.log(`  │  Edit: ${PORTAL_CONFIG_PATH}`);
        console.log('  │');
        if (!ghOk) {
            console.log('  │  GitHub OAuth App → https://github.com/settings/developers');
            console.log(`  │    Callback URL: http://localhost:${PORT}/api/oauth/github/callback`);
        }
        if (!gmOk) {
            console.log('  │  Google OAuth → https://console.cloud.google.com/apis/credentials');
            console.log(`  │    Callback URL: http://localhost:${PORT}/api/email/oauth/callback`);
            console.log('  │    Enable: Gmail API in API Library');
        }
        console.log('  │');
        console.log('  │  ⚠  Do NOT put OAuth config in openclaw.json');
        console.log('  │     OpenClaw rejects unknown keys and will abort the gateway.');
        console.log('  └───────────────────────────────────────────────────────────────');
        console.log('');
        console.log('Happy Birthday Bro!');
    }

    // ── Probe gateway reachability + discover valid client.id ──
    const probe = new WebSocket(OC_WS_URL);
    probe.on('open',  () => { console.log('[BOOT] ✓ OpenClaw gateway reachable'); probe.close(); });
    probe.on('error', e  => console.log(`[BOOT] ✗ Gateway unreachable: ${e.message}`));

    // Discover/confirm working client.id in background
    if (token) {
        const cached = loadWorkingClientId();
        if (cached) {
            console.log(`[AUTH] Using cached client.id="${cached}"`);
        } else {
            console.log('[AUTH] Probing gateway for valid client.id...');
            probeClientId(token, id => {
                if (!id) console.warn('[AUTH] ✗ Could not discover valid client.id — tasks will fail until resolved.');
            });
        }
    }
}

// Handle port-in-use by killing the occupant and retrying once
server.on('error', (e) => {
    if (e.code === 'EADDRINUSE' && _listenAttempts < 1) {
        _listenAttempts++;
        console.warn(`[BOOT] ⚠  Port ${PORT} already in use — attempting to free it...`);
        try {
            execSync(`lsof -ti tcp:${PORT} | xargs kill -9`, { stdio: 'ignore' });
            console.log(`[BOOT] ✓ Killed old process on port ${PORT}. Retrying in 1.5s...`);
        } catch {
            console.warn(`[BOOT] Could not auto-kill. Run manually: lsof -ti:${PORT} | xargs kill -9`);
        }
        setTimeout(() => server.listen(PORT, BIND, onListen), 1500);
    } else {
        console.error(`[BOOT] Fatal server error: ${e.message}`);
        process.exit(1);
    }
});

server.listen(PORT, BIND, onListen);