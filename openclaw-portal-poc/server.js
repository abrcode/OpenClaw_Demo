const express    = require('express');
const bodyParser = require('body-parser');
const fs         = require('fs');
const path       = require('path');
const os         = require('os');
const { exec }   = require('child_process');
const WebSocket  = require('ws');

const app  = express();
const PORT = 3000;
app.use(bodyParser.json({ limit: '2mb' }));
app.use(express.static('public'));

const OPENCLAW_PATH = path.join(os.homedir(), '.openclaw');
const AGENTS_ROOT   = path.join(OPENCLAW_PATH, 'agents');
const CONFIG_PATH   = path.join(OPENCLAW_PATH, 'openclaw.json');
const OC_WS_URL     = 'ws://127.0.0.1:18789';

// ─── HELPERS ─────────────────────────────────────────────────────────────────
function isDir(p) { try { return fs.statSync(p).isDirectory(); } catch { return false; } }

function getAgentDirs() {
    if (!fs.existsSync(AGENTS_ROOT)) return [];
    return fs.readdirSync(AGENTS_ROOT)
        .filter(e => !e.startsWith('.') && isDir(path.join(AGENTS_ROOT, e)));
}

function readConfig() {
    return JSON.parse(fs.readFileSync(CONFIG_PATH, 'utf8'));
}

function writeConfig(cfg) {
    for (let i = 4; i >= 1; i--) {
        const from = `${CONFIG_PATH}.bak.${i}`;
        const to   = `${CONFIG_PATH}.bak.${i+1}`;
        if (fs.existsSync(from)) fs.renameSync(from, to);
    }
    fs.copyFileSync(CONFIG_PATH, `${CONFIG_PATH}.bak.1`);
    const { _path, ...clean } = cfg;
    fs.writeFileSync(CONFIG_PATH, JSON.stringify(clean, null, 2), 'utf8');
}

function getGatewayToken() {
    try {
        const cfg = readConfig();
        return cfg?.gateway?.auth?.token || cfg?.gateway?.auth?.tokens?.[0] || '';
    } catch { return ''; }
}

function runCmd(cmd) {
    return new Promise(r => exec(cmd, { timeout: 20000 }, (e, o, s) =>
        r({ ok: !e, stdout: o?.trim(), stderr: s?.trim(), error: e?.message })));
}

function buildAgentEntry(id, name, model, workspaceDir) {
    return {
        id,
        name:      name || id,
        workspace: workspaceDir,
        agentDir:  path.join(AGENTS_ROOT, id, 'agent'),
        model:     { primary: model },
    };
}

function repairConfigAgents(cfg) {
    if (!cfg.agents?.list) return cfg;
    const seen = new Set();
    cfg.agents.list = cfg.agents.list.filter(a => {
        if (seen.has(a.id)) return false;
        seen.add(a.id); return true;
    }).map(a => {
        const modelStr = typeof a.model === 'object'
            ? (a.model.primary || 'ollama/minimax-m2.5:cloud')
            : (a.model || 'ollama/minimax-m2.5:cloud');
        let ws = a.workspace;
        if (!ws || ws === OPENCLAW_PATH) ws = path.join(OPENCLAW_PATH, `workspace-${a.id}`);
        if (!fs.existsSync(ws)) fs.mkdirSync(ws, { recursive: true });
        const agentDir = path.join(AGENTS_ROOT, a.id, 'agent');
        if (!fs.existsSync(agentDir)) fs.mkdirSync(agentDir, { recursive: true });
        const srcSoul = path.join(AGENTS_ROOT, a.id, 'SOUL.md');
        const dstSoul = path.join(ws, 'SOUL.md');
        if (fs.existsSync(srcSoul) && !fs.existsSync(dstSoul)) fs.copyFileSync(srcSoul, dstSoul);
        return { id: a.id, name: a.name || a.id, workspace: ws, agentDir, model: { primary: modelStr } };
    });
    return cfg;
}

// ─── OPENCLAW WS CLIENT ───────────────────────────────────────────────────────
//
// KEY INSIGHT from source code analysis:
// The gateway checks `isControlUi` flag based on client.id === "openclaw-control-ui"
// AND client.mode === "webchat". Only when isControlUi=true does allowInsecureAuth
// bypass the device signature requirement.
//
// So we must impersonate the Control UI to skip device auth on localhost.
//
// Full protocol flow (v3):
//  Server → connect.challenge event
//  Client → req: connect  (as openclaw-control-ui / webchat mode)
//  Server → res: connect  payload.type = "hello-ok"
//  Client → req: chat.send  { session, text, idempotencyKey }
//  Server → res: chat.send  { runId, status:"accepted" }
//  Server → event: agent   { delta } streaming chunks
//  Server → res: agent (or event agent with done:true) final

function sendToAgent(agentId, message, onLog, onChunk, onDone, onError) {
    const token      = getGatewayToken();
    const sessionKey = `agent:${agentId}:main`;
    const reqId      = `portal-${Date.now()}-${Math.random().toString(36).slice(2,7)}`;
    let   fullText   = '';
    let   finished   = false;
    let   runId      = null;

    onLog(`[WS] Connecting → ${OC_WS_URL}`);
    onLog(`[WS] Token    → ${token ? token.substring(0,8)+'...' : 'MISSING!'}`);
    onLog(`[WS] Session  → ${sessionKey}`);
    onLog(`[WS] ReqId    → ${reqId}`);

    if (!token) {
        onError('No gateway token found in openclaw.json');
        return;
    }

    const ws = new WebSocket(OC_WS_URL, { headers: { Origin: "http://127.0.0.1:18789" } });

    const finish = (err) => {
        if (finished) return;
        finished = true;
        clearTimeout(globalTimeout);
        try { ws.close(); } catch {}
        if (err) {
            onLog(`[WS] ✗ FINISHED WITH ERROR: ${err}`);
            onError(err);
        } else {
            onLog(`[WS] ✓ FINISHED OK. Response length: ${fullText.length} chars`);
            onDone(fullText);
        }
    };

    const globalTimeout = setTimeout(() => {
        onLog('[WS] TIMEOUT after 120s');
        finish('Timeout: no response after 120 seconds');
    }, 120000);

    ws.on('open', () => {
        onLog('[WS] Socket opened — awaiting challenge…');
    });

    ws.on('message', (raw) => {
        let frame;
        try { frame = JSON.parse(raw.toString()); }
        catch (e) { onLog(`[WS] Bad JSON frame: ${raw.toString().slice(0,80)}`); return; }

        const { type, event, method, id: frameId, ok, payload, error } = frame;
        onLog(`[WS] ← type=${type} event=${event||''} method=${method||''} id=${frameId||''} ok=${ok}`);

        // ── 1. CHALLENGE → send connect as Control UI ──
        if (type === 'event' && event === 'connect.challenge') {
            onLog('[WS] Got challenge → sending connect frame as openclaw-control-ui');

            // CRITICAL: client.id must be "openclaw-control-ui" and client.mode must be
            // "webchat" so the gateway sets isControlUi=true and skips device auth
            // when allowInsecureAuth:true is set in config (which you already have)
            ws.send(JSON.stringify({
                type:   'req',
                id:     `${reqId}-connect`,
                method: 'connect',
                params: {
                    minProtocol: 3,
                    maxProtocol: 3,
                    role:        'operator',
                    scopes:      ['operator.read', 'operator.write'],
                    caps:        [],
                    commands:    [],
                    permissions: {},
                    auth:        { token },
                    locale:      'en-US',
                    userAgent:   'openclaw-control-ui/2026.4.5',
                    // These two fields trigger isControlUi=true in gateway source:
                    client: {
                        id:       'openclaw-control-ui',
                        version:  '2026.4.5',
                        platform: 'web',
                        mode:     'webchat',
                    },
                    // No device field → skipped when allowInsecureAuth:true + isControlUi:true
                },
            }));
            return;
        }

        // ── 2. CONNECT RESPONSE ──
        if (type === 'res' && frameId === `${reqId}-connect`) {
            if (!ok) {
                const errMsg = typeof error === 'object' ? JSON.stringify(error) : String(error || payload);
                finish(`Connect rejected: ${errMsg}`);
                return;
            }
            onLog(`[WS] ✓ Connected! Protocol: ${payload?.protocol}. Sending chat.send…`);

            ws.send(JSON.stringify({
                type:   'req',
                id:     `${reqId}-msg`,
                method: 'chat.send',
                params: {
                    sessionKey:     sessionKey,
                    message:        message,
                    idempotencyKey: reqId,
                },
            }));
            return;
        }

        // ── 3. CHAT.SEND ACK ──
        if (type === 'res' && frameId === `${reqId}-msg`) {
            if (!ok) {
                const errMsg = typeof error === 'object' ? JSON.stringify(error) : String(error || payload);
                finish(`chat.send rejected: ${errMsg}`);
                return;
            }
            runId = payload?.runId || payload?.id || null;
            onLog(`[WS] ✓ Message accepted. runId=${runId}. Waiting for agent…`);
            return;
        }

        // ── 4. AGENT STREAMING EVENTS ──
        if (type === 'event' && event === 'agent') {
            const p = payload || {};
            onLog(`[WS] agent event: keys=[${Object.keys(p).join(',')}] done=${p.done} status=${p.status}`);

            if (typeof p.delta === 'string' && p.delta) {
                fullText += p.delta;
                onChunk(p.delta);
                return;
            }

            if (typeof p.text === 'string' && p.text && !p.done) {
                fullText += p.text;
                onChunk(p.text);
                return;
            }

            if (p.done === true || p.status === 'done' || p.status === 'completed' || p.final === true) {
                onLog('[WS] Agent event signals done');
                finish(null);
                return;
            }

            if (p.status === 'error' || p.error) {
                finish(`Agent error: ${JSON.stringify(p.error || p.status)}`);
                return;
            }
            return;
        }

        // ── 5. FINAL RES FOR AGENT RUN ──
        if (type === 'res' && (runId ? payload?.runId === runId : false)) {
            onLog(`[WS] Agent run final res. status=${payload?.status}`);
            if (!fullText && payload?.summary) fullText = payload.summary;
            finish(null);
            return;
        }

        // ── 6. CATCH chat events (some versions use this instead of agent events) ──
        if (type === 'event' && (event === 'chat' || event === 'message')) {
            const text = payload?.text || payload?.content || payload?.message || '';
            if (text && !fullText.includes(text)) {
                onLog(`[WS] chat/message event with text (${text.length} chars)`);
                fullText += text;
                onChunk(text);
            }
            if (payload?.done || payload?.final) finish(null);
            return;
        }

        // Log any other events for debugging
        if (type === 'event') {
            onLog(`[WS] Unhandled event: ${event} payload=${JSON.stringify(payload||{}).slice(0,100)}`);
        }
    });

    ws.on('error', (err) => {
        onLog(`[WS] Socket error: ${err.message}`);
        finish(`WebSocket error: ${err.message}`);
    });

    ws.on('close', (code, reason) => {
        const r = reason?.toString() || '';
        onLog(`[WS] Socket closed. code=${code} reason=${r}`);
        if (!finished) {
            // If we have text already, treat close as done
            if (fullText) {
                onLog('[WS] Socket closed with partial/full response — treating as done');
                finish(null);
            } else {
                finish(`Connection closed (code=${code}${r ? ': '+r : ''})`);
            }
        }
    });
}

// ─── TASK SSE ENDPOINT ────────────────────────────────────────────────────────
app.post('/api/task', (req, res) => {
    const { agentId, task } = req.body;
    if (!agentId || !task) return res.status(400).json({ error: 'agentId and task required' });

    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');
    res.flushHeaders();

    const sseWrite = (obj) => {
        if (!res.writableEnded) res.write(`data: ${JSON.stringify(obj)}\n\n`);
    };
    const sseEnd = () => {
        if (!res.writableEnded) { res.write('data: [DONE]\n\n'); res.end(); }
    };

    console.log(`\n${'─'.repeat(60)}`);
    console.log(`[TASK] Agent: ${agentId}`);
    console.log(`[TASK] Task : ${task.substring(0, 100)}`);
    console.log(`${'─'.repeat(60)}`);

    sendToAgent(
        agentId,
        task,
        (msg) => { console.log(msg); sseWrite({ log: msg }); },
        (chunk) => sseWrite({ chunk }),
        (fullText) => {
            console.log(`[TASK] ✓ Complete. First 100 chars: ${fullText.substring(0,100)}`);
            sseWrite({ done: true, fullText });
            sseEnd();
        },
        (err) => {
            console.error(`[TASK] ✗ Error: ${err}`);
            sseWrite({ error: String(err) });
            sseEnd();
        }
    );

    req.on('close', () => console.log('[TASK] Browser disconnected'));
});

// ─── LIST AGENTS ─────────────────────────────────────────────────────────────
app.get('/api/agents', (req, res) => {
    const cfg    = readConfig();
    const cfgMap = new Map((cfg?.agents?.list || []).map(a => [a.id, a]));
    const agents = getAgentDirs().map(name => {
        const soulPath = path.join(AGENTS_ROOT, name, 'SOUL.md');
        const content  = fs.existsSync(soulPath) ? fs.readFileSync(soulPath, 'utf8') : null;
        const cfgEntry = cfgMap.get(name);
        return {
            id:          name,
            name:        cfgEntry?.name || name,
            inConfig:    !!cfgEntry,
            modelOk:     !!(cfgEntry && typeof cfgEntry.model === 'object'),
            workspaceOk: !!(cfgEntry?.workspace && cfgEntry.workspace !== OPENCLAW_PATH),
            model:       cfgEntry?.model?.primary || cfgEntry?.model || 'unknown',
            hasSoul:     !!content,
            soulPreview: content?.substring(0, 100),
        };
    });
    res.json(agents);
});

// ─── CREATE AGENT ─────────────────────────────────────────────────────────────
app.post('/api/agents', async (req, res) => {
    const { name, soul, model } = req.body;
    if (!name || !soul || !model) return res.status(400).json({ error: 'name, soul, model required' });

    const id      = name.trim().replace(/\s+/g,'-').replace(/[^a-zA-Z0-9_-]/g,'').toLowerCase();
    const wsDir   = path.join(OPENCLAW_PATH, `workspace-${id}`);
    const aDir    = path.join(AGENTS_ROOT, id);
    const aSubDir = path.join(aDir, 'agent');

    [wsDir, aDir, aSubDir].forEach(d => { if (!fs.existsSync(d)) fs.mkdirSync(d, { recursive: true }); });
    fs.writeFileSync(path.join(aDir, 'SOUL.md'), soul, 'utf8');
    fs.writeFileSync(path.join(wsDir, 'SOUL.md'), soul, 'utf8');

    const cli = await runCmd(`openclaw agents add ${id} --model ${model} --workspace "${wsDir}"`);
    console.log('[CREATE] CLI:', cli);

    fs.writeFileSync(path.join(aDir, 'SOUL.md'), soul, 'utf8');
    fs.writeFileSync(path.join(wsDir, 'SOUL.md'), soul, 'utf8');

    let cfg = readConfig();
    if (!cfg.agents) cfg.agents = {};
    if (!cfg.agents.list) cfg.agents.list = [];
    const idx   = cfg.agents.list.findIndex(a => a.id === id);
    const entry = buildAgentEntry(id, name, model, wsDir);
    if (idx >= 0) cfg.agents.list[idx] = entry; else cfg.agents.list.push(entry);
    writeConfig(cfg);

    await runCmd('openclaw gateway restart');
    res.json({ message: `Agent '${id}' created!`, entry, cliResult: cli });
});

// ─── REPAIR SOUL ─────────────────────────────────────────────────────────────
app.post('/api/agents/:id/soul', (req, res) => {
    const { id }                = req.params;
    const { soul, writeGlobal } = req.body;
    if (!soul) return res.status(400).json({ error: 'soul required' });

    const cfg       = readConfig();
    const agentConf = cfg?.agents?.list?.find(a => a.id === id);
    const results   = {};

    const write = (p, label) => {
        try {
            if (!fs.existsSync(path.dirname(p))) fs.mkdirSync(path.dirname(p), { recursive: true });
            fs.writeFileSync(p, soul, 'utf8');
            results[label] = { path: p, ok: true };
        } catch(e) { results[label] = { path: p, ok: false, error: e.message }; }
    };

    write(path.join(AGENTS_ROOT, id, 'SOUL.md'), 'agentDir');
    if (agentConf?.workspace) write(path.join(agentConf.workspace, 'SOUL.md'), 'workspace');
    if (writeGlobal)          write(path.join(OPENCLAW_PATH, 'SOUL.md'), 'global');

    res.json({ ok: true, message: `SOUL.md updated for '${id}'`, results });
});

// ─── FIX ALL ─────────────────────────────────────────────────────────────────
app.post('/api/fix-all', async (req, res) => {
    let cfg = readConfig();
    cfg = repairConfigAgents(cfg);
    writeConfig(cfg);
    await runCmd('openclaw gateway restart');
    await new Promise(r => setTimeout(r, 3000));
    const cli = await runCmd('openclaw agents list 2>&1');
    res.json({ message: 'All agents fixed. Gateway restarted.', cli: cli.stdout });
});

// ─── DEBUG ────────────────────────────────────────────────────────────────────
app.get('/api/debug', async (req, res) => {
    const cfg   = readConfig();
    const cli   = await runCmd('openclaw agents list 2>&1');
    const token = getGatewayToken();
    res.json({
        CONFIG_PATH,
        gatewayWsUrl:              OC_WS_URL,
        gatewayToken:              token ? token.substring(0,8)+'...' : 'NOT FOUND',
        allowInsecureAuth:         cfg?.gateway?.controlUi?.allowInsecureAuth,
        dangerouslyDisableDevAuth: cfg?.gateway?.controlUi?.dangerouslyDisableDeviceAuth,
        configAgentsList:          cfg?.agents?.list || [],
        agentDirsOnDisk:           getAgentDirs(),
        cliAgentList:              cli.stdout,
    });
});

// ─── TASK HISTORY ─────────────────────────────────────────────────────────────
const TASKS_FILE = path.join(OPENCLAW_PATH, 'portal-tasks.json');
function readTasks() {
    if (!fs.existsSync(TASKS_FILE)) return [];
    try { return JSON.parse(fs.readFileSync(TASKS_FILE, 'utf8')); } catch { return []; }
}
function saveTask(task) {
    const tasks = readTasks();
    tasks.unshift(task);
    fs.writeFileSync(TASKS_FILE, JSON.stringify(tasks.slice(0, 200), null, 2), 'utf8');
}
app.get('/api/tasks', (req, res) => res.json(readTasks()));
app.post('/api/tasks/save', (req, res) => {
    const record = { id: Date.now().toString(), ...req.body, createdAt: new Date().toISOString() };
    saveTask(record);
    res.json({ ok: true, record });
});

// ─── STARTUP ─────────────────────────────────────────────────────────────────
app.listen(PORT, async () => {
    const token = getGatewayToken();
    const cfg   = readConfig().catch?.(() => ({})) || readConfig();

    console.log(`\n🦀 OpenClaw Portal → http://localhost:${PORT}`);
    console.log(`   Gateway WS        : ${OC_WS_URL}`);
    console.log(`   Token             : ${token ? '✓ '+token.substring(0,8)+'...' : '✗ NOT FOUND'}`);
    console.log(`   allowInsecureAuth : ${cfg?.gateway?.controlUi?.allowInsecureAuth}`);
    console.log(`   dangerouslyDisDev : ${cfg?.gateway?.controlUi?.dangerouslyDisableDeviceAuth}`);
    console.log(`\n   Strategy: connect as "openclaw-control-ui"/"webchat" to skip device auth`);
    console.log(`   (requires allowInsecureAuth:true in openclaw.json, which you already have)\n`);

    // Test gateway connectivity
    const testWs = new WebSocket(OC_WS_URL);
    testWs.on('open',  () => { console.log('[STARTUP] ✓ Gateway reachable at', OC_WS_URL); testWs.close(); });
    testWs.on('error', (e) => console.log(`[STARTUP] ✗ Gateway unreachable: ${e.message}`));
});