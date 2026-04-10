const express    = require('express');
const bodyParser = require('body-parser');
const fs         = require('fs');
const path       = require('path');
const os         = require('os');
const { exec }   = require('child_process');
const WebSocket  = require('ws');
const http       = require('http');
const crypto     = require('crypto');

const app    = express();
const server = http.createServer(app); // Use http server so we can attach WS for real-time push

// ─── CONFIG ───────────────────────────────────────────────────────────────────
// Bind to 0.0.0.0 so any device on your network can access the portal
const PORT          = 3000;
const BIND          = '0.0.0.0';
const OPENCLAW_PATH = path.join(os.homedir(), '.openclaw');
const AGENTS_ROOT   = path.join(OPENCLAW_PATH, 'agents');
const CONFIG_PATH   = path.join(OPENCLAW_PATH, 'openclaw.json');
const OC_WS_URL     = 'ws://127.0.0.1:18789';
const TASKS_FILE    = path.join(OPENCLAW_PATH, 'portal-tasks.json');

app.use(bodyParser.json({ limit: '2mb' }));
app.use(express.static('public'));

// ─── CORS — allow access from any device on network ──────────────────────────
app.use((req, res, next) => {
    res.header('Access-Control-Allow-Origin', '*');
    res.header('Access-Control-Allow-Headers', 'Content-Type');
    res.header('Access-Control-Allow-Methods', 'GET,POST,DELETE,OPTIONS');
    if (req.method === 'OPTIONS') return res.sendStatus(200);
    next();
});

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

function getLocalIPs() {
    const nets = os.networkInterfaces();
    const ips  = [];
    for (const name of Object.keys(nets)) {
        for (const net of nets[name]) {
            if (net.family === 'IPv4' && !net.internal) {
                ips.push({ name, address: net.address });
            }
        }
    }
    return ips;
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

// ─── TASK HISTORY ─────────────────────────────────────────────────────────────
function readTasks() {
    if (!fs.existsSync(TASKS_FILE)) return [];
    try { return JSON.parse(fs.readFileSync(TASKS_FILE, 'utf8')); } catch { return []; }
}
function saveTask(task) {
    const tasks = readTasks();
    tasks.unshift(task);
    fs.writeFileSync(TASKS_FILE, JSON.stringify(tasks.slice(0, 500), null, 2), 'utf8');
    // Push to all connected SSE clients (for cross-device real-time updates)
    broadcastSSE({ type: 'task_saved', task });
}

// ─── REAL-TIME SSE BROADCAST (for multi-device) ───────────────────────────────
// All connected browsers (from any device) get live updates
const sseClients = new Set();

function broadcastSSE(data) {
    const msg = `data: ${JSON.stringify(data)}\n\n`;
    for (const client of sseClients) {
        try { client.write(msg); } catch { sseClients.delete(client); }
    }
}

// SSE endpoint for real-time cross-device task updates
app.get('/api/events', (req, res) => {
    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');
    res.flushHeaders();
    res.write('data: {"type":"connected"}\n\n');
    sseClients.add(res);
    req.on('close', () => sseClients.delete(res));
});

// ─── AGENT DETECTION — check task text for @mentions ─────────────────────────
// If a task contains @AgentName, it gets delegated to that agent automatically
// e.g. "ask @QAEngineer to write test cases for this"
function detectDelegation(task, allAgents) {
    const delegations = [];
    // Match @agentname or @AgentName patterns
    const pattern = /@([a-zA-Z][a-zA-Z0-9_-]*)/g;
    let match;
    while ((match = pattern.exec(task)) !== null) {
        const mentioned = match[1].toLowerCase();
        const agent = allAgents.find(a =>
            a.id.toLowerCase() === mentioned ||
            a.name.toLowerCase() === mentioned ||
            a.name.toLowerCase().replace(/\s+/g, '') === mentioned
        );
        if (agent) delegations.push({ agent, mention: match[0] });
    }
    return delegations;
}

// ─── OPENCLAW WS CLIENT ───────────────────────────────────────────────────────
function sendToAgent(agentId, message, onLog, onChunk, onDone, onError) {
    const token      = getGatewayToken();
    const sessionKey = `agent:${agentId}:main`;
    const reqId      = `portal-${Date.now()}-${Math.random().toString(36).slice(2,7)}`;
    let   fullText   = '';
    let   finished   = false;
    let   runId      = null;

    onLog(`[WS] Agent=${agentId} session=${sessionKey}`);

    if (!token) { onError('No gateway token found in openclaw.json'); return; }

    const ws = new WebSocket(OC_WS_URL, { headers: { Origin: 'http://127.0.0.1:18789' } });

    const finish = (err) => {
        if (finished) return;
        finished = true;
        clearTimeout(globalTimeout);
        try { ws.close(); } catch {}
        if (err) { onLog(`[WS] ERROR: ${err}`); onError(err); }
        else      { onLog(`[WS] Done (${fullText.length} chars)`); onDone(fullText); }
    };

    const globalTimeout = setTimeout(() => finish('Timeout: no response after 120s'), 120000);

    ws.on('open', () => onLog('[WS] Opened'));

    ws.on('message', (raw) => {
        let frame;
        try { frame = JSON.parse(raw.toString()); } catch { return; }

        const { type, event, id: frameId, ok, payload, error } = frame;

        if (type === 'event' && event === 'connect.challenge') {
            ws.send(JSON.stringify({
                type: 'req', id: `${reqId}-connect`, method: 'connect',
                params: {
                    minProtocol: 3, maxProtocol: 3, role: 'operator',
                    scopes: ['operator.read', 'operator.write'],
                    caps: [], commands: [], permissions: {},
                    auth: { token }, locale: 'en-US',
                    userAgent: 'openclaw-control-ui/2026.4.5',
                    client: { id: 'openclaw-control-ui', version: '2026.4.5', platform: 'web', mode: 'webchat' },
                },
            }));
            return;
        }

        if (type === 'res' && frameId === `${reqId}-connect`) {
            if (!ok) { finish(`Connect rejected: ${JSON.stringify(error || payload)}`); return; }
            onLog('[WS] Connected → sending message');
            ws.send(JSON.stringify({
                type: 'req', id: `${reqId}-msg`, method: 'chat.send',
                params: { sessionKey, message, idempotencyKey: reqId },
            }));
            return;
        }

        if (type === 'res' && frameId === `${reqId}-msg`) {
            if (!ok) { finish(`chat.send rejected: ${JSON.stringify(error || payload)}`); return; }
            runId = payload?.runId || payload?.id || null;
            onLog(`[WS] Accepted runId=${runId}`);
            return;
        }

        if (type === 'event' && event === 'agent') {
            const p = payload || {};
            const { stream, data } = p;

            if (stream === 'assistant' || stream === 'delta' || stream === 'text') {
                const chunk = typeof data === 'string' ? data : (data?.delta ?? data?.content ?? '');
                if (chunk) { fullText += chunk; onChunk(chunk); }
                return;
            }
            if (stream === 'lifecycle') {
                if (data?.phase === 'end' || data?.phase === 'done') {
                    onLog(`[WS] lifecycle.end → done`); finish(null);
                }
                return;
            }
            if (stream === 'done' || stream === 'end' || stream === 'complete') { finish(null); return; }
            if (stream === 'error') { finish(`Agent error: ${JSON.stringify(data)}`); return; }
            return;
        }

        if (type === 'res' && payload?.runId && payload.runId === runId) { finish(null); return; }
        if (type === 'event' && (event === 'chat' || event === 'message')) return; // ignore history blobs
    });

    ws.on('error', (err) => finish(`WS error: ${err.message}`));
    ws.on('close', (code) => {
        if (!finished) finish(fullText ? null : `Connection closed (code=${code})`);
    });
}

// ─── TASK ENDPOINT ────────────────────────────────────────────────────────────
app.post('/api/task', async (req, res) => {
    const { agentId, task, fromAgent } = req.body;
    if (!agentId || !task) return res.status(400).json({ error: 'agentId and task required' });

    // Check for @mentions → auto-delegate sub-tasks
    const cfg        = readConfig();
    const allAgents  = getAgentDirs().map(name => {
        const cfgEntry = cfg?.agents?.list?.find(a => a.id === name);
        return { id: name, name: cfgEntry?.name || name };
    });
    const delegations = detectDelegation(task, allAgents);

    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');
    res.flushHeaders();

    const sseWrite = (obj) => { if (!res.writableEnded) res.write(`data: ${JSON.stringify(obj)}\n\n`); };
    const sseEnd   = ()    => { if (!res.writableEnded) { res.write('data: [DONE]\n\n'); res.end(); } };

    console.log(`\n${'─'.repeat(60)}`);
    console.log(`[TASK] Agent: ${agentId}${fromAgent ? ` (delegated from ${fromAgent})` : ''}`);
    console.log(`[TASK] Task : ${task.substring(0, 100)}`);
    if (delegations.length) console.log(`[TASK] Delegations detected: ${delegations.map(d=>d.agent.name).join(', ')}`);

    // Notify about delegations before starting
    if (delegations.length > 0) {
        sseWrite({
            type: 'delegation_detected',
            agents: delegations.map(d => ({ id: d.agent.id, name: d.agent.name, mention: d.mention }))
        });
    }

    // Run primary agent
    sendToAgent(
        agentId, task,
        (msg) => { console.log(msg); sseWrite({ log: msg }); },
        (chunk) => sseWrite({ chunk }),
        async (fullText) => {
            console.log(`[TASK] ✓ Primary agent done (${fullText.length} chars)`);
            sseWrite({ done: true, fullText });

            // Save primary task
            const taskRecord = {
                id:        `${Date.now()}-${Math.random().toString(36).slice(2,5)}`,
                agentId,   agentName: allAgents.find(a=>a.id===agentId)?.name || agentId,
                task,      response: fullText, status: 'done',
                fromAgent: fromAgent || null,
                delegatedTo: delegations.map(d => d.agent.id),
                createdAt: new Date().toISOString(),
            };
            saveTask(taskRecord);

            // Auto-delegate to @mentioned agents
            for (const { agent, mention } of delegations) {
                console.log(`[DELEGATE] → ${agent.name} (${agent.id})`);
                sseWrite({ type: 'delegation_start', agentId: agent.id, agentName: agent.name });

                // Build sub-task: strip the @mention and send context + original task
                const subTask = `[Delegated from ${allAgents.find(a=>a.id===agentId)?.name||agentId}]\n\n${task.replace(mention, '').trim()}`;

                await new Promise(resolve => {
                    let subResponse = '';
                    sendToAgent(
                        agent.id, subTask,
                        (msg) => { console.log(`  [${agent.id}] ${msg}`); sseWrite({ log: `[${agent.name}] ${msg}` }); },
                        (chunk) => { subResponse += chunk; sseWrite({ delegationChunk: chunk, agentId: agent.id, agentName: agent.name }); },
                        (subText) => {
                            console.log(`[DELEGATE] ✓ ${agent.name} done`);
                            sseWrite({ delegationDone: true, agentId: agent.id, agentName: agent.name, response: subText });
                            saveTask({
                                id:        `${Date.now()}-${Math.random().toString(36).slice(2,5)}`,
                                agentId:   agent.id, agentName: agent.name,
                                task:      subTask, response: subText, status: 'done',
                                fromAgent: agentId,
                                createdAt: new Date().toISOString(),
                            });
                            resolve();
                        },
                        (err) => {
                            console.error(`[DELEGATE] ✗ ${agent.name}: ${err}`);
                            sseWrite({ delegationError: err, agentId: agent.id, agentName: agent.name });
                            resolve();
                        }
                    );
                });
            }

            sseEnd();
        },
        (err) => {
            console.error(`[TASK] ✗ ${err}`);
            sseWrite({ error: String(err) });
            sseEnd();
        }
    );

    req.on('close', () => console.log('[TASK] Client disconnected'));
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
    res.json({ message: 'All agents fixed.', cli: cli.stdout });
});

// ─── TASK HISTORY ─────────────────────────────────────────────────────────────
app.get('/api/tasks', (req, res) => res.json(readTasks()));
app.delete('/api/tasks', (req, res) => {
    fs.writeFileSync(TASKS_FILE, '[]', 'utf8');
    res.json({ ok: true });
});
app.post('/api/tasks/save', (req, res) => {
    const record = { id: Date.now().toString(), ...req.body, createdAt: new Date().toISOString() };
    saveTask(record);
    res.json({ ok: true, record });
});

// ─── NETWORK INFO ─────────────────────────────────────────────────────────────
app.get('/api/network', (req, res) => {
    const ips = getLocalIPs();
    res.json({
        port: PORT,
        ips,
        urls: ips.map(ip => `http://${ip.address}:${PORT}`),
        hostname: os.hostname(),
    });
});

// ─── DEBUG ────────────────────────────────────────────────────────────────────
app.get('/api/debug', async (req, res) => {
    const cfg   = readConfig();
    const cli   = await runCmd('openclaw agents list 2>&1');
    const token = getGatewayToken();
    res.json({
        CONFIG_PATH,
        gatewayWsUrl:     OC_WS_URL,
        gatewayToken:     token ? token.substring(0,8)+'...' : 'NOT FOUND',
        allowInsecureAuth: cfg?.gateway?.controlUi?.allowInsecureAuth,
        networkUrls:      getLocalIPs().map(ip => `http://${ip.address}:${PORT}`),
        configAgentsList: cfg?.agents?.list || [],
        agentDirsOnDisk:  getAgentDirs(),
        cliAgentList:     cli.stdout,
    });
});

// ─── STARTUP ─────────────────────────────────────────────────────────────────
server.listen(PORT, BIND, async () => {
    const token = getGatewayToken();
    const ips   = getLocalIPs();

    console.log(`\n🦀 OpenClaw Portal`);
    console.log(`   Local     : http://localhost:${PORT}`);
    ips.forEach(ip => console.log(`   Network   : http://${ip.address}:${PORT}  ← open on phone/other device`));
    console.log(`   Gateway   : ${OC_WS_URL}`);
    console.log(`   Token     : ${token ? '✓ '+token.substring(0,8)+'...' : '✗ NOT FOUND'}`);
    console.log(`\n   Features:`);
    console.log(`   ✓ Cross-device access (open network URL on any device)`);
    console.log(`   ✓ Agent delegation via @mention in tasks`);
    console.log(`   ✓ Real-time task updates across all connected devices\n`);

    const testWs = new WebSocket(OC_WS_URL);
    testWs.on('open',  () => { console.log('[STARTUP] ✓ OpenClaw gateway reachable'); testWs.close(); });
    testWs.on('error', (e) => console.log(`[STARTUP] ✗ Gateway unreachable: ${e.message}`));
});