const express    = require('express');
const bodyParser = require('body-parser');
const fs         = require('fs');
const path       = require('path');
const os         = require('os');
const { exec }   = require('child_process');
const WebSocket  = require('ws');
const http       = require('http');

const app    = express();
const server = http.createServer(app);
const PORT   = 3000;
const BIND   = '0.0.0.0';

app.use(bodyParser.json({ limit: '4mb' }));
app.use(express.static('public'));
app.use((req, res, next) => {
    res.header('Access-Control-Allow-Origin', '*');
    res.header('Access-Control-Allow-Headers', 'Content-Type');
    res.header('Access-Control-Allow-Methods', 'GET,POST,PUT,DELETE,OPTIONS');
    if (req.method === 'OPTIONS') return res.sendStatus(200);
    next();
});

// ─── PATHS ────────────────────────────────────────────────────────────────────
const HOME        = os.homedir();
const OC          = path.join(HOME, '.openclaw');
const AGENTS_ROOT = path.join(OC, 'agents');
const CONFIG_PATH = path.join(OC, 'openclaw.json');
const TASKS_FILE  = path.join(OC, 'portal-tasks.json');
const OC_WS_URL   = 'ws://127.0.0.1:18789';

const WORKSPACE_FILES = ['SOUL.md','AGENTS.md','IDENTITY.md','USER.md','TOOLS.md','HEARTBEAT.md','MEMORY.md'];

// ─── HELPERS ──────────────────────────────────────────────────────────────────
const isDir  = p => { try { return fs.statSync(p).isDirectory(); } catch { return false; } };
const readF  = p => { try { return fs.readFileSync(p, 'utf8'); } catch { return ''; } };
const writeF = (p, c) => { fs.mkdirSync(path.dirname(p), { recursive: true }); fs.writeFileSync(p, c, 'utf8'); };

function getAgentDirs() {
    if (!fs.existsSync(AGENTS_ROOT)) return [];
    return fs.readdirSync(AGENTS_ROOT).filter(e => !e.startsWith('.') && isDir(path.join(AGENTS_ROOT, e)));
}

function readConfig() {
    try { return JSON.parse(fs.readFileSync(CONFIG_PATH, 'utf8')); } catch { return {}; }
}

function writeConfig(cfg) {
    const bak = `${CONFIG_PATH}.bak.${Date.now()}`;
    if (fs.existsSync(CONFIG_PATH)) fs.copyFileSync(CONFIG_PATH, bak);
    const baks = fs.readdirSync(path.dirname(CONFIG_PATH))
        .filter(f => f.startsWith('openclaw.json.bak.')).sort();
    baks.slice(0, -5).forEach(b => { try { fs.unlinkSync(path.join(path.dirname(CONFIG_PATH), b)); } catch {} });
    const { _path, ...clean } = cfg;
    fs.writeFileSync(CONFIG_PATH, JSON.stringify(clean, null, 2), 'utf8');
}

function getGatewayToken() {
    const cfg = readConfig();
    return cfg?.gateway?.auth?.token || cfg?.gateway?.auth?.tokens?.[0] || '';
}

function getLocalIPs() {
    const nets = os.networkInterfaces(), ips = [];
    for (const name of Object.keys(nets))
        for (const net of nets[name])
            if (net.family === 'IPv4' && !net.internal) ips.push({ name, address: net.address });
    return ips;
}

function runCmd(cmd) {
    return new Promise(r => exec(cmd, { timeout: 25000 }, (e, o, s) =>
        r({ ok: !e, stdout: o?.trim(), stderr: s?.trim(), error: e?.message })));
}

function getAgentWorkspace(agentId) {
    const cfg = readConfig();
    const entry = cfg?.agents?.list?.find(a => a.id === agentId);
    if (entry?.workspace) return entry.workspace.replace(/^~/, HOME);
    return path.join(OC, `workspace-${agentId}`);
}

// ─── SSE BROADCAST ────────────────────────────────────────────────────────────
const sseClients = new Set();
function broadcast(data) {
    const msg = `data: ${JSON.stringify(data)}\n\n`;
    for (const c of sseClients) { try { c.write(msg); } catch { sseClients.delete(c); } }
}

app.get('/api/events', (req, res) => {
    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');
    res.flushHeaders();
    res.write('data: {"type":"connected"}\n\n');
    sseClients.add(res);
    req.on('close', () => sseClients.delete(res));
});

// ─── DELEGATION DETECTION ─────────────────────────────────────────────────────
function detectDelegation(task, allAgents) {
    const pattern = /@([a-zA-Z][a-zA-Z0-9_-]*)/g;
    const results = []; let m;
    while ((m = pattern.exec(task)) !== null) {
        const id = m[1].toLowerCase();
        const ag = allAgents.find(a =>
            a.id.toLowerCase() === id ||
            a.name.toLowerCase() === id ||
            a.name.toLowerCase().replace(/\s+/g, '') === id
        );
        if (ag) results.push({ agent: ag, mention: m[0] });
    }
    return results;
}

// ─── OPENCLAW WS CLIENT ───────────────────────────────────────────────────────
// Fixed: proper lifecycle handling, no premature timeout, full stream support
function sendToAgent(agentId, message, onLog, onChunk, onDone, onError) {
    const token      = getGatewayToken();
    const sessionKey = `agent:${agentId}:main`;
    const reqId      = `portal-${Date.now()}-${Math.random().toString(36).slice(2, 7)}`;
    let fullText     = '';
    let finished     = false;
    let connected    = false;
    let msgSent      = false;
    let gotChunk     = false;
    let runId        = null;

    if (!token) { onError('No gateway token found in openclaw.json'); return; }

    onLog(`[WS] Connecting → ${OC_WS_URL}`);
    onLog(`[WS] Agent: ${agentId} | Session: ${sessionKey}`);

    const ws = new WebSocket(OC_WS_URL, {
        headers: { 'Origin': 'http://127.0.0.1:18789' },
        handshakeTimeout: 10000,
    });

    // ── finish helper ─────────────────────────────────────────────────────────
    const finish = (err) => {
        if (finished) return;
        finished = true;
        clearTimeout(gTimeout);
        clearInterval(heartbeatInterval);
        try { ws.terminate(); } catch {}

        if (err) {
            onLog(`[WS] ✗ Error: ${err}`);
            onError(String(err));
        } else {
            onLog(`[WS] ✓ Done — ${fullText.length} chars`);
            onDone(fullText);
        }
    };

    // 3-minute hard timeout — generous enough for slow local models
    const gTimeout = setTimeout(() => {
        if (gotChunk) {
            // We got some text, treat as done rather than erroring
            onLog('[WS] Timeout — but we have text, treating as complete');
            finish(null);
        } else {
            finish('No response after 3 minutes. Check if your agent model is running.');
        }
    }, 180000);

    // Keep SSE connection alive with periodic keepalive log
    const heartbeatInterval = setInterval(() => {
        if (!finished) onLog('[WS] … waiting for agent response');
    }, 15000);

    // ── WebSocket events ──────────────────────────────────────────────────────
    ws.on('open', () => {
        onLog('[WS] Socket opened — awaiting challenge');
    });

    ws.on('message', (raw) => {
        let frame;
        try { frame = JSON.parse(raw.toString()); } catch {
            onLog(`[WS] Bad JSON: ${raw.toString().slice(0, 60)}`);
            return;
        }

        const { type, event, id: fid, ok, payload, error } = frame;

        // ── 1. Challenge → Connect ────────────────────────────────────────────
        if (type === 'event' && event === 'connect.challenge') {
            onLog('[WS] Got challenge → authenticating as openclaw-control-ui');
            ws.send(JSON.stringify({
                type: 'req',
                id: `${reqId}-c`,
                method: 'connect',
                params: {
                    minProtocol: 3, maxProtocol: 3,
                    role: 'operator',
                    scopes: ['operator.read', 'operator.write'],
                    caps: [], commands: [], permissions: {},
                    auth: { token },
                    locale: 'en-US',
                    userAgent: 'openclaw-control-ui/2026.4.5',
                    client: {
                        id: 'openclaw-control-ui',
                        version: '2026.4.5',
                        platform: 'web',
                        mode: 'webchat',
                    },
                },
            }));
            return;
        }

        // ── 2. Connect response ───────────────────────────────────────────────
        if (type === 'res' && fid === `${reqId}-c`) {
            if (!ok) {
                finish(`Auth failed: ${JSON.stringify(error || payload)}`);
                return;
            }
            connected = true;
            onLog(`[WS] ✓ Authenticated (protocol ${payload?.protocol})`);
            onLog('[WS] Sending message to agent…');
            ws.send(JSON.stringify({
                type: 'req',
                id: `${reqId}-m`,
                method: 'chat.send',
                params: {
                    sessionKey,
                    message,
                    idempotencyKey: reqId,
                },
            }));
            return;
        }

        // ── 3. Message accepted ───────────────────────────────────────────────
        if (type === 'res' && fid === `${reqId}-m`) {
            if (!ok) {
                finish(`Message rejected: ${JSON.stringify(error || payload)}`);
                return;
            }
            msgSent = true;
            runId = payload?.runId || payload?.id || null;
            onLog(`[WS] ✓ Message accepted — runId: ${runId || 'N/A'}`);
            onLog('[WS] Agent is thinking…');
            return;
        }

        // ── 4. Agent streaming events ─────────────────────────────────────────
        if (type === 'event' && event === 'agent') {
            const p = payload || {};
            const stream = p.stream;
            const data   = p.data;

            onLog(`[WS] agent stream="${stream}" data=${JSON.stringify(data).slice(0, 60)}`);

            // Text chunks — data.delta is incremental, data.text is cumulative
            if (stream === 'assistant' || stream === 'delta' || stream === 'text') {
                // Always prefer delta (incremental), fall back to text only if no delta
                let chunk = '';
                if (typeof data === 'string') {
                    chunk = data;
                } else if (data?.delta !== undefined && data.delta !== null) {
                    chunk = String(data.delta);
                } else if (data?.content !== undefined) {
                    chunk = String(data.content);
                }
                // Avoid duplicate: if delta not available, use text but only the new part
                if (!chunk && data?.text) {
                    const newPart = String(data.text).slice(fullText.length);
                    if (newPart) chunk = newPart;
                }
                if (chunk) {
                    fullText += chunk;
                    gotChunk = true;
                    onChunk(chunk);
                }
                return;
            }

            // Lifecycle events — end/done signals completion
            if (stream === 'lifecycle') {
                const phase = data?.phase;
                onLog(`[WS] lifecycle phase="${phase}"`);
                if (phase === 'end' || phase === 'done' || phase === 'complete') {
                    finish(null);
                }
                // 'start' means agent started — continue waiting
                return;
            }

            // Explicit done/end streams
            if (stream === 'done' || stream === 'end' || stream === 'complete' || stream === 'finish') {
                // Extract any final text if present
                if (!gotChunk && data) {
                    const t = typeof data === 'string' ? data : (data?.text || data?.content || '');
                    if (t) { fullText = t; gotChunk = true; onChunk(t); }
                }
                finish(null);
                return;
            }

            // Error stream
            if (stream === 'error') {
                const errMsg = typeof data === 'string' ? data : (data?.message || JSON.stringify(data));
                finish(`Agent error: ${errMsg}`);
                return;
            }

            // Skip non-content streams silently
            // (start, tool, thinking, heartbeat, etc.)
            return;
        }

        // ── 5. Final res for agent run ────────────────────────────────────────
        if (type === 'res' && runId && payload?.runId === runId) {
            onLog('[WS] Agent run final response received');
            if (!gotChunk) {
                const t = payload?.summary || payload?.text || payload?.content || payload?.message || '';
                if (t) { fullText = t; gotChunk = true; onChunk(t); }
            }
            finish(null);
            return;
        }

        // ── 6. Chat history events — ignore content, just log ─────────────────
        if (type === 'event' && (event === 'chat' || event === 'message')) {
            // These carry full JSON history blobs — don't try to extract text
            onLog(`[WS] chat history update (ignored)`);
            return;
        }

        // ── 7. Other events — log but don't act ───────────────────────────────
        if (type === 'event') {
            onLog(`[WS] event: ${event}`);
        }
    });

    ws.on('error', (err) => {
        onLog(`[WS] Socket error: ${err.message}`);
        finish(`WebSocket error: ${err.message}`);
    });

    ws.on('close', (code, reason) => {
        const r = reason?.toString?.() || '';
        onLog(`[WS] Socket closed (code=${code}${r ? ' reason=' + r : ''})`);
        if (!finished) {
            if (gotChunk) {
                // We have text — treat close as done
                onLog('[WS] Socket closed with data — treating as complete');
                finish(null);
            } else if (connected && msgSent) {
                // Connected and sent but got nothing back
                finish('Agent did not respond. The model may be offline or busy.');
            } else {
                finish(`Connection closed unexpectedly (code=${code})`);
            }
        }
    });
}

// ─── TASK SSE ENDPOINT ────────────────────────────────────────────────────────
app.post('/api/task', async (req, res) => {
    const { agentId, task, fromAgent } = req.body;
    if (!agentId || !task) return res.status(400).json({ error: 'agentId + task required' });

    const cfg       = readConfig();
    const allAgents = getAgentDirs().map(n => {
        const e = cfg?.agents?.list?.find(a => a.id === n);
        return { id: n, name: e?.name || n };
    });
    const delegations = detectDelegation(task, allAgents);

    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');
    res.setHeader('X-Accel-Buffering', 'no'); // disable nginx buffering if present
    res.flushHeaders();

    const sse = (o) => {
        try {
            if (!res.writableEnded) res.write(`data: ${JSON.stringify(o)}\n\n`);
        } catch {}
    };
    const end = () => {
        try {
            if (!res.writableEnded) { res.write('data: [DONE]\n\n'); res.end(); }
        } catch {}
    };

    console.log(`\n${'─'.repeat(56)}`);
    console.log(`[TASK] Agent: ${agentId}`);
    console.log(`[TASK] Task:  ${task.substring(0, 80)}`);
    if (delegations.length) console.log(`[TASK] Delegations: ${delegations.map(d => d.agent.name).join(', ')}`);
    console.log(`${'─'.repeat(56)}`);

    if (delegations.length) {
        sse({ type: 'delegation_detected', agents: delegations.map(d => ({ agentId: d.agent.id, agentName: d.agent.name, mention: d.mention })) });
    }

    // Run primary agent
    sendToAgent(
        agentId, task,
        (msg) => { console.log(msg); sse({ log: msg }); },
        (chunk) => sse({ chunk }),
        async (fullText) => {
            console.log(`[TASK] ✓ Primary done (${fullText.length} chars)`);
            sse({ done: true, fullText });

            // Save to history
            const agentName = allAgents.find(a => a.id === agentId)?.name || agentId;
            saveTask({
                id: `${Date.now()}-${Math.random().toString(36).slice(2, 5)}`,
                agentId, agentName, task, response: fullText,
                status: 'done', fromAgent: fromAgent || null,
                delegatedTo: delegations.map(d => d.agent.id),
                createdAt: new Date().toISOString(),
            });

            // Run delegations sequentially
            for (const { agent, mention } of delegations) {
                console.log(`[DELEGATE] → ${agent.name}`);
                sse({ type: 'delegation_start', agentId: agent.id, agentName: agent.name });
                const subTask = `[Delegated from ${agentName}]\n\n${task.replace(mention, '').trim()}`;

                await new Promise(resolve => {
                    sendToAgent(
                        agent.id, subTask,
                        (msg) => { console.log(`  [${agent.id}] ${msg}`); sse({ log: `[${agent.name}] ${msg}` }); },
                        (chunk) => sse({ delegationChunk: chunk, agentId: agent.id, agentName: agent.name }),
                        (subText) => {
                            console.log(`[DELEGATE] ✓ ${agent.name} done (${subText.length} chars)`);
                            sse({ delegationDone: true, agentId: agent.id, agentName: agent.name, response: subText });
                            saveTask({
                                id: `${Date.now()}-${Math.random().toString(36).slice(2, 5)}`,
                                agentId: agent.id, agentName: agent.name,
                                task: subTask, response: subText, status: 'done',
                                fromAgent: agentId, createdAt: new Date().toISOString(),
                            });
                            resolve();
                        },
                        (err) => {
                            console.error(`[DELEGATE] ✗ ${agent.name}: ${err}`);
                            sse({ delegationError: String(err), agentId: agent.id, agentName: agent.name });
                            resolve();
                        }
                    );
                });
            }

            end();
        },
        (err) => {
            console.error(`[TASK] ✗ Error: ${err}`);
            sse({ error: String(err) });
            end();
        }
    );

    req.on('close', () => console.log('[TASK] Client disconnected'));
});

// ─── WORKSPACE FILES API ──────────────────────────────────────────────────────
app.get('/api/agents/:id/workspace', (req, res) => {
    const { id } = req.params;
    const ws = getAgentWorkspace(id);
    const files = {};
    for (const fname of WORKSPACE_FILES) {
        const p = path.join(ws, fname);
        files[fname] = { content: readF(p), exists: fs.existsSync(p), path: p };
    }
    const memDir = path.join(ws, 'memory');
    const memFiles = [];
    if (fs.existsSync(memDir)) {
        fs.readdirSync(memDir).filter(f => f.endsWith('.md')).sort().reverse().slice(0, 10)
            .forEach(f => memFiles.push({ name: f, content: readF(path.join(memDir, f)) }));
    }
    res.json({ agentId: id, workspacePath: ws, files, memoryFiles: memFiles });
});

app.put('/api/agents/:id/workspace/:file', (req, res) => {
    const { id, file } = req.params;
    const { content } = req.body;
    if (!WORKSPACE_FILES.includes(file) && !file.match(/^\d{4}-\d{2}-\d{2}\.md$/))
        return res.status(400).json({ error: 'Invalid file name' });
    const ws = getAgentWorkspace(id);
    const p = file.match(/^\d{4}-\d{2}-\d{2}\.md$/)
        ? path.join(ws, 'memory', file)
        : path.join(ws, file);
    try { writeF(p, content || ''); res.json({ ok: true, path: p, size: (content || '').length }); }
    catch (e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/agents/:id/identity', async (req, res) => {
    const { id } = req.params;
    const { name, emoji, theme, model } = req.body;
    let cfg = readConfig();
    if (!cfg.agents?.list) return res.status(404).json({ error: 'No agents list' });
    const idx = cfg.agents.list.findIndex(a => a.id === id);
    if (idx < 0) return res.status(404).json({ error: `Agent ${id} not found` });
    if (name) cfg.agents.list[idx].name = name;
    if (emoji || theme) cfg.agents.list[idx].identity = { ...(cfg.agents.list[idx].identity || {}), ...(emoji ? { emoji } : {}), ...(theme ? { theme } : {}) };
    if (model) cfg.agents.list[idx].model = { primary: model };
    writeConfig(cfg);
    await runCmd('openclaw gateway restart');
    res.json({ ok: true, entry: cfg.agents.list[idx] });
});

// ─── AGENTS CRUD ──────────────────────────────────────────────────────────────
app.get('/api/agents', (req, res) => {
    const cfg    = readConfig();
    const cfgMap = new Map((cfg?.agents?.list || []).map(a => [a.id, a]));
    const agents = getAgentDirs().map(id => {
        const e  = cfgMap.get(id);
        const ws = getAgentWorkspace(id);
        const files = {};
        for (const f of WORKSPACE_FILES) files[f] = fs.existsSync(path.join(ws, f));
        return {
            id, name: e?.name || id,
            inConfig: !!e, workspaceOk: !!(e?.workspace && e.workspace !== OC),
            model: e?.model?.primary || e?.model || 'unknown',
            identity: e?.identity || {},
            workspace: ws, files,
        };
    });
    res.json(agents);
});

app.post('/api/agents', async (req, res) => {
    const { name, soul, model, agentsmd, identitymd, usermd, toolsmd } = req.body;
    if (!name || !soul || !model) return res.status(400).json({ error: 'name, soul, model required' });
    const id      = name.trim().replace(/\s+/g, '-').replace(/[^a-zA-Z0-9_-]/g, '').toLowerCase();
    const wsDir   = path.join(OC, `workspace-${id}`);
    const aDir    = path.join(AGENTS_ROOT, id);
    const aSubDir = path.join(aDir, 'agent');
    [wsDir, aDir, aSubDir].forEach(d => { if (!fs.existsSync(d)) fs.mkdirSync(d, { recursive: true }); });
    const filesToWrite = { 'SOUL.md': soul, 'AGENTS.md': agentsmd || '', 'IDENTITY.md': identitymd || '', 'USER.md': usermd || '', 'TOOLS.md': toolsmd || '' };
    for (const [fname, content] of Object.entries(filesToWrite)) {
        if (content) { writeF(path.join(aDir, fname), content); writeF(path.join(wsDir, fname), content); }
    }
    const cli = await runCmd(`openclaw agents add ${id} --model ${model} --workspace "${wsDir}"`);
    for (const [fname, content] of Object.entries(filesToWrite)) {
        if (content) { writeF(path.join(aDir, fname), content); writeF(path.join(wsDir, fname), content); }
    }
    let cfg = readConfig();
    if (!cfg.agents) cfg.agents = {};
    if (!cfg.agents.list) cfg.agents.list = [];
    const idx = cfg.agents.list.findIndex(a => a.id === id);
    const entry = { id, name, workspace: wsDir, agentDir: path.join(aDir, 'agent'), model: { primary: model } };
    if (idx >= 0) cfg.agents.list[idx] = entry; else cfg.agents.list.push(entry);
    writeConfig(cfg);
    await runCmd('openclaw gateway restart');
    res.json({ ok: true, message: `Agent '${id}' created`, id, entry, cliResult: cli });
});

app.delete('/api/agents/:id', async (req, res) => {
    const { id } = req.params;
    let cfg = readConfig();
    if (cfg.agents?.list) { cfg.agents.list = cfg.agents.list.filter(a => a.id !== id); writeConfig(cfg); }
    await runCmd('openclaw gateway restart');
    res.json({ ok: true, message: `'${id}' removed from config` });
});

app.post('/api/fix-all', async (req, res) => {
    let cfg = readConfig();
    if (!cfg.agents?.list) return res.json({ ok: false, error: 'No agents.list' });
    const seen = new Set();
    cfg.agents.list = cfg.agents.list.filter(a => { if (seen.has(a.id)) return false; seen.add(a.id); return true; }).map(a => {
        const modelStr = typeof a.model === 'object' ? (a.model.primary || 'ollama/minimax-m2.5:cloud') : (a.model || 'ollama/minimax-m2.5:cloud');
        let ws = a.workspace;
        if (!ws || ws === OC) ws = path.join(OC, `workspace-${a.id}`);
        if (!fs.existsSync(ws)) fs.mkdirSync(ws, { recursive: true });
        const aDir = path.join(AGENTS_ROOT, a.id, 'agent');
        if (!fs.existsSync(aDir)) fs.mkdirSync(aDir, { recursive: true });
        const srcSoul = path.join(AGENTS_ROOT, a.id, 'SOUL.md');
        const dstSoul = path.join(ws, 'SOUL.md');
        if (fs.existsSync(srcSoul) && !fs.existsSync(dstSoul)) fs.copyFileSync(srcSoul, dstSoul);
        return { id: a.id, name: a.name || a.id, workspace: ws, agentDir: aDir, model: { primary: modelStr }, ...(a.identity ? { identity: a.identity } : {}) };
    });
    writeConfig(cfg);
    await runCmd('openclaw gateway restart');
    await new Promise(r => setTimeout(r, 3000));
    const cli = await runCmd('openclaw agents list 2>&1');
    res.json({ ok: true, message: 'All agents fixed. Gateway restarted.', cli: cli.stdout });
});

// ─── TASK HISTORY ─────────────────────────────────────────────────────────────
function readTasks() { try { return JSON.parse(fs.readFileSync(TASKS_FILE, 'utf8')); } catch { return []; } }
function saveTask(t) {
    const tasks = readTasks(); tasks.unshift(t);
    fs.writeFileSync(TASKS_FILE, JSON.stringify(tasks.slice(0, 500), null, 2), 'utf8');
    broadcast({ type: 'task_saved', task: t });
}
app.get('/api/tasks', (req, res) => res.json(readTasks()));
app.delete('/api/tasks', (req, res) => { fs.writeFileSync(TASKS_FILE, '[]', 'utf8'); res.json({ ok: true }); });
app.post('/api/tasks/save', (req, res) => {
    const r = { id: Date.now().toString(), ...req.body, createdAt: new Date().toISOString() };
    saveTask(r); res.json({ ok: true, record: r });
});

// ─── NETWORK + DEBUG ──────────────────────────────────────────────────────────
app.get('/api/network', (req, res) => {
    const ips = getLocalIPs();
    res.json({ port: PORT, ips, urls: ips.map(i => `http://${i.address}:${PORT}`), hostname: os.hostname() });
});

app.get('/api/debug', async (req, res) => {
    const cfg = readConfig(); const cli = await runCmd('openclaw agents list 2>&1');
    const token = getGatewayToken();
    res.json({
        CONFIG_PATH, token: token ? token.substring(0, 8) + '...' : 'NOT FOUND',
        allowInsecureAuth: cfg?.gateway?.controlUi?.allowInsecureAuth,
        configAgentsList: cfg?.agents?.list || [],
        agentDirs: getAgentDirs(),
        cliOutput: cli.stdout,
        networkUrls: getLocalIPs().map(i => `http://${i.address}:${PORT}`),
    });
});

// ─── STARTUP ──────────────────────────────────────────────────────────────────
server.listen(PORT, BIND, () => {
    const token = getGatewayToken();
    const ips   = getLocalIPs();
    console.log(`\n🦀  Nexus.AI Portal`);
    console.log(`    Local    → http://localhost:${PORT}`);
    ips.forEach(i => console.log(`    Network  → http://${i.address}:${PORT}`));
    console.log(`    Gateway  → ${OC_WS_URL}`);
    console.log(`    Token    → ${token ? '✓ ' + token.substring(0, 8) + '...' : '✗ NOT FOUND'}\n`);
    const ws = new WebSocket(OC_WS_URL);
    ws.on('open',  () => { console.log('[BOOT] ✓ OpenClaw gateway reachable\n'); ws.close(); });
    ws.on('error', e  => console.log(`[BOOT] ✗ Gateway: ${e.message}\n`));
});