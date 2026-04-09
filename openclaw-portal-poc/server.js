const express    = require('express');
const bodyParser = require('body-parser');
const fs         = require('fs');
const path       = require('path');
const os         = require('os');
const { exec }   = require('child_process');

const app  = express();
const PORT = 3000;
app.use(bodyParser.json());
app.use(express.static('public'));

const OPENCLAW_PATH = path.join(os.homedir(), '.openclaw');
const AGENTS_ROOT   = path.join(OPENCLAW_PATH, 'agents');
const CONFIG_PATH   = path.join(OPENCLAW_PATH, 'openclaw.json');

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
    // Keep up to 5 backups
    for (let i = 4; i >= 1; i--) {
        const from = CONFIG_PATH + `.bak.${i}`;
        const to   = CONFIG_PATH + `.bak.${i + 1}`;
        if (fs.existsSync(from)) fs.renameSync(from, to);
    }
    fs.copyFileSync(CONFIG_PATH, CONFIG_PATH + '.bak.1');
    fs.writeFileSync(CONFIG_PATH, JSON.stringify(cfg, null, 2), 'utf8');
    console.log(`[CONFIG] Written → ${CONFIG_PATH}`);
}

function runCmd(cmd) {
    return new Promise(resolve => {
        exec(cmd, { timeout: 20000 }, (err, stdout, stderr) => {
            resolve({ ok: !err, stdout: stdout?.trim(), stderr: stderr?.trim(), error: err?.message });
        });
    });
}

// THE KEY FIX: correct agent entry shape that OpenClaw CLI actually recognises
// - model must be object { primary: "..." }
// - workspace must be a DEDICATED folder, not the root ~/.openclaw
// - agentDir must point to agents/<id>/agent  (where auth-profiles.json etc live)
function buildAgentEntry(id, name, model, workspaceDir) {
    const modelStr = typeof model === 'object' ? (model.primary || 'ollama/minimax-m2.5:cloud') : model;
    return {
        id,
        name:      name || id,
        workspace: workspaceDir,
        agentDir:  path.join(AGENTS_ROOT, id, 'agent'),
        model:     { primary: modelStr },
    };
}

// Repair ALL agents in the list so they have the right shape
function repairConfigAgents(cfg) {
    if (!cfg.agents?.list) return cfg;

    const seen = new Set();
    const fixed = [];

    for (const a of cfg.agents.list) {
        // De-duplicate by id (there's a duplicate MailSense in your config)
        if (seen.has(a.id)) { console.log(`[DEDUP] Removing duplicate id: ${a.id}`); continue; }
        seen.add(a.id);

        // Fix model field
        const modelStr = typeof a.model === 'object'
            ? (a.model.primary || cfg.agents?.defaults?.model?.primary || 'ollama/minimax-m2.5:cloud')
            : (a.model || cfg.agents?.defaults?.model?.primary || 'ollama/minimax-m2.5:cloud');

        // Fix workspace — if it's the root openclaw path, give it a proper dedicated one
        let ws = a.workspace;
        if (!ws || ws === OPENCLAW_PATH || ws === OPENCLAW_PATH + '/') {
            ws = path.join(OPENCLAW_PATH, `workspace-${a.id}`);
            console.log(`[FIX] Agent '${a.id}' workspace was root → changed to ${ws}`);
        }

        // Ensure workspace folder exists
        if (!fs.existsSync(ws)) {
            fs.mkdirSync(ws, { recursive: true });
            console.log(`[MKDIR] Created workspace: ${ws}`);
        }

        // Ensure agentDir exists
        const agentDir = path.join(AGENTS_ROOT, a.id, 'agent');
        if (!fs.existsSync(agentDir)) {
            fs.mkdirSync(agentDir, { recursive: true });
        }

        // Copy SOUL.md from agents/<id>/SOUL.md into the workspace if not there
        const srcSoul  = path.join(AGENTS_ROOT, a.id, 'SOUL.md');
        const destSoul = path.join(ws, 'SOUL.md');
        if (fs.existsSync(srcSoul) && !fs.existsSync(destSoul)) {
            fs.copyFileSync(srcSoul, destSoul);
            console.log(`[SOUL] Copied SOUL.md → ${destSoul}`);
        }

        fixed.push({
            id:       a.id,
            name:     a.name || a.id,
            workspace: ws,
            agentDir,
            model:    { primary: modelStr },
        });
    }

    cfg.agents.list = fixed;
    return cfg;
}

// ─── DEBUG ────────────────────────────────────────────────────────────────────
app.get('/api/debug', async (req, res) => {
    const cfg      = readConfig();
    const cliList  = await runCmd('openclaw agents list 2>&1');
    res.json({
        CONFIG_PATH,
        configAgentsList: cfg?.agents?.list || [],
        agentDirsOnDisk:  getAgentDirs(),
        cliAgentList:     cliList,
    });
});

// ─── REPAIR ALL EXISTING AGENTS ───────────────────────────────────────────────
// This is the main fix — call once to fix all broken agent entries
app.post('/api/fix-all', async (req, res) => {
    let cfg = readConfig();
    cfg = repairConfigAgents(cfg);
    writeConfig(cfg);

    // Restart gateway so it picks up the fixed config
    const restart = await runCmd('openclaw gateway restart');
    console.log('[RESTART]', restart);

    // Wait 3 seconds then check CLI
    await new Promise(r => setTimeout(r, 3000));
    const cliList = await runCmd('openclaw agents list 2>&1');

    res.json({
        message:          'All agents repaired in openclaw.json. Gateway restarted.',
        fixedAgentsList:  cfg.agents.list.map(a => ({ id: a.id, workspace: a.workspace, model: a.model })),
        cliAfterFix:      cliList,
        nextStep:         'Refresh the OpenClaw dashboard → go to Chat → check the agent dropdown.',
    });
});

// ─── LIST AGENTS ─────────────────────────────────────────────────────────────
app.get('/api/agents', (req, res) => {
    const cfg      = readConfig();
    const cfgMap   = new Map((cfg?.agents?.list || []).map(a => [a.id, a]));
    const agents   = getAgentDirs().map(name => {
        const soulPath = path.join(AGENTS_ROOT, name, 'SOUL.md');
        const content  = fs.existsSync(soulPath) ? fs.readFileSync(soulPath, 'utf8') : null;
        const cfgEntry = cfgMap.get(name);
        const modelOk  = cfgEntry && typeof cfgEntry.model === 'object' && cfgEntry.model.primary;
        const wsOk     = cfgEntry && cfgEntry.workspace && cfgEntry.workspace !== OPENCLAW_PATH;
        return {
            name,
            inConfig:    !!cfgEntry,
            modelOk:     !!modelOk,
            workspaceOk: !!wsOk,
            hasSoul:     !!content,
            soulPreview: content?.substring(0, 80),
        };
    });
    res.json(agents);
});

// ─── CREATE AGENT ─────────────────────────────────────────────────────────────
app.post('/api/agents', async (req, res) => {
    const { name, soul, model } = req.body;
    if (!name || !soul || !model)
        return res.status(400).json({ error: 'name, soul, and model are required.' });

    const id           = name.trim().replace(/\s+/g,'-').replace(/[^a-zA-Z0-9_-]/g,'').toLowerCase();
    const workspaceDir = path.join(OPENCLAW_PATH, `workspace-${id}`);
    const agentDir     = path.join(AGENTS_ROOT, id);
    const agentSubDir  = path.join(agentDir, 'agent');
    const soulPath     = path.join(agentDir, 'SOUL.md');
    const wsSoulPath   = path.join(workspaceDir, 'SOUL.md');

    // 1. Create all needed directories
    [workspaceDir, agentDir, agentSubDir].forEach(d => {
        if (!fs.existsSync(d)) fs.mkdirSync(d, { recursive: true });
    });

    // 2. Write SOUL.md to both locations
    fs.writeFileSync(soulPath, soul, 'utf8');
    fs.writeFileSync(wsSoulPath, soul, 'utf8');

    // 3. Run CLI
    const cliResult = await runCmd(`openclaw agents add ${id} --model ${model} --workspace "${workspaceDir}"`);
    console.log('[CLI]', cliResult);

    // 4. Re-write SOUL.md after CLI
    fs.writeFileSync(soulPath, soul, 'utf8');
    fs.writeFileSync(wsSoulPath, soul, 'utf8');

    // 5. Patch config with CORRECT shape
    let cfg   = readConfig();
    if (!cfg.agents)      cfg.agents = {};
    if (!cfg.agents.list) cfg.agents.list = [];

    const idx = cfg.agents.list.findIndex(a => a.id === id);
    const entry = buildAgentEntry(id, name, model, workspaceDir);
    if (idx >= 0) cfg.agents.list[idx] = entry;
    else          cfg.agents.list.push(entry);

    writeConfig(cfg);

    // 6. Restart gateway
    const restart = await runCmd('openclaw gateway restart');

    res.json({
        message:      `Agent '${id}' created with correct config shape!`,
        entry,
        cliResult,
        restartResult: restart,
        nextStep:     'Wait 5 seconds → refresh OpenClaw dashboard → Chat dropdown.',
    });
});

// ─── REPAIR SOUL ─────────────────────────────────────────────────────────────
app.post('/api/agents/:name/soul', (req, res) => {
    const { name }              = req.params;
    const { soul, writeGlobal } = req.body;
    if (!soul) return res.status(400).json({ error: 'soul is required.' });

    const cfg       = readConfig();
    const agentConf = cfg?.agents?.list?.find(a => a.id === name);
    const results   = {};

    const write = (p, label) => {
        try {
            if (!fs.existsSync(path.dirname(p))) fs.mkdirSync(path.dirname(p), { recursive: true });
            fs.writeFileSync(p, soul, 'utf8');
            results[label] = { path: p, ok: true };
        } catch (e) { results[label] = { path: p, ok: false, error: e.message }; }
    };

    write(path.join(AGENTS_ROOT, name, 'SOUL.md'), 'agentDir');
    if (agentConf?.workspace) write(path.join(agentConf.workspace, 'SOUL.md'), 'workspace');
    if (writeGlobal)          write(path.join(OPENCLAW_PATH, 'SOUL.md'), 'global');

    res.json({ ok: true, message: `SOUL.md updated for '${name}'`, results });
});

app.listen(PORT, () => {
    console.log(`\nPortal  → http://localhost:${PORT}`);
    console.log(`Debug   → http://localhost:${PORT}/api/debug`);
    console.log(`\n⚡ To fix all existing agents at once:`);
    console.log(`   curl -X POST http://localhost:${PORT}/api/fix-all | python3 -m json.tool\n`);
});