// Precision Games server (WebSocket) - friend requests, presence and parties.
// Runs on any Node.js host (Render, Railway, Fly.io, a VPS ...). Players connect with  wss://your-app.onrender.com
const http = require('http');
const fs = require('fs');
const path = require('path');
const WebSocket = require('ws');

const PORT = process.env.PORT || 7777;
const DATA = process.env.DATA_DIR || __dirname;
const idRx = /^[A-Za-z0-9_]{3,16}#[0-9]{4}$/;

let users = {};                 // id -> secret key (the first player to use an ID owns it)
let pend = [];                  // [kind, to, from]   kinds: REQ ACC DEC REM
const online = new Map();       // id -> client
const parties = new Map();      // leader -> [members, leader first]

function log(s) { console.log(new Date().toTimeString().slice(0, 8) + '  ' + s); }
function load() {
  try { users = JSON.parse(fs.readFileSync(path.join(DATA, 'users.json'), 'utf8')); } catch (e) { users = {}; }
  try { pend = JSON.parse(fs.readFileSync(path.join(DATA, 'pending.json'), 'utf8')); } catch (e) { pend = []; }
}
function saveUsers() { try { fs.writeFileSync(path.join(DATA, 'users.json'), JSON.stringify(users)); } catch (e) {} }
function savePend() { try { fs.writeFileSync(path.join(DATA, 'pending.json'), JSON.stringify(pend)); } catch (e) {} }

function send(c, ...parts) {
  try { if (c && c.ws && c.ws.readyState === WebSocket.OPEN) { c.ws.send(parts.join('\t')); return true; } } catch (e) {}
  return false;
}
function presence(c, up) {
  for (const o of online.values()) if (o !== c && o.watch.has(c.id)) send(o, 'PRES', c.id, up ? '1' : '0', c.name);
}
function addPending(kind, to, from) {
  if (pend.some(x => x[0] === kind && x[1] === to && x[2] === from)) return;
  pend.push([kind, to, from]); savePend();
}
function notify(to, kind, live, from) {
  if (!idRx.test(to)) return;
  const o = online.get(to);
  if (o) send(o, live, from); else addPending(kind, to, from);
}

// ---- parties (max 5)
function partyOf(id) { for (const [leader, mem] of parties) if (mem.includes(id)) return leader; return ''; }
function leaderOf(id) { return partyOf(id) || id; }
function sendParty(id, leader, joined) { const o = online.get(id); if (o) send(o, 'PARTY', leader, joined); }
function broadcastParty(leader) {
  const mem = parties.get(leader); const joined = mem.join(',');
  for (const id of mem.slice()) sendParty(id, leader, joined);
}
function joinParty(c, leader) {
  if (leader === c.id || !online.has(leader)) return;
  if (partyOf(c.id) === leader) return;
  leaveParty(c);
  let mem = parties.get(leader);
  if (!mem) { mem = [leader]; parties.set(leader, mem); }
  if (mem.length >= 5) { send(c, 'ERR', 'full'); return; }
  mem.push(c.id); broadcastParty(leader); log(c.id + ' joined the party of ' + leader);
}
function leaveParty(c) {
  let leader = partyOf(c.id); if (!leader) return;
  const mem = parties.get(leader); mem.splice(mem.indexOf(c.id), 1);
  if (mem.length <= 1) {
    const last = mem.length === 1 ? mem[0] : ''; parties.delete(leader);
    if (last) sendParty(last, '', '');
  } else {
    if (leader === c.id) { parties.delete(leader); leader = mem[0]; parties.set(leader, mem); }
    broadcastParty(leader);
  }
}

function handle(c, p) {
  const cmd = p[0];
  if (cmd === 'HELLO') {
    if (p.length < 4 || !idRx.test(p[1]) || p[3].length < 8 || p[3].length > 64) { send(c, 'ERR', 'bad'); return; }
    const id = p[1], key = p[3];
    if (users[id] && users[id] !== key) { send(c, 'ERR', 'taken'); log(id + ' rejected (ID already owned by someone else)'); return; }
    if (!users[id]) { users[id] = key; saveUsers(); log('new player ' + id); }
    const old = online.get(id);
    if (old && old !== c) { online.delete(id); try { old.ws.close(); } catch (e) {} }
    c.id = id; c.name = (p[2] || '').slice(0, 20); online.set(id, c); send(c, 'WELCOME'); log(id + ' online (' + online.size + ' connected)');
    const mine = pend.filter(x => x[1] === id);
    for (const x of mine) { send(c, x[0] === 'REQ' ? 'FREQ' : x[0] === 'ACC' ? 'FACC' : x[0] === 'DEC' ? 'FDEC' : 'FREM', x[2]); }
    if (mine.length) { pend = pend.filter(x => x[1] !== id); savePend(); }
    presence(c, true);
    return;
  }
  if (!c.id) return;
  if (cmd === 'PING') { send(c, 'PONG'); return; }
  if (cmd === 'FRIENDS') {
    c.watch.clear();
    if (p.length > 1 && p[1]) for (const f of p[1].split(',')) {
      if (!idRx.test(f)) continue;
      c.watch.add(f); const o = online.get(f);
      send(c, 'PRES', f, o ? '1' : '0', o ? o.name : '');
    }
    return;
  }
  if (cmd === 'FREQ' && p.length > 1) {
    const t = p[1];
    if (!idRx.test(t) || t === c.id) { send(c, 'ERR', 'bad'); return; }
    if (!users[t]) { send(c, 'ERR', 'nouser', t); return; }
    const o = online.get(t); if (o) send(o, 'FREQ', c.id); else addPending('REQ', t, c.id);
    log(c.id + ' -> friend request -> ' + t);
    return;
  }
  if (cmd === 'FACC' && p.length > 1) { notify(p[1], 'ACC', 'FACC', c.id); return; }
  if (cmd === 'FDEC' && p.length > 1) { notify(p[1], 'DEC', 'FDEC', c.id); return; }
  if (cmd === 'FREM' && p.length > 1) { notify(p[1], 'REM', 'FREM', c.id); return; }
  if (cmd === 'PINV' && p.length > 1) {
    const o = online.get(p[1]); if (o) send(o, 'PINV', c.id, leaderOf(c.id)); else send(c, 'ERR', 'offline', p[1]);
    return;
  }
  if (cmd === 'PACC' && p.length > 1) { joinParty(c, p[1]); return; }
  if (cmd === 'PLEAVE') { leaveParty(c); send(c, 'PARTY', '', ''); return; }
}

function drop(c) {
  if (!c.id) return;
  if (online.get(c.id) === c) { online.delete(c.id); leaveParty(c); presence(c, false); log(c.id + ' offline'); }
}

load();
const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
  res.end('Precision Games server is running. Players online: ' + online.size + '\n');
});
const wss = new WebSocket.Server({ server, maxPayload: 4096 });
wss.on('connection', (ws) => {
  const c = { ws, id: '', name: '', watch: new Set() };
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });
  ws.on('message', (data) => {
    const line = data.toString();
    if (line.length > 600) return;
    try { handle(c, line.split('\t')); } catch (e) { log('error: ' + e.message); }
  });
  ws.on('close', () => drop(c));
  ws.on('error', () => {});
});
setInterval(() => {
  wss.clients.forEach((ws) => { if (!ws.isAlive) { ws.terminate(); return; } ws.isAlive = false; try { ws.ping(); } catch (e) {} });
}, 30000);
server.listen(PORT, () => log('Precision Games server is listening on port ' + PORT + '  (known players: ' + Object.keys(users).length + ')'));
