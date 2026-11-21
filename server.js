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
let avatars = {};               // id -> base64 JPEG (small profile picture)
let profiles = {};              // id -> {name, bio, decor, banner}
const chat = new Set();         // ids currently inside the chat room
const voice = new Map();        // id -> {muted}
let history = [];               // room messages
let dms = {};                   // "idA|idB" (sorted) -> [messages]
let dmq = {};                   // id -> undelivered DM messages
let seq = 0;

function log(s) { console.log(new Date().toTimeString().slice(0, 8) + '  ' + s); }
function load() {
  try { users = JSON.parse(fs.readFileSync(path.join(DATA, 'users.json'), 'utf8')); } catch (e) { users = {}; }
  try { pend = JSON.parse(fs.readFileSync(path.join(DATA, 'pending.json'), 'utf8')); } catch (e) { pend = []; }
  try { avatars = JSON.parse(fs.readFileSync(path.join(DATA, 'avatars.json'), 'utf8')); } catch (e) { avatars = {}; }
  try { profiles = JSON.parse(fs.readFileSync(path.join(DATA, 'profiles.json'), 'utf8')); } catch (e) { profiles = {}; }
  try { history = JSON.parse(fs.readFileSync(path.join(DATA, 'history.json'), 'utf8')); } catch (e) { history = []; }
  try { dms = JSON.parse(fs.readFileSync(path.join(DATA, 'dms.json'), 'utf8')); } catch (e) { dms = {}; }
  try { dmq = JSON.parse(fs.readFileSync(path.join(DATA, 'dmq.json'), 'utf8')); } catch (e) { dmq = {}; }
}
function saveJson(name, obj) { try { fs.writeFileSync(path.join(DATA, name), JSON.stringify(obj)); } catch (e) {} }
function saveAvatars() { try { fs.writeFileSync(path.join(DATA, 'avatars.json'), JSON.stringify(avatars)); } catch (e) {} }
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

// ================= Poppy's ChatRoom =================
const clean = (t, n) => String(t || '').replace(/[\t\r\n]+/g, ' ').slice(0, n);
function chatSend(fn) { for (const id of chat) { const o = online.get(id); if (o) fn(o); } }
function usersList() { return [...chat].map(id => id + ':' + ((online.get(id) || {}).name || '') + ':' + ((profiles[id] || {}).decor || 0)).join('|'); }
function reactStr(m) { return Object.keys(m.react || {}).filter(k => m.react[k].length).map(k => k + '~' + m.react[k].join(',')).join('|'); }
function msgLine(tag, m) { return [tag, m.id, m.from, m.name, m.ts, m.text, reactStr(m)]; }
function pairKey(a, b) { return a < b ? a + '|' + b : b + '|' + a; }
function sendProfile(to, id) {
  const pr = profiles[id] || {}; const o = online.get(id);
  send(to, 'PROF', id, pr.name || (o ? o.name : ''), pr.bio || '', String(pr.decor || 0), o ? '1' : '0');
  if (avatars[id]) send(to, 'AV', id, avatars[id]);
  if (pr.banner) send(to, 'BN', id, pr.banner);
}
let sharer = '';               // id of the player sharing their screen in the voice room
function broadcastShare() { for (const id of voice.keys()) { const o = online.get(id); if (o) send(o, 'SSHARE', sharer); } }
function leaveVoice(id) { if (!voice.delete(id)) return; if (sharer === id) sharer = ''; broadcastVoice(); broadcastShare(); }
function voiceList() { return [...voice.entries()].map(([id, v]) => id + ':' + (v.muted ? 1 : 0)).join('|'); }
function broadcastVoice() { const l = voiceList(); chatSend(o => send(o, 'VUSERS', l)); }
function leaveChat(c) {
  if (!chat.has(c.id)) return;
  leaveVoice(c.id);
  chat.delete(c.id); chatSend(o => { send(o, 'LEFT', c.id, c.name); send(o, 'USERS', usersList()); });
}
const lastMsg = new Map(); const RATE = process.env.NO_RATE ? 0 : 150;
function handleChat(c, p) {
  const cmd = p[0];
  if (cmd === 'JOINCHAT') {
    if (!chat.has(c.id)) {
      chat.add(c.id); const pr = profiles[c.id] = profiles[c.id] || {}; pr.name = c.name; saveJson('profiles.json', profiles);
      chatSend(o => { if (o !== c) send(o, 'JOINED', c.id, c.name); send(o, 'USERS', usersList()); });
    } else send(c, 'USERS', usersList());
    for (const m of history.slice(-100)) send(c, ...msgLine('H', m));
    send(c, 'HEND', '1'); send(c, 'VUSERS', voiceList());
    for (const m of (dmq[c.id] || [])) send(c, 'D', m.id, m.from, m.to, m.name, m.ts, m.text, reactStr(m));
    if (dmq[c.id]) { delete dmq[c.id]; saveJson('dmq.json', dmq); }
    return true;
  }
  if (cmd === 'LEAVECHAT') { leaveChat(c); return true; }
  if (cmd === 'GETPROF' && p.length > 1) { if (p[1].length < 40) sendProfile(c, p[1]); return true; }
  if (cmd === 'SETPROF') {
    const pr = profiles[c.id] = profiles[c.id] || {}; pr.name = c.name; pr.bio = clean(p[1], 190); pr.decor = Math.max(0, Math.min(9, parseInt(p[2]) || 0));
    saveJson('profiles.json', profiles);
    const targets = new Set([...chat]); for (const o of online.values()) if (o.watch.has(c.id)) targets.add(o.id);
    for (const id of targets) { const o = online.get(id); if (o) send(o, 'PROF', c.id, c.name, pr.bio, String(pr.decor), '1'); }
    return true;
  }
  if (cmd === 'BANNER' && p.length > 1) {
    if (p[1].length > 42000 || !/^[A-Za-z0-9+\/=]+$/.test(p[1])) { send(c, 'ERR', 'banner'); return true; }
    const pr = profiles[c.id] = profiles[c.id] || {}; pr.banner = p[1]; saveJson('profiles.json', profiles);
    chatSend(o => { if (o !== c) send(o, 'BN', c.id, p[1]); }); return true;
  }
  if (cmd === 'MSG' && p.length > 1) {
    if (!chat.has(c.id)) return true; const now = Date.now(); if (now - (lastMsg.get(c.id) || 0) < RATE) return true; lastMsg.set(c.id, now);
    const text = clean(p.slice(1).join(' '), 1000); if (!text.trim()) return true;
    const m = { id: now + '-' + (++seq), from: c.id, name: c.name, ts: now, text, react: {} };
    history.push(m); if (history.length > 200) history = history.slice(-200); saveJson('history.json', history);
    chatSend(o => send(o, ...msgLine('M', m))); return true;
  }
  if (cmd === 'DM' && p.length > 2) {
    const to = p[1]; if (!idRx.test(to) || !users[to]) { send(c, 'ERR', 'nouser', to); return true; }
    const now = Date.now(); if (now - (lastMsg.get(c.id) || 0) < RATE) return true; lastMsg.set(c.id, now);
    const text = clean(p.slice(2).join(' '), 1000); if (!text.trim()) return true;
    const m = { id: now + '-' + (++seq), from: c.id, to, name: c.name, ts: now, text, react: {} };
    const k = pairKey(c.id, to); (dms[k] = dms[k] || []).push(m); if (dms[k].length > 150) dms[k] = dms[k].slice(-150); saveJson('dms.json', dms);
    send(c, 'D', m.id, m.from, m.to, m.name, m.ts, m.text, ''); const o = online.get(to);
    if (o) send(o, 'D', m.id, m.from, m.to, m.name, m.ts, m.text, ''); else { (dmq[to] = dmq[to] || []).push(m); saveJson('dmq.json', dmq); }
    return true;
  }
  if (cmd === 'DMHIST' && p.length > 1) {
    for (const m of (dms[pairKey(c.id, p[1])] || []).slice(-80)) send(c, 'D', m.id, m.from, m.to, m.name, m.ts, m.text, reactStr(m));
    send(c, 'DEND', p[1]); return true;
  }
  if (cmd === 'TYPING' && p.length > 1) {
    if (p[1] === 'room') chatSend(o => { if (o !== c) send(o, 'TYPING', c.id, c.name, 'room'); });
    else { const o = online.get(p[1]); if (o) send(o, 'TYPING', c.id, c.name, 'dm'); }
    return true;
  }
  if (cmd === 'REACT' && p.length > 3) {
    const scope = p[1], mid = p[2], emoji = clean(p[3], 24); if (!emoji || /[|~,]/.test(emoji)) return true;
    let m;
    if (scope === 'room') m = history.find(x => x.id === mid); else m = (dms[pairKey(c.id, scope)] || []).find(x => x.id === mid);
    if (!m) return true; m.react = m.react || {}; const arr = m.react[emoji] = m.react[emoji] || [];
    const i = arr.indexOf(c.id); if (i >= 0) arr.splice(i, 1); else if (Object.keys(m.react).length <= 12) arr.push(c.id);
    if (!arr.length) delete m.react[emoji];
    if (scope === 'room') { saveJson('history.json', history); chatSend(o => send(o, 'RX', 'room', mid, reactStr(m))); }
    else { saveJson('dms.json', dms); send(c, 'RX', scope, mid, reactStr(m)); const o = online.get(scope); if (o) send(o, 'RX', c.id, mid, reactStr(m)); }
    return true;
  }
  if (cmd === 'VJOIN') { if (chat.has(c.id)) { voice.set(c.id, { muted: false }); broadcastVoice(); send(c, 'SSHARE', sharer); } return true; }
  if (cmd === 'VLEAVE') { leaveVoice(c.id); return true; }
  if (cmd === 'SSTART') { if (voice.has(c.id)) { if (sharer && sharer !== c.id && voice.has(sharer)) send(c, 'ERR', 'busy'); else { sharer = c.id; broadcastShare(); } } return true; }
  if (cmd === 'SSTOP') { if (sharer === c.id) { sharer = ''; broadcastShare(); } return true; }
  if (cmd === 'SF' && p.length > 1) { if (c.id === sharer && p[1].length < 110000) for (const id of voice.keys()) { if (id !== c.id) { const o = online.get(id); if (o && o.ws.bufferedAmount < 300000) send(o, 'SF', c.id, p[1]); } } return true; }
  if (cmd === 'VMUTE') { const v = voice.get(c.id); if (v) { v.muted = p[1] === '1'; broadcastVoice(); } return true; }
  if (cmd === 'VA' && p.length > 1) { if (voice.has(c.id) && p[1].length < 6000) for (const id of voice.keys()) { if (id !== c.id) { const o = online.get(id); if (o) send(o, 'VA', c.id, p[1]); } } return true; }
  return false;
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
    c.id = id; c.name = (p[2] || '').slice(0, 20); online.set(id, c); send(c, 'WELCOME', 'poppy2'); log(id + ' online (' + online.size + ' connected)');
    const mine = pend.filter(x => x[1] === id);
    for (const x of mine) { send(c, x[0] === 'REQ' ? 'FREQ' : x[0] === 'ACC' ? 'FACC' : x[0] === 'DEC' ? 'FDEC' : 'FREM', x[2]); }
    if (mine.length) { pend = pend.filter(x => x[1] !== id); savePend(); }
    presence(c, true);
    return;
  }
  if (!c.id) return;
  if (cmd === 'PING') { send(c, 'PONG'); return; }
  if (handleChat(c, p)) return;
  if (cmd === 'FRIENDS') {
    c.watch.clear();
    if (p.length > 1 && p[1]) for (const f of p[1].split(',')) {
      if (!idRx.test(f)) continue;
      c.watch.add(f); const o = online.get(f);
      send(c, 'PRES', f, o ? '1' : '0', o ? o.name : '');
      if (avatars[f]) send(c, 'AV', f, avatars[f]);
    }
    return;
  }
  if (cmd === 'AVATAR' && p.length > 1) {
    if (p[1].length > 12000 || !/^[A-Za-z0-9+\/=]+$/.test(p[1])) { send(c, 'ERR', 'avatar'); return; }
    avatars[c.id] = p[1]; saveAvatars();
    for (const o of online.values()) if (o !== c && (o.watch.has(c.id) || chat.has(o.id))) send(o, 'AV', c.id, p[1]);
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
  if (online.get(c.id) === c) { leaveChat(c); online.delete(c.id); leaveParty(c); presence(c, false); log(c.id + ' offline'); }
}

load();
const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
  res.end('Poppy server v2 (chat, voice, screen share, games) is running. Players online: ' + online.size + '\n');
});
const wss = new WebSocket.Server({ server, maxPayload: 262144 });
wss.on('connection', (ws) => {
  const c = { ws, id: '', name: '', watch: new Set() };
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });
  ws.on('message', (data) => {
    const line = data.toString();
    if (line.length > (line.startsWith('SF\t') ? 120000 : 45000)) return;
    try { handle(c, line.split('\t')); } catch (e) { log('error: ' + e.message); }
  });
  ws.on('close', () => drop(c));
  ws.on('error', () => {});
});
setInterval(() => {
  wss.clients.forEach((ws) => { if (!ws.isAlive) { ws.terminate(); return; } ws.isAlive = false; try { ws.ping(); } catch (e) {} });
}, 30000);
server.listen(PORT, () => log('Precision Games server is listening on port ' + PORT + '  (known players: ' + Object.keys(users).length + ')'));
