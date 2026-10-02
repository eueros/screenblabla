// ScreenShare — servidor de sinalização. As telas trafegam direto entre os navegadores (WebRTC).
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const MAX_USERS = 15;
const rooms = new Map(); // code -> { clients: Map(id -> client), counter }
const INDEX = path.join(__dirname, 'public', 'index.html');

function newCode() {
  const abc = 'abcdefghjkmnpqrstuvwxyz23456789';
  let c;
  do {
    c = Array.from(crypto.randomBytes(8), b => abc[b % abc.length]).join('');
  } while (rooms.has(c));
  return c;
}

const server = http.createServer((req, res) => {
  if (req.method === 'POST' && req.url === '/api/rooms') {
    const code = newCode();
    rooms.set(code, { clients: new Map(), counter: 0 });
    // sala criada e nunca usada é descartada (não é limite de uso: só limpa lixo)
    setTimeout(() => {
      const r = rooms.get(code);
      if (r && r.clients.size === 0) rooms.delete(code);
    }, 10 * 60 * 1000);
    res.writeHead(200, { 'Content-Type': 'application/json' });
    return res.end(JSON.stringify({ code }));
  }
  if (req.method === 'GET' && (req.url === '/' || /^\/r\/[a-z0-9]+$/.test(req.url.split('?')[0]))) {
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    return fs.createReadStream(INDEX).pipe(res);
  }
  res.writeHead(404);
  res.end('Not found');
});

const wss = new WebSocketServer({ server, path: '/ws', maxPayload: 64 * 1024 });

function send(ws, obj) {
  if (ws.readyState === 1) ws.send(JSON.stringify(obj));
}
function broadcast(room, obj, exceptId) {
  for (const [id, c] of room.clients) if (id !== exceptId) send(c.ws, obj);
}

wss.on('connection', (ws, req) => {
  const code = new URL(req.url, 'http://x').searchParams.get('room');
  const room = rooms.get(code);
  if (!room) { send(ws, { type: 'error', reason: 'not-found' }); return ws.close(); }
  // Limite aplicado aqui, no servidor
  if (room.clients.size >= MAX_USERS) { send(ws, { type: 'error', reason: 'full' }); return ws.close(); }

  const id = crypto.randomBytes(6).toString('hex');
  const client = { id, ws, name: 'Convidado ' + (++room.counter), share: null, alive: true };
  const existing = [...room.clients.values()].map(c => ({ id: c.id, name: c.name, share: c.share }));
  room.clients.set(id, client);

  send(ws, { type: 'welcome', id, name: client.name, peers: existing, max: MAX_USERS });
  broadcast(room, { type: 'peer-joined', peer: { id, name: client.name, share: null }, count: room.clients.size }, id);
  send(ws, { type: 'count', count: room.clients.size });

  ws.on('pong', () => { client.alive = true; });
  ws.on('message', raw => {
    let m;
    try { m = JSON.parse(raw); } catch { return; }
    if (m.type === 'signal') {
      const target = room.clients.get(m.to);
      if (target) send(target.ws, { type: 'signal', from: id, data: m.data });
    } else if (m.type === 'share') {
      client.share = m.on ? String(m.label || '').slice(0, 40) : null;
      broadcast(room, { type: 'share', id, share: client.share }, id);
    }
  });
  ws.on('close', () => {
    room.clients.delete(id);
    if (room.clients.size === 0) return rooms.delete(code); // última pessoa saiu
    broadcast(room, { type: 'peer-left', id, count: room.clients.size });
  });
});

setInterval(() => {
  for (const ws of wss.clients) {
    const alive = ws._alive !== false;
    ws._alive = false;
    ws.ping();
  }
}, 30000);
wss.on('connection', ws => { ws._alive = true; ws.on('pong', () => { ws._alive = true; }); });
setInterval(() => {
  for (const ws of wss.clients) if (ws._alive === false) ws.terminate();
}, 65000);

server.listen(PORT, () => console.log('ScreenShare em http://localhost:' + PORT));
