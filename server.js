const http = require('http');
const WebSocket = require('ws');
const crypto = require('crypto');

const port = process.env.PORT || 10000;
const rooms = new Map();

function sendHttp(res, code, body, type = 'text/plain; charset=utf-8') {
  res.writeHead(code, {
    'Content-Type': type,
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type'
  });
  res.end(body);
}

const server = http.createServer((req, res) => {
  if (req.method === 'OPTIONS') return sendHttp(res, 204, '');
  if (req.url === '/health' || req.url === '/' || req.url === '/ws') {
    return sendHttp(res, 200, 'Vigi Messenger signaling server OK\nWebSocket: ready on /ws\n');
  }
  return sendHttp(res, 404, 'Not found\n');
});

const wss = new WebSocket.Server({ noServer: true });

server.on('upgrade', (req, socket, head) => {
  const path = (req.url || '').split('?')[0];
  // Render accepte le WebSocket sur le même domaine HTTPS. On accepte /ws et / pour compatibilité.
  if (path !== '/ws' && path !== '/') {
    socket.write('HTTP/1.1 404 Not Found\r\n\r\n');
    socket.destroy();
    return;
  }
  wss.handleUpgrade(req, socket, head, ws => wss.emit('connection', ws, req));
});

function id() { return crypto.randomBytes(4).toString('hex'); }
function send(ws, payload) {
  if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(payload));
}
function safeString(value, fallback = '') {
  return String(value || fallback).slice(0, 100);
}
function publicClient(c) { return { id: c.id, name: c.name, role: c.role || 'user' }; }
function roomMembers(room) {
  return [...(rooms.get(room) || new Map()).values()].filter(c => c.ws.readyState === WebSocket.OPEN);
}
function broadcast(room, payload, exceptId = null) {
  for (const client of roomMembers(room)) {
    if (client.id !== exceptId) send(client.ws, payload);
  }
}
function leave(client) {
  if (!client.room) return;
  const map = rooms.get(client.room);
  if (map) {
    map.delete(client.id);
    broadcast(client.room, { type: 'peer-left', id: client.id, name: client.name });
    broadcast(client.room, { type: 'participants', participants: roomMembers(client.room).map(publicClient) });
    if (!map.size) rooms.delete(client.room);
  }
  client.room = null;
}

wss.on('connection', ws => {
  const client = { id: id(), ws, room: null, name: 'Invité', role: 'user', alive: true };
  send(ws, { type: 'hello', id: client.id, maxParticipants: 12, server: 'render-ws-ok' });

  ws.on('pong', () => { client.alive = true; });

  ws.on('message', raw => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return send(ws, { type: 'error', message: 'JSON invalide' }); }

    if (msg.type === 'ping') return send(ws, { type: 'pong', time: Date.now() });

    if (msg.type === 'join') {
      leave(client);
      const room = safeString(msg.room, 'levigilant');
      const members = roomMembers(room);
      if (members.length >= 12) return send(ws, { type: 'room-full', maxParticipants: 12 });
      client.room = room;
      client.name = safeString(msg.name || msg.from || client.id, client.id);
      client.role = msg.role === 'admin' ? 'admin' : 'user';
      if (!rooms.has(room)) rooms.set(room, new Map());
      rooms.get(room).set(client.id, client);
      send(ws, { type: 'joined', id: client.id, room, participants: roomMembers(room).map(publicClient) });
      broadcast(room, { type: 'peer-joined', participant: publicClient(client) }, client.id);
      broadcast(room, { type: 'participants', participants: roomMembers(room).map(publicClient) });
      return;
    }

    if (!client.room) return send(ws, { type: 'error', message: 'Rejoins un salon avant d’envoyer.' });
    const payload = { ...msg, room: client.room, from: client.id, sender: client.name, role: client.role };

    if (msg.to) {
      const target = rooms.get(client.room)?.get(msg.to);
      if (target) send(target.ws, payload);
    } else {
      broadcast(client.room, payload, client.id);
    }
  });

  ws.on('close', () => leave(client));
  ws.on('error', () => leave(client));
});

setInterval(() => {
  for (const client of [...rooms.values()].flatMap(room => [...room.values()])) {
    if (client.ws.readyState !== WebSocket.OPEN) { leave(client); continue; }
    if (!client.alive) { client.ws.terminate(); leave(client); continue; }
    client.alive = false;
    try { client.ws.ping(); } catch { leave(client); }
  }
}, 25000);

server.listen(port, '0.0.0.0', () => {
  console.log('Vigi Messenger signaling server listening on port ' + port);
});
