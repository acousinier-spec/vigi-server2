const http = require('http');
const WebSocket = require('ws');
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');

const port = process.env.PORT || 10000;
const DATA_FILE = process.env.DATA_FILE || path.join(__dirname, 'vigi-data.json');
const rooms = new Map();
const sessions = new Map(); // token -> email
let db = { users: {} }; // email -> { email, name, passHash, contacts: [] }

function loadDb() {
  try { if (fs.existsSync(DATA_FILE)) db = JSON.parse(fs.readFileSync(DATA_FILE, 'utf8')); }
  catch (e) { console.error('Impossible de charger la base locale:', e.message); }
  if (!db.users) db.users = {};
}
function saveDb() {
  try { fs.writeFileSync(DATA_FILE, JSON.stringify(db, null, 2)); }
  catch (e) { console.error('Impossible de sauvegarder la base locale:', e.message); }
}
loadDb();

function hashPassword(password, salt) {
  return crypto.createHash('sha256').update(String(password || '') + ':' + salt).digest('hex');
}
function createPasswordHash(password) {
  const salt = crypto.randomBytes(12).toString('hex');
  return salt + ':' + hashPassword(password, salt);
}
function verifyPassword(password, stored) {
  const [salt, hash] = String(stored || '').split(':');
  return !!salt && !!hash && hashPassword(password, salt) === hash;
}
function token() { return crypto.randomBytes(24).toString('hex'); }
function cleanEmail(v) { return String(v || '').trim().toLowerCase().slice(0, 120); }
function cleanName(v, fallback = 'Utilisateur') { return String(v || fallback).trim().slice(0, 80) || fallback; }
function cleanId(v) { return String(v || ('contact_' + Date.now() + '_' + crypto.randomBytes(3).toString('hex'))).slice(0, 80); }
function publicUser(u) { return { email: u.email, name: u.name, contacts: Array.isArray(u.contacts) ? u.contacts : [] }; }
function normalizeContact(c) {
  return {
    id: cleanId(c.id),
    name: cleanName(c.name, 'Contact'),
    email: cleanEmail(c.email),
    status: String(c.status || (c.email ? 'Contact synchronisé' : 'Contact sans email')).slice(0, 80),
    kind: c.kind === 'group' ? 'group' : 'contact',
    blocked: !!c.blocked,
    inDirectory: true,
    remoteId: String(c.remoteId || '').slice(0, 100)
  };
}
function upsertContact(user, contact) {
  user.contacts = Array.isArray(user.contacts) ? user.contacts : [];
  const c = normalizeContact(contact);
  const key = (c.email || c.id || c.name).toLowerCase();
  const i = user.contacts.findIndex(x => String(x.email || x.id || x.name).toLowerCase() === key);
  if (i >= 0) user.contacts[i] = { ...user.contacts[i], ...c, blocked: !!user.contacts[i].blocked || !!c.blocked };
  else user.contacts.push(c);
  saveDb();
  return user.contacts;
}
function removeContact(user, contactId) {
  user.contacts = (user.contacts || []).filter(c => c.id !== contactId && c.email !== contactId);
  saveDb();
  return user.contacts;
}
function blockContact(user, contactId, blocked) {
  user.contacts = user.contacts || [];
  const c = user.contacts.find(x => x.id === contactId || x.email === contactId);
  if (c) c.blocked = !!blocked;
  saveDb();
  return user.contacts;
}

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
    return sendHttp(res, 200, 'Vigi Messenger signaling server OK\nWebSocket: ready on /ws\nAccounts: login/register enabled\nContacts: synchronized when server storage is available\n');
  }
  return sendHttp(res, 404, 'Not found\n');
});

const wss = new WebSocket.Server({ noServer: true });
server.on('upgrade', (req, socket, head) => {
  const reqPath = (req.url || '').split('?')[0];
  if (reqPath !== '/ws' && reqPath !== '/') {
    socket.write('HTTP/1.1 404 Not Found\r\n\r\n');
    socket.destroy();
    return;
  }
  wss.handleUpgrade(req, socket, head, ws => wss.emit('connection', ws, req));
});

function id() { return crypto.randomBytes(4).toString('hex'); }
function send(ws, payload) { if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(payload)); }
function safeString(value, fallback = '') { return String(value || fallback).slice(0, 100); }
function publicClient(c) { return { id: c.id, name: c.name, email: c.email || '', role: c.role || 'user' }; }
function roomMembers(room) { return [...(rooms.get(room) || new Map()).values()].filter(c => c.ws.readyState === WebSocket.OPEN); }
function broadcast(room, payload, exceptId = null) { for (const client of roomMembers(room)) if (client.id !== exceptId) send(client.ws, payload); }
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
function requireAuth(client, ws) {
  if (!client.user) { send(ws, { type: 'auth-required', message: 'Connexion utilisateur requise.' }); return false; }
  return true;
}
function authOk(client, ws, user) {
  client.user = user;
  client.email = user.email;
  client.name = user.name;
  const t = token(); sessions.set(t, user.email);
  send(ws, { type: 'auth-ok', token: t, user: publicUser(user) });
}

wss.on('connection', ws => {
  const client = { id: id(), ws, room: null, name: 'Invité', email: '', role: 'user', alive: true, user: null };
  send(ws, { type: 'hello', id: client.id, maxParticipants: 12, server: 'render-ws-ok-auth' });
  ws.on('pong', () => { client.alive = true; });

  ws.on('message', raw => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return send(ws, { type: 'error', message: 'JSON invalide' }); }
    if (msg.type === 'ping') return send(ws, { type: 'pong', time: Date.now() });

    if (msg.type === 'auth-register') {
      const email = cleanEmail(msg.email); const name = cleanName(msg.name); const password = String(msg.password || '');
      if (!email || !email.includes('@')) return send(ws, { type: 'auth-error', message: 'Email invalide.' });
      if (password.length < 4) return send(ws, { type: 'auth-error', message: 'Mot de passe trop court (minimum 4 caractères).' });
      if (db.users[email]) return send(ws, { type: 'auth-error', message: 'Ce compte existe déjà. Connecte-toi.' });
      const user = { email, name, passHash: createPasswordHash(password), contacts: [] };
      db.users[email] = user; saveDb();
      return authOk(client, ws, user);
    }
    if (msg.type === 'auth-login') {
      const email = cleanEmail(msg.email); const password = String(msg.password || ''); const user = db.users[email];
      if (!user || !verifyPassword(password, user.passHash)) return send(ws, { type: 'auth-error', message: 'Email ou mot de passe incorrect.' });
      return authOk(client, ws, user);
    }
    if (msg.type === 'auth-resume') {
      const email = sessions.get(String(msg.token || ''));
      const user = email ? db.users[email] : null;
      if (!user) return send(ws, { type: 'auth-required' });
      return authOk(client, ws, user);
    }

    if (!requireAuth(client, ws)) return;

    if (msg.type === 'profile-update') {
      client.user.name = cleanName(msg.name, client.user.name);
      client.name = client.user.name; saveDb();
      send(ws, { type: 'auth-ok', token: msg.token || '', user: publicUser(client.user) });
      if (client.room) broadcast(client.room, { type: 'participants', participants: roomMembers(client.room).map(publicClient) });
      return;
    }
    if (msg.type === 'contact-add') return send(ws, { type: 'contact-list', contacts: upsertContact(client.user, msg.contact || {}) });
    if (msg.type === 'contact-delete') return send(ws, { type: 'contact-list', contacts: removeContact(client.user, String(msg.contactId || '')) });
    if (msg.type === 'contact-block') return send(ws, { type: 'contact-list', contacts: blockContact(client.user, String(msg.contactId || ''), !!msg.blocked) });
    if (msg.type === 'contacts-get') return send(ws, { type: 'contact-list', contacts: client.user.contacts || [] });

    if (msg.type === 'join') {
      leave(client);
      const room = safeString(msg.room, 'levigilant');
      const members = roomMembers(room);
      if (members.length >= 12) return send(ws, { type: 'room-full', maxParticipants: 12 });
      client.room = room;
      client.name = client.user.name;
      client.email = client.user.email;
      client.role = msg.role === 'admin' ? 'admin' : 'user';
      if (!rooms.has(room)) rooms.set(room, new Map());
      rooms.get(room).set(client.id, client);
      send(ws, { type: 'joined', id: client.id, room, participants: roomMembers(room).map(publicClient) });
      broadcast(room, { type: 'peer-joined', participant: publicClient(client) }, client.id);
      broadcast(room, { type: 'participants', participants: roomMembers(room).map(publicClient) });
      return;
    }

    if (!client.room) return send(ws, { type: 'error', message: 'Rejoins un salon avant d’envoyer.' });
    const payload = { ...msg, room: client.room, from: client.id, sender: client.name, email: client.email, role: client.role };
    if (msg.to) {
      const target = rooms.get(client.room)?.get(msg.to);
      if (target) send(target.ws, payload);
    } else broadcast(client.room, payload, client.id);
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

server.listen(port, '0.0.0.0', () => console.log('Vigi Messenger signaling server listening on port ' + port));
