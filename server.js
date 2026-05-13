const http = require('http');
const WebSocket = require('ws');
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
const { MongoClient } = require('mongodb');
let LiveKitAccessToken = null;
try { ({ AccessToken: LiveKitAccessToken } = require('livekit-server-sdk')); } catch (e) { /* ESM fallback chargé à la demande */ }

const port = process.env.PORT || 10000;
const JWT_SECRET = process.env.JWT_SECRET || 'CHANGE_ME_RENDER_SECRET_' + crypto.randomBytes(16).toString('hex');
const MONGO_URI = process.env.MONGO_URI || '';
const DATA_FILE = process.env.DATA_FILE || path.join(__dirname, 'vigi-data.json');
const MAX_PARTICIPANTS = 12;
const HISTORY_LIMIT = 100;
const PRESENCE_ROOM = 'vigi-presence';
const ADMIN_EMAILS = ['a.cousinier@gmail.com','j.leduc@levigilant.com'];
const LIVEKIT_URL = process.env.LIVEKIT_URL || '';
const LIVEKIT_API_KEY = process.env.LIVEKIT_API_KEY || '';
const LIVEKIT_API_SECRET = process.env.LIVEKIT_API_SECRET || '';
function liveKitConfigured(){ return !!(LIVEKIT_URL && LIVEKIT_API_KEY && LIVEKIT_API_SECRET); }
async function getLiveKitAccessTokenClass(){
  if (LiveKitAccessToken) return LiveKitAccessToken;
  try { const mod = await import('livekit-server-sdk'); LiveKitAccessToken = mod.AccessToken; return LiveKitAccessToken; }
  catch (e) { throw new Error('LiveKit SDK absent. Lance npm install après avoir mis à jour package.json.'); }
}

const rooms = new Map();
const activeConferenceCalls = new Map();
let store;

function cleanEmail(v) { return String(v || '').trim().toLowerCase().slice(0, 160); }
function cleanName(v, fallback = 'Utilisateur') { return String(v || fallback).trim().slice(0, 80) || fallback; }
function cleanId(v) { return String(v || ('contact_' + Date.now() + '_' + crypto.randomBytes(3).toString('hex'))).slice(0, 120); }
function safeString(value, fallback = '') { return String(value || fallback).slice(0, 200); }
function escapeRegex(value) { return String(value || '').replace(/[.*+?^${}()|[\]\\]/g, '\\$&'); }
function messageBelongsToEmail(message, email) {
  const e = cleanEmail(email);
  if (!e) return false;
  return cleanEmail(message?.fromEmail) === e || cleanEmail(message?.toEmail) === e || String(message?.conversation || '').toLowerCase().includes(e);
}
function messageCreatedAtMs(message) {
  const raw = message?.createdAt || message?.time || message?.date || message?.timestamp || 0;
  const d = raw instanceof Date ? raw : new Date(raw);
  const t = d.getTime();
  return Number.isFinite(t) ? t : 0;
}
function publicUser(u) { return { email: u.email, name: u.name, contacts: Array.isArray(u.contacts) ? u.contacts : [], conferences: Array.isArray(u.conferences) ? u.conferences : [] }; }
function isJeanLeducEmail(email) { return cleanEmail(email) === 'j.leduc@levigilant.com'; }
const JEAN_REQUIRED_CONTACTS = [
  { id:'contact_alexandre_admin', name:'Alexandre', email:'a.cousinier@gmail.com', status:'Contact synchronisé', kind:'contact', inDirectory:true },
  { id:'contact_marylin_default', name:'Marylin', email:'', status:'Contact', kind:'contact', inDirectory:true },
  { id:'contact_sam_default', name:'Sam', email:'', status:'Contact', kind:'contact', inDirectory:true },
  { id:'contact_sebastien_default', name:'Sébastien', email:'solagraciagapeo@gmail.com', status:'Contact synchronisé', kind:'contact', inDirectory:true },
  { id:'contact_audrey_default', name:'Audrey', email:'', status:'Contact', kind:'contact', inDirectory:true }
];
const FABRICE_CONTACT = { id:'contact_fabrice_default', name:'Fabrice', email:'fredericsfamily@gmail.com', status:'Contact synchronisé', kind:'contact', inDirectory:true };
function isFabriceTargetUser(user) {
  const email = cleanEmail(user?.email);
  const name = String(user?.name || '').trim().toLowerCase();
  return email === 'a.cousinier@gmail.com' ||
    email === 'j.leduc@levigilant.com' ||
    email === 'solagraciagapeo@gmail.com' ||
    name.includes('marylin') || name.includes('marilyn') ||
    name.includes('sam') ||
    name.includes('audrey') ||
    name.includes('sébastien') || name.includes('sebastien') ||
    name.includes('alexandre');
}

function normalizeContact(c) {
  return {
    id: cleanId(c.id),
    name: cleanName(c.name, 'Contact'),
    email: cleanEmail(c.email),
    status: String(c.status || (c.email ? 'Contact synchronisé' : 'Contact sans email')).slice(0, 100),
    kind: c.kind === 'group' ? 'group' : 'contact',
    blocked: !!c.blocked,
    inDirectory: true,
    remoteId: String(c.remoteId || '').slice(0, 120)
  };
}
function contactKey(c) { return String(c?.email || c?.id || c?.name || '').toLowerCase(); }
function normalizeConference(conf) {
  const participants = Array.isArray(conf?.participants) ? conf.participants.map(cleanEmail).filter(Boolean) : [];
  const conferenceId = safeString(conf?.conferenceId || conf?.id || ('conf_' + Date.now()));
  return {
    conferenceId,
    id: conferenceId,
    kind: 'conference',
    name: cleanName(conf?.name, 'Conférence'),
    ownerEmail: cleanEmail(conf?.ownerEmail),
    participants: [...new Set(participants)],
    active: !!conf?.active,
    livekitRoom: safeString(conf?.livekitRoom || ''),
    createdAt: conf?.createdAt || new Date().toISOString()
  };
}
function conversationKey(room, senderEmail, targetEmail) {
  if (targetEmail) return ['dm', room, senderEmail, targetEmail].sort().join('::');
  return 'room::' + room;
}
function sendHttp(res, code, body, type = 'text/plain; charset=utf-8') {
  res.writeHead(code, {
    'Content-Type': type,
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, Authorization'
  });
  res.end(body);
}
function readJson(req) {
  return new Promise((resolve, reject) => {
    let data = '';
    req.on('data', chunk => {
      data += chunk;
      if (data.length > 1024 * 1024) reject(new Error('Payload trop volumineux'));
    });
    req.on('end', () => {
      try { resolve(data ? JSON.parse(data) : {}); } catch { reject(new Error('JSON invalide')); }
    });
    req.on('error', reject);
  });
}
// Lecture de gros corps (pour les chunks de fichiers — jusqu'à 2 MB)
function readLargeBody(req, maxBytes = 2 * 1024 * 1024) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    let total = 0;
    req.on('data', chunk => {
      total += chunk.length;
      if (total > maxBytes) return reject(new Error('Chunk trop volumineux'));
      chunks.push(chunk);
    });
    req.on('end', () => {
      try { resolve(JSON.parse(Buffer.concat(chunks).toString('utf8'))); }
      catch { reject(new Error('JSON invalide')); }
    });
    req.on('error', reject);
  });
}
function sendJson(res, code, obj) { return sendHttp(res, code, JSON.stringify(obj), 'application/json; charset=utf-8'); }
async function userFromAuth(req) {
  const header = req.headers.authorization || '';
  const token = header.startsWith('Bearer ') ? header.slice(7) : '';
  const decoded = verifyToken(token);
  if (!decoded?.email) return null;
  return await store.getUser(cleanEmail(decoded.email));
}
function sanitizeRoomName(v) { return String(v || '').toLowerCase().replace(/[^a-z0-9_-]+/g, '-').replace(/^-|-$/g, '').slice(0, 120) || ('vigi-' + Date.now()); }
function id() { return crypto.randomBytes(4).toString('hex'); }
function send(ws, payload) { if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(payload)); }
function publicClient(c) { return { id: c.id, name: c.name, email: c.email || '', role: c.role || 'user' }; }
function roomMembers(room) { return [...(rooms.get(room) || new Map()).values()].filter(c => c.ws.readyState === WebSocket.OPEN); }
function broadcast(room, payload, exceptId = null) { for (const client of roomMembers(room)) if (client.id !== exceptId) send(client.ws, payload); }
function conferenceCallKey(conferenceId, livekitRoom = '') {
  return safeString(conferenceId || livekitRoom || '').toLowerCase();
}
function addConferenceCaller(client, msg = {}) {
  const key = conferenceCallKey(msg.conferenceId, msg.livekitRoom);
  if (!key || !client?.user?.email) return 0;
  if (!activeConferenceCalls.has(key)) activeConferenceCalls.set(key, new Map());
  activeConferenceCalls.get(key).set(client.id, {
    id: client.id,
    email: cleanEmail(client.user.email),
    name: client.user.name || client.name || '',
    conferenceId: safeString(msg.conferenceId || ''),
    livekitRoom: safeString(msg.livekitRoom || ''),
    joinedAt: Date.now()
  });
  return activeConferenceCalls.get(key).size;
}
function removeConferenceCaller(client, msg = {}) {
  const wantedKey = conferenceCallKey(msg.conferenceId, msg.livekitRoom);
  let remaining = 0;
  const changes = [];
  for (const [key, callers] of activeConferenceCalls) {
    if (wantedKey && key !== wantedKey) continue;
    let info = null;
    if (callers.has(client.id)) info = callers.get(client.id);
    callers.delete(client.id);
    if (client?.user?.email) {
      const email = cleanEmail(client.user.email);
      for (const [id, item] of [...callers]) {
        if (cleanEmail(item.email) === email) {
          if (!info) info = item;
          callers.delete(id);
        }
      }
    }
    const activeCount = callers.size;
    changes.push({
      conferenceId: safeString(msg.conferenceId || info?.conferenceId || key),
      livekitRoom: safeString(msg.livekitRoom || info?.livekitRoom || ''),
      activeCount
    });
    if (!activeCount) activeConferenceCalls.delete(key);
    else remaining += activeCount;
  }
  client._conferencePresenceChanges = changes;
  return remaining;
}
function conferenceActiveCount(conferenceId, livekitRoom = '') {
  const key = conferenceCallKey(conferenceId, livekitRoom);
  if (!key) return 0;
  return activeConferenceCalls.get(key)?.size || 0;
}
function broadcastConferenceCallPresence(conferenceId, livekitRoom, activeCount) {
  broadcast(PRESENCE_ROOM, {
    type: 'conference-call-presence',
    conferenceId: safeString(conferenceId || ''),
    livekitRoom: safeString(livekitRoom || ''),
    activeCount: Number(activeCount || 0)
  });
}
function leave(client) {
  removeConferenceCaller(client);
  for (const change of (client._conferencePresenceChanges || [])) {
    broadcastConferenceCallPresence(change.conferenceId, change.livekitRoom, change.activeCount);
  }
  client._conferencePresenceChanges = [];
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
function signToken(user) {
  return jwt.sign({ email: user.email, name: user.name }, JWT_SECRET, { expiresIn: '365d' });
}
function verifyToken(token) {
  try { return jwt.verify(String(token || ''), JWT_SECRET); } catch { return null; }
}

class FileStore {
  constructor(file) { this.file = file; this.db = { users: {}, messages: [] }; this.load(); }
  load() {
    try { if (fs.existsSync(this.file)) this.db = JSON.parse(fs.readFileSync(this.file, 'utf8')); }
    catch (e) { console.error('Impossible de charger la base locale:', e.message); }
    if (!this.db.users) this.db.users = {};
    if (!Array.isArray(this.db.messages)) this.db.messages = [];
    for (const email of Object.keys(this.db.users)) { if (!Array.isArray(this.db.users[email].conferences)) this.db.users[email].conferences = []; }
  }
  save() { try { fs.writeFileSync(this.file, JSON.stringify(this.db, null, 2)); } catch (e) { console.error('Sauvegarde locale impossible:', e.message); } }
  async getUser(email) { return this.db.users[email] || null; }
  async listUsers() { return Object.values(this.db.users || {}); }
  async createUser(user) { if (!Array.isArray(user.conferences)) user.conferences = []; this.db.users[user.email] = user; this.save(); return user; }
  async updateUser(email, patch) { const u = this.db.users[email]; if (!u) return null; Object.assign(u, patch); this.save(); return u; }
  async upsertContact(email, contact) {
    const u = this.db.users[email]; if (!u) return [];
    u.contacts = Array.isArray(u.contacts) ? u.contacts : [];
    const c = normalizeContact(contact);
    const key = contactKey(c);
    const i = u.contacts.findIndex(x => contactKey(x) === key);
    if (i >= 0) u.contacts[i] = { ...u.contacts[i], ...c, blocked: !!u.contacts[i].blocked || !!c.blocked };
    else u.contacts.push(c);
    this.save(); return u.contacts;
  }
  async removeContact(email, contactId) {
    const u = this.db.users[email]; if (!u) return [];
    u.contacts = (u.contacts || []).filter(c => c.id !== contactId && c.email !== contactId);
    this.save(); return u.contacts;
  }
  async blockContact(email, contactId, blocked) {
    const u = this.db.users[email]; if (!u) return [];
    u.contacts = u.contacts || [];
    const c = u.contacts.find(x => x.id === contactId || x.email === contactId);
    if (c) c.blocked = !!blocked;
    this.save(); return u.contacts;
  }
  async saveMessage(msg) { this.db.messages.push(msg); if (this.db.messages.length > 5000) this.db.messages = this.db.messages.slice(-5000); this.save(); return msg; }
  async history(key) { return this.db.messages.filter(m => m.conversation === key).slice(-HISTORY_LIMIT); }
  async deleteMessagesForUserBefore(email, beforeMs) {
    const limit = Number(beforeMs || 0);
    if (!cleanEmail(email) || !Number.isFinite(limit) || limit <= 0) return 0;
    const before = this.db.messages.length;
    this.db.messages = this.db.messages.filter(m => !(messageBelongsToEmail(m, email) && messageCreatedAtMs(m) <= limit));
    const removed = before - this.db.messages.length;
    if (removed > 0) this.save();
    return removed;
  }
  async saveConferenceForUsers(conf) {
    const c = normalizeConference(conf);
    const targets = [...new Set([c.ownerEmail, ...c.participants].filter(Boolean))];
    for (const email of targets) {
      const u = this.db.users[email];
      if (!u) continue;
      u.conferences = Array.isArray(u.conferences) ? u.conferences : [];
      const idx = u.conferences.findIndex(x => x.conferenceId === c.conferenceId);
      if (idx >= 0) u.conferences[idx] = { ...u.conferences[idx], ...c };
      else u.conferences.push(c);
    }
    this.save();
    return c;
  }
  async getConferences(email) { const u = this.db.users[email]; return Array.isArray(u?.conferences) ? u.conferences : []; }
  async removeConferenceForUser(email, conferenceId) {
    const u = this.db.users[email]; if(!u) return [];
    u.conferences = (u.conferences || []).filter(c => String(c.conferenceId || c.id || '') !== String(conferenceId || ''));
    this.save(); return u.conferences;
  }
  // Fichiers sur disque (fallback local — non persistant sur Render)
  async saveFile(transferId, fileName, buffer) {
    const dir = path.join(__dirname, 'vigi-uploads', transferId);
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(path.join(dir, fileName), buffer);
  }
  async fileExists(transferId, fileName) {
    return fs.existsSync(path.join(__dirname, 'vigi-uploads', transferId, fileName));
  }
  streamFile(transferId, fileName, res) {
    return new Promise((resolve, reject) => {
      const p = path.join(__dirname, 'vigi-uploads', transferId, fileName);
      if (!fs.existsSync(p)) return reject(new Error('not found'));
      const stat = fs.statSync(p);
      res.setHeader('Content-Length', stat.size);
      fs.createReadStream(p).pipe(res).on('finish', resolve).on('error', reject);
    });
  }
}


class MongoStore {
  constructor(client) {
    this.client = client;
    this.db = client.db(process.env.MONGO_DB || 'vigi_messenger');
    this.users = this.db.collection('users');
    this.messages = this.db.collection('messages');
  }
  async init() {
    await this.users.createIndex({ email: 1 }, { unique: true });
    await this.messages.createIndex({ conversation: 1, createdAt: -1 });
    // GridFS bucket pour les fichiers — persistant dans MongoDB Atlas
    const { GridFSBucket } = require('mongodb');
    this.bucket = new GridFSBucket(this.db, { bucketName: 'vigi_files' });
  }
  // Sauvegarde un fichier dans GridFS (survit aux redémarrages Render)
  async saveFile(transferId, fileName, buffer, metadata = {}) {
    const uploadStream = this.bucket.openUploadStream(fileName, {
      metadata: { transferId, ...metadata, createdAt: new Date() }
    });
    await new Promise((resolve, reject) => {
      uploadStream.on('finish', resolve);
      uploadStream.on('error', reject);
      uploadStream.write(buffer);
      uploadStream.end();
    });
    // Nettoyer les anciens fichiers du même transferId
    try {
      const old = await this.bucket.find({ 'metadata.transferId': transferId }).toArray();
      for (const f of old) if (String(f._id) !== String(uploadStream.id)) await this.bucket.delete(f._id);
    } catch {}
    return uploadStream.id;
  }
  // Vérifie si un fichier existe dans GridFS
  async fileExists(transferId, fileName) {
    const files = await this.bucket.find({ 'metadata.transferId': transferId, filename: fileName }).limit(1).toArray();
    return files.length > 0;
  }
  // Stream un fichier depuis GridFS vers la réponse HTTP
  streamFile(transferId, fileName, res) {
    return new Promise((resolve, reject) => {
      const stream = this.bucket.openDownloadStreamByName(fileName, {});
      // Chercher par transferId si le nom seul ne suffit pas
      stream.on('error', async () => {
        try {
          const files = await this.bucket.find({ 'metadata.transferId': transferId }).limit(1).toArray();
          if (!files.length) return reject(new Error('not found'));
          this.bucket.openDownloadStream(files[0]._id).pipe(res).on('finish', resolve).on('error', reject);
        } catch (e) { reject(e); }
      });
      stream.pipe(res).on('finish', resolve).on('error', reject);
    });
  }
  async getUser(email) { return await this.users.findOne({ email }); }
  async listUsers() { return await this.users.find({}).project({ email:1, name:1 }).toArray(); }
  async createUser(user) { await this.users.insertOne(user); return user; }
  async updateUser(email, patch) { await this.users.updateOne({ email }, { $set: patch }); return await this.getUser(email); }
  async upsertContact(email, contact) {
    const user = await this.getUser(email); if (!user) return [];
    const contacts = Array.isArray(user.contacts) ? user.contacts : [];
    const c = normalizeContact(contact);
    const key = contactKey(c);
    const i = contacts.findIndex(x => contactKey(x) === key);
    if (i >= 0) contacts[i] = { ...contacts[i], ...c, blocked: !!contacts[i].blocked || !!c.blocked };
    else contacts.push(c);
    await this.users.updateOne({ email }, { $set: { contacts } });
    return contacts;
  }
  async removeContact(email, contactId) {
    const user = await this.getUser(email); if (!user) return [];
    const contacts = (user.contacts || []).filter(c => c.id !== contactId && c.email !== contactId);
    await this.users.updateOne({ email }, { $set: { contacts } });
    return contacts;
  }
  async blockContact(email, contactId, blocked) {
    const user = await this.getUser(email); if (!user) return [];
    const contacts = user.contacts || [];
    const c = contacts.find(x => x.id === contactId || x.email === contactId);
    if (c) c.blocked = !!blocked;
    await this.users.updateOne({ email }, { $set: { contacts } });
    return contacts;
  }
  async saveMessage(msg) { await this.messages.insertOne(msg); return msg; }
  async history(key) { return await this.messages.find({ conversation: key }).sort({ createdAt: -1 }).limit(HISTORY_LIMIT).toArray().then(a => a.reverse()); }
  async deleteMessagesForUserBefore(email, beforeMs) {
    const e = cleanEmail(email);
    const limit = Number(beforeMs || 0);
    if (!e || !Number.isFinite(limit) || limit <= 0) return 0;
    const beforeDate = new Date(limit);
    const res = await this.messages.deleteMany({
      createdAt: { $lte: beforeDate },
      $or: [
        { fromEmail: e },
        { toEmail: e },
        { conversation: { $regex: escapeRegex(e), $options: 'i' } }
      ]
    });
    return res.deletedCount || 0;
  }
  async saveConferenceForUsers(conf) {
    const c = normalizeConference(conf);
    const targets = [...new Set([c.ownerEmail, ...c.participants].filter(Boolean))];
    for (const email of targets) {
      const user = await this.getUser(email);
      if (!user) continue;
      const conferences = Array.isArray(user.conferences) ? user.conferences : [];
      const idx = conferences.findIndex(x => x.conferenceId === c.conferenceId);
      if (idx >= 0) conferences[idx] = { ...conferences[idx], ...c };
      else conferences.push(c);
      await this.users.updateOne({ email }, { $set: { conferences } });
    }
    return c;
  }
  async getConferences(email) { const user = await this.getUser(email); return Array.isArray(user?.conferences) ? user.conferences : []; }
  async removeConferenceForUser(email, conferenceId) {
    const user = await this.getUser(email); if(!user) return [];
    const conferences = (user.conferences || []).filter(c => String(c.conferenceId || c.id || '') !== String(conferenceId || ''));
    await this.users.updateOne({ email }, { $set: { conferences } });
    return conferences;
  }
}


async function ensureManagedContacts(user) {
  if (!user?.email) return user;
  if (isJeanLeducEmail(user.email)) {
    const users = typeof store.listUsers === 'function' ? await store.listUsers() : [];
    for (const u of users || []) {
      const email = cleanEmail(u.email);
      if (!email || email === cleanEmail(user.email)) continue;
      await store.upsertContact(user.email, { name: cleanName(u.name, email), email, status:'Contact synchronisé', kind:'contact', inDirectory:true });
    }
    for (const c of JEAN_REQUIRED_CONTACTS) await store.upsertContact(user.email, c);
  }
  if (isFabriceTargetUser(user)) {
    await store.upsertContact(user.email, FABRICE_CONTACT);
  }
  return await store.getUser(user.email);
}
async function ensureJeanLeducContacts(user) {
  return ensureManagedContacts(user);
}

async function initStore() {
  if (MONGO_URI) {
    const client = new MongoClient(MONGO_URI);
    await client.connect();
    const mongo = new MongoStore(client);
    await mongo.init();
    console.log('MongoDB connected: persistent users, contacts, messages enabled');
    return mongo;
  }
  console.warn('MONGO_URI absent: fallback fichier local. Sur Render, ce stockage peut être perdu au redémarrage.');
  return new FileStore(DATA_FILE);
}

async function main() {
  store = await initStore();

  // --- Stockage temporaire pour les transferts de fichiers par chunks ---
  const UPLOAD_DIR = path.join(__dirname, 'vigi-uploads');
  if (!fs.existsSync(UPLOAD_DIR)) fs.mkdirSync(UPLOAD_DIR, { recursive: true });
  const chunkStore = new Map(); // transferId → { chunks, totalChunks, fileName, fileSize, ... }
  // Nettoyage automatique toutes les heures (fichiers > 24h supprimés)
  setInterval(() => {
    const cutoff = Date.now() - 24 * 60 * 60 * 1000;
    for (const [tid, info] of chunkStore) { if (info.createdAt < cutoff) chunkStore.delete(tid); }
    try {
      for (const f of fs.readdirSync(UPLOAD_DIR)) {
        const fp = path.join(UPLOAD_DIR, f);
        if (fs.statSync(fp).mtimeMs < cutoff) fs.rmSync(fp, { recursive: true, force: true });
      }
    } catch {}
  }, 60 * 60 * 1000);
  const server = http.createServer(async (req, res) => {
    if (req.method === 'OPTIONS') return sendHttp(res, 204, '');
    const pathname = (req.url || '/').split('?')[0];

    if (req.method === 'GET' && (pathname === '/health' || pathname === '/' || pathname === '/ws')) {
      return sendHttp(res, 200, `Vigi Messenger signaling server OK\nWebSocket: ready on /ws\nAccounts: enabled\nContacts: synchronized\nStorage: ${MONGO_URI ? 'MongoDB persistent' : 'local fallback'}\nLiveKit: ${liveKitConfigured() ? 'configured' : 'not configured'}\n`);
    }

    // ── POST /upload-chunk ── Réception d'un morceau de fichier ──────────
    if (req.method === 'POST' && pathname === '/upload-chunk') {
      try {
        const user = await userFromAuth(req);
        if (!user) return sendJson(res, 401, { ok: false, message: 'Non authentifié' });
        const body = await readLargeBody(req, 2 * 1024 * 1024);
        const { transferId, chunkIndex, totalChunks, fileName, fileSize, toClientId, room, sender, data } = body;
        if (!transferId || chunkIndex == null || !totalChunks || !data)
          return sendJson(res, 400, { ok: false, message: 'Paramètres manquants' });
        if (!chunkStore.has(transferId)) {
          chunkStore.set(transferId, {
            chunks: new Map(), totalChunks: Number(totalChunks),
            fileName: String(fileName || 'fichier').replace(/[^\w.\- ]/g, '_').slice(0, 200),
            fileSize: Number(fileSize || 0), fromEmail: user.email,
            toEmail: cleanEmail(body.toEmail || ''),
            toClientId: String(toClientId || ''), room: String(room || ''),
            sender: String(sender || user.name || ''), createdAt: Date.now()
          });
        }
        const transfer = chunkStore.get(transferId);
        transfer.chunks.set(Number(chunkIndex), Buffer.from(data, 'base64'));
        // Tous les chunks reçus → assembler le fichier
        if (transfer.chunks.size === transfer.totalChunks) {
          const buffers = [];
          for (let i = 0; i < transfer.totalChunks; i++) buffers.push(transfer.chunks.get(i));
          const fileBuffer = Buffer.concat(buffers);
          // Sauvegarder dans MongoDB GridFS (persistant) ou sur disque (fallback local)
          await store.saveFile(transferId, transfer.fileName, fileBuffer, {
            fromEmail: transfer.fromEmail, toEmail: transfer.toEmail
          });
          const downloadUrl = `/download/${transferId}/${encodeURIComponent(transfer.fileName)}`;
          // Sauvegarder le message dans la DB (sinon il disparaît au history-get)
          const conv = conversationKey(PRESENCE_ROOM, transfer.fromEmail, transfer.toEmail);
          await store.saveMessage({
            type: 'file', conversation: conv, room: PRESENCE_ROOM,
            fromEmail: transfer.fromEmail, fromName: transfer.sender,
            toEmail: transfer.toEmail, text: 'Document envoyé',
            file: { name: transfer.fileName, size: transfer.fileSize, url: downloadUrl, transferId },
            createdAt: new Date(), time: new Date().toISOString()
          });
          // Chercher le destinataire par email dans toutes les rooms (plus fiable que clientId)
          let target = rooms.get(transfer.room)?.get(transfer.toClientId);
          if (!target && transfer.toEmail) {
            for (const [, roomMap] of rooms) {
              for (const [, client] of roomMap) {
                if (cleanEmail(client.email) === transfer.toEmail) { target = client; break; }
              }
              if (target) break;
            }
          }
          if (target) send(target.ws, { type: 'file', sender: transfer.sender, email: transfer.fromEmail,
            text: 'Document envoyé', file: { name: transfer.fileName, size: transfer.fileSize, url: downloadUrl, transferId } });
          chunkStore.delete(transferId);
          return sendJson(res, 200, { ok: true, complete: true, url: downloadUrl });
        }
        return sendJson(res, 200, { ok: true, complete: false, received: transfer.chunks.size });
      } catch (e) {
        console.error('upload-chunk error:', e);
        return sendJson(res, 500, { ok: false, message: e.message });
      }
    }

    // ── GET /download/:transferId/:filename ── Téléchargement ────────────
    if (req.method === 'GET' && pathname.startsWith('/download/')) {
      const parts = pathname.slice('/download/'.length).split('/');
      if (parts.length < 2) return sendHttp(res, 404, 'Not found');
      const safeTid  = parts[0].replace(/[^a-zA-Z0-9\-]/g, '');
      const safeName = decodeURIComponent(parts[1]).replace(/[/\\]/g, '');
      res.writeHead(200, {
        'Content-Type': 'application/octet-stream',
        'Content-Disposition': `attachment; filename="${safeName}"`,
        'Access-Control-Allow-Origin': '*'
      });
      try {
        await store.streamFile(safeTid, safeName, res);
      } catch {
        if (!res.headersSent) sendHttp(res, 404, 'Fichier introuvable ou expiré (24h max)');
        else res.end();
      }
      return;
    }

    if (req.method === 'POST' && pathname === '/api/livekit/token') {
      try {
        if (!liveKitConfigured()) {
          return sendJson(res, 503, {
            message: 'LiveKit non configuré sur Render. Ajoute LIVEKIT_URL, LIVEKIT_API_KEY et LIVEKIT_API_SECRET dans Environment.'
          });
        }

        const user = await userFromAuth(req);
        if (!user) return sendJson(res, 401, { message: 'Session expirée. Reconnecte-toi.' });

        const body = await readJson(req);
        const roomName = sanitizeRoomName(body.roomName || ('vigi-room-' + Date.now()));
        const displayName = cleanName(body.displayName || user.name, user.name || 'Utilisateur');
        const AccessToken = await getLiveKitAccessTokenClass();
        const at = new AccessToken(LIVEKIT_API_KEY, LIVEKIT_API_SECRET, {
          identity: user.email,
          name: displayName
        });

        at.addGrant({
          room: roomName,
          roomJoin: true,
          canPublish: true,
          canSubscribe: true,
          canPublishData: true
        });

        const token = await at.toJwt();
        return sendJson(res, 200, { url: LIVEKIT_URL, token, roomName });
      } catch (e) {
        console.error('LiveKit token error:', e);
        return sendJson(res, 500, { message: 'Erreur token LiveKit: ' + (e.message || e) });
      }
    }

    return sendHttp(res, 404, 'Not found\n');
  });

  const wss = new WebSocket.Server({ noServer: true });
  server.on('upgrade', (req, socket, head) => {
    const reqPath = (req.url || '').split('?')[0];
    if (reqPath !== '/ws' && reqPath !== '/') {
      socket.write('HTTP/1.1 404 Not Found\r\n\r\n'); socket.destroy(); return;
    }
    wss.handleUpgrade(req, socket, head, ws => wss.emit('connection', ws, req));
  });

  async function authOk(client, ws, user) {
    user = await ensureManagedContacts(user);
    client.user = user; client.email = user.email; client.name = user.name;
    send(ws, { type: 'auth-ok', token: signToken(user), user: publicUser(user) });
  }

  wss.on('connection', ws => {
    const client = { id: id(), ws, room: null, name: 'Invité', email: '', role: 'user', alive: true, user: null };
    send(ws, { type: 'hello', id: client.id, maxParticipants: MAX_PARTICIPANTS, server: 'render-ws-pro' });
    ws.on('pong', () => { client.alive = true; });

    ws.on('message', async raw => {
      let msg;
      try { msg = JSON.parse(raw); } catch { return send(ws, { type: 'error', message: 'JSON invalide' }); }
      try {
        if (msg.type === 'ping') return send(ws, { type: 'pong', time: Date.now() });
        if (msg.type === 'auth-register') {
          const email = cleanEmail(msg.email); const name = cleanName(msg.name); const password = String(msg.password || '');
          if (!email || !email.includes('@')) return send(ws, { type: 'auth-error', message: 'Email invalide.' });
          if (password.length < 6) return send(ws, { type: 'auth-error', message: 'Mot de passe trop court (minimum 6 caractères).' });
          if (await store.getUser(email)) return send(ws, { type: 'auth-error', message: 'Ce compte existe déjà. Connecte-toi.' });
          const passwordHash = await bcrypt.hash(password, 12);
          const user = { email, name, passwordHash, contacts: [], conferences: [], createdAt: new Date(), updatedAt: new Date() };
          await store.createUser(user);
          return authOk(client, ws, user);
        }
        if (msg.type === 'auth-login') {
          const email = cleanEmail(msg.email); const password = String(msg.password || ''); let user = await store.getUser(email);
          if (!user) return send(ws, { type: 'auth-error', message: 'Compte introuvable.' });
          if (password && !(await bcrypt.compare(password, user.passwordHash || ''))) return send(ws, { type: 'auth-error', message: 'Email ou mot de passe incorrect.' });
          return authOk(client, ws, user);
        }
        if (msg.type === 'auth-jean-direct') {
          const email = 'j.leduc@levigilant.com';
          let user = await store.getUser(email);
          if (!user) {
            user = { email, name:'Jean Leduc', passwordHash:'', contacts: [], conferences: [], createdAt: new Date(), updatedAt: new Date() };
            await store.createUser(user);
          }
          return authOk(client, ws, user);
        }
        if (msg.type === 'auth-resume') {
          const decoded = verifyToken(msg.token);
          const user = decoded?.email ? await store.getUser(cleanEmail(decoded.email)) : null;
          if (!user) return send(ws, { type: 'auth-required' });
          return authOk(client, ws, user);
        }

        if (!requireAuth(client, ws)) return;

        if (msg.type === 'profile-update') {
          const user = await store.updateUser(client.user.email, { name: cleanName(msg.name, client.user.name), updatedAt: new Date() });
          client.user = user; client.name = user.name;
          send(ws, { type: 'auth-ok', token: signToken(user), user: publicUser(user) });
          if (client.room) broadcast(client.room, { type: 'participants', participants: roomMembers(client.room).map(publicClient) });
          return;
        }
        if (msg.type === 'contact-add') return send(ws, { type: 'contact-list', contacts: await store.upsertContact(client.user.email, msg.contact || {}) });
        if (msg.type === 'contact-delete') { const contactId = String(msg.contactId || ''); if (ADMIN_EMAILS.includes(cleanEmail(contactId))) return send(ws, { type: 'contact-list', contacts: (await store.getUser(client.user.email))?.contacts || [] }); return send(ws, { type: 'contact-list', contacts: await store.removeContact(client.user.email, contactId) }); }
        if (msg.type === 'contact-block') { const contactId = String(msg.contactId || ''); if (ADMIN_EMAILS.includes(cleanEmail(contactId))) return send(ws, { type: 'contact-list', contacts: (await store.getUser(client.user.email))?.contacts || [] }); return send(ws, { type: 'contact-list', contacts: await store.blockContact(client.user.email, contactId, !!msg.blocked) }); }
        if (msg.type === 'contacts-get') {
          let user = await store.getUser(client.user.email);
          user = await ensureManagedContacts(user);
          client.user = user;
          return send(ws, { type: 'contact-list', contacts: user?.contacts || [] });
        }
        if (msg.type === 'conferences-get') {
          return send(ws, { type: 'conference-list', conferences: await store.getConferences(client.user.email) });
        }
        if (msg.type === 'conference-save') {
          const conf = normalizeConference({ ...(msg.conference || {}), ownerEmail: client.user.email });
          const existingConferences = await store.getConferences(client.user.email);
          const existingConf = (existingConferences || []).find(c => String(c.conferenceId || c.id || '') === String(conf.conferenceId || conf.id || ''));
          if (existingConf?.active && conf.active === false && conferenceActiveCount(conf.conferenceId, conf.livekitRoom || existingConf.livekitRoom) > 0) {
            conf.active = true;
            conf.livekitRoom = existingConf.livekitRoom || conf.livekitRoom;
          }
          await store.saveConferenceForUsers(conf);
          const recipients = [...new Set([cleanEmail(conf.ownerEmail), ...(conf.participants || []).map(cleanEmail)].filter(Boolean))];
          for (const other of roomMembers(PRESENCE_ROOM)) {
            if (other.user && recipients.includes(cleanEmail(other.user.email))) send(other.ws, { type:'conference-created', conference: conf, conferenceId: conf.conferenceId, ownerEmail: conf.ownerEmail, participants: conf.participants, sender: client.user.name, email: client.user.email });
          }
          return send(ws, { type: 'conference-list', conferences: await store.getConferences(client.user.email) });
        }
        if (msg.type === 'conference-delete') {
          const confId = safeString(msg.conferenceId || '');
          const conferences = await store.getConferences(client.user.email);
          const conf = (conferences || []).find(c => String(c.conferenceId || c.id || '') === confId);
          if (!conf) return send(ws, { type: 'conference-list', conferences: conferences || [] });
          if (cleanEmail(conf.ownerEmail) !== cleanEmail(client.user.email)) return send(ws, { type:'error', message:'Seul le créateur peut supprimer cette conférence.' });
          const recipients = [...new Set([cleanEmail(conf.ownerEmail), ...(conf.participants || []).map(cleanEmail)].filter(Boolean))];
          for (const email of recipients) await store.removeConferenceForUser(email, confId);
          for (const other of roomMembers(PRESENCE_ROOM)) {
            if (other.user && recipients.includes(cleanEmail(other.user.email))) send(other.ws, { type:'conference-deleted', conferenceId: confId, sender: client.user.name, email: client.user.email });
          }
          return send(ws, { type: 'conference-list', conferences: await store.getConferences(client.user.email) });
        }
        if (msg.type === 'conference-call-join') {
          const activeCount = addConferenceCaller(client, msg);
          broadcastConferenceCallPresence(msg.conferenceId, msg.livekitRoom, activeCount);
          return send(ws, { type:'conference-call-presence', conferenceId: safeString(msg.conferenceId || ''), livekitRoom: safeString(msg.livekitRoom || ''), activeCount });
        }
        if (msg.type === 'conference-call-leave') {
          const activeCount = removeConferenceCaller(client, msg);
          for (const change of (client._conferencePresenceChanges || [])) {
            broadcastConferenceCallPresence(change.conferenceId, change.livekitRoom, change.activeCount);
          }
          client._conferencePresenceChanges = [];
          return send(ws, { type:'conference-call-presence', conferenceId: safeString(msg.conferenceId || ''), livekitRoom: safeString(msg.livekitRoom || ''), activeCount });
        }
        if (msg.type === 'conference-ended') {
          const confId = safeString(msg.conferenceId || '');
          const livekitRoom = safeString(msg.livekitRoom || '');
          const activeCount = conferenceActiveCount(confId, livekitRoom);
          // Sécurité : un départ participant / fermeture d'application ne doit jamais
          // fermer la conférence chez les autres. On ignore toute fin non forcée.
          if (!msg.force || activeCount > 0) {
            return send(ws, { type:'conference-end-ignored', conferenceId: confId, livekitRoom, activeCount });
          }
          const conferences = await store.getConferences(client.user.email);
          const conf = (conferences || []).find(c => String(c.conferenceId || c.id || '') === confId);
          if (!conf) return send(ws, { type:'conference-end-ignored', conferenceId: confId, livekitRoom, activeCount });
          if (cleanEmail(conf.ownerEmail) !== cleanEmail(client.user.email)) return send(ws, { type:'conference-end-ignored', conferenceId: confId, livekitRoom, activeCount });
          conf.active = false;
          await store.saveConferenceForUsers(conf);
          const recipients = [...new Set([cleanEmail(conf.ownerEmail), ...(conf.participants || []).map(cleanEmail)].filter(Boolean))];
          for (const other of roomMembers(PRESENCE_ROOM)) {
            if (other.user && recipients.includes(cleanEmail(other.user.email))) send(other.ws, { type:'conference-ended', conferenceId: confId, livekitRoom, force:true, sender: client.user.name, email: client.user.email });
          }
          return send(ws, { type:'conference-list', conferences: await store.getConferences(client.user.email) });
        }
        if (msg.type === 'messages-clear-before') {
          const before = Number(msg.before || 0);
          const removed = await store.deleteMessagesForUserBefore(client.user.email, before);
          return send(ws, { type: 'messages-cleared', before, removed });
        }
        if (msg.type === 'history-get') {
          const key = safeString(msg.conversation || conversationKey(safeString(msg.room, client.room || 'levigilant'), client.email, cleanEmail(msg.targetEmail)));
          return send(ws, { type: 'message-history', conversation: key, messages: await store.history(key) });
        }

        if (msg.type === 'join') {
          leave(client);
          const room = safeString(msg.room, 'levigilant');
          const members = roomMembers(room);
          if (members.length >= MAX_PARTICIPANTS && !room.startsWith('vigi-presence')) return send(ws, { type: 'room-full', maxParticipants: MAX_PARTICIPANTS });
          client.room = room; client.name = client.user.name; client.email = client.user.email; client.role = msg.role === 'admin' ? 'admin' : 'user';
          if (!rooms.has(room)) rooms.set(room, new Map());
          rooms.get(room).set(client.id, client);
          send(ws, { type: 'joined', id: client.id, room, participants: roomMembers(room).map(publicClient) });
          broadcast(room, { type: 'peer-joined', participant: publicClient(client) }, client.id);
          broadcast(room, { type: 'participants', participants: roomMembers(room).map(publicClient) });
          const history = await store.history(conversationKey(room));
          if (history.length) send(ws, { type: 'message-history', conversation: conversationKey(room), messages: history });
          return;
        }

        if (!client.room) return send(ws, { type: 'error', message: 'Rejoins un salon avant d’envoyer.' });
        const payload = { ...msg, room: client.room, from: client.id, sender: client.name, email: client.email, role: client.role };

        if (msg.type === 'chat' || msg.type === 'file') {
          const target = msg.to ? rooms.get(client.room)?.get(msg.to) : null;
          const targetEmail = target?.email || cleanEmail(msg.targetEmail);
          const conv = msg.to ? conversationKey(client.room, client.email, targetEmail) : conversationKey(client.room);
          const saved = {
            type: msg.type, conversation: conv, room: client.room, fromEmail: client.email, fromName: client.name,
            toEmail: targetEmail || '', text: String(msg.text || '').slice(0, 5000), file: msg.file || null,
            createdAt: new Date(), time: new Date().toISOString()
          };
          await store.saveMessage(saved);
          payload.conversation = conv;
        }

        if (msg.to) {
          const target = rooms.get(client.room)?.get(msg.to);
          if (target) send(target.ws, payload);
        } else broadcast(client.room, payload, client.id);
      } catch (e) {
        console.error('Message error:', e);
        send(ws, { type: 'error', message: 'Erreur serveur: ' + e.message });
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

  server.listen(port, '0.0.0.0', () => console.log('Vigi Messenger PRO server listening on port ' + port));
}

main().catch(err => {
  console.error('Fatal server startup error:', err);
  process.exit(1);
});
