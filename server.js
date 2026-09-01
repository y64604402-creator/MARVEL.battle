// Pillar Rumble - realtime relay server
//
// A lightweight WebSocket server that lets Pillar Rumble clients find each
// other by "room code" and relay position/attack messages between them in
// real time (no polling, no storage - just push events). One process can
// host many rooms at once; everything is kept in memory.
//
// Run locally:   npm install && npm start
// Deploy:        see README.md

const http = require('http');
const WebSocket = require('ws');

const PORT = process.env.PORT || 8080;
const STALE_MS = 15000; // drop a connection if we haven't heard from it in this long

const COLOR_PALETTE = [
  0xFF5C5C, 0x4D96FF, 0x6BCB77, 0xFFD93D,
  0xB388FF, 0xFF9F1C, 0x06D6A0, 0xFF6FB1, 0x5AC8FA
];

// simple HTTP handler so hosting platforms have something to health-check
const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('Pillar Rumble relay server is running.\n');
});

const wss = new WebSocket.Server({ server });

// rooms: Map<roomCode, Map<playerId, PlayerState>>
const rooms = new Map();

// presence: everyone with the app open right now, regardless of whether
// they're in a match room. Used to show "who else is playing" on the home
// screen. Separate from `rooms`, which is only for active match sessions.
const presence = new Map(); // playerId -> {ws, name, icon, lastSeen}

// global chat history for the current JST day. Cleared automatically once
// the JST date rolls over (not on a fixed timer - checked whenever it
// matters, so it self-corrects even if the process has been running a
// while).
let chatHistory = []; // {name, icon, text, ts}
const CHAT_MAX_KEEP = 500; // hard cap even within a single day
const JST_OFFSET_MS = 9 * 60 * 60 * 1000;

function jstDayStartMs(nowMs) {
  const shifted = nowMs + JST_OFFSET_MS;
  const dayMs = 24 * 60 * 60 * 1000;
  return Math.floor(shifted / dayMs) * dayMs - JST_OFFSET_MS;
}

function pruneChatHistory() {
  const boundary = jstDayStartMs(Date.now());
  chatHistory = chatHistory.filter(m => m.ts >= boundary);
  if (chatHistory.length > CHAT_MAX_KEEP) {
    chatHistory = chatHistory.slice(chatHistory.length - CHAT_MAX_KEEP);
  }
}

function presenceSummary(id, p) {
  return { id, name: p.name, icon: p.icon };
}

function broadcastPresence(obj, exceptId) {
  const msg = JSON.stringify(obj);
  for (const [id, p] of presence) {
    if (id === exceptId) continue;
    if (p.ws.readyState === WebSocket.OPEN) p.ws.send(msg);
  }
}


function getRoom(code) {
  let room = rooms.get(code);
  if (!room) { room = new Map(); rooms.set(code, room); }
  return room;
}

function roomHostId(room) {
  let bestId = null, bestTs = Infinity;
  for (const [id, p] of room) {
    if (p.joinedAt < bestTs || (p.joinedAt === bestTs && (bestId === null || id < bestId))) {
      bestTs = p.joinedAt;
      bestId = id;
    }
  }
  return bestId;
}

function pickColor(room) {
  const used = new Set(Array.from(room.values()).map(p => p.color));
  for (const c of COLOR_PALETTE) if (!used.has(c)) return c;
  return COLOR_PALETTE[Math.floor(Math.random() * COLOR_PALETTE.length)];
}

function genId() {
  return Math.random().toString(36).slice(2, 10) + Date.now().toString(36).slice(-4);
}

function safeSend(ws, obj) {
  if (ws.readyState === WebSocket.OPEN) {
    try { ws.send(JSON.stringify(obj)); } catch (e) { /* ignore */ }
  }
}

function broadcast(room, obj, exceptId) {
  const msg = JSON.stringify(obj);
  for (const [id, p] of room) {
    if (id === exceptId) continue;
    if (p.ws.readyState === WebSocket.OPEN) p.ws.send(msg);
  }
}

function playerSummary(id, p) {
  return {
    id, name: p.name, color: p.color, joinedAt: p.joinedAt, charId: p.charId, icon: p.icon,
    x: p.x, y: p.y, z: p.z, yaw: p.yaw, alive: p.alive
  };
}

wss.on('connection', (ws) => {
  let roomCode = null;
  let playerId = null;
  let presenceId = null;

  function cleanup() {
    if (presenceId && presence.has(presenceId)) {
      presence.delete(presenceId);
      broadcastPresence({ t: 'presence_left', id: presenceId });
      presenceId = null;
    }
    if (!roomCode || !playerId) return;
    const room = rooms.get(roomCode);
    if (room && room.has(playerId)) {
      room.delete(playerId);
      broadcast(room, { t: 'player_left', id: playerId });
      if (room.size === 0) {
        rooms.delete(roomCode);
      } else {
        broadcast(room, { t: 'host_changed', hostId: roomHostId(room) });
      }
    }
    roomCode = null;
    playerId = null;
  }

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch (e) { return; }
    if (!msg || typeof msg.t !== 'string') return;

    // ---- global presence: "who else has the app open right now" ----
    // independent of match rooms, so it works even on the home screen
    // before anyone has joined/started a match
    if (msg.t === 'presence_hello') {
      if (presenceId) return; // already registered
      presenceId = genId();
      presence.set(presenceId, {
        ws,
        name: (msg.name || 'Player').toString().slice(0, 10),
        icon: (msg.icon || '').toString().slice(0, 60000),
        lastSeen: Date.now()
      });
      const others = Array.from(presence.entries())
        .filter(([id]) => id !== presenceId)
        .map(([id, p]) => presenceSummary(id, p));
      safeSend(ws, { t: 'presence_list', players: others });
      pruneChatHistory();
      safeSend(ws, { t: 'presence_chat_history', messages: chatHistory });
      const me = presence.get(presenceId);
      broadcastPresence({ t: 'presence_joined', id: presenceId, name: me.name, icon: me.icon }, presenceId);
      return;
    }
    if (msg.t === 'presence_bye') {
      if (presenceId && presence.has(presenceId)) {
        presence.delete(presenceId);
        broadcastPresence({ t: 'presence_left', id: presenceId });
        presenceId = null;
      }
      return;
    }
    if (msg.t === 'presence_ping') {
      if (presenceId && presence.has(presenceId)) {
        presence.get(presenceId).lastSeen = Date.now();
      }
      return;
    }
    if (msg.t === 'presence_update') {
      if (presenceId && presence.has(presenceId)) {
        const p = presence.get(presenceId);
        if (typeof msg.name === 'string') p.name = msg.name.slice(0, 10);
        if (typeof msg.icon === 'string') p.icon = msg.icon.slice(0, 60000);
        broadcastPresence({ t: 'presence_updated', id: presenceId, name: p.name, icon: p.icon }, presenceId);
      }
      return;
    }
    if (msg.t === 'presence_chat') {
      if (presenceId && presence.has(presenceId)) {
        const p = presence.get(presenceId);
        const text = (msg.text || '').toString().trim().slice(0, 200);
        if (text) {
          const chatMsg = { id: presenceId, name: p.name, icon: p.icon, text, ts: Date.now() };
          pruneChatHistory();
          chatHistory.push(chatMsg);
          broadcastPresence({ t: 'presence_chat', ...chatMsg }, presenceId);
        }
      }
      return;
    }

    // ---- join a room ----
    if (msg.t === 'join') {
      if (roomCode) return; // already joined
      roomCode = (msg.room || 'LOBBY').toString().trim().toUpperCase().slice(0, 16) || 'LOBBY';
      const room = getRoom(roomCode);
      playerId = genId();
      const color = pickColor(room);
      const joinedAt = Date.now();

      room.set(playerId, {
        ws, name: (msg.name || 'Player').toString().slice(0, 10),
        charId: (msg.charId || '').toString().slice(0, 40),
        icon: (msg.icon || '').toString().slice(0, 60000),
        color, joinedAt, lastSeen: Date.now(),
        x: 0, y: 0, z: 0, yaw: 0, alive: true, attacking: false
      });

      const players = Array.from(room.entries()).map(([id, p]) => playerSummary(id, p));
      safeSend(ws, { t: 'welcome', id: playerId, room: roomCode, hostId: roomHostId(room), players });
      const me = room.get(playerId);
      broadcast(room, { t: 'player_joined', id: playerId, name: me.name, color: me.color, joinedAt, charId: me.charId, icon: me.icon }, playerId);
      broadcast(room, { t: 'host_changed', hostId: roomHostId(room) });
      return;
    }

    if (!roomCode || !playerId) return;
    const room = rooms.get(roomCode);
    if (!room || !room.has(playerId)) return;
    const p = room.get(playerId);
    p.lastSeen = Date.now();

    switch (msg.t) {
      case 'state': {
        p.x = msg.x; p.y = msg.y; p.z = msg.z; p.yaw = msg.yaw;
        p.alive = !!msg.alive; p.attacking = !!msg.attacking;
        broadcast(room, {
          t: 'state', id: playerId, x: p.x, y: p.y, z: p.z, yaw: p.yaw,
          alive: p.alive, attacking: p.attacking
        }, playerId);
        break;
      }
      case 'hit': {
        const target = room.get(msg.targetId);
        if (target) safeSend(target.ws, { t: 'hit', from: playerId, dirX: msg.dirX, dirZ: msg.dirZ, force: msg.force, up: msg.up, dmg: msg.dmg });
        break;
      }
      case 'grapple': {
        const target = room.get(msg.targetId);
        if (target) safeSend(target.ws, { t: 'grapple', from: playerId, x: msg.x, y: msg.y, z: msg.z, dmg: msg.dmg });
        break;
      }
      case 'smoke': {
        // broadcast the smoke cloud's location/radius/duration to everyone
        // else in the room, so Radar Sense can tell who's standing in it
        broadcast(room, { t: 'smoke', x: msg.x, y: msg.y, z: msg.z, radius: msg.radius, duration: msg.duration }, playerId);
        break;
      }
      case 'chat': {
        // room-scoped chat - only the people currently in this same match
        // room see it (separate from the global presence chat)
        const text = (msg.text || '').toString().trim().slice(0, 200);
        if (text) {
          broadcast(room, { t: 'chat', from: playerId, name: p.name, icon: p.icon, text }, playerId);
        }
        break;
      }
      case 'start': {
        // only the current host is allowed to trigger the countdown
        if (playerId === roomHostId(room)) {
          const payload = { t: 'start_countdown', ts: Date.now() };
          broadcast(room, payload);
          safeSend(ws, payload);
        }
        break;
      }
      case 'rules': {
        // only the current host is allowed to set match rules (solo/team, team assignments)
        if (playerId === roomHostId(room)) {
          broadcast(room, { t: 'rules', mode: msg.mode, teams: msg.teams }, playerId);
        }
        break;
      }
      case 'leave': {
        cleanup();
        break;
      }
    }
  });

  ws.on('close', cleanup);
  ws.on('error', cleanup);
});

// safety sweep in case a socket dies without firing 'close'
setInterval(() => {
  pruneChatHistory();
  const now = Date.now();
  for (const [id, p] of presence) {
    if (p.ws.readyState !== WebSocket.OPEN || now - p.lastSeen > STALE_MS) {
      presence.delete(id);
      broadcastPresence({ t: 'presence_left', id });
    }
  }
  for (const [code, room] of rooms) {
    for (const [id, p] of room) {
      if (now - p.lastSeen > STALE_MS) {
        room.delete(id);
        broadcast(room, { t: 'player_left', id });
      }
    }
    if (room.size === 0) rooms.delete(code);
    else broadcast(room, { t: 'host_changed', hostId: roomHostId(room) });
  }
}, 5000);

server.listen(PORT, () => {
  console.log('Pillar Rumble relay server listening on port ' + PORT);
});
