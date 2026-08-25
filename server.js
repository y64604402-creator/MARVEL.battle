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
    id, name: p.name, color: p.color, joinedAt: p.joinedAt,
    x: p.x, y: p.y, z: p.z, yaw: p.yaw, alive: p.alive
  };
}

wss.on('connection', (ws) => {
  let roomCode = null;
  let playerId = null;

  function cleanup() {
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
        color, joinedAt, lastSeen: Date.now(),
        x: 0, y: 0, z: 0, yaw: 0, alive: true, attacking: false
      });

      const players = Array.from(room.entries()).map(([id, p]) => playerSummary(id, p));
      safeSend(ws, { t: 'welcome', id: playerId, room: roomCode, hostId: roomHostId(room), players });
      const me = room.get(playerId);
      broadcast(room, { t: 'player_joined', id: playerId, name: me.name, color: me.color, joinedAt }, playerId);
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
        if (target) safeSend(target.ws, { t: 'hit', from: playerId, dirX: msg.dirX, dirZ: msg.dirZ });
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
  const now = Date.now();
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
