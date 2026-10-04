// ============================================================
// Long Drive Web — Online Server (без npm зависимостей)
// Запуск: node server.js
// ============================================================
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const PORT = process.env.PORT || 3000;
const UPLOAD_DIR = path.join(__dirname, 'public', 'uploads');
if (!fs.existsSync(UPLOAD_DIR)) fs.mkdirSync(UPLOAD_DIR, { recursive: true });

// ============================================================
// Состояние
// ============================================================
let currentVideo = null;
const players = new Map(); // socket -> nickname

// ============================================================
// WebSocket helpers (реализация вручную)
// ============================================================
const WS_MAGIC = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';

function wsAccept(key) {
  return crypto.createHash('sha1').update(key + WS_MAGIC).digest('base64');
}

function wsEncode(str) {
  const payload = Buffer.from(str, 'utf8');
  const len = payload.length;
  let header;
  if (len < 126) {
    header = Buffer.alloc(2);
    header[0] = 0x81;
    header[1] = len;
  } else if (len < 65536) {
    header = Buffer.alloc(4);
    header[0] = 0x81;
    header[1] = 126;
    header.writeUInt16BE(len, 2);
  } else {
    header = Buffer.alloc(10);
    header[0] = 0x81;
    header[1] = 127;
    header.writeUInt32BE(0, 2);
    header.writeUInt32BE(len, 6);
  }
  return Buffer.concat([header, payload]);
}

function wsDecode(buf) {
  if (buf.length < 2) return null;
  const opcode = buf[0] & 0x0f;
  const masked = (buf[1] & 0x80) !== 0;
  let len = buf[1] & 0x7f;
  let offset = 2;
  if (len === 126) {
    if (buf.length < 4) return null;
    len = buf.readUInt16BE(2);
    offset = 4;
  } else if (len === 127) {
    if (buf.length < 10) return null;
    len = Number(buf.readBigUInt64BE(2));
    offset = 10;
  }
  if (masked) {
    if (buf.length < offset + 4 + len) return null;
    const mask = buf.slice(offset, offset + 4);
    offset += 4;
    const data = buf.slice(offset, offset + len);
    for (let i = 0; i < data.length; i++) data[i] ^= mask[i % 4];
    return { opcode, data };
  }
  if (buf.length < offset + len) return null;
  return { opcode, data: buf.slice(offset, offset + len) };
}

function broadcast(msg, exceptWs = null) {
  const data = JSON.stringify(msg);
  const frame = wsEncode(data);
  for (const [socket] of players) {
    if (socket !== exceptWs && !socket.destroyed) {
      try { socket.write(frame); } catch (e) {}
    }
  }
}
function broadcastPlayers() {
  broadcast({ type: 'players', list: Array.from(players.values()) });
}

// ============================================================
// Multipart parser (для загрузки mp4)
// ============================================================
function parseMultipart(buffer, boundary) {
  const delimiter = Buffer.from('--' + boundary);
  const parts = [];
  let start = buffer.indexOf(delimiter);
  while (start !== -1) {
    start += delimiter.length;
    if (buffer[start] === 0x2d && buffer[start + 1] === 0x2d) break;
    if (buffer[start] === 0x0d && buffer[start + 1] === 0x0a) start += 2;
    const headerEnd = buffer.indexOf('\r\n\r\n', start);
    if (headerEnd === -1) break;
    const headers = buffer.slice(start, headerEnd).toString('utf8');
    const dataStart = headerEnd + 4;
    const nextDelim = buffer.indexOf(delimiter, dataStart);
    if (nextDelim === -1) break;
    const dataEnd = nextDelim - 2;
    parts.push({ headers, data: buffer.slice(dataStart, dataEnd) });
    start = nextDelim;
  }
  return parts;
}

// ============================================================
// MIME типы
// ============================================================
const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.js':   'application/javascript; charset=utf-8',
  '.css':  'text/css; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.png':  'image/png',
  '.jpg':  'image/jpeg',
  '.jpeg': 'image/jpeg',
  '.svg':  'image/svg+xml',
  '.ico':  'image/x-icon',
  '.mp4':  'video/mp4',
  '.webm': 'video/webm',
  '.ogg':  'video/ogg',
  '.mov':  'video/quicktime',
};

// ============================================================
// HTTP сервер
// ============================================================
const server = http.createServer((req, res) => {
  const url = req.url.split('?')[0];

  // ---- API ----
  if (req.method === 'POST' && url === '/upload') {
    handleUpload(req, res);
    return;
  }
  if (req.method === 'GET' && url === '/current') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ currentVideo, players: Array.from(players.values()) }));
    return;
  }

  // ---- Статика ----
  let filePath = path.join(__dirname, 'public', url === '/' ? 'index.html' : url);
  const publicDir = path.join(__dirname, 'public');
  if (!filePath.startsWith(publicDir)) {
    res.writeHead(403); res.end('Forbidden');
    return;
  }
  fs.stat(filePath, (err, stat) => {
    if (err || !stat.isFile()) {
      res.writeHead(404, { 'Content-Type': 'text/plain' });
      res.end('Not Found');
      return;
    }
    const ext = path.extname(filePath).toLowerCase();
    const mime = MIME[ext] || 'application/octet-stream';

    // Для видео — поддержка Range (перемотка)
    if (mime.startsWith('video/')) {
      const range = req.headers.range;
      if (range) {
        const parts = range.replace(/bytes=/, '').split('-');
        const start = parseInt(parts[0], 10);
        const end = parts[1] ? parseInt(parts[1], 10) : stat.size - 1;
        const chunkSize = end - start + 1;
        res.writeHead(206, {
          'Content-Range': `bytes ${start}-${end}/${stat.size}`,
          'Accept-Ranges': 'bytes',
          'Content-Length': chunkSize,
          'Content-Type': mime,
        });
        fs.createReadStream(filePath, { start, end }).pipe(res);
      } else {
        res.writeHead(200, {
          'Content-Length': stat.size,
          'Content-Type': mime,
          'Accept-Ranges': 'bytes',
        });
        fs.createReadStream(filePath).pipe(res);
      }
      return;
    }
    res.writeHead(200, { 'Content-Type': mime });
    fs.createReadStream(filePath).pipe(res);
  });
});

// ============================================================
// Загрузка видео
// ============================================================
const MAX_UPLOAD = 500 * 1024 * 1024; // 500 МБ

function handleUpload(req, res) {
  const ct = req.headers['content-type'] || '';
  const m = ct.match(/boundary=(.+)$/);
  if (!m) { res.writeHead(400); res.end('no boundary'); return; }
  const boundary = m[1];

  const chunks = [];
  let totalLen = 0;
  let aborted = false;

  req.on('data', chunk => {
    if (aborted) return;
    chunks.push(chunk);
    totalLen += chunk.length;
    if (totalLen > MAX_UPLOAD) {
      aborted = true;
      res.writeHead(413); res.end('too big');
      req.destroy();
    }
  });

  req.on('end', () => {
    if (aborted) return;
    const buffer = Buffer.concat(chunks);
    const parts = parseMultipart(buffer, boundary);

    let filePart = null;
    let nickname = 'Anonymous';
    for (const p of parts) {
      if (/name="nickname"/.test(p.headers)) {
        nickname = p.data.toString('utf8').slice(0, 32);
      }
      if (/name="video"/.test(p.headers)) {
        filePart = p;
      }
    }
    if (!filePart) { res.writeHead(400); res.end('no file'); return; }

    const fnameMatch = filePart.headers.match(/filename="([^"]+)"/);
    const origName = fnameMatch ? fnameMatch[1] : 'video.mp4';
    const ext = path.extname(origName) || '.mp4';
    const fname = 'video_' + Date.now() + ext;
    const fpath = path.join(UPLOAD_DIR, fname);

    fs.writeFile(fpath, filePart.data, (err) => {
      if (err) { res.writeHead(500); res.end('write error'); return; }
      const videoUrl = '/uploads/' + fname;
      currentVideo = {
        url: videoUrl,
        name: origName,
        startedAt: Date.now(),
        from: nickname,
      };
      console.log(`📺 Новое видео: ${origName} от ${nickname} (${(filePart.data.length/1024/1024).toFixed(1)} MB)`);
      broadcast({ type: 'play', video: currentVideo });
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: true, url: videoUrl, video: currentVideo }));
    });
  });

  req.on('error', () => { aborted = true; });
}

// ============================================================
// WebSocket server
// ============================================================
server.on('upgrade', (req, socket) => {
  const key = req.headers['sec-websocket-key'];
  if (!key) { socket.destroy(); return; }

  const accept = wsAccept(key);
  socket.write(
    'HTTP/1.1 101 Switching Protocols\r\n' +
    'Upgrade: websocket\r\n' +
    'Connection: Upgrade\r\n' +
    'Sec-WebSocket-Accept: ' + accept + '\r\n' +
    '\r\n'
  );

  socket.on('data', (buf) => {
    const frame = wsDecode(buf);
    if (!frame) return;
    if (frame.opcode === 0x08) { socket.end(); return; }
    if (frame.opcode === 0x09) { socket.write(Buffer.from([0x8a, 0])); return; }
    if (frame.opcode !== 0x01) return;

    let msg;
    try { msg = JSON.parse(frame.data.toString('utf8')); } catch (e) { return; }

    switch (msg.type) {
      case 'join': {
        const nickname = (msg.nickname || 'Anonymous').slice(0, 16);
        players.set(socket, nickname);
        console.log(`👤 ${nickname} вошёл (онлайн: ${players.size})`);
        socket.write(wsEncode(JSON.stringify({
          type: 'init',
          currentVideo,
          players: Array.from(players.values()),
        })));
        broadcastPlayers();
        broadcast({ type: 'join', nickname }, socket);
        break;
      }
      case 'play': {
        if (!msg.url) return;
        currentVideo = {
          url: msg.url,
          name: msg.name || 'video',
          startedAt: Date.now(),
          from: players.get(socket) || 'Anonymous',
        };
        broadcast({ type: 'play', video: currentVideo });
        break;
      }
      case 'stop': {
        currentVideo = null;
        broadcast({ type: 'stop', from: players.get(socket) || 'Anonymous' });
        break;
      }
    }
  });

  socket.on('close', () => {
    const nick = players.get(socket);
    if (nick) {
      console.log(`👤 ${nick} вышел (онлайн: ${players.size - 1})`);
      players.delete(socket);
      broadcastPlayers();
      broadcast({ type: 'leave', nickname: nick });
    }
  });

  socket.on('error', () => {});
});

// ============================================================
// Запуск
// ============================================================
server.listen(PORT, () => {
  console.log('');
  console.log('  ╔══════════════════════════════════════════╗');
  console.log('  ║    LONG DRIVE WEB — ONLINE               ║');
  console.log('  ╠══════════════════════════════════════════╣');
  console.log(`  ║    Открой: http://localhost:${PORT}         ║`);
  console.log('  ╚══════════════════════════════════════════╝');
  console.log('');
  console.log('  Загруженные видео: ' + UPLOAD_DIR);
  console.log('');
});
