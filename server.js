// АнонЧат — WebSocket сервер
// Встанови залежності: npm install ws
// Запуск: node server.js

const { WebSocketServer } = require('ws');
const http = require('http');
const fs = require('fs');
const path = require('path');

const PORT = 2312;

// HTTP сервер — роздає index.html
const httpServer = http.createServer((req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Headers', '*');

  if (req.method === 'GET' && (req.url === '/' || req.url === '/index.html')) {
    // Шукаємо index.html поряд з server.js
    const filePath = path.join(__dirname, 'index.html');
    console.log(`[HTTP] Запит сторінки, шукаю: ${filePath}`);
    fs.readFile(filePath, (err, data) => {
      if (err) {
        console.error(`[HTTP] Файл не знайдено: ${filePath}`);
        res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
        res.end(`<h2>Помилка: index.html не знайдено</h2><p>Поклади index.html поряд з server.js у папці:<br><code>${__dirname}</code></p>`);
        return;
      }
      console.log(`[HTTP] Відправляю index.html (${data.length} байт)`);
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
      res.end(data);
    });
  } else {
    res.writeHead(404);
    res.end('Not found');
  }
});

// WebSocket сервер
const wss = new WebSocketServer({ server: httpServer });

// Зберігаємо клієнтів і історію
const clients = new Map();   // ws -> { nick, room }
const history = {            // room -> [msg, ...]
  general: [], random: [], tech: [], music: [], off: []
};
const MAX_HISTORY = 100;

function broadcast(data, room = null, excludeWs = null) {
  const json = JSON.stringify(data);
  for (const [ws, info] of clients.entries()) {
    if (ws === excludeWs) continue;
    if (room && info.room !== room) continue;
    if (ws.readyState === 1) ws.send(json);
  }
}

function sendOnlineCount() {
  const count = clients.size;
  const countsMap = {};
  for (const room of Object.keys(history)) countsMap[room] = 0;
  for (const [, info] of clients.entries()) {
    if (countsMap[info.room] !== undefined) countsMap[info.room]++;
  }
  for (const [ws] of clients.entries()) {
    if (ws.readyState === 1) {
      ws.send(JSON.stringify({ type: 'online', count }));
      ws.send(JSON.stringify({ type: 'room_counts', counts: countsMap }));
    }
  }
}

wss.on('connection', (ws, req) => {
  const ip = req.socket.remoteAddress;
  console.log(`[+] Нове підключення: ${ip}`);

  clients.set(ws, { nick: 'Anon', room: 'general' });

  ws.on('message', (raw) => {
    let data;
    try { data = JSON.parse(raw); } catch { return; }

    const client = clients.get(ws);
    if (!client) return;

    switch (data.type) {
      case 'join': {
        const room = data.room && history[data.room] ? data.room : 'general';
        const nick = (data.nick || 'Anon').slice(0, 20).replace(/[<>]/g, '');
        client.nick = nick;
        client.room = room;

        // Відправити історію кімнати
        ws.send(JSON.stringify({ type: 'history', room, messages: history[room] }));

        // Повідомити кімнату про приєднання
        broadcast({ type: 'system', text: `${nick} приєднався`, room }, room, ws);

        sendOnlineCount();
        console.log(`[JOIN] ${nick} -> #${room}`);
        break;
      }

      case 'leave': {
        const room = data.room;
        broadcast({ type: 'system', text: `${client.nick} покинув`, room }, room, ws);
        break;
      }

      case 'message': {
        if (!data.text || typeof data.text !== 'string') break;
        const text = data.text.trim().slice(0, 1000);
        if (!text) break;

        const room = client.room;
        const msg = {
          type: 'message',
          nick: client.nick,
          room,
          text,
          time: Date.now()
        };

        // Зберегти в історії
        if (!history[room]) history[room] = [];
        history[room].push(msg);
        if (history[room].length > MAX_HISTORY) history[room].shift();

        // Розіслати всім у кімнаті (крім відправника — він сам собі вже відобразив)
        broadcast(msg, room, ws);
        console.log(`[MSG] #${room} | ${client.nick}: ${text.slice(0, 60)}`);
        break;
      }
    }
  });

  ws.on('close', () => {
    const client = clients.get(ws);
    if (client) {
      broadcast(
        { type: 'system', text: `${client.nick} вийшов`, room: client.room },
        client.room, ws
      );
      console.log(`[-] Відключився: ${client.nick}`);
    }
    clients.delete(ws);
    sendOnlineCount();
  });

  ws.on('error', (err) => {
    console.error('[WS ERROR]', err.message);
  });

  sendOnlineCount();
});

httpServer.listen(PORT, '0.0.0.0', () => {
  console.log(`\n🚀 АнонЧат сервер запущено`);
  console.log(`   HTTP: http://172.16.82.29:${PORT}`);
  console.log(`   WS:   ws://172.16.82.29:${PORT}`);
  console.log(`\n   Відкрий http://172.16.82.29:${PORT} у браузері\n`);
});
