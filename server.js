const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: '*' } });
const PORT = process.env.PORT || 3000;
app.get('/', (_, res) => res.send('Vigi Messenger signaling server online'));
io.on('connection', socket => {
  socket.on('join', room => socket.join(room));
  socket.on('signal', ({ room, data }) => socket.to(room).emit('signal', data));
  socket.on('chat', ({ room, data }) => socket.to(room).emit('chat', data));
  socket.on('file-meta', ({ room, data }) => socket.to(room).emit('file-meta', data));
});
server.listen(PORT, () => console.log('Vigi signaling server on port ' + PORT));
