const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');
const path = require('path');

const app = express();
app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

const server = http.createServer(app);
const io = new Server(server, {
  cors: {
    origin: '*',
    methods: ['GET', 'POST']
  }
});

// --- IN-MEMORY STATE (No DB, completely ephemeral) ---
// Waiting Queue: array of { socketId, userId, userName, gender, targetGender, joinedAt }
let waitingQueue = [];

// Active matches: matchId -> { user1: { socketId, gender, name }, user2: { socketId, gender, name }, callState: null }
const activeMatches = new Map();

// Quick lookup: socketId -> matchId
const socketToMatchMap = new Map();

// --- MATCHMAKING LOGIC ---
function findMatch(currentUser) {
  for (let i = 0; i < waitingQueue.length; i++) {
    const candidate = waitingQueue[i];

    // Don't match user with themselves
    if (candidate.socketId === currentUser.socketId) continue;

    // Check gender compatibility both ways
    const userWantsCandidate = currentUser.targetGender === 'any' || currentUser.targetGender === candidate.gender;
    const candidateWantsUser = candidate.targetGender === 'any' || candidate.targetGender === currentUser.gender;

    if (userWantsCandidate && candidateWantsUser) {
      // Remove candidate from queue
      waitingQueue.splice(i, 1);
      return candidate;
    }
  }
  return null;
}

// --- REST API DIAGNOSTICS ---
app.get('/api/health', (req, res) => {
  res.json({
    status: 'healthy',
    waitingQueueCount: waitingQueue.length,
    activeMatchesCount: activeMatches.size,
    totalConnectedSockets: io.engine.clientsCount
  });
});

app.get('/api/queue/stats', (req, res) => {
  const counts = { male: 0, female: 0, other: 0 };
  waitingQueue.forEach(u => {
    if (counts[u.gender] !== undefined) counts[u.gender]++;
  });
  res.json({
    waitingUsers: counts,
    totalWaiting: waitingQueue.length,
    activeRooms: activeMatches.size
  });
});

// --- SOCKET.IO EVENT HANDLERS ---
io.on('connection', (socket) => {
  console.log(`[Socket] Connected: ${socket.id}`);

  // 1. Enter Matchmaking Queue
  socket.on('match:find', ({ gender = 'other', targetGender = 'any', userName = 'Anonymous' }) => {
    // If user is already in a match, remove them first
    leaveCurrentMatch(socket);

    // Remove from queue if already waiting
    waitingQueue = waitingQueue.filter(u => u.socketId !== socket.id);

    const currentUser = {
      socketId: socket.id,
      userId: socket.id,
      userName,
      gender,
      targetGender,
      joinedAt: Date.now()
    };

    const matchedPeer = findMatch(currentUser);

    if (matchedPeer) {
      const matchId = `match_${Date.now()}_${Math.random().toString(36).substring(7)}`;

      // Store active session
      activeMatches.set(matchId, {
        user1: { socketId: socket.id, gender: currentUser.gender, name: currentUser.userName },
        user2: { socketId: matchedPeer.socketId, gender: matchedPeer.gender, name: matchedPeer.userName },
        callState: null
      });

      socketToMatchMap.set(socket.id, matchId);
      socketToMatchMap.set(matchedPeer.socketId, matchId);

      // Join both to socket room
      socket.join(matchId);
      const peerSocket = io.sockets.sockets.get(matchedPeer.socketId);
      if (peerSocket) peerSocket.join(matchId);

      // Notify both users
      socket.emit('match:found', {
        matchId,
        peer: { name: matchedPeer.userName, gender: matchedPeer.gender }
      });

      if (peerSocket) {
        peerSocket.emit('match:found', {
          matchId,
          peer: { name: currentUser.userName, gender: currentUser.gender }
        });
      }

      console.log(`[Match] Pair created ${matchId}: (${currentUser.gender}) <-> (${matchedPeer.gender})`);
    } else {
      // Put in waiting queue
      waitingQueue.push(currentUser);
      socket.emit('match:waiting', { message: 'Searching for a gender-matched partner...' });
      console.log(`[Queue] User ${socket.id} (${gender} seeking ${targetGender}) entered queue. Total waiting: ${waitingQueue.length}`);
    }
  });

  // 2. Ephemeral Chat (No DB saving)
  socket.on('chat:send', ({ matchId, message }) => {
    if (!matchId || !message) return;
    // Broadcast message to the matched peer only
    socket.to(matchId).emit('chat:receive', {
      message,
      timestamp: Date.now()
    });
  });

  socket.on('chat:typing', ({ matchId, isTyping }) => {
    if (matchId) {
      socket.to(matchId).emit('chat:peer_typing', { isTyping });
    }
  });

  // 3. Audio/Video Call Button Click Flow
  // Caller clicks "Phone Call" button
  socket.on('call:initiate', ({ matchId, callType = 'audio' }) => {
    const match = activeMatches.get(matchId);
    if (!match) {
      socket.emit('call:unavailable', { reason: 'Match session expired or not found' });
      return;
    }

    match.callState = {
      callerSocketId: socket.id,
      callType,
      status: 'ringing'
    };

    // Acknowledge caller that it is ringing
    socket.emit('call:ringing', { matchId });

    // Send incoming call prompt with audio alert to the partner
    socket.to(matchId).emit('call:incoming', {
      matchId,
      callType,
      callerName: (match.user1.socketId === socket.id) ? match.user1.name : match.user2.name
    });

    console.log(`[Call] Initiate call in ${matchId} by ${socket.id}`);
  });

  // Partner accepts the incoming call
  socket.on('call:accept', ({ matchId }) => {
    const match = activeMatches.get(matchId);
    if (!match || !match.callState) return;

    match.callState.status = 'active';

    // Notify caller that call was accepted so they can create WebRTC offer
    socket.to(matchId).emit('call:accepted', { matchId });
    socket.emit('call:connected', { matchId });

    console.log(`[Call] Accepted in ${matchId}`);
  });

  // Partner rejects or declines the call
  socket.on('call:reject', ({ matchId }) => {
    const match = activeMatches.get(matchId);
    if (!match) return;

    match.callState = null;
    socket.to(matchId).emit('call:rejected', { matchId, reason: 'Call declined by user' });
    console.log(`[Call] Rejected in ${matchId}`);
  });

  // Either user hangs up the call
  socket.on('call:end', ({ matchId }) => {
    const match = activeMatches.get(matchId);
    if (match) {
      match.callState = null;
    }
    // Notify peer in the room
    socket.to(matchId).emit('call:ended', { matchId });
    socket.emit('call:ended', { matchId });
    console.log(`[Call] Call ended in ${matchId}`);
  });

  // 4. WebRTC Signaling Relays (SDP Offer, Answer, ICE candidates)
  socket.on('webrtc:offer', ({ matchId, sdp }) => {
    socket.to(matchId).emit('webrtc:offer', { sdp });
  });

  socket.on('webrtc:answer', ({ matchId, sdp }) => {
    socket.to(matchId).emit('webrtc:answer', { sdp });
  });

  socket.on('webrtc:ice-candidate', ({ matchId, candidate }) => {
    socket.to(matchId).emit('webrtc:ice-candidate', { candidate });
  });

  // 5. Leave / Next / Skip Match
  socket.on('match:leave', () => {
    leaveCurrentMatch(socket);
  });

  // 6. Disconnect
  socket.on('disconnect', () => {
    console.log(`[Socket] Disconnected: ${socket.id}`);
    waitingQueue = waitingQueue.filter(u => u.socketId !== socket.id);
    leaveCurrentMatch(socket);
  });

  function leaveCurrentMatch(sock) {
    const matchId = socketToMatchMap.get(sock.id);
    if (!matchId) return;

    const match = activeMatches.get(matchId);
    if (match) {
      // Notify the other user that partner left
      sock.to(matchId).emit('match:peer_left', { matchId, reason: 'Partner left the chat' });
      // Clean up maps
      socketToMatchMap.delete(match.user1.socketId);
      socketToMatchMap.delete(match.user2.socketId);
      activeMatches.delete(matchId);
      sock.leave(matchId);
    }
  }
});

const PORT = process.env.PORT || 4000;
server.listen(PORT, () => {
  console.log(`=======================================================`);
  console.log(`🚀 Ephemeral Gender Match & WebRTC Call Server Started!`);
  console.log(`👉 Running on http://localhost:${PORT}`);
  console.log(`=======================================================`);
});
