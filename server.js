require('dotenv').config();
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');
const path = require('path');
const Redis = require('ioredis');

const app = express();
app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

const server = http.createServer(app);
const io = new Server(server, {
  cors: {
    origin: '*',
    methods: ['GET', 'POST']
  },
  pingInterval: 5000,
  pingTimeout: 10000
});

// --- REDIS CONNECTION ---
const rawRedisUrl = (process.env.REDIS || process.env.REDIS_URL || 'redis://127.0.0.1:6379')
  .trim()
  .replace(/^["']|["']$/g, '');

const redis = new Redis(rawRedisUrl, {
  maxRetriesPerRequest: 3,
  enableReadyCheck: false,
  tls: rawRedisUrl.startsWith('rediss://') ? { rejectUnauthorized: false } : undefined
});

redis.on('connect', () => {
  console.log('✅ Connected to Redis successfully!');
  // Clean stale users and sockets from previous sessions
  redis.del('online:users', 'online:sockets').catch(() => { });
});

redis.on('error', (err) => {
  console.error('❌ Redis Connection Error:', err.message);
});

// --- REAL-TIME UNIQUE ONLINE USERS TRACKING (Deduplicated by userId) ---
// Map of userId -> Set of active socket IDs
const onlineUserSockets = new Map();

async function getOnlineUsersCount() {
  const baseOffset = parseInt(process.env.ONLINE_BASE_COUNT || '0', 10);
  let uniqueUsers = onlineUserSockets.size;
  try {
    if (redis.status === 'ready' || redis.status === 'connect') {
      const redisUsers = await redis.scard('online:users');
      if (redisUsers > 0) {
        uniqueUsers = Math.max(uniqueUsers, redisUsers);
      }
    }
  } catch (err) {
    // Fall back to in-memory unique user count
  }
  return Math.max(1, uniqueUsers + baseOffset);
}

let broadcastTimer = null;
function broadcastOnlineUsers() {
  if (broadcastTimer) return;
  broadcastTimer = setTimeout(async () => {
    broadcastTimer = null;
    try {
      const count = await getOnlineUsersCount();
      io.emit('online:count', { count, total: count });
      io.emit('users:online', { count, total: count });
      console.log(`[OnlineUsers] Real-time unique online count: ${count}`);
    } catch (e) {
      console.error('[OnlineUsers] Broadcast error:', e.message);
    }
  }, 100);
}

// --- DATE & TTL HELPERS ---
function getTodayUTC() {
  return new Date().toISOString().slice(0, 10); // "YYYY-MM-DD"
}

function getSecondsUntilMidnightUTC() {
  const now = new Date();
  const midnight = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate() + 1, 0, 0, 0));
  return Math.max(60, Math.floor((midnight.getTime() - now.getTime()) / 1000));
}

// Active matches in memory for fast WebRTC signaling:
// matchId -> { user1: { socketId, userId, gender, name }, user2: { socketId, userId, gender, name }, callState, matchedAt, isCrossGender, messageCount }
const activeMatches = new Map();
const socketToMatchMap = new Map();

// Disconnect grace period (prevents emitting disconnect or dropping active sessions on page refresh)
const pendingDisconnects = new Map();
const DISCONNECT_GRACE_PERIOD_MS = 3500;

// --- REDIS QUEUE HELPERS ---
async function removeFromAllRedisQueues(socketId) {
  try {
    const genders = ['male', 'female', 'other'];
    for (const g of genders) {
      const queueKey = `queue:${g}`;
      const items = await redis.lrange(queueKey, 0, -1);
      for (const item of items) {
        try {
          const parsed = JSON.parse(item);
          if (parsed.socketId === socketId) {
            await redis.lrem(queueKey, 0, item);
          }
        } catch (e) { }
      }
    }
  } catch (err) {
    console.error('[Redis] Error removing from queues:', err.message);
  }
}

// --- REDIS MATCHMAKING ENGINE ---
async function findMatchInRedis(currentUser) {
  const { userId, socketId, gender, targetGender } = currentUser;
  const today = getTodayUTC();
  const ttl = getSecondsUntilMidnightUTC();

  const isMale = gender === 'male';
  const isFemale = gender === 'female';

  // 1. Check daily M <-> F limit for current user
  let hasUsedDailyQuota = false;
  if (isMale || isFemale) {
    const dailyCount = await redis.get(`limit:mf:${userId}:${today}`);
    hasUsedDailyQuota = dailyCount && parseInt(dailyCount, 10) >= 1;
  }

  // If user specifically requested cross-gender, but already used today's 1 quota
  if ((isMale && targetGender === 'female' && hasUsedDailyQuota) ||
    (isFemale && targetGender === 'male' && hasUsedDailyQuota)) {
    return { quotaExceeded: true, resetsInSeconds: ttl };
  }

  // 2. Determine target queues by Priority & Fallback
  let targetQueueOrder = [];
  if (isMale) {
    if (!hasUsedDailyQuota && (targetGender === 'female' || targetGender === 'any')) {
      targetQueueOrder = ['female', 'other', 'male']; // Priority 1: Female -> 2: Other -> 3: Male
    } else {
      targetQueueOrder = ['other', 'male']; // Female blocked today
    }
  } else if (isFemale) {
    if (!hasUsedDailyQuota && (targetGender === 'male' || targetGender === 'any')) {
      targetQueueOrder = ['male', 'other', 'female']; // Priority 1: Male -> 2: Other -> 3: Female
    } else {
      targetQueueOrder = ['other', 'female']; // Male blocked today
    }
  } else {
    // 'other' can match with anyone without quota
    targetQueueOrder = targetGender === 'any' ? ['female', 'male', 'other'] : [targetGender, 'other'];
  }

  // 3. Scan queues based on priority
  for (const qGender of targetQueueOrder) {
    const queueKey = `queue:${qGender}`;
    const rawCandidates = await redis.lrange(queueKey, 0, -1);

    for (const rawItem of rawCandidates) {
      let candidate;
      try {
        candidate = JSON.parse(rawItem);
      } catch (e) {
        await redis.lrem(queueKey, 0, rawItem);
        continue;
      }

      // Check active socket liveness (prevent ghost users)
      const candSocket = io.sockets.sockets.get(candidate.socketId);
      if (!candSocket || !candSocket.connected) {
        await redis.lrem(queueKey, 0, rawItem);
        continue;
      }

      // Rule: Do not match with self
      if (candidate.userId === userId || candidate.socketId === socketId) {
        continue;
      }

      // Rule: SAME-DAY PAIR DEDUPLICATION (Cannot match same user twice on the same day)
      const alreadyConnectedToday = await redis.sismember(`history:${userId}:${today}`, candidate.userId);
      if (alreadyConnectedToday) {
        continue; // Skip: already connected today (allowed again tomorrow!)
      }

      // Rule: Candidate cross-gender daily check
      const isCrossGender = (gender === 'male' && qGender === 'female') || (gender === 'female' && qGender === 'male');
      if (isCrossGender) {
        const candDaily = await redis.get(`limit:mf:${candidate.userId}:${today}`);
        if (candDaily && parseInt(candDaily, 10) >= 1) {
          continue; // Candidate has already used today's quota
        }
      }

      // Check candidate's target preference compatibility
      const candWantsUser = candidate.targetGender === 'any' || candidate.targetGender === gender;
      if (!candWantsUser) {
        continue;
      }

      // --- MATCH FOUND! ---
      // Atomically remove candidate from queue
      const removed = await redis.lrem(queueKey, 1, rawItem);
      if (removed <= 0) {
        continue; // Candidate was already claimed by another concurrent request
      }

      // Record daily history (Both users cannot rematch today; key expires at midnight)
      const multi = redis.multi();
      multi.sadd(`history:${userId}:${today}`, candidate.userId);
      multi.expire(`history:${userId}:${today}`, ttl);
      multi.sadd(`history:${candidate.userId}:${today}`, userId);
      multi.expire(`history:${candidate.userId}:${today}`, ttl);

      // Increment daily cross-gender counter if M <-> F
      if (isCrossGender) {
        multi.incr(`limit:mf:${userId}:${today}`);
        multi.expire(`limit:mf:${userId}:${today}`, ttl);
        multi.incr(`limit:mf:${candidate.userId}:${today}`);
        multi.expire(`limit:mf:${candidate.userId}:${today}`, ttl);
      }

      await multi.exec();

      return {
        matched: true,
        candidate,
        isCrossGender
      };
    }
  }

  // No match found in any priority queue -> Add user to their gender queue
  await redis.rpush(`queue:${gender}`, JSON.stringify(currentUser));
  return { matched: false };
}

// --- REST API DIAGNOSTICS & METRICS ---
app.get('/api/online-count', async (req, res) => {
  try {
    const count = await getOnlineUsersCount();
    res.json({
      success: true,
      count,
      total: count,
      rawClientsCount: io.engine.clientsCount
    });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

app.get('/api/health', async (req, res) => {
  try {
    const maleCount = await redis.llen('queue:male');
    const femaleCount = await redis.llen('queue:female');
    const otherCount = await redis.llen('queue:other');
    const onlineCount = await getOnlineUsersCount();

    res.json({
      status: 'healthy',
      redis: redis.status,
      onlineUsers: onlineCount,
      waitingQueue: {
        male: maleCount,
        female: femaleCount,
        other: otherCount,
        total: maleCount + femaleCount + otherCount
      },
      activeMatchesCount: activeMatches.size,
      totalConnectedSockets: io.engine.clientsCount
    });
  } catch (err) {
    res.status(500).json({ status: 'error', error: err.message });
  }
});

app.get('/api/queue/stats', async (req, res) => {
  try {
    const male = await redis.llen('queue:male');
    const female = await redis.llen('queue:female');
    const other = await redis.llen('queue:other');
    const onlineCount = await getOnlineUsersCount();
    res.json({
      onlineUsers: onlineCount,
      waitingUsers: { male, female, other },
      totalWaiting: male + female + other,
      activeRooms: activeMatches.size
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// --- SOCKET.IO EVENT HANDLERS ---
io.on('connection', async (socket) => {
  const userId = socket.handshake.auth?.userId || socket.handshake.query?.userId || socket.id;
  socket.userId = userId;
  console.log(`[Socket] Connected: ${socket.id} (user: ${userId})`);

  // Check if this user reconnected during the page refresh grace period
  if (pendingDisconnects.has(userId)) {
    const pending = pendingDisconnects.get(userId);
    clearTimeout(pending.timer);
    pendingDisconnects.delete(userId);
    console.log(`[Reconnection] User ${userId} refreshed/reconnected! Cancelled disconnect emit.`);

    // Clean up old socket from tracking
    if (onlineUserSockets.has(userId)) {
      onlineUserSockets.get(userId).delete(pending.socketId);
    }
    try {
      await redis.srem('online:sockets', pending.socketId);
    } catch (e) { }

    // If user was in an active match when refreshing, seamlessly restore it
    if (pending.matchId) {
      const match = activeMatches.get(pending.matchId);
      if (match) {
        if (match.user1.userId === userId) {
          match.user1.socketId = socket.id;
        } else if (match.user2.userId === userId) {
          match.user2.socketId = socket.id;
        }
        socketToMatchMap.delete(pending.socketId);
        socketToMatchMap.set(socket.id, pending.matchId);
        socket.join(pending.matchId);

        const partner = (match.user1.userId === userId) ? match.user2 : match.user1;
        socket.emit('match:found', {
          matchId: pending.matchId,
          peer: { name: partner.name, gender: partner.gender },
          reconnected: true
        });
        socket.to(pending.matchId).emit('match:peer_reconnected', { matchId: pending.matchId });
      }
    }
  }

  // Track unique user in memory (userId -> Set of socket IDs)
  if (!onlineUserSockets.has(userId)) {
    onlineUserSockets.set(userId, new Set());
  }
  onlineUserSockets.get(userId).add(socket.id);

  // Track in Redis unique user set
  try {
    await redis.sadd('online:users', userId);
    await redis.sadd('online:sockets', socket.id);
  } catch (e) { }

  // Broadcast real-time online count to all clients
  broadcastOnlineUsers();

  // Send current online count immediately to newly connected client
  getOnlineUsersCount().then((count) => {
    socket.emit('online:count', { count, total: count });
    socket.emit('users:online', { count, total: count });
  }).catch(() => { });

  // Direct pull request for online users count
  socket.on('online:get', async () => {
    const count = await getOnlineUsersCount();
    socket.emit('online:count', { count, total: count });
    socket.emit('users:online', { count, total: count });
  });

  // 1. Enter Matchmaking Queue
  socket.on('match:find', async (data = {}) => {
    const gender = data.gender || 'other';
    const targetGender = data.targetGender || 'any';
    const userName = data.userName || 'Anonymous';
    const userId = data.userId || socket.id;

    // Leave any current active match first
    await leaveCurrentMatch(socket);

    // Remove from all queues if already waiting
    await removeFromAllRedisQueues(socket.id);

    const currentUser = {
      socketId: socket.id,
      userId,
      userName,
      gender,
      targetGender,
      joinedAt: Date.now()
    };

    try {
      const result = await findMatchInRedis(currentUser);

      // Daily limit exceeded condition
      if (result.quotaExceeded) {
        socket.emit('match:limit_reached', {
          code: 'DAILY_CROSS_GENDER_LIMIT',
          message: 'You have used your 1 Male-Female match for today. You can still match with "Other"!',
          resetsInSeconds: result.resetsInSeconds
        });
        socket.emit('match:waiting', {
          message: 'Daily Male-Female match limit reached for today. You can still match with "Other"!'
        });
        return;
      }

      if (result.matched && result.candidate) {
        const matchedPeer = result.candidate;
        const matchId = `match_${Date.now()}_${Math.random().toString(36).substring(7)}`;

        // Store active session in memory (for low-latency WebRTC relay)
        activeMatches.set(matchId, {
          user1: { socketId: socket.id, userId: currentUser.userId, gender: currentUser.gender, name: currentUser.userName },
          user2: { socketId: matchedPeer.socketId, userId: matchedPeer.userId, gender: matchedPeer.gender, name: matchedPeer.userName },
          callState: null,
          matchedAt: Date.now(),
          isCrossGender: result.isCrossGender,
          messageCount: 0
        });

        socketToMatchMap.set(socket.id, matchId);
        socketToMatchMap.set(matchedPeer.socketId, matchId);

        // Join both to socket room
        socket.join(matchId);
        const peerSocket = io.sockets.sockets.get(matchedPeer.socketId);
        if (peerSocket) peerSocket.join(matchId);

        // Save session in Redis with 2-hour TTL
        await redis.hset(`match:${matchId}`, {
          user1: JSON.stringify(currentUser),
          user2: JSON.stringify(matchedPeer),
          isCrossGender: result.isCrossGender ? '1' : '0',
          createdAt: Date.now().toString()
        });
        await redis.expire(`match:${matchId}`, 7200);

        // Notify both users (Exact original socket emit schema preserved!)
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

        console.log(`[Match] Pair created ${matchId}: (${currentUser.gender}) <-> (${matchedPeer.gender}) [CrossGender: ${result.isCrossGender}]`);
      } else {
        // Enqueued in Redis
        socket.emit('match:waiting', { message: 'Searching for a gender-matched partner...' });
        console.log(`[Queue] User ${userId} (${gender} seeking ${targetGender}) entered queue.`);
      }
    } catch (err) {
      console.error('[Match] Error during matchmaking:', err.message);
      socket.emit('match:waiting', { message: 'Searching for partner...' });
    }
  });

  // 2. Ephemeral Chat (No DB saving)
  socket.on('chat:send', ({ matchId, message }) => {
    if (!matchId || !message) return;
    const match = activeMatches.get(matchId);
    if (match) {
      match.messageCount = (match.messageCount || 0) + 1;
    }
    // Broadcast message to the matched peer only (exact original schema)
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

    socket.emit('call:ringing', { matchId });

    socket.to(matchId).emit('call:incoming', {
      matchId,
      callType,
      callerName: (match.user1.socketId === socket.id) ? match.user1.name : match.user2.name
    });

    console.log(`[Call] Initiate call in ${matchId} by ${socket.id}`);
  });

  socket.on('call:accept', ({ matchId }) => {
    const match = activeMatches.get(matchId);
    if (!match || !match.callState) return;

    match.callState.status = 'active';

    socket.to(matchId).emit('call:accepted', { matchId });
    socket.emit('call:connected', { matchId });

    console.log(`[Call] Accepted in ${matchId}`);
  });

  socket.on('call:reject', ({ matchId }) => {
    const match = activeMatches.get(matchId);
    if (!match) return;

    match.callState = null;
    socket.to(matchId).emit('call:rejected', { matchId, reason: 'Call declined by user' });
    console.log(`[Call] Rejected in ${matchId}`);
  });

  socket.on('call:end', ({ matchId }) => {
    const match = activeMatches.get(matchId);
    if (match) {
      match.callState = null;
    }
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

  // 5. Leave / Next / Skip Match (User explicitly chose to leave)
  socket.on('match:leave', async () => {
    if (socket.userId && pendingDisconnects.has(socket.userId)) {
      clearTimeout(pendingDisconnects.get(socket.userId).timer);
      pendingDisconnects.delete(socket.userId);
    }
    await removeFromAllRedisQueues(socket.id);
    await leaveCurrentMatch(socket);
  });

  // 6. Disconnect (With Page-Refresh Grace Period)
  socket.on('disconnect', async (reason) => {
    console.log(`[Socket] Disconnected: ${socket.id} (user: ${socket.userId}, reason: ${reason})`);

    const userId = socket.userId;
    const matchId = socketToMatchMap.get(socket.id);

    // Cancel any prior timer for this userId
    if (userId && pendingDisconnects.has(userId)) {
      clearTimeout(pendingDisconnects.get(userId).timer);
    }

    // Set grace period timer: wait 3.5s before emitting disconnect & tearing down match
    const timer = setTimeout(async () => {
      pendingDisconnects.delete(userId);
      console.log(`[Socket] Grace period ended for ${userId}. Performing full cleanup.`);

      if (userId && onlineUserSockets.has(userId)) {
        onlineUserSockets.get(userId).delete(socket.id);
        if (onlineUserSockets.get(userId).size === 0) {
          onlineUserSockets.delete(userId);
          try {
            await redis.srem('online:users', userId);
          } catch (e) { }
        }
      }

      try {
        await redis.srem('online:sockets', socket.id);
      } catch (e) { }

      await removeFromAllRedisQueues(socket.id);
      await leaveCurrentMatch(socket);
      broadcastOnlineUsers();
    }, DISCONNECT_GRACE_PERIOD_MS);

    if (userId) {
      pendingDisconnects.set(userId, {
        timer,
        socketId: socket.id,
        matchId,
        disconnectedAt: Date.now()
      });
    }
  });

  async function leaveCurrentMatch(sock) {
    const matchId = socketToMatchMap.get(sock.id);
    if (!matchId) return;

    const match = activeMatches.get(matchId);
    if (match) {
      // 30-Second Grace Period Quota Refund:
      // If Male <-> Female session lasted < 30 seconds with 0 messages, refund innocent partner
      if (match.isCrossGender) {
        const sessionDuration = Date.now() - (match.matchedAt || 0);
        if (sessionDuration < 30000 && (match.messageCount || 0) === 0) {
          const today = getTodayUTC();
          const partner = (match.user1.socketId === sock.id) ? match.user2 : match.user1;
          try {
            await redis.decr(`limit:mf:${partner.userId}:${today}`);
            console.log(`[Quota] Early disconnect (<30s). Refunded daily quota for partner: ${partner.userId}`);
          } catch (e) { }
        }
      }

      // Notify peer that partner left
      sock.to(matchId).emit('match:peer_left', { matchId, reason: 'Partner left the chat' });

      // Clean up maps and rooms
      socketToMatchMap.delete(match.user1.socketId);
      socketToMatchMap.delete(match.user2.socketId);
      activeMatches.delete(matchId);
      await redis.del(`match:${matchId}`).catch(() => { });
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
