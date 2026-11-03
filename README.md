# Telegram Business Multi-Account Auto-Responder Framework ⚡

A high-performance, single-file Telegram Auto-Responder built with **FastAPI**, **Telegram Business Bot API**, **Motor (Async MongoDB)**, and **HTTPX**.

Manage multiple personal Telegram accounts with zero `API_ID` / `API_HASH` friction. Configure everything through an **interactive Telegram DM Setup Wizard** (`/settings`) or through the **built-in Dark Mode Web Dashboard**.

Runs seamlessly on the **Render Free Tier** (0.1 vCPU, 500MB RAM) with an ultra-lightweight memory footprint of **< 60MB RAM**.

---

## 🌟 Key Features

1. **Multi-Account Concurrency via Telegram Business**:
   - Single `@BotFather` bot handles unlimited connected personal Telegram accounts.
   - Each account has completely isolated settings, trigger rules, multi-response bubbles, AI fallback choices, and message counters.
2. **Sequential Multi-Response Bubbles (Dynamic Add & Remove)**:
   - The bot can send **1, 2, 3, 4, or more sequential response bubbles** for a single customer message event.
   - First bubble responds immediately; subsequent bubbles follow with natural **~0.9s typing delays** and native Telegram `typing` chat actions.
   - Add or remove bubbles dynamically on the Web Dashboard (`[➕ Add Response Bubble]` / `[🗑️ Remove]`) or inside Telegram DM (`[➕ Add Next Bubble]` / `[🗑️ Remove Last Bubble]`).
3. **Direct Quote Reply (Quoting Customer Message)**:
   - Uses Telegram Bot API's native `reply_parameters={"message_id": incoming_msg_id, "allow_sending_without_reply": true}`.
   - The bot directly **quotes and replies** to the customer's incoming message bubble in the business chat.
   - Toggleable **ON / OFF** per account with a single click.
4. **Keyword Conditions vs Reply to All Dual Mode**:
   - **Keyword Trigger ON**: Checks conditions (different keywords ➔ different replies). When an incoming DM contains a configured keyword (e.g. `pricing`, `help`, `link`), it sends that keyword's specific reply sequence (with rich preview cards for links). Non-matching DMs are ignored cleanly.
   - **Keyword Trigger OFF**: Replies to **ALL incoming DMs** with the **Customized Response Bubbles** (with rich link previews for `https://t.me/...`), no matter what keyword or text the sender typed!
   - Toggleable directly in the Telegram DM wizard (`[🔑 Keywords: ON/OFF]`) or the Web Dashboard.
5. **Smart Away Mode (Auto-Offline Detection)**:
   - When the user is **actively chatting** on their Telegram account, the bot automatically **deactivates** so it never speaks over the human.
   - When the user goes **offline / idle** (e.g. no messages sent for > 5 minutes), the bot automatically **activates** and manages all incoming DMs!
   - Fully toggleable ON/OFF per account with adjustable inactivity timeouts (2m, 5m, 10m, 15m).
6. **Live Analytics & Customer Interaction Audit Log**:
   - Detailed telemetry tracking: Total DMs responded, Keyword Matches, Reply-All Custom Replies, and AI Fallbacks.
   - Rolling in-memory and MongoDB audit log of recent customer interactions (customer handle, incoming text, bot reply bubbles, response type, timestamp).
   - Works seamlessly with or without an active MongoDB connection.
7. **Database Persistence (MongoDB with In-Memory Fallback)**:
   - Built using non-blocking async **Motor** (`motor.motor_asyncio`).
   - Persists all account configurations, response bubbles, custom triggers, AI model selections, and audit message logs.
   - Restores all accounts into a fast in-memory cache on startup for sub-millisecond response latency.
8. **Real-Time Web Dashboard**:
   - Served directly on root `GET /` with live status indicators, dynamic bubble manager, enlarged textareas, prominent `💾 Save Settings & Messages` button, and bulk synchronization tools.

---

## 🗄️ Database Architecture (MongoDB)

| Collection | Key Fields | Purpose |
|---|---|---|
| `accounts` | `_id (user_id)`, `connection_id`, `username`, `custom_messages`, `quote_reply`, `triggers`, `ai_fallback`, `model`, `active`, `processed_count` | Stores account settings, multi-bubbles, and state. |
| `message_logs` | `account_id`, `customer_id`, `customer_name`, `incoming_text`, `response_type`, `matched_keyword`, `ai_model`, `response_text`, `timestamp` | Detailed interaction audit logs and analytics. |
| `bot_settings` | `_id ("global")`, `default_triggers_raw`, `default_model` | Global fallback presets. |

---

## 🤖 Telegram DM Setup Wizard Blueprint

Send `/settings` to the bot in a private chat to access the wizard:

```text
┌───────────────────────────────────────────────┐
│ ⚙️ Manage Account Settings                    │
│ 👉 Select the account whose settings to edit: │
│                                               │
│ [ ✅ 👤 @alice (14 DMs)                     ] │
│ [ ⏸️ 👤 @bob (5 DMs)                        ] │
│ [ 🔄 Refresh List  ] [ ⚡ Bulk Sync All      ] │
│ [ 📊 System Stats  ] [ ❌ Close Menu         ] │
└───────────────────────────────────────────────┘
                        │
                        ▼ (Tap an account)
┌───────────────────────────────────────────────┐
│ ⚙️ SETTINGS                                   │
│ Account: @alice (Alice)                       │
│ • Status: Active ✅ (Replying to DMs)         │
│ • Owner Presence: 💤 OFFLINE                  │
│ • Smart Away Mode: ❌ DISABLED                │
│ • Direct Quote Reply: ✅ ENABLED              │
│ • Active Mode: 🔑 KEYWORDS (Rules)            │
│ • Response Bubbles: 2 bubble(s)               │
│ • AI Fallback: ❌ DISABLED                    │
│ • DMs Auto-Responded: 14                      │
│                                               │
│ [ 🤖 AI Fallback: OFF ] [ ⚡ Status: ACTIVE  ] │
│ [ 🌙 Away Mode: OFF   ] [ ⏱️ Timeout: 5m      ] │
│ [ 🔑 Keywords: ON     ] [ ↩️ Quote: ON       ] │
│ [ 💬 Response Bubbles ] [ 📊 Analytics       ] │
│ [ 📝 View Triggers    ] [ ➕ Add Trigger     ] │
│ [ 🧠 Change Model     ] [ 🔄 Reload          ] │
│ [ ⬅️ Back to Accounts ] [ ✅ Close           ] │
└───────────────────────────────────────────────┘
                        │
                        ▼ (Tap Response Bubbles)
┌───────────────────────────────────────────────┐
│ 💬 MULTI-RESPONSE BUBBLE MANAGER              │
│ Bubble #1 (Quotes Customer):                  │
│ <blockquote>Hello! Thanks for connecting.</blockquote>  │
│ Bubble #2:                                    │
│ <blockquote>Join our group: https://t.me/+xyz</blockquote>│
│                                               │
│ [ ➕ Add Next Bubble  ] [ 🗑️ Remove Last    ] │
│ [ ✏️ Edit Bubble #1   ] [ 👁️ Test Preview   ] │
│ [ ↩️ Quote Reply: ON  ] [ 🔄 Reset Default   ] │
│ [ ⬅️ Back to Settings                        ] │
└───────────────────────────────────────────────┘
```

---

## 🚀 Quick Setup

### 1. Requirements
- Python 3.10+
- 1 Bot Token from [@BotFather](https://t.me/BotFather)
- 1 API Key from [OpenRouter](https://openrouter.ai/keys)
- MongoDB Connection String (MongoDB Atlas free cluster or local)

### 2. Enable Business Mode in @BotFather
1. Open [@BotFather](https://t.me/BotFather) on Telegram.
2. Send `/mybots` and select your bot.
3. Tap **Bot Settings** → **Telegram Business** → Turn ON **"Connect to Business"**.

### 3. Installation
```bash
git clone https://github.com/DKT882/tgAutometa.git
cd tgAutometa
pip install -r requirements.txt
```

Create `.env`:
```ini
BOT_TOKEN=7123456789:AAHxxxxxxxxxxxxxxxxxxxx
OPENROUTER_API_KEY=sk-or-v1-xxxxxxxxxxxxxxxxxxxx
MONGO_URI=mongodb+srv://user:pass@cluster.mongodb.net/?retryWrites=true&w=majority
PORT=8000
```

Start the application:
```bash
python main.py
```

---

## 🩺 Health & Telemetry Router (`/health`)

Access `GET /health` or `GET /api/health` to monitor the operational status, database heartbeat, and bot state:

```json
{
  "status": "UP",
  "service": "TelegramBusinessAutoResponder",
  "telegram_bot": {
    "status": "ONLINE",
    "username": "bioLinkChannelBot",
    "id": 9208828911
  },
  "database": {
    "type": "MongoDB Atlas",
    "connected": true,
    "metrics": {
      "connected": true,
      "accounts_saved": 4,
      "messages_logged": 6644,
      "triggers_matched": 5536,
      "reply_all_sent": 890,
      "ai_fallbacks_generated": 218,
      "last_heartbeat": "2026-11-04 03:10:00.000000"
    }
  },
  "stats": {
    "connected_accounts": 4,
    "active_auto_responders": 4,
    "total_dms_processed": 6644,
    "keyword_matches_count": 5536,
    "reply_all_count": 890,
    "ai_fallbacks_count": 218,
    "away_mode_active": 2,
    "quote_reply_active": 4,
    "keyword_triggers_active": 3,
    "reply_all_active": 1,
    "total_triggers_configured": 12,
    "openrouter_status": "READY"
  },
  "uptime": "22h 23m 46s",
  "uptime_seconds": 80626,
  "timestamp": "2026-11-04 03:10:00 UTC"
}
```

---

## ☁️ Deploying to Render Free Tier

1. Create a **New Web Service** on [Render](https://render.com) and link the repository.
2. Configure:
   - **Environment**: Python 3
   - **Build Command**: `pip install -r requirements.txt`
   - **Start Command**: `uvicorn main:app --host 0.0.0.0 --port $PORT`
   - **Health Check Path**: `/health`
3. Add **Environment Variables**:
   - `BOT_TOKEN`: Your `@BotFather` bot token.
   - `OPENROUTER_API_KEY`: Your OpenRouter API key.
   - `MONGO_URI`: Your MongoDB connection URI.
4. Deploy! Your dashboard and Telegram DM wizard will run 24/7.

---

## 📄 License
MIT License.
