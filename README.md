# Telegram Business Multi-Account Auto-Responder Framework ⚡

A high-performance, single-file Telegram Auto-Responder built with **FastAPI**, **Telegram Business Bot API**, **Motor (Async MongoDB)**, and **HTTPX**.

Manage multiple personal Telegram accounts with zero `API_ID` / `API_HASH` friction. Configure everything through an **interactive Telegram DM Setup Wizard** (`/settings`) or through the **built-in Dark Mode Web Dashboard**.

Runs seamlessly on the **Render Free Tier** (0.1 vCPU, 500MB RAM) with an ultra-lightweight memory footprint of **< 60MB RAM**.

---

## 🌟 Key Features

1. **Multi-Account Concurrency via Telegram Business**:
   - Single `@BotFather` bot handles unlimited connected personal Telegram accounts.
   - Each account has completely isolated settings, trigger rules, AI fallback choices, and message counters.
2. **Database Persistence (MongoDB)**:
   - Built using non-blocking async **Motor** (`motor.motor_asyncio`).
   - Persists all account configurations, custom triggers, AI model selections, and audit message logs.
   - Restores all accounts into a fast in-memory cache on startup for sub-millisecond response latency.
3. **Interactive Telegram DM Setup Wizard**:
   - Send `/settings` or `/start` to the bot in private DM.
   - **Account Picker**: Interactive menu listing all connected accounts with active status indicators.
   - **Account Settings Panel**: Toggle AI fallback ON/OFF, toggle auto-responding status, view/add triggers, pick AI models, and inspect analytics.
4. **Dual Responding Pipeline**:
   - **Step A (Triggers)**: Predefined keyword containment matching (`.lower()`).
   - **Step B (OpenRouter AI Fallback)**: Sends Telegram's native `typing...` action indicator and generates a 2-sentence conversational response via OpenRouter API.
5. **Real-Time Web Dashboard**:
   - Served directly on root `GET /` with live status indicators, bulk synchronization tools, and per-account settings forms.

---

## 🗄️ Database Architecture (MongoDB)

| Collection | Key Fields | Purpose |
|---|---|---|
| `accounts` | `_id (user_id)`, `connection_id`, `username`, `triggers`, `ai_fallback`, `model`, `active`, `processed_count` | Stores account settings, triggers, and state. |
| `message_logs` | `account_id`, `customer_id`, `incoming_text`, `response_type`, `matched_keyword`, `ai_model`, `response_text`, `timestamp` | Detailed interaction audit logs and analytics. |
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
│ • AI Fallback: ✅ ENABLED                     │
│ • AI Model: google/gemini-2.5-flash           │
│ • Predefined Triggers: 3 active rule(s)       │
│ • DMs Auto-Responded: 14                      │
│                                               │
│ [ 🤖 AI Fallback: ON  ] [ ⚡ Status: ACTIVE  ] │
│ [ 📝 View Triggers    ] [ ➕ Add Trigger     ] │
│ [ 🧠 Change Model     ] [ 📊 Analytics       ] │
│ [ ⬅️ Back to Accounts ] [ 🔄 Reload          ] │
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
      "ai_fallbacks_generated": 1108,
      "last_heartbeat": "2026-11-03 18:20:08.904000"
    }
  },
  "stats": {
    "connected_accounts": 4,
    "active_auto_responders": 4,
    "total_triggers_configured": 12,
    "openrouter_status": "READY"
  },
  "uptime": "22h 23m 46s",
  "uptime_seconds": 80626,
  "timestamp": "2026-11-03 18:20:20 UTC"
}
```

---

## ☁️ Deploying to Render Free Tier

1. Create a **New Web Service** on [Render](https://render.com) and link the repo.
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
