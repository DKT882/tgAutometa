# Telegram Multi-Account Auto-Responder Userbot Framework ⚡

A high-performance, single-file, production-ready Telegram Userbot Auto-Responder built with **FastAPI**, **Telethon (MTProto API)**, and **HTTPX**.

Specifically architected for low-resource cloud deployments (such as the **Render Free Tier**: 0.1 vCPU, 500MB RAM, 5GB outbound transfer limits) with an in-memory footprint of **< 120MB**.

---

## 🚀 Features

- **Multi-Account MTProto Concurrency**: Supports up to 5 concurrent Telegram userbot profiles via Telethon's `StringSession`, running background asynchronous client tasks without local SQLite disk storage.
- **Dual Responding Pipeline**:
  - **Step A (Predefined Triggers)**: Instant containment keyword matching (`.lower()`) on incoming 1-on-1 private DMs. Groups, supergroups, and channels are strictly ignored.
  - **Step B (AI Fallback Mode)**: Dispatches unhandled DMs to the **OpenRouter API** using a globally pooled `httpx.AsyncClient` with native Telegram `typing...` chat feedback indicators.
- **Built-in Server-Side Rendered UI**:
  - Lightweight, responsive dark-mode dashboard at `/`. Zero external CDN or script dependencies.
  - Per-account controls: connection toggle (Activate/Deactivate), custom triggers, AI fallback switch, and AI model selection.
  - **Persistent Bulk Update Tool**: Synchronize rules, AI fallback state, and model configuration across all 5 accounts with one click.
- **Production Lifecycle & Resiliency**:
  - Controlled FastAPI `lifespan` management ensures smooth startup and safe disconnection handshakes (`SIGTERM` handling).
  - Health check endpoint at `/health` for Render continuous uptime monitoring.

---

## 🛠️ Quick Start

### 1. Prerequisites
- Python 3.10+
- Telegram API ID and Hash from [my.telegram.org](https://my.telegram.org)
- Telethon `StringSession` strings for your accounts
- OpenRouter API key from [openrouter.ai](https://openrouter.ai)

### 2. Installation
```bash
git clone https://github.com/DKT882/tgAutometa.git
cd tgAutometa
pip install -r requirements.txt
```

### 3. Configuration
Copy `.env.example` to `.env` and fill in your credentials:
```bash
cp .env.example .env
```

| Variable | Description |
|---|---|
| `TELEGRAM_API_ID` | Telegram MTProto API ID |
| `TELEGRAM_API_HASH` | Telegram MTProto API Hash |
| `OPENROUTER_API_KEY` | OpenRouter API Key (e.g. `sk-or-...`) |
| `DEFAULT_AI_MODEL` | Default model string (defaults to `google/gemini-2.5-flash`) |
| `SESSION_1` to `SESSION_5` | Telethon StringSession tokens for Slots 1–5 |
| `PORT` | Web port (default: `8000`) |

### 4. Running the Service
```bash
python main.py
# or
uvicorn main:app --host 0.0.0.0 --port 8000
```
Open [http://localhost:8000](http://localhost:8000) to view the control dashboard.

---

## ☁️ Deploying to Render

1. Create a **New Web Service** on Render and link this repository.
2. Configure settings:
   - **Environment**: Python 3
   - **Build Command**: `pip install -r requirements.txt`
   - **Start Command**: `uvicorn main:app --host 0.0.0.0 --port $PORT`
   - **Health Check Path**: `/health`
3. In **Environment Variables**, add:
   - `TELEGRAM_API_ID`
   - `TELEGRAM_API_HASH`
   - `OPENROUTER_API_KEY`
   - `SESSION_1` (and any other sessions up to `SESSION_5`)
4. Deploy! Your dashboard will be live at `https://your-app-name.onrender.com`.

---

## 📄 License
MIT License.
