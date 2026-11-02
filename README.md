# Telegram Business Multi-Account Auto-Responder Framework ⚡

A high-performance, single-file Telegram Auto-Responder built with **FastAPI**, **Telegram Business Bot API**, and **HTTPX**.

Instead of complex `API_ID`, `API_HASH`, or phone session strings, this framework uses **Telegram Business** to connect a single `@BotFather` bot to **unlimited personal Telegram accounts**, managing each account's incoming private DMs completely isolated from each other.

Runs seamlessly on the **Render Free Tier** (0.1 vCPU, 500MB RAM) with an ultra-lightweight memory footprint of **< 50MB RAM**.

---

## 🌟 How It Works (Telegram Business Architecture)

1. You create a single bot with `@BotFather` and run this service with `BOT_TOKEN`.
2. Any personal Telegram account links your bot in their Telegram app:
   - Go to **Settings** → **Telegram Business** → **Chatbots**.
   - Add your bot `@YourBot` and check **"Reply to messages"**.
3. Once linked:
   - When customers or friends send private DMs to that personal account, Telegram securely delivers a `business_message` update to your bot.
   - **Step A (Triggers)**: The bot checks for keyword matches (`.lower()`) and sends instant automated replies directly on behalf of that account.
   - **Step B (OpenRouter AI Fallback)**: If no keyword matches, the bot displays Telegram's native `typing...` indicator and generates a 2-sentence conversational response using OpenRouter AI.
4. **Isolated Controls**: Each connected account gets its own independent card on the web dashboard to configure custom triggers, toggle AI fallback, select AI models, or pause auto-replies.
5. **Persistent Bulk Update Tool**: Synchronize triggers and AI fallback across ALL connected accounts in a single click.

---

## 🚀 Quick Setup

### 1. Requirements
- Python 3.10+
- 1 Bot Token from [@BotFather](https://t.me/BotFather)
- 1 API Key from [OpenRouter](https://openrouter.ai/keys)

### 2. Enable Business Mode in @BotFather
1. Open [@BotFather](https://t.me/BotFather) on Telegram.
2. Send `/mybots` and select your bot.
3. Tap **Bot Settings** → **Telegram Business** → Turn ON **"Connect to Business"**.

### 3. Installation & Run
```bash
git clone https://github.com/DKT882/tgAutometa.git
cd tgAutometa
pip install -r requirements.txt
```

Create `.env`:
```ini
BOT_TOKEN=7123456789:AAHxxxxxxxxxxxxxxxxxxxx
OPENROUTER_API_KEY=sk-or-v1-xxxxxxxxxxxxxxxxxxxx
PORT=8000
```

Start the application:
```bash
python main.py
# or
uvicorn main:app --host 0.0.0.0 --port 8000
```
Open [http://localhost:8000](http://localhost:8000) to view the real-time control dashboard.

---

## 🔗 Connecting Accounts

Each personal Telegram account you want to manage simply connects to the bot:
1. In the Telegram app, go to **Settings** → **Telegram Business** → **Chatbots**.
2. Enter your bot's username (e.g. `@YourBot`).
3. Check the box **"Reply to messages"**.
4. The account will immediately appear as a card on your web dashboard!

---

## ☁️ Deploying to Render Free Tier

1. Push your repository to GitHub.
2. Create a **New Web Service** on [Render](https://render.com) and link the repo.
3. Configure:
   - **Environment**: Python 3
   - **Build Command**: `pip install -r requirements.txt`
   - **Start Command**: `uvicorn main:app --host 0.0.0.0 --port $PORT`
   - **Health Check Path**: `/health`
4. Add **Environment Variables**:
   - `BOT_TOKEN`: Your `@BotFather` bot token.
   - `OPENROUTER_API_KEY`: Your OpenRouter API key.
5. Deploy! Your dashboard and multi-account auto-responder will run 24/7.

---

## 📄 License
MIT License.
