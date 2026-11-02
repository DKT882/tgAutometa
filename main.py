"""
Telegram Business Multi-Account Auto-Responder Framework
Production-ready single-file FastAPI + Telegram Business Bot API service.
Zero API_ID/API_HASH required — powered by a single @BotFather Bot Token.
"""

import asyncio
import html
import logging
import os
import sys
import urllib.parse
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Dict, List, Optional

import httpx
import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, RedirectResponse

# ---------------------------------------------------------------------------
# Logging Configuration
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s]: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("tg_business_bot")

# ---------------------------------------------------------------------------
# Global Environment & Configuration
# ---------------------------------------------------------------------------
BOT_TOKEN = (os.getenv("BOT_TOKEN") or os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
OPENROUTER_API_KEY = (
    os.getenv("OPENROUTER_API_KEY") or os.getenv("OPENAI_API_KEY") or ""
).strip()
DEFAULT_MODEL = os.getenv("DEFAULT_AI_MODEL", "google/gemini-2.5-flash").strip()
PORT = int(os.getenv("PORT", 8000))

# Shared HTTPX AsyncClient instance across the entire application lifecycle
http_client: Optional[httpx.AsyncClient] = None
bot_info: Dict[str, str] = {"username": "BotFatherBot", "first_name": "Business Bot"}
polling_task: Optional[asyncio.Task] = None

# Default trigger rules for all accounts
DEFAULT_TRIGGERS_RAW = (
    "hello:Hello! Thanks for reaching out. How can I assist you today?,\n"
    "pricing:Our services start at $19/mo. For custom tiers, contact our team!,\n"
    "support:You can reach out to our support team at support@example.com."
)


# ---------------------------------------------------------------------------
# Rule Parsing Utilities
# ---------------------------------------------------------------------------
def parse_triggers(raw: str) -> Dict[str, str]:
    """
    Parse comma-separated or newline-separated trigger strings into a dictionary.
    Format: 'keyword:response text'
    Handles commas embedded inside the response text safely.
    """
    rules: Dict[str, str] = {}
    if not raw:
        return rules

    lines = raw.replace("\r\n", "\n").split("\n")
    for line in lines:
        line = line.strip()
        if not line:
            continue

        parts = [p.strip() for p in line.split(",") if p.strip()]
        current_kw: Optional[str] = None
        current_resp_parts: List[str] = []

        for part in parts:
            if ":" in part:
                if current_kw and current_resp_parts:
                    rules[current_kw] = ", ".join(current_resp_parts)
                kw, resp = part.split(":", 1)
                current_kw = kw.strip().lower()
                current_resp_parts = [resp.strip()]
            else:
                if current_kw:
                    current_resp_parts.append(part)

        if current_kw and current_resp_parts:
            rules[current_kw] = ", ".join(current_resp_parts)

    return rules


# ---------------------------------------------------------------------------
# In-Memory State Model (Per Connected Account)
# ---------------------------------------------------------------------------
@dataclass
class ConnectedAccount:
    connection_id: str
    user_id: int
    username: str = ""
    first_name: str = ""
    can_reply: bool = True
    is_enabled: bool = True
    triggers_raw: str = DEFAULT_TRIGGERS_RAW
    triggers: Dict[str, str] = field(default_factory=dict)
    ai_fallback: bool = True
    model: str = DEFAULT_MODEL
    active: bool = True
    processed_count: int = 0
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)


# In-memory dictionary mapping user_id -> ConnectedAccount
accounts: Dict[int, ConnectedAccount] = {}
# Fast lookup mapping business_connection_id -> user_id
connection_to_user: Dict[str, int] = {}


# ---------------------------------------------------------------------------
# Telegram Bot API Helpers
# ---------------------------------------------------------------------------
async def tg_api_call(method: str, payload: dict) -> Optional[dict]:
    """
    Execute a Telegram Bot API method using the global shared HTTPX client.
    """
    global http_client
    if not http_client or not BOT_TOKEN:
        return None

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/{method}"
    try:
        response = await http_client.post(url, json=payload, timeout=25.0)
        data = response.json()
        if not data.get("ok"):
            logger.error(f"Telegram API {method} error: {data.get('description')}")
            return None
        return data.get("result")
    except Exception as exc:
        logger.error(f"Telegram API request exception on {method}: {exc}")
        return None


async def send_typing_indicator(business_connection_id: str, chat_id: int):
    """
    Send native chat action 'typing' on behalf of the business account.
    """
    await tg_api_call(
        "sendChatAction",
        {
            "business_connection_id": business_connection_id,
            "chat_id": chat_id,
            "action": "typing",
        },
    )


async def send_business_message(
    business_connection_id: str, chat_id: int, text: str
) -> bool:
    """
    Send a message in a private DM on behalf of the connected business account.
    """
    result = await tg_api_call(
        "sendMessage",
        {
            "business_connection_id": business_connection_id,
            "chat_id": chat_id,
            "text": text,
        },
    )
    return result is not None


# ---------------------------------------------------------------------------
# AI Generation via OpenRouter (Step B Fallback)
# ---------------------------------------------------------------------------
async def generate_openrouter_reply(user_message: str, model: str) -> Optional[str]:
    """
    Generate an AI reply via OpenRouter using the global HTTPX client.
    Enforces strict brevity guidelines.
    """
    global http_client
    if not http_client or not OPENROUTER_API_KEY:
        return None

    headers = {
        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
        "Content-Type": "application/json",
        "HTTP-Referer": "https://render.com",
        "X-Title": "TG Business AutoResponder",
    }

    payload = {
        "model": model or DEFAULT_MODEL,
        "messages": [
            {
                "role": "system",
                "content": (
                    "You are a fast personal assistant via Telegram DM. "
                    "Keep replies brief, conversational, and under 2 sentences."
                ),
            },
            {"role": "user", "content": user_message},
        ],
        "max_tokens": 120,
        "temperature": 0.7,
    }

    try:
        response = await http_client.post(
            "https://openrouter.ai/api/v1/chat/completions",
            headers=headers,
            json=payload,
            timeout=20.0,
        )
        if response.status_code == 200:
            data = response.json()
            choices = data.get("choices", [])
            if choices:
                content = choices[0].get("message", {}).get("content", "")
                return content.strip() if content else None
        else:
            logger.error(
                f"OpenRouter API returned HTTP {response.status_code}: {response.text}"
            )
    except httpx.TimeoutException:
        logger.warning("OpenRouter API request timed out (20s).")
    except Exception as exc:
        logger.error(f"OpenRouter API exception: {exc}")

    return None


# ---------------------------------------------------------------------------
# Business Message Processing Pipeline (Triggers -> AI Fallback)
# ---------------------------------------------------------------------------
async def process_business_message(message: dict):
    """
    Handles incoming private DMs sent to any connected business account.
    """
    connection_id = message.get("business_connection_id")
    if not connection_id:
        return

    # Look up account configuration
    user_id = connection_to_user.get(connection_id)
    account = accounts.get(user_id) if user_id else None
    if not account or not account.active or not account.can_reply:
        return

    # Ignore messages sent by the business account owner themselves
    sender = message.get("from", {})
    sender_id = sender.get("id")
    if sender_id == account.user_id:
        return

    # Ignore messages from automated bots
    if sender.get("is_bot", False):
        return

    chat_id = message.get("chat", {}).get("id")
    raw_text = (message.get("text") or message.get("caption") or "").strip()
    if not raw_text or not chat_id:
        return

    logger.info(
        f"[Account @{account.username or account.user_id}] Incoming DM from {sender_id}: '{raw_text[:50]}...'"
    )

    # -----------------------------------------------------------------------
    # Step A: Predefined Trigger Containment Matching (.lower() optimized)
    # -----------------------------------------------------------------------
    text_lower = raw_text.lower()
    matched_reply: Optional[str] = None
    matched_keyword: Optional[str] = None

    for kw, reply_text in account.triggers.items():
        if kw in text_lower:
            matched_reply = reply_text
            matched_keyword = kw
            break

    if matched_reply:
        logger.info(
            f"[Account @{account.username or account.user_id}] Trigger matched '{matched_keyword}'. Sending reply."
        )
        success = await send_business_message(
            account.connection_id, chat_id, matched_reply
        )
        if success:
            account.processed_count += 1
        return

    # -----------------------------------------------------------------------
    # Step B: AI Chatbot Fallback Mode via OpenRouter
    # -----------------------------------------------------------------------
    if account.ai_fallback:
        if not OPENROUTER_API_KEY:
            logger.warning(
                f"[Account @{account.username or account.user_id}] AI Fallback requested, but OPENROUTER_API_KEY is not set."
            )
            return

        logger.info(
            f"[Account @{account.username or account.user_id}] No trigger matched. Initiating AI fallback (model: {account.model})."
        )

        # Native chat behavior alert: display 'typing...' feedback to sender
        await send_typing_indicator(account.connection_id, chat_id)

        ai_reply = await generate_openrouter_reply(raw_text, account.model)
        if ai_reply:
            success = await send_business_message(
                account.connection_id, chat_id, ai_reply
            )
            if success:
                account.processed_count += 1
                logger.info(
                    f"[Account @{account.username or account.user_id}] AI reply delivered to sender {sender_id}."
                )
        else:
            logger.warning(
                f"[Account @{account.username or account.user_id}] AI generation returned empty content."
            )


# ---------------------------------------------------------------------------
# Business Connection Registration
# ---------------------------------------------------------------------------
async def process_business_connection(conn: dict):
    """
    Handles when a Telegram user connects or disconnects this bot in their account.
    """
    connection_id = conn.get("id")
    user = conn.get("user", {})
    user_id = user.get("id")
    if not connection_id or not user_id:
        return

    is_enabled = conn.get("is_enabled", False)
    can_reply = conn.get("can_reply", False)
    username = user.get("username") or ""
    first_name = user.get("first_name") or ""

    if user_id in accounts:
        # Existing account updated
        account = accounts[user_id]
        account.connection_id = connection_id
        account.is_enabled = is_enabled
        account.can_reply = can_reply
        account.active = is_enabled
        account.username = username
        account.first_name = first_name
        connection_to_user[connection_id] = user_id
        logger.info(
            f"Updated Business Connection for @{username} (ID: {user_id}): enabled={is_enabled}, can_reply={can_reply}"
        )
    else:
        # New account connected
        account = ConnectedAccount(
            connection_id=connection_id,
            user_id=user_id,
            username=username,
            first_name=first_name,
            can_reply=can_reply,
            is_enabled=is_enabled,
            triggers_raw=DEFAULT_TRIGGERS_RAW,
            triggers=parse_triggers(DEFAULT_TRIGGERS_RAW),
            ai_fallback=True,
            model=DEFAULT_MODEL,
            active=is_enabled,
        )
        accounts[user_id] = account
        connection_to_user[connection_id] = user_id
        logger.info(
            f"[Connected] New Business Account linked: @{username} (ID: {user_id}), can_reply={can_reply}"
        )


# ---------------------------------------------------------------------------
# Background Polling Worker
# ---------------------------------------------------------------------------
async def run_bot_polling():
    """
    Long-polling worker fetching updates from Telegram Bot API.
    Handles standard messages, business connections, and business DMs.
    """
    global http_client
    offset = 0
    allowed_updates = [
        "message",
        "business_connection",
        "business_message",
        "edited_business_message",
    ]

    logger.info("Telegram Bot API polling worker initiated...")
    while True:
        try:
            if not http_client or not BOT_TOKEN:
                await asyncio.sleep(2.0)
                continue

            payload = {
                "offset": offset,
                "timeout": 20,
                "allowed_updates": allowed_updates,
            }

            url = f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates"
            resp = await http_client.post(url, json=payload, timeout=30.0)
            if resp.status_code != 200:
                await asyncio.sleep(3.0)
                continue

            data = resp.json()
            if not data.get("ok"):
                logger.error(f"Polling error: {data.get('description')}")
                await asyncio.sleep(4.0)
                continue

            updates = data.get("result", [])
            for update in updates:
                offset = update["update_id"] + 1

                # 1. Telegram Business Connection Update
                if "business_connection" in update:
                    await process_business_connection(update["business_connection"])

                # 2. Telegram Business Message Update (Incoming DMs on connected accounts)
                elif "business_message" in update:
                    await process_business_message(update["business_message"])

                # 3. Direct Message to the Bot itself (Help & Connection Instructions)
                elif "message" in update:
                    msg = update["message"]
                    chat_id = msg.get("chat", {}).get("id")
                    text = msg.get("text", "")
                    if text.startswith("/start") or text.startswith("/help"):
                        help_text = (
                            f"👋 Hello! I am your Multi-Account Business Auto-Responder.\n\n"
                            f"📌 To connect me to your personal Telegram account:\n"
                            f"1. Open Telegram Settings -> Telegram Business -> Chatbots\n"
                            f"2. Add @{bot_info.get('username')}\n"
                            f"3. Enable 'Reply to messages'\n"
                            f"4. Access the web dashboard to customize triggers and AI fallback!\n"
                        )
                        await tg_api_call(
                            "sendMessage", {"chat_id": chat_id, "text": help_text}
                        )

        except asyncio.CancelledError:
            logger.info("Telegram polling worker cancelled.")
            break
        except httpx.TimeoutException:
            continue
        except Exception as exc:
            logger.error(f"Exception inside polling loop: {exc}")
            await asyncio.sleep(2.0)


# ---------------------------------------------------------------------------
# FastAPI Application & Lifespan
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Manages connection pooling and background worker tasks cleanly.
    """
    global http_client, bot_info, polling_task

    logger.info("Initializing global HTTPX async client (pool max: 25)...")
    http_client = httpx.AsyncClient(
        timeout=httpx.Timeout(25.0, connect=5.0),
        limits=httpx.Limits(
            max_keepalive_connections=10, max_connections=25, keepalive_expiry=30.0
        ),
    )

    if BOT_TOKEN:
        # Fetch bot identity from Telegram
        me = await tg_api_call("getMe", {})
        if me:
            bot_info["username"] = me.get("username", "UnknownBot")
            bot_info["first_name"] = me.get("first_name", "Business Bot")
            logger.info(
                f"Bot verified: @{bot_info['username']} ({bot_info['first_name']})"
            )
        else:
            logger.error("Failed to verify BOT_TOKEN with Telegram API!")

        # Start background polling worker
        polling_task = asyncio.create_task(run_bot_polling())
    else:
        logger.warning("BOT_TOKEN is not configured! Waiting for user setup.")

    yield

    logger.info("Shutdown signal received. Cancelling background polling task...")
    if polling_task and not polling_task.done():
        polling_task.cancel()
        try:
            await asyncio.wait_for(polling_task, timeout=2.0)
        except (asyncio.CancelledError, asyncio.TimeoutError):
            pass

    if http_client:
        await http_client.aclose()
        logger.info("Global HTTPX client closed.")
    logger.info("Shutdown completed cleanly.")


app = FastAPI(
    title="Telegram Business Multi-Account Auto-Responder",
    lifespan=lifespan,
    docs_url=None,
    redoc_url=None,
)


# ---------------------------------------------------------------------------
# Server-Side Rendered UI Generator
# ---------------------------------------------------------------------------
def render_dashboard(msg: str = "", err: str = "") -> str:
    """
    Generates a single-page responsive dark-mode HTML dashboard without external CDNs.
    """
    active_count = sum(1 for a in accounts.values() if a.active and a.can_reply)
    total_accounts = len(accounts)

    banner_html = ""
    if msg:
        banner_html += f'<div class="alert alert-success">✓ {html.escape(msg)}</div>'
    if err:
        banner_html += f'<div class="alert alert-danger">⚠ {html.escape(err)}</div>'

    # Render Account Cards
    cards_html = ""
    if not accounts:
        cards_html = f"""
        <div class="empty-state">
            <div class="empty-icon">📱</div>
            <h3>No Accounts Connected Yet</h3>
            <p>Connect any personal Telegram account to this bot in 3 simple steps:</p>
            <ol class="steps-list">
                <li>Open your Telegram app & go to <strong>Settings</strong> → <strong>Telegram Business</strong> → <strong>Chatbots</strong></li>
                <li>Add your bot: <code>@{html.escape(bot_info.get("username", "your_bot"))}</code></li>
                <li>Check the box <strong>"Reply to messages"</strong> and tap Save.</li>
            </ol>
            <p class="empty-note">Once linked, the account will instantly appear here with full controls!</p>
        </div>
        """
    else:
        for user_id in sorted(accounts.keys()):
            acc = accounts[user_id]
            if acc.active and acc.can_reply:
                badge = '<span class="status-badge badge-active"><span class="dot pulse"></span> Active & Replying</span>'
                toggle_btn = (
                    f'<form method="POST" action="/account/{user_id}/toggle" style="margin:0;">'
                    f'<button type="submit" class="btn btn-danger btn-sm">Pause Bot</button>'
                    f'</form>'
                )
            elif not acc.can_reply:
                badge = '<span class="status-badge badge-inactive"><span class="dot"></span> Read-Only (Check Reply Permission)</span>'
                toggle_btn = (
                    f'<button class="btn btn-disabled btn-sm" disabled>No Reply Rights</button>'
                )
            else:
                badge = '<span class="status-badge badge-empty"><span class="dot"></span> Paused</span>'
                toggle_btn = (
                    f'<form method="POST" action="/account/{user_id}/toggle" style="margin:0;">'
                    f'<button type="submit" class="btn btn-success btn-sm">Resume Bot</button>'
                    f'</form>'
                )

            ai_checked = "checked" if acc.ai_fallback else ""
            user_title = (
                f"@{html.escape(acc.username)}"
                if acc.username
                else f"Account #{acc.user_id}"
            )

            cards_html += f"""
            <div class="card account-card">
                <div class="card-header">
                    <div>
                        <h3 class="card-title">{user_title}</h3>
                        <div class="card-subtitle">{html.escape(acc.first_name)} (User ID: {acc.user_id})</div>
                    </div>
                    <div class="card-header-actions">
                        {badge}
                        {toggle_btn}
                    </div>
                </div>

                <div class="stat-pill-row">
                    <span class="stat-pill">DMs Auto-Responded: <strong>{acc.processed_count}</strong></span>
                    <span class="stat-pill">Status: <strong>{'Enabled' if acc.is_enabled else 'Disconnected'}</strong></span>
                </div>

                <form method="POST" action="/account/{user_id}/update" class="account-form">
                    <div class="form-group">
                        <label>Predefined Triggers (comma or newline separated: keyword:response)</label>
                        <textarea name="triggers" rows="3" class="form-control" placeholder="keyword:response text, ...">{html.escape(acc.triggers_raw)}</textarea>
                    </div>

                    <div class="form-row">
                        <div class="form-group" style="flex: 2;">
                            <label>AI Model String (OpenRouter)</label>
                            <input type="text" name="model" class="form-control" value="{html.escape(acc.model)}" placeholder="e.g. google/gemini-2.5-flash">
                        </div>
                        <div class="form-group toggle-group" style="flex: 1;">
                            <label>AI Fallback</label>
                            <label class="switch">
                                <input type="checkbox" name="ai_fallback" value="on" {ai_checked}>
                                <span class="slider"></span>
                            </label>
                        </div>
                    </div>

                    <div class="card-footer">
                        <button type="submit" class="btn btn-secondary btn-full">Save @{html.escape(acc.username or str(acc.user_id))} Settings</button>
                    </div>
                </form>
            </div>
            """

    # Global Bulk Config values (reference from first account or default)
    bulk_triggers = (
        list(accounts.values())[0].triggers_raw if accounts else DEFAULT_TRIGGERS_RAW
    )
    bulk_model = list(accounts.values())[0].model if accounts else DEFAULT_MODEL

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>TG Business Multi-Account AutoResponder</title>
    <style>
        :root {{
            --bg-base: #0a0d14;
            --bg-surface: #121722;
            --bg-card: #182030;
            --border-color: #273349;
            --text-main: #f1f5f9;
            --text-muted: #94a3b8;
            --accent-blue: #38bdf8;
            --accent-green: #22c55e;
            --accent-red: #ef4444;
            --accent-amber: #f59e0b;
            --font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
        }}
        * {{ box-sizing: border-box; margin: 0; padding: 0; }}
        body {{
            background: var(--bg-base);
            color: var(--text-main);
            font-family: var(--font-family);
            line-height: 1.5;
            padding: 24px 16px;
        }}
        .container {{
            max-width: 1280px;
            margin: 0 auto;
        }}
        header {{
            display: flex;
            flex-wrap: wrap;
            justify-content: space-between;
            align-items: center;
            gap: 16px;
            margin-bottom: 24px;
            padding-bottom: 16px;
            border-bottom: 1px solid var(--border-color);
        }}
        .brand {{
            display: flex;
            align-items: center;
            gap: 12px;
        }}
        .brand-icon {{
            background: linear-gradient(135deg, #0284c7, #38bdf8);
            width: 44px;
            height: 44px;
            border-radius: 10px;
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 22px;
            font-weight: bold;
            color: #fff;
            box-shadow: 0 4px 12px rgba(56, 189, 248, 0.25);
        }}
        .brand-text h1 {{
            font-size: 20px;
            font-weight: 700;
            color: var(--text-main);
        }}
        .brand-text p {{
            font-size: 13px;
            color: var(--text-muted);
        }}
        .stats-bar {{
            display: flex;
            flex-wrap: wrap;
            gap: 12px;
        }}
        .stat-chip {{
            background: var(--bg-surface);
            border: 1px solid var(--border-color);
            padding: 6px 14px;
            border-radius: 20px;
            font-size: 13px;
            display: flex;
            align-items: center;
            gap: 8px;
        }}
        .stat-value {{
            font-weight: 600;
            color: var(--accent-blue);
        }}
        .alert {{
            padding: 12px 16px;
            border-radius: 8px;
            margin-bottom: 20px;
            font-size: 14px;
        }}
        .alert-success {{
            background: rgba(34, 197, 94, 0.15);
            border: 1px solid rgba(34, 197, 94, 0.3);
            color: #4ade80;
        }}
        .alert-danger {{
            background: rgba(239, 68, 68, 0.15);
            border: 1px solid rgba(239, 68, 68, 0.3);
            color: #f87171;
        }}
        .bulk-card {{
            background: linear-gradient(180deg, #182238 0%, #131a29 100%);
            border: 1px solid #3b82f6;
            border-radius: 12px;
            padding: 20px;
            margin-bottom: 28px;
            box-shadow: 0 4px 20px rgba(0, 0, 0, 0.3);
        }}
        .bulk-header {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 16px;
        }}
        .bulk-title {{
            font-size: 17px;
            font-weight: 700;
            color: #60a5fa;
            display: flex;
            align-items: center;
            gap: 8px;
        }}
        .bulk-desc {{
            font-size: 13px;
            color: var(--text-muted);
            margin-bottom: 16px;
        }}
        .grid {{
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(380px, 1fr));
            gap: 20px;
        }}
        .card {{
            background: var(--bg-surface);
            border: 1px solid var(--border-color);
            border-radius: 12px;
            padding: 20px;
            display: flex;
            flex-direction: column;
            transition: transform 0.15s ease, border-color 0.15s ease;
        }}
        .card:hover {{
            border-color: #3b4e6d;
        }}
        .card-header {{
            display: flex;
            justify-content: space-between;
            align-items: flex-start;
            margin-bottom: 14px;
            gap: 10px;
        }}
        .card-title {{
            font-size: 16px;
            font-weight: 600;
            color: var(--text-main);
        }}
        .card-subtitle {{
            font-size: 12px;
            color: var(--text-muted);
        }}
        .card-header-actions {{
            display: flex;
            align-items: center;
            gap: 8px;
        }}
        .status-badge {{
            display: inline-flex;
            align-items: center;
            gap: 6px;
            padding: 4px 10px;
            border-radius: 12px;
            font-size: 12px;
            font-weight: 600;
        }}
        .badge-active {{
            background: rgba(34, 197, 94, 0.15);
            color: #4ade80;
            border: 1px solid rgba(34, 197, 94, 0.3);
        }}
        .badge-inactive {{
            background: rgba(245, 158, 11, 0.15);
            color: #fbbf24;
            border: 1px solid rgba(245, 158, 11, 0.3);
        }}
        .badge-empty {{
            background: rgba(148, 163, 184, 0.12);
            color: #94a3b8;
            border: 1px solid rgba(148, 163, 184, 0.25);
        }}
        .dot {{
            width: 8px;
            height: 8px;
            border-radius: 50%;
            background: currentColor;
        }}
        .pulse {{
            box-shadow: 0 0 0 0 rgba(34, 197, 94, 0.7);
            animation: pulse 1.8s infinite;
        }}
        @keyframes pulse {{
            0% {{ box-shadow: 0 0 0 0 rgba(34, 197, 94, 0.7); }}
            70% {{ box-shadow: 0 0 0 6px rgba(34, 197, 94, 0); }}
            100% {{ box-shadow: 0 0 0 0 rgba(34, 197, 94, 0); }}
        }}
        .stat-pill-row {{
            display: flex;
            gap: 8px;
            margin-bottom: 14px;
        }}
        .stat-pill {{
            background: #0d121c;
            border: 1px solid var(--border-color);
            padding: 4px 10px;
            border-radius: 6px;
            font-size: 11px;
            color: var(--text-muted);
        }}
        .stat-pill strong {{
            color: var(--text-main);
        }}
        .form-group {{
            margin-bottom: 14px;
        }}
        .form-row {{
            display: flex;
            gap: 12px;
            align-items: flex-start;
        }}
        label {{
            display: block;
            font-size: 12px;
            font-weight: 500;
            color: var(--text-muted);
            margin-bottom: 6px;
        }}
        .form-control {{
            width: 100%;
            background: #0d121c;
            border: 1px solid var(--border-color);
            border-radius: 6px;
            padding: 8px 12px;
            color: var(--text-main);
            font-size: 13px;
            font-family: inherit;
            outline: none;
            transition: border-color 0.15s ease;
        }}
        .form-control:focus {{
            border-color: var(--accent-blue);
        }}
        textarea.form-control {{
            resize: vertical;
            min-height: 70px;
        }}
        .toggle-group {{
            display: flex;
            flex-direction: column;
            align-items: center;
        }}
        .switch {{
            position: relative;
            display: inline-block;
            width: 44px;
            height: 24px;
            margin-top: 4px;
        }}
        .switch input {{
            opacity: 0;
            width: 0;
            height: 0;
        }}
        .slider {{
            position: absolute;
            cursor: pointer;
            top: 0; left: 0; right: 0; bottom: 0;
            background-color: #243044;
            transition: .2s;
            border-radius: 24px;
        }}
        .slider:before {{
            position: absolute;
            content: "";
            height: 18px;
            width: 18px;
            left: 3px;
            bottom: 3px;
            background-color: white;
            transition: .2s;
            border-radius: 50%;
        }}
        input:checked + .slider {{
            background-color: #3b82f6;
        }}
        input:checked + .slider:before {{
            transform: translateX(20px);
        }}
        .btn {{
            display: inline-flex;
            align-items: center;
            justify-content: center;
            font-family: inherit;
            font-weight: 600;
            border-radius: 6px;
            cursor: pointer;
            border: none;
            transition: all 0.15s ease;
            text-decoration: none;
        }}
        .btn-sm {{
            padding: 5px 12px;
            font-size: 12px;
        }}
        .btn-full {{
            width: 100%;
            padding: 9px 16px;
            font-size: 13px;
        }}
        .btn-success {{
            background: #16a34a;
            color: #fff;
        }}
        .btn-success:hover {{ background: #15803d; }}
        .btn-danger {{
            background: #dc2626;
            color: #fff;
        }}
        .btn-danger:hover {{ background: #b91c1c; }}
        .btn-secondary {{
            background: #273349;
            color: var(--text-main);
        }}
        .btn-secondary:hover {{ background: #334155; }}
        .btn-primary {{
            background: linear-gradient(135deg, #2563eb, #3b82f6);
            color: #fff;
            box-shadow: 0 2px 10px rgba(37, 99, 235, 0.3);
        }}
        .btn-primary:hover {{ opacity: 0.95; }}
        .btn-disabled {{
            background: #1e293b;
            color: #64748b;
            cursor: not-allowed;
        }}
        .card-footer {{
            margin-top: auto;
            padding-top: 10px;
        }}
        .empty-state {{
            background: var(--bg-surface);
            border: 2px dashed var(--border-color);
            border-radius: 12px;
            padding: 40px 20px;
            text-align: center;
            max-width: 600px;
            margin: 0 auto;
        }}
        .empty-icon {{
            font-size: 44px;
            margin-bottom: 12px;
        }}
        .empty-state h3 {{
            font-size: 18px;
            margin-bottom: 8px;
        }}
        .empty-state p {{
            color: var(--text-muted);
            font-size: 14px;
            margin-bottom: 16px;
        }}
        .steps-list {{
            text-align: left;
            display: inline-block;
            background: #0d121c;
            padding: 16px 24px 16px 40px;
            border-radius: 8px;
            margin-bottom: 16px;
            font-size: 13px;
            line-height: 1.8;
        }}
        .steps-list code {{
            background: #1e293b;
            padding: 2px 6px;
            border-radius: 4px;
            color: var(--accent-blue);
        }}
        .empty-note {{
            font-size: 12px;
            color: var(--accent-green) !important;
        }}
        footer {{
            margin-top: 40px;
            padding-top: 20px;
            border-top: 1px solid var(--border-color);
            display: flex;
            flex-wrap: wrap;
            justify-content: space-between;
            align-items: center;
            color: var(--text-muted);
            font-size: 13px;
            gap: 12px;
        }}
        footer a {{
            color: var(--accent-blue);
            text-decoration: none;
        }}
        @media (max-width: 768px) {{
            .grid {{ grid-template-columns: 1fr; }}
            .form-row {{ flex-direction: column; }}
            .toggle-group {{ align-items: flex-start; }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <header>
            <div class="brand">
                <div class="brand-icon">🤖</div>
                <div class="brand-text">
                    <h1>Telegram Business AutoResponder</h1>
                    <p>Multi-Account Manager powered by @BotFather & Telegram Business API</p>
                </div>
            </div>
            <div class="stats-bar">
                <div class="stat-chip">
                    <span>Bot:</span>
                    <span class="stat-value">@{html.escape(bot_info.get("username", "Not Configured"))}</span>
                </div>
                <div class="stat-chip">
                    <span>Connected Accounts:</span>
                    <span class="stat-value">{active_count} / {total_accounts} Active</span>
                </div>
                <div class="stat-chip">
                    <span>OpenRouter:</span>
                    <span class="stat-value" style="color: {'#4ade80' if OPENROUTER_API_KEY else '#fbbf24'}">{'Ready' if OPENROUTER_API_KEY else 'API Key Missing'}</span>
                </div>
                <div class="stat-chip">
                    <span>API_ID/Hash:</span>
                    <span class="stat-value" style="color: #4ade80;">Not Needed!</span>
                </div>
            </div>
        </header>

        {banner_html}

        <!-- Persistent Bulk Update & Multi-Sync Tool -->
        <div class="bulk-card">
            <div class="bulk-header">
                <div class="bulk-title">
                    <span>⚡ Bulk Update & Multi-Sync Tool</span>
                </div>
                <span class="status-badge" style="background: rgba(59, 130, 246, 0.2); color: #93c5fd; border: 1px solid rgba(59, 130, 246, 0.3);">
                    Syncs All Connected Accounts
                </span>
            </div>
            <div class="bulk-desc">
                Overwrite trigger keywords, AI fallback switches, and AI model choices across ALL connected accounts in a single click.
            </div>
            <form method="POST" action="/bulk-update">
                <div class="form-group">
                    <label>Bulk Trigger Rules (keyword:response text, separated by commas or newlines)</label>
                    <textarea name="triggers" rows="2" class="form-control" placeholder="hello:Hi there!, pricing:Our pricing is $10/mo">{html.escape(bulk_triggers)}</textarea>
                </div>
                <div class="form-row">
                    <div class="form-group" style="flex: 2;">
                        <label>AI Model String for All Accounts</label>
                        <input type="text" name="model" class="form-control" value="{html.escape(bulk_model)}" placeholder="e.g. google/gemini-2.5-flash">
                    </div>
                    <div class="form-group toggle-group" style="flex: 1;">
                        <label>AI Fallback Mode</label>
                        <label class="switch">
                            <input type="checkbox" name="ai_fallback" value="on" checked>
                            <span class="slider"></span>
                        </label>
                    </div>
                </div>
                <button type="submit" class="btn btn-primary btn-full">⚡ Overwrite & Synchronize All Accounts</button>
            </form>
        </div>

        <!-- Connected Accounts Grid -->
        <div class="grid">
            {cards_html}
        </div>

        <footer>
            <div>Operational Health Endpoint: <a href="/health" target="_blank">/health</a> (Render Free Tier Keep-Alive)</div>
            <div>Powered by FastAPI + Telegram Business Bot API (No API_ID / API_HASH required)</div>
        </footer>
    </div>
</body>
</html>
"""


# ---------------------------------------------------------------------------
# HTTP Endpoints & Form Action Handlers
# ---------------------------------------------------------------------------
@app.get("/", response_class=HTMLResponse)
async def dashboard_view(msg: str = "", err: str = ""):
    """
    Renders the lightweight HTML UI dashboard.
    """
    return HTMLResponse(content=render_dashboard(msg=msg, err=err), status_code=200)


@app.get("/health")
async def health_check():
    """
    Operational monitoring endpoint for Render health checks and uptime monitoring.
    """
    active_count = sum(1 for a in accounts.values() if a.active and a.can_reply)
    return {
        "status": "healthy",
        "bot_username": bot_info.get("username"),
        "connected_accounts": len(accounts),
        "active_accounts": active_count,
        "openrouter_configured": bool(OPENROUTER_API_KEY),
    }


@app.post("/bulk-update")
async def bulk_update_accounts(request: Request):
    """
    Overwrites trigger rules, AI fallback state, and model string for all connected accounts.
    """
    body = (await request.body()).decode("utf-8")
    data = urllib.parse.parse_qs(body)

    triggers_raw = data.get("triggers", [""])[0].strip()
    ai_fallback = "ai_fallback" in data
    model = data.get("model", [DEFAULT_MODEL])[0].strip() or DEFAULT_MODEL
    parsed_rules = parse_triggers(triggers_raw)

    for acc in accounts.values():
        acc.triggers_raw = triggers_raw
        acc.triggers = parsed_rules
        acc.ai_fallback = ai_fallback
        acc.model = model

    logger.info(
        f"Bulk update executed: applied {len(parsed_rules)} triggers, AI fallback={ai_fallback}, model='{model}' across {len(accounts)} accounts."
    )
    return RedirectResponse(
        url="/?msg=All+connected+accounts+synchronized+successfully", status_code=303
    )


@app.post("/account/{user_id}/update")
async def update_account_config(user_id: int, request: Request):
    """
    Updates configuration for an individual connected business account.
    """
    if user_id not in accounts:
        return RedirectResponse(
            url="/?err=Invalid+account+identifier", status_code=303
        )

    acc = accounts[user_id]
    body = (await request.body()).decode("utf-8")
    data = urllib.parse.parse_qs(body)

    acc.triggers_raw = data.get("triggers", [""])[0].strip()
    acc.triggers = parse_triggers(acc.triggers_raw)
    acc.ai_fallback = "ai_fallback" in data
    acc.model = data.get("model", [DEFAULT_MODEL])[0].strip() or DEFAULT_MODEL

    logger.info(
        f"[Account @{acc.username or user_id}] Updated: {len(acc.triggers)} triggers, AI fallback={acc.ai_fallback}, model='{acc.model}'"
    )
    return RedirectResponse(
        url=f"/?msg=Account+settings+saved+successfully", status_code=303
    )


@app.post("/account/{user_id}/toggle")
async def toggle_account_active(user_id: int):
    """
    Toggles the active auto-responding state for a connected account.
    """
    if user_id not in accounts:
        return RedirectResponse(
            url="/?err=Invalid+account+identifier", status_code=303
        )

    acc = accounts[user_id]
    acc.active = not acc.active
    status_str = "resumed" if acc.active else "paused"
    logger.info(f"[Account @{acc.username or user_id}] Auto-responder {status_str}.")
    return RedirectResponse(
        url=f"/?msg=Account+auto-responder+{status_str}", status_code=303
    )


# ---------------------------------------------------------------------------
# Direct Execution Entry Point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=PORT, reload=False, workers=1)
