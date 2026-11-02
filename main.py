"""
Telegram Multi-Account Auto-Responder Userbot Framework
Production-ready single-file FastAPI + Telethon service optimized for Render Free Tier.
"""

import asyncio
import html
import logging
import os
import sys
import urllib.parse
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Dict, Optional

import httpx
import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from telethon import TelegramClient, events
from telethon.sessions import StringSession

# ---------------------------------------------------------------------------
# Logging Configuration
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s]: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("tg_userbot")

# ---------------------------------------------------------------------------
# Global Environment & Configuration
# ---------------------------------------------------------------------------
TELEGRAM_API_ID_RAW = os.getenv("TELEGRAM_API_ID") or os.getenv("API_ID") or "0"
try:
    API_ID = int(TELEGRAM_API_ID_RAW.strip())
except ValueError:
    API_ID = 0

API_HASH = (os.getenv("TELEGRAM_API_HASH") or os.getenv("API_HASH") or "").strip()
OPENROUTER_API_KEY = (os.getenv("OPENROUTER_API_KEY") or os.getenv("OPENAI_API_KEY") or "").strip()
DEFAULT_MODEL = os.getenv("DEFAULT_AI_MODEL", "google/gemini-2.5-flash")
PORT = int(os.getenv("PORT", 8000))

# Shared HTTPX AsyncClient instance across the entire application lifecycle
http_client: Optional[httpx.AsyncClient] = None

# Default trigger rules used to initialize slots
DEFAULT_TRIGGERS_RAW = (
    "hello:Hello! Thanks for reaching out. How can I assist you today?,\n"
    "pricing:Our services start at $19/mo. For custom enterprise tiers, let us know!,\n"
    "support:You can reach out to our team at support@example.com."
)


# ---------------------------------------------------------------------------
# Rule Parsing Utilities
# ---------------------------------------------------------------------------
def parse_triggers(raw: str) -> Dict[str, str]:
    """
    Parse comma-separated or newline-separated trigger strings into a dictionary.
    Format: 'keyword:response text'
    Handles commas embedded within the response text safely.
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
        current_resp_parts: list[str] = []

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
# In-Memory State Model
# ---------------------------------------------------------------------------
@dataclass
class AccountSlot:
    slot_id: int
    session_str: str = ""
    active: bool = False
    connected: bool = False
    user_id: Optional[int] = None
    username: str = ""
    first_name: str = ""
    error_message: str = ""
    triggers_raw: str = DEFAULT_TRIGGERS_RAW
    triggers: Dict[str, str] = field(default_factory=dict)
    ai_fallback: bool = True
    model: str = DEFAULT_MODEL
    client: Optional[TelegramClient] = None
    task: Optional[asyncio.Task] = None
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)


# In-memory dictionary for up to 5 concurrent profile slots
accounts: Dict[int, AccountSlot] = {}

for slot_num in range(1, 6):
    env_session = os.getenv(f"SESSION_{slot_num}", "").strip()
    accounts[slot_num] = AccountSlot(
        slot_id=slot_num,
        session_str=env_session,
        triggers_raw=DEFAULT_TRIGGERS_RAW,
        triggers=parse_triggers(DEFAULT_TRIGGERS_RAW),
        ai_fallback=True,
        model=DEFAULT_MODEL,
    )


# ---------------------------------------------------------------------------
# AI Generation via OpenRouter (Step B Fallback)
# ---------------------------------------------------------------------------
async def generate_openrouter_reply(user_message: str, model: str) -> Optional[str]:
    """
    Generate an AI reply via OpenRouter using the global HTTPX client.
    Enforces strict brevity guidelines as requested.
    """
    global http_client
    if not http_client or not OPENROUTER_API_KEY:
        return None

    headers = {
        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
        "Content-Type": "application/json",
        "HTTP-Referer": "https://render.com",
        "X-Title": "TG Userbot AutoResponder",
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
# Message Handler Pipeline (Step A Triggers -> Step B AI Fallback)
# ---------------------------------------------------------------------------
async def handle_incoming_dm(slot: AccountSlot, event: events.NewMessage.Event):
    """
    Process incoming direct messages (DMs). Groups and channels are completely ignored.
    """
    try:
        # Strictly ignore non-private chats (basic groups, supergroups, and broadcast channels)
        if not event.is_private:
            return

        # Ignore outgoing messages sent by the userbot account itself
        if event.out:
            return

        # Ignore messages sent by automated bot accounts to prevent infinite reply loops
        sender = await event.get_sender()
        if sender and getattr(sender, "bot", False):
            return

        raw_text = (event.raw_text or "").strip()
        if not raw_text:
            return

        sender_id = event.sender_id
        logger.info(
            f"[Slot {slot.slot_id}] Incoming DM from {sender_id}: '{raw_text[:50]}...'"
        )

        # -------------------------------------------------------------------
        # Step A: Predefined Trigger Containment Matching (.lower() optimized)
        # -------------------------------------------------------------------
        text_lower = raw_text.lower()
        matched_reply: Optional[str] = None
        matched_keyword: Optional[str] = None

        for kw, reply_text in slot.triggers.items():
            if kw in text_lower:
                matched_reply = reply_text
                matched_keyword = kw
                break

        if matched_reply:
            logger.info(
                f"[Slot {slot.slot_id}] Trigger matched keyword '{matched_keyword}'. Sending instant response."
            )
            await event.reply(matched_reply)
            return

        # -------------------------------------------------------------------
        # Step B: AI Chatbot Fallback Mode via OpenRouter
        # -------------------------------------------------------------------
        if slot.ai_fallback:
            if not OPENROUTER_API_KEY:
                logger.warning(
                    f"[Slot {slot.slot_id}] AI Fallback requested, but OPENROUTER_API_KEY is not set."
                )
                return

            if not slot.client:
                return

            logger.info(
                f"[Slot {slot.slot_id}] No trigger matched. Initiating AI fallback using model: {slot.model}"
            )

            # Native chat behavior alert: display 'typing...' feedback cleanly
            async with slot.client.action(event.chat_id, "typing"):
                ai_reply = await generate_openrouter_reply(raw_text, slot.model)
                if ai_reply:
                    await event.reply(ai_reply)
                    logger.info(
                        f"[Slot {slot.slot_id}] AI response delivered to sender {sender_id}."
                    )
                else:
                    logger.warning(
                        f"[Slot {slot.slot_id}] AI reply resolution returned no content."
                    )

    except Exception as exc:
        logger.error(
            f"[Slot {slot.slot_id}] Error handling incoming message: {exc}",
            exc_info=True,
        )


# ---------------------------------------------------------------------------
# Account Lifecycle Management
# ---------------------------------------------------------------------------
async def run_client_task(slot_id: int):
    """
    Background worker maintaining Telethon's event listener loop for a single account.
    """
    slot = accounts.get(slot_id)
    if not slot or not slot.client:
        return

    try:
        logger.info(f"[Slot {slot_id}] Background listener running...")
        await slot.client.run_until_disconnected()
    except asyncio.CancelledError:
        logger.info(f"[Slot {slot_id}] Background listener task cancelled.")
    except Exception as exc:
        logger.error(f"[Slot {slot_id}] Telegram client loop error: {exc}")
        slot.error_message = str(exc)
    finally:
        slot.connected = False
        logger.info(f"[Slot {slot_id}] Client connection closed.")


async def activate_account(slot_id: int) -> bool:
    """
    Initializes and starts a Telethon MTProto client for the given account slot.
    """
    slot = accounts.get(slot_id)
    if not slot:
        return False

    async with slot.lock:
        if slot.connected and slot.client:
            return True

        slot.error_message = ""

        if not API_ID or not API_HASH:
            slot.error_message = "TELEGRAM_API_ID or TELEGRAM_API_HASH is not configured."
            logger.error(f"[Slot {slot_id}] Connection aborted: {slot.error_message}")
            return False

        if not slot.session_str:
            slot.error_message = f"SESSION_{slot_id} is missing. Set it in environment variables or UI."
            logger.warning(f"[Slot {slot_id}] Connection aborted: {slot.error_message}")
            return False

        try:
            logger.info(f"[Slot {slot_id}] Connecting MTProto client via StringSession...")
            client = TelegramClient(
                StringSession(slot.session_str),
                API_ID,
                API_HASH,
                auto_reconnect=True,
                connection_retries=5,
                retry_delay=2,
            )

            await client.connect()

            if not await client.is_user_authorized():
                slot.error_message = "StringSession is unauthorized, invalid, or expired."
                logger.error(f"[Slot {slot_id}] Auth failed: {slot.error_message}")
                await client.disconnect()
                return False

            me = await client.get_me()
            slot.user_id = me.id
            slot.username = me.username or ""
            slot.first_name = me.first_name or ""
            slot.client = client
            slot.connected = True
            slot.active = True

            # Register dynamic event handler bound to this slot
            @client.on(events.NewMessage(incoming=True))
            async def event_handler(event: events.NewMessage.Event, current_slot=slot):
                await handle_incoming_dm(current_slot, event)

            # Spawn concurrent background task
            slot.task = asyncio.create_task(run_client_task(slot_id))
            logger.info(
                f"[Slot {slot_id}] Connected successfully as @{slot.username or slot.user_id} ({slot.first_name})"
            )
            return True

        except Exception as exc:
            slot.error_message = str(exc)
            slot.connected = False
            slot.active = False
            logger.error(f"[Slot {slot_id}] Activation exception: {exc}", exc_info=True)
            if slot.client:
                try:
                    await slot.client.disconnect()
                except Exception:
                    pass
                slot.client = None
            return False


async def deactivate_account(slot_id: int) -> bool:
    """
    Safely disconnects MTProto client and stops background task for an account slot.
    """
    slot = accounts.get(slot_id)
    if not slot:
        return False

    async with slot.lock:
        slot.active = False
        if slot.client:
            try:
                if slot.client.is_connected():
                    await slot.client.disconnect()
            except Exception as exc:
                logger.warning(f"[Slot {slot_id}] Disconnect warning: {exc}")
            slot.client = None

        if slot.task and not slot.task.done():
            slot.task.cancel()
            try:
                await asyncio.wait_for(slot.task, timeout=2.5)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass
            slot.task = None

        slot.connected = False
        logger.info(f"[Slot {slot_id}] Deactivated successfully.")
        return True


# ---------------------------------------------------------------------------
# FastAPI Application & Lifespan
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifespan context manager to handle application boot and graceful shutdown handshakes.
    """
    global http_client
    logger.info("Initializing global HTTPX async client (pool max: 25)...")
    http_client = httpx.AsyncClient(
        timeout=httpx.Timeout(20.0, connect=5.0),
        limits=httpx.Limits(
            max_keepalive_connections=10, max_connections=25, keepalive_expiry=30.0
        ),
    )

    logger.info("FastAPI service started. Launching configured Telegram accounts...")
    for slot_id, slot in accounts.items():
        if slot.session_str:
            asyncio.create_task(activate_account(slot_id))

    yield

    logger.info("Shutdown signal received. Initiating graceful MTProto disconnection...")
    shutdown_tasks = [deactivate_account(sid) for sid in accounts.keys()]
    if shutdown_tasks:
        await asyncio.gather(*shutdown_tasks, return_exceptions=True)

    if http_client:
        await http_client.aclose()
        logger.info("Global HTTPX client closed.")
    logger.info("Shutdown sequence finished cleanly.")


app = FastAPI(
    title="Telegram Multi-Account Auto-Responder",
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
    active_count = sum(1 for s in accounts.values() if s.connected)
    total_slots = len(accounts)

    banner_html = ""
    if msg:
        banner_html += (
            f'<div class="alert alert-success">✓ {html.escape(msg)}</div>'
        )
    if err:
        banner_html += (
            f'<div class="alert alert-danger">⚠ {html.escape(err)}</div>'
        )

    # Render Account Cards
    cards_html = ""
    for slot_id in sorted(accounts.keys()):
        slot = accounts[slot_id]
        if slot.connected:
            badge = '<span class="status-badge badge-active"><span class="dot pulse"></span> Connected</span>'
            user_title = f"@{html.escape(slot.username)}" if slot.username else f"ID: {slot.user_id}"
            toggle_btn = (
                f'<form method="POST" action="/account/{slot_id}/toggle" style="margin:0;">'
                f'<button type="submit" class="btn btn-danger btn-sm">Disconnect</button>'
                f'</form>'
            )
        elif slot.session_str:
            badge = '<span class="status-badge badge-inactive"><span class="dot"></span> Inactive</span>'
            user_title = "Ready to Connect"
            toggle_btn = (
                f'<form method="POST" action="/account/{slot_id}/toggle" style="margin:0;">'
                f'<button type="submit" class="btn btn-success btn-sm">Connect</button>'
                f'</form>'
            )
        else:
            badge = '<span class="status-badge badge-empty"><span class="dot"></span> No Session</span>'
            user_title = "Unconfigured"
            toggle_btn = (
                f'<button class="btn btn-disabled btn-sm" disabled>No Session</button>'
            )

        err_banner = ""
        if slot.error_message:
            err_banner = f'<div class="card-error">Error: {html.escape(slot.error_message)}</div>'

        ai_checked = "checked" if slot.ai_fallback else ""

        cards_html += f"""
        <div class="card account-card">
            <div class="card-header">
                <div>
                    <h3 class="card-title">Slot #{slot_id}: {user_title}</h3>
                    <div class="card-subtitle">{html.escape(slot.first_name) if slot.first_name else "Telegram Userbot Profile"}</div>
                </div>
                <div class="card-header-actions">
                    {badge}
                    {toggle_btn}
                </div>
            </div>

            {err_banner}

            <form method="POST" action="/account/{slot_id}/update" class="account-form">
                <div class="form-group">
                    <label>Predefined Triggers (comma or newline separated: keyword:response)</label>
                    <textarea name="triggers" rows="3" class="form-control" placeholder="keyword:response text, ...">{html.escape(slot.triggers_raw)}</textarea>
                </div>

                <div class="form-row">
                    <div class="form-group" style="flex: 2;">
                        <label>AI Model String (OpenRouter)</label>
                        <input type="text" name="model" class="form-control" value="{html.escape(slot.model)}" placeholder="e.g. google/gemini-2.5-flash">
                    </div>
                    <div class="form-group toggle-group" style="flex: 1;">
                        <label>AI Fallback</label>
                        <label class="switch">
                            <input type="checkbox" name="ai_fallback" value="on" {ai_checked}>
                            <span class="slider"></span>
                        </label>
                    </div>
                </div>

                <div class="form-group">
                    <label>Session String (Leave unchanged or update)</label>
                    <input type="password" name="session_str" class="form-control session-input" value="{html.escape(slot.session_str)}" placeholder="Paste Telethon StringSession here...">
                </div>

                <div class="card-footer">
                    <button type="submit" class="btn btn-secondary btn-full">Save Slot #{slot_id} Settings</button>
                </div>
            </form>
        </div>
        """

    # Global Bulk Config values (take from slot 1 as reference)
    bulk_triggers = accounts[1].triggers_raw if accounts else DEFAULT_TRIGGERS_RAW
    bulk_model = accounts[1].model if accounts else DEFAULT_MODEL

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>TG Multi-Account AutoResponder</title>
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
            --accent-purple: #a855f7;
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
        .card-error {{
            background: rgba(239, 68, 68, 0.12);
            border: 1px solid rgba(239, 68, 68, 0.3);
            border-radius: 6px;
            padding: 8px 12px;
            font-size: 12px;
            color: #fca5a5;
            margin-bottom: 14px;
            word-break: break-word;
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
        .session-input {{
            font-family: monospace;
            font-size: 11px;
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
        footer a:hover {{
            text-decoration: underline;
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
                <div class="brand-icon">TG</div>
                <div class="brand-text">
                    <h1>AutoResponder Userbot Hub</h1>
                    <p>Low-Footprint Multi-Account Telethon MTProto Engine</p>
                </div>
            </div>
            <div class="stats-bar">
                <div class="stat-chip">
                    <span>Active Bots:</span>
                    <span class="stat-value">{active_count} / {total_slots}</span>
                </div>
                <div class="stat-chip">
                    <span>Telegram API:</span>
                    <span class="stat-value" style="color: {'#4ade80' if API_ID and API_HASH else '#f87171'}">{'Configured' if API_ID and API_HASH else 'Missing ID/Hash'}</span>
                </div>
                <div class="stat-chip">
                    <span>OpenRouter:</span>
                    <span class="stat-value" style="color: {'#4ade80' if OPENROUTER_API_KEY else '#fbbf24'}">{'Ready' if OPENROUTER_API_KEY else 'API Key Not Set'}</span>
                </div>
                <div class="stat-chip">
                    <span>Target RAM:</span>
                    <span class="stat-value">&lt; 120MB (Render Free Tier)</span>
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
                    Syncs Slots 1–5 Simultaneously
                </span>
            </div>
            <div class="bulk-desc">
                Overwrite trigger keywords, AI fallback switches, and model specifications across ALL accounts in a single click.
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
                <button type="submit" class="btn btn-primary btn-full">⚡ Overwrite & Synchronize All 5 Accounts</button>
            </form>
        </div>

        <!-- Account Cards Grid -->
        <div class="grid">
            {cards_html}
        </div>

        <footer>
            <div>Operational Health Endpoint: <a href="/health" target="_blank">/health</a> (Monitored for Render Free Tier Keep-Alive)</div>
            <div>Built with FastAPI, Uvicorn & Telethon MTProto Client API</div>
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
    Renders the lightweight HTML UI panel.
    """
    return HTMLResponse(content=render_dashboard(msg=msg, err=err), status_code=200)


@app.get("/health")
async def health_check():
    """
    Operational monitoring endpoint for Render health checks and uptime monitoring.
    """
    active_count = sum(1 for s in accounts.values() if s.connected)
    return {
        "status": "healthy",
        "active_accounts": active_count,
        "total_slots": len(accounts),
        "telegram_configured": bool(API_ID and API_HASH),
        "openrouter_configured": bool(OPENROUTER_API_KEY),
    }


@app.post("/bulk-update")
async def bulk_update_accounts(request: Request):
    """
    Overwrites trigger rules, AI fallback state, and model string for all 5 accounts.
    """
    body = (await request.body()).decode("utf-8")
    data = urllib.parse.parse_qs(body)

    triggers_raw = data.get("triggers", [""])[0].strip()
    ai_fallback = "ai_fallback" in data
    model = data.get("model", [DEFAULT_MODEL])[0].strip() or DEFAULT_MODEL
    parsed_rules = parse_triggers(triggers_raw)

    for slot in accounts.values():
        slot.triggers_raw = triggers_raw
        slot.triggers = parsed_rules
        slot.ai_fallback = ai_fallback
        slot.model = model

    logger.info(
        f"Bulk update executed: applied {len(parsed_rules)} triggers, AI fallback={ai_fallback}, model='{model}' across all accounts."
    )
    return RedirectResponse(
        url="/?msg=All+profile+slots+synchronized+successfully", status_code=303
    )


@app.post("/account/{slot_id}/update")
async def update_account_config(slot_id: int, request: Request):
    """
    Updates configuration for an individual profile slot.
    """
    if slot_id not in accounts:
        return RedirectResponse(url="/?err=Invalid+slot+identifier", status_code=303)

    slot = accounts[slot_id]
    body = (await request.body()).decode("utf-8")
    data = urllib.parse.parse_qs(body)

    slot.triggers_raw = data.get("triggers", [""])[0].strip()
    slot.triggers = parse_triggers(slot.triggers_raw)
    slot.ai_fallback = "ai_fallback" in data
    slot.model = data.get("model", [DEFAULT_MODEL])[0].strip() or DEFAULT_MODEL

    new_session = data.get("session_str", [""])[0].strip()
    if new_session != slot.session_str:
        slot.session_str = new_session
        slot.error_message = ""

    logger.info(
        f"[Slot {slot_id}] Configuration updated: {len(slot.triggers)} rules, AI fallback={slot.ai_fallback}, model='{slot.model}'"
    )
    return RedirectResponse(
        url=f"/?msg=Slot+{slot_id}+configuration+saved", status_code=303
    )


@app.post("/account/{slot_id}/toggle")
async def toggle_account_connection(slot_id: int):
    """
    Toggles the MTProto runtime connection state for an individual account slot.
    """
    if slot_id not in accounts:
        return RedirectResponse(url="/?err=Invalid+slot+identifier", status_code=303)

    slot = accounts[slot_id]
    if slot.connected:
        await deactivate_account(slot_id)
        return RedirectResponse(
            url=f"/?msg=Slot+{slot_id}+disconnected", status_code=303
        )
    else:
        success = await activate_account(slot_id)
        if success:
            return RedirectResponse(
                url=f"/?msg=Slot+{slot_id}+connected+successfully", status_code=303
            )
        else:
            err_msg = urllib.parse.quote_plus(
                slot.error_message or "Connection failed"
            )
            return RedirectResponse(
                url=f"/?err=Slot+{slot_id}+failed:+{err_msg}", status_code=303
            )


# ---------------------------------------------------------------------------
# Direct Execution Entry Point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=PORT, reload=False, workers=1)

