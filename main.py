"""
Telegram Business Multi-Account Auto-Responder Framework
Production-ready single-file FastAPI + Telegram Business Bot API + MongoDB persistence.
Includes interactive Telegram DM Setup Wizard & Real-Time Server-Side Web Dashboard.
"""

import asyncio
from datetime import datetime, timezone
import html
import logging
import os
import re
import sys
import urllib.parse
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import httpx
import motor.motor_asyncio
import uvicorn
from fastapi import APIRouter, FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

# Pre-compiled high-performance regular expressions
LINK_NORMALIZE_REGEX = re.compile(
    r"(?<!https://)(?<!http://)\b((?:t|telegram)\.me/[^\s]+)"
)
RULE_SPLIT_REGEX = re.compile(r",\s*(?=[a-zA-Z0-9_\-\s]+:)")

# ---------------------------------------------------------------------------
# Logging Configuration
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s]: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("tg_business_bot")
# Suppress noisy background reconnection tracebacks from pymongo driver
logging.getLogger("pymongo").setLevel(logging.CRITICAL)

# ---------------------------------------------------------------------------
# Global Environment & Configuration
# ---------------------------------------------------------------------------
BOT_TOKEN = (os.getenv("BOT_TOKEN") or os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
OPENROUTER_API_KEY = (
    os.getenv("OPENROUTER_API_KEY") or os.getenv("OPENAI_API_KEY") or ""
).strip()
DEFAULT_MODEL = os.getenv("DEFAULT_AI_MODEL", "google/gemini-2.5-flash").strip()
MONGO_URI = (
    os.getenv("MONGO_URI") or os.getenv("MONGODB_URI") or os.getenv("MONGO_URL") or ""
).strip()
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "tg_business_autoresponder").strip()
PORT = int(os.getenv("PORT", 8000))

# Service start time for uptime tracking
service_start_time = datetime.now(timezone.utc)
SERVICE_NAME = os.getenv("SERVICE_NAME", "TelegramBusinessAutoResponder").strip()

# Shared HTTPX AsyncClient and Bot identity
http_client: Optional[httpx.AsyncClient] = None
bot_info: Dict[str, Any] = {
    "username": "BotFatherBot",
    "first_name": "Business Bot",
    "id": None,
}
polling_task: Optional[asyncio.Task] = None

# Default trigger rules for all accounts
DEFAULT_TRIGGERS_RAW = (
    "hello:Hello! Thanks for reaching out. How can I assist you today?,\n"
    "pricing:Our services start at $19/mo. For custom tiers, contact our team!,\n"
    "support:You can reach out to our support team at support@example.com."
)

# Default custom auto-reply message (supports plain text and rich link preview cards)
DEFAULT_CUSTOM_MESSAGE = (
    os.getenv("DEFAULT_CUSTOM_MESSAGE")
    or "Hello! Thanks for connecting. How can I assist you today?"
).strip()

# User session state for interactive Telegram DM Wizard
# e.g., {user_id: {"action": "awaiting_trigger", "target_account_id": 123456}}
user_wizard_state: Dict[int, Dict[str, Any]] = {}


# ---------------------------------------------------------------------------
# Database Layer (MongoDB via Motor async driver)
# ---------------------------------------------------------------------------
class DatabaseManager:
    """
    Manages non-blocking async persistence with MongoDB.
    Maintains collections for accounts, audit message logs, and global settings.
    """

    def __init__(self):
        self.client: Optional[motor.motor_asyncio.AsyncIOMotorClient] = None
        self.db = None
        self.accounts_col = None
        self.logs_col = None
        self.settings_col = None
        self.connected = False

    async def connect(self):
        if not MONGO_URI:
            logger.warning(
                "MONGO_URI not configured. Operating in in-memory mode with volatile persistence."
            )
            return

        # Check for unreplaced placeholder template strings in MONGO_URI
        if any(
            p in MONGO_URI
            for p in ["<password>", "<db_password>", "username:password", "<username>"]
        ):
            logger.error(
                "[DB] MONGO_URI contains unreplaced placeholder text (like '<password>' or 'username:password'). "
                "Please replace it with your actual MongoDB Atlas database username and password in Render Environment Variables. "
                "Operating in in-memory mode safely."
            )
            return

        try:
            logger.info("Connecting to MongoDB cluster...")
            self.client = motor.motor_asyncio.AsyncIOMotorClient(
                MONGO_URI,
                serverSelectionTimeoutMS=4000,
                connectTimeoutMS=4000,
                maxPoolSize=20,
                minPoolSize=1,
            )
            # Ping database to verify connection
            await self.client.admin.command("ping")
            self.db = self.client[MONGO_DB_NAME]
            self.accounts_col = self.db["accounts"]
            self.logs_col = self.db["message_logs"]
            self.settings_col = self.db["bot_settings"]

            # Initialize database indexes
            await self.accounts_col.create_index("connection_id", sparse=True)
            await self.accounts_col.create_index("username")
            await self.logs_col.create_index([("account_id", 1), ("timestamp", -1)])
            await self.logs_col.create_index("timestamp")

            self.connected = True
            logger.info(
                f"[DB] Successfully connected to MongoDB database '{MONGO_DB_NAME}'. Indexes verified."
            )
        except Exception as exc:
            self.connected = False
            if self.client:
                try:
                    self.client.close()
                except Exception:
                    pass
                self.client = None
            self.db = None
            err_msg = str(exc)
            if (
                "Authentication failed" in err_msg
                or "bad auth" in err_msg
                or "code 8000" in err_msg
            ):
                logger.error(
                    "[DB] MongoDB Authentication Failed (bad auth): The username or password in MONGO_URI is incorrect. "
                    "Please verify your Database User credentials in MongoDB Atlas (Security -> Database Access). "
                    "Falling back to in-memory mode safely."
                )
            else:
                logger.error(
                    f"[DB] Failed to connect to MongoDB ({err_msg}). Falling back to in-memory mode.",
                    exc_info=False,
                )

    async def close(self):
        if self.client:
            self.client.close()
            logger.info("[DB] MongoDB client connection pool closed.")

    async def load_all_accounts(self) -> List[dict]:
        if not self.connected or self.accounts_col is None:
            return []
        try:
            cursor = self.accounts_col.find({})
            return await cursor.to_list(length=100)
        except Exception as exc:
            logger.error(f"[DB] Error loading accounts from MongoDB: {exc}")
            return []

    async def save_account(self, account: "ConnectedAccount"):
        if not self.connected or self.accounts_col is None:
            return
        try:
            doc = {
                "_id": account.user_id,
                "user_id": account.user_id,
                "connection_id": account.connection_id,
                "username": account.username,
                "first_name": account.first_name,
                "can_reply": account.can_reply,
                "is_enabled": account.is_enabled,
                "active": account.active,
                "away_mode": account.away_mode,
                "away_timeout_minutes": account.away_timeout_minutes,
                "last_owner_activity": account.last_owner_activity,
                "keyword_triggers": account.keyword_triggers,
                "trigger_all": account.trigger_all,
                "custom_message": account.custom_message,
                "triggers_raw": account.triggers_raw,
                "triggers": account.triggers,
                "ai_fallback": account.ai_fallback,
                "model": account.model,
                "processed_count": account.processed_count,
                "updated_at": datetime.now(timezone.utc),
            }
            await self.accounts_col.update_one(
                {"_id": account.user_id},
                {"$set": doc, "$setOnInsert": {"created_at": datetime.now(timezone.utc)}},
                upsert=True,
            )
        except Exception as exc:
            logger.error(f"[DB] Failed to save account {account.user_id}: {exc}")

    async def increment_processed_count(self, user_id: int):
        if not self.connected or self.accounts_col is None:
            return
        try:
            await self.accounts_col.update_one(
                {"_id": user_id},
                {
                    "$inc": {"processed_count": 1},
                    "$set": {"updated_at": datetime.now(timezone.utc)},
                },
            )
        except Exception as exc:
            logger.error(f"[DB] Failed to increment count for user {user_id}: {exc}")

    async def log_interaction(
        self,
        account_id: int,
        customer_id: int,
        customer_name: str,
        incoming_text: str,
        response_type: str,
        response_text: str,
        matched_keyword: Optional[str] = None,
        ai_model: Optional[str] = None,
    ):
        if not self.connected or self.logs_col is None:
            return
        try:
            log_doc = {
                "account_id": account_id,
                "customer_id": customer_id,
                "customer_name": customer_name,
                "incoming_text": incoming_text[:500],
                "response_type": response_type,
                "matched_keyword": matched_keyword,
                "ai_model": ai_model,
                "response_text": response_text[:1000],
                "timestamp": datetime.now(timezone.utc),
            }
            await self.logs_col.insert_one(log_doc)
        except Exception as exc:
            logger.error(f"[DB] Failed to record message audit log: {exc}")

    async def get_account_stats(self, account_id: int) -> dict:
        if not self.connected or self.logs_col is None:
            return {"total": 0, "triggers": 0, "ai": 0}
        try:
            total = await self.logs_col.count_documents({"account_id": account_id})
            triggers = await self.logs_col.count_documents(
                {"account_id": account_id, "response_type": "trigger"}
            )
            ai = await self.logs_col.count_documents(
                {"account_id": account_id, "response_type": "ai_fallback"}
            )
            return {"total": total, "triggers": triggers, "ai": ai}
        except Exception:
            return {"total": 0, "triggers": 0, "ai": 0}

    async def get_health_metrics(self) -> dict:
        now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")
        if not self.connected or self.client is None or self.db is None:
            triggers_sum = sum(a.processed_count for a in accounts.values())
            return {
                "connected": False,
                "accounts_saved": len(accounts),
                "messages_logged": 0,
                "triggers_matched": triggers_sum,
                "ai_fallbacks_generated": 0,
                "last_heartbeat": now_str,
            }
        try:
            await self.client.admin.command("ping")
            accounts_count = await self.accounts_col.count_documents({})
            messages_count = await self.logs_col.count_documents({})
            triggers_count = await self.logs_col.count_documents(
                {"response_type": "trigger"}
            )
            ai_count = await self.logs_col.count_documents(
                {"response_type": "ai_fallback"}
            )
            return {
                "connected": True,
                "accounts_saved": accounts_count,
                "messages_logged": messages_count,
                "triggers_matched": triggers_count,
                "ai_fallbacks_generated": ai_count,
                "last_heartbeat": now_str,
            }
        except Exception as exc:
            logger.error(f"[DB] Health heartbeat ping failed: {exc}")
            return {
                "connected": False,
                "accounts_saved": len(accounts),
                "messages_logged": 0,
                "triggers_matched": 0,
                "ai_fallbacks_generated": 0,
                "last_heartbeat": now_str,
            }


db_manager = DatabaseManager()


# ---------------------------------------------------------------------------
# Rule Parsing Utilities
# ---------------------------------------------------------------------------
def parse_triggers(raw: str) -> Dict[str, str]:
    """
    Parse comma-separated or newline-separated trigger strings into a dictionary.
    Format: 'keyword:response text'
    Handles colons inside URLs (e.g. https://...) and commas inside replies safely.
    """
    rules: Dict[str, str] = {}
    if not raw:
        return rules

    lines = raw.replace("\r\n", "\n").split("\n")
    for line in lines:
        line = line.strip()
        if not line:
            continue

        # Split multiple rules on the same line only if preceded by a comma before a new keyword:
        tokens = RULE_SPLIT_REGEX.split(line)
        for tok in tokens:
            tok = tok.strip()
            if ":" in tok:
                kw, resp = tok.split(":", 1)
                kw_clean = kw.strip().lower()
                resp_clean = resp.strip().rstrip(",")
                if kw_clean and resp_clean:
                    rules[kw_clean] = resp_clean

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
    keyword_triggers: bool = True
    trigger_all: bool = False
    custom_message: str = DEFAULT_CUSTOM_MESSAGE
    triggers_raw: str = DEFAULT_TRIGGERS_RAW
    triggers: Dict[str, str] = field(default_factory=dict)
    ai_fallback: bool = True
    model: str = DEFAULT_MODEL
    active: bool = True
    processed_count: int = 0
    away_mode: bool = False
    away_timeout_minutes: int = 5
    last_owner_activity: Optional[datetime] = None
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


def normalize_links_for_preview(text: str) -> str:
    """
    Ensures standalone 't.me/...' or 'telegram.me/...' links have 'https://' prefix
    so Telegram Bot API reliably renders the rich preview card.
    """
    return LINK_NORMALIZE_REGEX.sub(r"https://\1", text)


async def send_business_message(
    business_connection_id: str, chat_id: int, text: str
) -> bool:
    """
    Send a message in a private DM on behalf of the connected business account.
    Guarantees rich preview cards (image, title, join group button) for Telegram invite links.
    """
    formatted_text = normalize_links_for_preview(text)
    payload = {
        "business_connection_id": business_connection_id,
        "chat_id": chat_id,
        "text": formatted_text,
        "link_preview_options": {
            "is_disabled": False,
            "prefer_large_media": True,
            "show_above_text": False,
        },
        "disable_web_page_preview": False,
    }
    result = await tg_api_call("sendMessage", payload)
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

    # Handle messages sent by the business account owner themselves
    sender = message.get("from", {})
    sender_id = sender.get("id")
    if sender_id == account.user_id:
        account.last_owner_activity = datetime.now(timezone.utc)
        logger.info(
            f"[Account @{account.username or account.user_id}] Owner is active. Updated last activity timestamp."
        )
        return

    # Smart Away Mode: Suppress bot response when the account owner is active
    if account.away_mode and account.last_owner_activity:
        idle_seconds = (datetime.now(timezone.utc) - account.last_owner_activity).total_seconds()
        timeout_seconds = account.away_timeout_minutes * 60
        if idle_seconds < timeout_seconds:
            remaining_mins = max(1, int((timeout_seconds - idle_seconds) / 60))
            logger.info(
                f"[Account @{account.username or account.user_id}] Smart Away Mode: "
                f"Owner is ONLINE (active {int(idle_seconds)}s ago). Auto-responder deactivated for {remaining_mins}m."
            )
            return

    # Ignore messages from automated bots
    if sender.get("is_bot", False):
        return

    chat_id = message.get("chat", {}).get("id")
    raw_text = (message.get("text") or message.get("caption") or "").strip()
    if not raw_text or not chat_id:
        return

    customer_name = (
        f"{sender.get('first_name', '')} {sender.get('last_name', '')}".strip()
        or sender.get("username", "")
        or str(sender_id)
    )

    logger.info(
        f"[Account @{account.username or account.user_id}] Incoming DM from {customer_name}: '{raw_text[:50]}...'"
    )

    # -----------------------------------------------------------------------
    # Step A: Keyword Trigger / Conditions Mode (When keyword_triggers is ON)
    # -----------------------------------------------------------------------
    if account.keyword_triggers:
        text_lower = raw_text.lower()
        matched_reply: Optional[str] = None
        matched_keyword: Optional[str] = None

        for kw, reply_text in account.triggers.items():
            if kw and kw in text_lower:
                matched_reply = reply_text
                matched_keyword = kw
                break

        if matched_reply:
            logger.info(
                f"[Account @{account.username or account.user_id}] Keyword condition matched '{matched_keyword}'. Sending specific reply."
            )
            success = await send_business_message(
                account.connection_id, chat_id, matched_reply
            )
            if success:
                account.processed_count += 1
                asyncio.create_task(db_manager.increment_processed_count(account.user_id))
                asyncio.create_task(
                    db_manager.log_interaction(
                        account_id=account.user_id,
                        customer_id=sender_id,
                        customer_name=customer_name,
                        incoming_text=raw_text,
                        response_type="keyword_trigger",
                        response_text=matched_reply,
                        matched_keyword=matched_keyword,
                    )
                )
            return

        # If keyword_triggers is ON, but no keyword matched:
        # Check if AI Fallback is enabled
        if account.ai_fallback:
            if not OPENROUTER_API_KEY:
                logger.warning(
                    f"[Account @{account.username or account.user_id}] AI Fallback requested, but OPENROUTER_API_KEY is not set."
                )
                return

            logger.info(
                f"[Account @{account.username or account.user_id}] No keyword condition matched. Initiating AI fallback (model: {account.model})."
            )
            await send_typing_indicator(account.connection_id, chat_id)
            ai_reply = await generate_openrouter_reply(raw_text, account.model)
            if ai_reply:
                success = await send_business_message(
                    account.connection_id, chat_id, ai_reply
                )
                if success:
                    account.processed_count += 1
                    asyncio.create_task(
                        db_manager.increment_processed_count(account.user_id)
                    )
                    asyncio.create_task(
                        db_manager.log_interaction(
                            account_id=account.user_id,
                            customer_id=sender_id,
                            customer_name=customer_name,
                            incoming_text=raw_text,
                            response_type="ai_fallback",
                            response_text=ai_reply,
                            ai_model=account.model,
                        )
                    )
                    logger.info(
                        f"[Account @{account.username or account.user_id}] AI reply delivered to sender {sender_id}."
                    )
            else:
                logger.warning(
                    f"[Account @{account.username or account.user_id}] AI generation returned empty content."
                )
            return

        # If keyword_triggers is ON and no keyword matched and AI is OFF -> do not reply
        logger.info(
            f"[Account @{account.username or account.user_id}] Keyword Trigger is ON, but incoming DM did not match any keyword conditions. Ignored."
        )
        return

    # -----------------------------------------------------------------------
    # Step B: Keyword Trigger is OFF -> Reply to ALL Messages Mode
    # -----------------------------------------------------------------------
    # "if off then it will reply on all keywords (no matter what that keyword)"
    reply_content = account.custom_message or DEFAULT_CUSTOM_MESSAGE
    logger.info(
        f"[Account @{account.username or account.user_id}] Keyword Trigger is OFF. Replying to message with Customized Auto-Reply Message."
    )
    success = await send_business_message(
        account.connection_id, chat_id, reply_content
    )
    if success:
        account.processed_count += 1
        asyncio.create_task(db_manager.increment_processed_count(account.user_id))
        asyncio.create_task(
            db_manager.log_interaction(
                account_id=account.user_id,
                customer_id=sender_id,
                customer_name=customer_name,
                incoming_text=raw_text,
                response_type="all_messages_custom",
                response_text=reply_content,
                matched_keyword="[ALL_MESSAGES]",
            )
        )
    return


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

    # Persist in MongoDB
    await db_manager.save_account(account)


# ---------------------------------------------------------------------------
# Interactive Telegram DM Setup Wizard (Matches Provided UI Blueprint)
# ---------------------------------------------------------------------------
def build_accounts_menu() -> tuple[str, list]:
    """
    Screen 1: Main accounts selection menu (inspired by image 1).
    """
    text = (
        "<b>⚙️ Manage Account Settings</b>\n"
        "👉 <i>Select the account whose settings you want to change.</i>\n\n"
        "If an account doesn't appear here:\n"
        "• Open Telegram <b>Settings</b> ➔ <b>Telegram Business</b> ➔ <b>Chatbots</b>\n"
        f"• Add @{bot_info.get('username')} and enable <b>'Reply to messages'</b>\n"
        "• Press <b>'🔄 Refresh List'</b> below."
    )

    keyboard = []
    if not accounts:
        keyboard.append(
            [
                {
                    "text": "ℹ️ No Accounts Connected Yet",
                    "callback_data": "noop",
                }
            ]
        )
    else:
        for uid, acc in accounts.items():
            name_label = f"👤 @{acc.username}" if acc.username else f"👤 {acc.first_name}"
            status_dot = "✅" if (acc.active and acc.can_reply) else "⏸️"
            keyboard.append(
                [
                    {
                        "text": f"{status_dot} {name_label} ({acc.processed_count} DMs)",
                        "callback_data": f"acc_{uid}",
                    }
                ]
            )

    keyboard.append(
        [
            {"text": "🔄 Refresh List", "callback_data": "refresh_list"},
            {"text": "⚡ Bulk Sync All", "callback_data": "bulk_sync_menu"},
        ]
    )
    keyboard.append(
        [
            {"text": "📊 System Stats", "callback_data": "sys_stats"},
            {"text": "❌ Close Menu", "callback_data": "close_menu"},
        ]
    )

    return text, keyboard


def build_account_settings_menu(acc: ConnectedAccount) -> tuple[str, list]:
    """
    Screen 2: Detailed Account Settings panel (inspired by image 1 & 2).
    """
    status_text = "Active ✅ (Replying)" if (acc.active and acc.can_reply) else "Paused ⏸️"
    ai_status = "✅ ENABLED" if acc.ai_fallback else "❌ DISABLED"
    away_status = (
        "✅ ENABLED (Deactivates when online)"
        if acc.away_mode
        else "❌ DISABLED (Always active)"
    )
    trig_all_status = (
        "✅ ENABLED (Replies to all DMs)"
        if acc.trigger_all
        else "❌ DISABLED (Keyword rules only)"
    )

    if not acc.last_owner_activity:
        presence_str = "💤 OFFLINE (No recent outgoing DMs)"
    else:
        idle_s = int(
            (datetime.now(timezone.utc) - acc.last_owner_activity).total_seconds()
        )
        if idle_s < (acc.away_timeout_minutes * 60):
            presence_str = f"🟢 ONLINE (Active {int(idle_s / 60)}m ago)"
        else:
            presence_str = f"💤 OFFLINE (Idle for {int(idle_s / 60)}m)"

    triggers_count = len(acc.triggers)
    custom_snippet = (
        (acc.custom_message[:40] + "...")
        if len(acc.custom_message) > 40
        else acc.custom_message
    )

    if acc.keyword_triggers:
        mode_str = "🔑 KEYWORD CONDITIONS (Specific replies per keyword)"
        kw_btn_text = "🔑 Keywords: ON"
    else:
        mode_str = "💬 REPLY TO ALL (Custom reply for all messages)"
        kw_btn_text = "💬 Keywords: OFF (Reply All)"

    text = (
        f"<b>SETTINGS</b>\n"
        f"<b>Account:</b> @{html.escape(acc.username or str(acc.user_id))} ({html.escape(acc.first_name)})\n\n"
        f"• <b>Status:</b> {status_text}\n"
        f"• <b>Owner Presence:</b> {presence_str}\n"
        f"• <b>Smart Away Mode:</b> {away_status}\n"
        f"• <b>Active Mode:</b> {mode_str}\n"
        f"• <b>Custom Message:</b> <i>{html.escape(custom_snippet)}</i>\n"
        f"• <b>AI Fallback:</b> {ai_status}\n"
        f"• <b>AI Model:</b> <code>{html.escape(acc.model)}</code>\n"
        f"• <b>Keyword Conditions:</b> {triggers_count} active rule(s)\n"
        f"• <b>DMs Auto-Responded:</b> {acc.processed_count}\n\n"
        f"<i>Select one of the settings that you want to change:</i>"
    )

    ai_btn_text = "🤖 AI Fallback: ON" if acc.ai_fallback else "🤖 AI Fallback: OFF"
    active_btn_text = "⚡ Status: ACTIVE" if acc.active else "⚡ Status: PAUSED"
    away_btn_text = "🌙 Away Mode: ON" if acc.away_mode else "🌙 Away Mode: OFF"

    keyboard = [
        [
            {"text": ai_btn_text, "callback_data": f"tog_ai_{acc.user_id}"},
            {"text": active_btn_text, "callback_data": f"tog_act_{acc.user_id}"},
        ],
        [
            {"text": away_btn_text, "callback_data": f"tog_away_{acc.user_id}"},
            {
                "text": f"⏱️ Timeout: {acc.away_timeout_minutes}m",
                "callback_data": f"away_time_{acc.user_id}",
            },
        ],
        [
            {"text": kw_btn_text, "callback_data": f"tog_kw_{acc.user_id}"},
            {"text": "💬 Custom Message", "callback_data": f"cust_msg_menu_{acc.user_id}"},
        ],
        [
            {"text": "📝 View Triggers", "callback_data": f"view_trig_{acc.user_id}"},
            {"text": "➕ Add Trigger", "callback_data": f"add_trig_{acc.user_id}"},
        ],
        [
            {"text": "🧠 Change Model", "callback_data": f"pick_mod_{acc.user_id}"},
            {"text": "📊 Analytics", "callback_data": f"acc_stats_{acc.user_id}"},
        ],
        [
            {"text": "⬅️ Back", "callback_data": "back_to_accounts"},
            {"text": "✅ Close", "callback_data": "close_menu"},
            {"text": "🔄 Reload", "callback_data": f"acc_{acc.user_id}"},
        ],
    ]

    return text, keyboard


async def handle_callback_query(cq: dict):
    """
    Processes all inline button clicks inside Telegram DM wizard.
    """
    cq_id = cq["id"]
    from_user = cq.get("from", {})
    from_user_id = from_user.get("id")
    chat_id = cq.get("message", {}).get("chat", {}).get("id")
    message_id = cq.get("message", {}).get("message_id")
    data = cq.get("data", "")

    async def answer(text: str = "", alert: bool = False):
        await tg_api_call(
            "answerCallbackQuery",
            {"callback_query_id": cq_id, "text": text, "show_alert": alert},
        )

    async def edit(text: str, keyboard: list):
        await tg_api_call(
            "editMessageText",
            {
                "chat_id": chat_id,
                "message_id": message_id,
                "text": text,
                "parse_mode": "HTML",
                "reply_markup": {"inline_keyboard": keyboard},
            },
        )

    if data == "noop":
        await answer()
        return

    if data == "close_menu":
        await answer("Menu closed.")
        await tg_api_call(
            "deleteMessage", {"chat_id": chat_id, "message_id": message_id}
        )
        return

    if data in ("back_to_accounts", "refresh_list"):
        text, kb = build_accounts_menu()
        await edit(text, kb)
        await answer("List updated.")
        return

    if data == "sys_stats":
        active_count = sum(1 for a in accounts.values() if a.active and a.can_reply)
        total_dms = sum(a.processed_count for a in accounts.values())
        db_status = "Connected ✅" if db_manager.connected else "In-Memory (Volatile) ⚠️"
        stat_text = (
            f"<b>📊 SYSTEM ANALYTICS & STATUS</b>\n\n"
            f"• <b>Bot Username:</b> @{html.escape(bot_info.get('username', ''))}\n"
            f"• <b>Total Connected Accounts:</b> {len(accounts)}\n"
            f"• <b>Active Auto-Responders:</b> {active_count}\n"
            f"• <b>Total Messages Handled:</b> {total_dms}\n"
            f"• <b>MongoDB Persistence:</b> {db_status}\n"
            f"• <b>OpenRouter API:</b> {'Ready ✅' if OPENROUTER_API_KEY else 'Not Set ❌'}\n"
        )
        kb = [[{"text": "⬅️ Back", "callback_data": "back_to_accounts"}]]
        await edit(stat_text, kb)
        await answer()
        return

    if data == "bulk_sync_menu":
        confirm_text = (
            "<b>⚡ Bulk Synchronization Tool</b>\n\n"
            "This action will overwrite keyword triggers, AI fallback toggle, "
            "and default AI models across <b>ALL</b> connected accounts simultaneously.\n\n"
            "Do you want to proceed?"
        )
        kb = [
            [
                {
                    "text": "✅ Yes, Sync All Accounts",
                    "callback_data": "confirm_bulk_sync",
                }
            ],
            [{"text": "⬅️ Cancel", "callback_data": "back_to_accounts"}],
        ]
        await edit(confirm_text, kb)
        await answer()
        return

    if data == "confirm_bulk_sync":
        default_parsed = parse_triggers(DEFAULT_TRIGGERS_RAW)
        for acc in accounts.values():
            acc.triggers_raw = DEFAULT_TRIGGERS_RAW
            acc.triggers = default_parsed
            acc.ai_fallback = True
            acc.model = DEFAULT_MODEL
            await db_manager.save_account(acc)
        await answer("All accounts synchronized successfully!", alert=True)
        text, kb = build_accounts_menu()
        await edit(text, kb)
        return

    # Account Specific Routes
    if data.startswith("acc_"):
        uid = int(data.split("_")[1])
        acc = accounts.get(uid)
        if not acc:
            await answer("Account not found.", alert=True)
            return
        text, kb = build_account_settings_menu(acc)
        await edit(text, kb)
        await answer()
        return

    # Toggle AI Fallback
    if data.startswith("tog_ai_"):
        uid = int(data.split("_")[2])
        acc = accounts.get(uid)
        if acc:
            acc.ai_fallback = not acc.ai_fallback
            await db_manager.save_account(acc)
            status_msg = "AI Fallback Enabled" if acc.ai_fallback else "AI Fallback Disabled"
            await answer(status_msg)
            text, kb = build_account_settings_menu(acc)
            await edit(text, kb)
        return

    # Toggle Active Status
    if data.startswith("tog_act_"):
        uid = int(data.split("_")[2])
        acc = accounts.get(uid)
        if acc:
            acc.active = not acc.active
            await db_manager.save_account(acc)
            status_msg = "Auto-Responder Resumed" if acc.active else "Auto-Responder Paused"
            await answer(status_msg)
            text, kb = build_account_settings_menu(acc)
            await edit(text, kb)
        return

    # Toggle Smart Away Mode
    if data.startswith("tog_away_"):
        uid = int(data.split("_")[2])
        acc = accounts.get(uid)
        if acc:
            acc.away_mode = not acc.away_mode
            await db_manager.save_account(acc)
            status_msg = (
                "Smart Away Mode Enabled: Bot auto-deactivates when you are online, auto-activates when you are offline."
                if acc.away_mode
                else "Smart Away Mode Disabled: Bot is always auto-replying."
            )
            await answer(status_msg, alert=True)
            text, kb = build_account_settings_menu(acc)
            await edit(text, kb)
        return

    # Cycle Away Timeout
    if data.startswith("away_time_"):
        uid = int(data.split("_")[2])
        acc = accounts.get(uid)
        if acc:
            delays = [2, 5, 10, 15]
            curr_idx = delays.index(acc.away_timeout_minutes) if acc.away_timeout_minutes in delays else 1
            next_delay = delays[(curr_idx + 1) % len(delays)]
            acc.away_timeout_minutes = next_delay
            await db_manager.save_account(acc)
            await answer(f"Away threshold set to {next_delay} minutes of inactivity.")
            text, kb = build_account_settings_menu(acc)
            await edit(text, kb)
        return

    # Toggle Keyword Triggers Mode / Reply All Mode
    if data.startswith(("tog_kw_", "tog_trig_all_")):
        parts = data.split("_")
        uid = int(parts[-1])
        acc = accounts.get(uid)
        if acc:
            acc.keyword_triggers = not acc.keyword_triggers
            acc.trigger_all = not acc.keyword_triggers
            await db_manager.save_account(acc)
            if acc.keyword_triggers:
                status_msg = "🔑 Keyword Conditions Mode ON: Bot will check different keywords for different replies."
            else:
                status_msg = "💬 Reply to All Mode ON: Bot will reply to ALL incoming messages with your custom message (regardless of keyword)."
            await answer(status_msg, alert=True)
            text, kb = build_account_settings_menu(acc)
            await edit(text, kb)
        return

    # Customize Message Submenu
    if data.startswith("cust_msg_menu_"):
        uid = int(data.split("_")[3])
        acc = accounts.get(uid)
        if not acc:
            return
        cust_text = (
            f"<b>💬 CUSTOMIZE AUTO-REPLY MESSAGE</b>\n"
            f"<b>Account:</b> @{html.escape(acc.username or str(uid))} ({html.escape(acc.first_name)})\n\n"
            f"<b>Active Message:</b>\n"
            f"<blockquote>{html.escape(acc.custom_message)}</blockquote>\n\n"
            f"💡 <b>Tips & Link Preview Support:</b>\n"
            f"• You can put text, emojis, and group/channel invite links (e.g. <code>https://t.me/+...</code>).\n"
            f"• Telegram automatically generates the rich preview card with group photo and Join/View button (matching Image 2)!\n"
            f"• Tap <b>'👁️ Test Link Preview'</b> below to test how it looks in chat right now."
        )
        kb = [
            [
                {"text": "✏️ Edit Custom Message", "callback_data": f"edit_cust_msg_{uid}"},
                {"text": "👁️ Test Link Preview", "callback_data": f"test_preview_{uid}"},
            ],
            [
                {"text": "🔄 Reset Default", "callback_data": f"reset_cust_msg_{uid}"},
                {"text": "⬅️ Back to Settings", "callback_data": f"acc_{uid}"},
            ],
        ]
        await edit(cust_text, kb)
        await answer()
        return

    # Edit Custom Message Prompt
    if data.startswith("edit_cust_msg_"):
        uid = int(data.split("_")[3])
        acc = accounts.get(uid)
        if not acc:
            return
        user_wizard_state[from_user_id] = {
            "action": "awaiting_custom_message",
            "target_account_id": uid,
        }
        prompt_text = (
            f"<b>✏️ Edit Custom Reply Message for @{html.escape(acc.username or str(uid))}</b>\n\n"
            f"Send your new custom reply message in your next chat message.\n\n"
            f"<b>Example (Text + Invite Link):</b>\n"
            f"<code>Hello! Thanks for reaching out. Join our exclusive group:\nhttps://t.me/+G2ltaftHIqswMGU1</code>\n\n"
            f"<i>Telegram will automatically attach the rich link preview card!</i>"
        )
        kb = [[{"text": "⬅️ Cancel", "callback_data": f"cust_msg_menu_{uid}"}]]
        await edit(prompt_text, kb)
        await answer()
        return

    # Test Link Preview in User's DM
    if data.startswith("test_preview_"):
        uid = int(data.split("_")[2])
        acc = accounts.get(uid)
        if not acc:
            return
        await answer("Sending preview card below... 👇")
        preview_text = normalize_links_for_preview(acc.custom_message)
        await tg_api_call(
            "sendMessage",
            {
                "chat_id": chat_id,
                "text": preview_text,
                "link_preview_options": {
                    "is_disabled": False,
                    "prefer_large_media": True,
                    "show_above_text": False,
                },
                "disable_web_page_preview": False,
            },
        )
        return

    # Reset Custom Message to Default
    if data.startswith("reset_cust_msg_"):
        uid = int(data.split("_")[3])
        acc = accounts.get(uid)
        if acc:
            acc.custom_message = DEFAULT_CUSTOM_MESSAGE
            await db_manager.save_account(acc)
            await answer("Reset to default auto-reply message.", alert=True)
            cust_text = (
                f"<b>💬 CUSTOMIZE AUTO-REPLY MESSAGE</b>\n"
                f"<b>Account:</b> @{html.escape(acc.username or str(uid))} ({html.escape(acc.first_name)})\n\n"
                f"<b>Active Message:</b>\n"
                f"<blockquote>{html.escape(acc.custom_message)}</blockquote>\n\n"
                f"💡 <b>Tips & Link Preview Support:</b>\n"
                f"• You can put text, emojis, and group/channel invite links (e.g. <code>https://t.me/+...</code>).\n"
                f"• Telegram automatically generates the rich preview card with group photo and Join/View button!\n"
                f"• Tap <b>'👁️ Test Link Preview'</b> below to test how it looks in chat right now."
            )
            kb = [
                [
                    {"text": "✏️ Edit Custom Message", "callback_data": f"edit_cust_msg_{uid}"},
                    {"text": "👁️ Test Link Preview", "callback_data": f"test_preview_{uid}"},
                ],
                [
                    {"text": "🔄 Reset Default", "callback_data": f"reset_cust_msg_{uid}"},
                    {"text": "⬅️ Back to Settings", "callback_data": f"acc_{uid}"},
                ],
            ]
            await edit(cust_text, kb)
        return

    # Pick Model Menu
    if data.startswith("pick_mod_"):
        uid = int(data.split("_")[2])
        acc = accounts.get(uid)
        if not acc:
            return
        mod_text = (
            f"<b>🧠 Select AI Model for @{html.escape(acc.username or str(uid))}</b>\n\n"
            f"Current: <code>{html.escape(acc.model)}</code>\n\n"
            f"Choose a low-latency model optimized for Telegram DMs:"
        )
        kb = [
            [
                {
                    "text": "⚡ Gemini 2.5 Flash",
                    "callback_data": f"setmod_{uid}_google/gemini-2.5-flash",
                },
                {
                    "text": "🦙 LLaMA 3.3 70B",
                    "callback_data": f"setmod_{uid}_meta-llama/llama-3.3-70b-instruct:free",
                },
            ],
            [
                {
                    "text": "🧠 Claude 3.5 Haiku",
                    "callback_data": f"setmod_{uid}_anthropic/claude-3.5-haiku",
                },
                {
                    "text": "🤖 GPT-4o Mini",
                    "callback_data": f"setmod_{uid}_openai/gpt-4o-mini",
                },
            ],
            [{"text": "⬅️ Back to Settings", "callback_data": f"acc_{uid}"}],
        ]
        await edit(mod_text, kb)
        await answer()
        return

    # Set Model Action
    if data.startswith("setmod_"):
        parts = data.split("_", 2)
        uid = int(parts[1])
        selected_model = parts[2]
        acc = accounts.get(uid)
        if acc:
            acc.model = selected_model
            await db_manager.save_account(acc)
            await answer(f"Model updated to: {selected_model}", alert=True)
            text, kb = build_account_settings_menu(acc)
            await edit(text, kb)
        return

    # View Triggers
    if data.startswith("view_trig_"):
        uid = int(data.split("_")[2])
        acc = accounts.get(uid)
        if not acc:
            return
        trig_list_text = (
            f"<b>📝 Active Trigger Rules for @{html.escape(acc.username or str(uid))}</b>\n\n"
        )
        if not acc.triggers:
            trig_list_text += "<i>No triggers configured. Messages default to AI fallback.</i>"
        else:
            for idx, (kw, resp) in enumerate(acc.triggers.items(), 1):
                trig_list_text += f"{idx}. <b>\"{html.escape(kw)}\"</b> ➔ <i>{html.escape(resp[:60])}...</i>\n"

        kb = [
            [
                {
                    "text": "➕ Add / Replace Trigger",
                    "callback_data": f"add_trig_{uid}",
                },
                {
                    "text": "🔄 Reset Defaults",
                    "callback_data": f"reset_trig_{uid}",
                },
            ],
            [{"text": "⬅️ Back to Settings", "callback_data": f"acc_{uid}"}],
        ]
        await edit(trig_list_text, kb)
        await answer()
        return

    # Reset Triggers
    if data.startswith("reset_trig_"):
        uid = int(data.split("_")[2])
        acc = accounts.get(uid)
        if acc:
            acc.triggers_raw = DEFAULT_TRIGGERS_RAW
            acc.triggers = parse_triggers(DEFAULT_TRIGGERS_RAW)
            await db_manager.save_account(acc)
            await answer("Triggers reset to default.", alert=True)
            text, kb = build_account_settings_menu(acc)
            await edit(text, kb)
        return

    # Add Trigger (Prompt for text input)
    if data.startswith("add_trig_"):
        uid = int(data.split("_")[2])
        acc = accounts.get(uid)
        if not acc:
            return
        user_wizard_state[from_user_id] = {
            "action": "awaiting_trigger",
            "target_account_id": uid,
        }
        prompt_text = (
            f"<b>➕ Add Custom Trigger for @{html.escape(acc.username or str(uid))}</b>\n\n"
            f"Send your trigger rule in your next chat message using the format:\n"
            f"<code>keyword:response text</code>\n\n"
            f"<b>Example:</b>\n"
            f"<code>support:Please email our team at help@example.com</code>"
        )
        kb = [[{"text": "⬅️ Cancel", "callback_data": f"acc_{uid}"}]]
        await edit(prompt_text, kb)
        await answer()
        return

    # Account Analytics
    if data.startswith("acc_stats_"):
        uid = int(data.split("_")[2])
        acc = accounts.get(uid)
        if not acc:
            return
        stats = await db_manager.get_account_stats(uid)
        stat_text = (
            f"<b>📊 Account Analytics: @{html.escape(acc.username or str(uid))}</b>\n\n"
            f"• <b>Total DMs Processed:</b> {acc.processed_count}\n"
            f"• <b>Trigger Keyword Matches:</b> {stats.get('triggers', 0)}\n"
            f"• <b>AI Generated Fallbacks:</b> {stats.get('ai', 0)}\n"
            f"• <b>Configured Model:</b> <code>{html.escape(acc.model)}</code>\n"
            f"• <b>Active Status:</b> {'Replying ✅' if acc.active else 'Paused ⏸️'}\n"
        )
        kb = [[{"text": "⬅️ Back to Settings", "callback_data": f"acc_{uid}"}]]
        await edit(stat_text, kb)
        await answer()
        return


# ---------------------------------------------------------------------------
# Background Polling Worker
# ---------------------------------------------------------------------------
async def run_bot_polling():
    """
    Long-polling worker fetching updates from Telegram Bot API.
    Handles standard messages, callbacks, business connections, and business DMs.
    """
    global http_client
    offset = 0
    allowed_updates = [
        "message",
        "edited_message",
        "callback_query",
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

                # 1. Inline Keyboard Button Click (Setup Wizard)
                if "callback_query" in update:
                    await handle_callback_query(update["callback_query"])

                # 2. Telegram Business Connection Update
                elif "business_connection" in update:
                    await process_business_connection(update["business_connection"])

                # 3. Telegram Business Message Update (Incoming DMs on connected accounts)
                elif "business_message" in update:
                    await process_business_message(update["business_message"])

                # 4. Direct Message to the Bot itself (Commands & Text Input Wizard)
                elif "message" in update:
                    msg = update["message"]
                    chat_id = msg.get("chat", {}).get("id")
                    from_user = msg.get("from", {})
                    from_user_id = from_user.get("id")
                    text = (msg.get("text") or "").strip()

                    # If user sends a command, cancel any pending wizard input state
                    if text.startswith("/"):
                        user_wizard_state.pop(from_user_id, None)

                    # Handle Wizard Text Input (e.g. adding a trigger rule or editing custom message)
                    if from_user_id in user_wizard_state:
                        state = user_wizard_state.pop(from_user_id)
                        action = state.get("action")
                        target_id = state.get("target_account_id")
                        acc = accounts.get(target_id)

                        if action == "awaiting_custom_message" and acc and text:
                            acc.custom_message = text
                            await db_manager.save_account(acc)
                            confirm_msg = (
                                f"✅ <b>Custom Message Saved for @{html.escape(acc.username or str(target_id))}!</b>\n\n"
                                f"Below is how your message will appear in private DMs (with rich link preview):"
                            )
                            kb = [
                                [
                                    {
                                        "text": "⬅️ Back to Settings",
                                        "callback_data": f"acc_{target_id}",
                                    },
                                    {
                                        "text": "💬 Custom Message Menu",
                                        "callback_data": f"cust_msg_menu_{target_id}",
                                    },
                                ]
                            ]
                            await tg_api_call(
                                "sendMessage",
                                {
                                    "chat_id": chat_id,
                                    "text": confirm_msg,
                                    "parse_mode": "HTML",
                                    "reply_markup": {"inline_keyboard": kb},
                                },
                            )
                            # Send live preview of the message
                            preview_text = normalize_links_for_preview(acc.custom_message)
                            await tg_api_call(
                                "sendMessage",
                                {
                                    "chat_id": chat_id,
                                    "text": preview_text,
                                    "link_preview_options": {
                                        "is_disabled": False,
                                        "prefer_large_media": True,
                                        "show_above_text": False,
                                    },
                                    "disable_web_page_preview": False,
                                },
                            )
                            continue

                        elif action == "awaiting_trigger" and acc and ":" in text:
                            kw, resp_val = text.split(":", 1)
                            kw = kw.strip().lower()
                            resp_val = resp_val.strip()
                            if kw and resp_val:
                                acc.triggers[kw] = resp_val
                                acc.triggers_raw += f",\n{kw}:{resp_val}"
                                await db_manager.save_account(acc)
                                confirm = (
                                    f"✅ <b>Trigger Saved!</b>\n\n"
                                    f"Keyword: <code>{html.escape(kw)}</code>\n"
                                    f"Reply: <i>{html.escape(resp_val)}</i>"
                                )
                                kb = [
                                    [
                                        {
                                            "text": "⬅️ Back to Settings",
                                            "callback_data": f"acc_{target_id}",
                                        }
                                    ]
                                ]
                                await tg_api_call(
                                    "sendMessage",
                                    {
                                        "chat_id": chat_id,
                                        "text": confirm,
                                        "parse_mode": "HTML",
                                        "reply_markup": {"inline_keyboard": kb},
                                    },
                                )
                                continue

                    # Commands
                    if text.startswith(("/start", "/settings", "/menu")):
                        menu_text, menu_kb = build_accounts_menu()
                        await tg_api_call(
                            "sendMessage",
                            {
                                "chat_id": chat_id,
                                "text": menu_text,
                                "parse_mode": "HTML",
                                "reply_markup": {"inline_keyboard": menu_kb},
                            },
                        )
                    elif text.startswith("/help"):
                        help_text = (
                            f"👋 <b>Welcome to Multi-Account AutoResponder!</b>\n\n"
                            f"📌 <b>Connect your Telegram Account:</b>\n"
                            f"1. Open Telegram <b>Settings</b> ➔ <b>Telegram Business</b> ➔ <b>Chatbots</b>\n"
                            f"2. Add @{bot_info.get('username')}\n"
                            f"3. Enable <b>'Reply to messages'</b>\n\n"
                            f"⚙️ Send /settings to open your interactive management wizard!\n"
                            f"🌐 You can also configure everything via your web dashboard."
                        )
                        await tg_api_call(
                            "sendMessage",
                            {
                                "chat_id": chat_id,
                                "text": help_text,
                                "parse_mode": "HTML",
                            },
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
    Manages database connection pool, global HTTP client, and polling workers cleanly.
    """
    global http_client, bot_info, polling_task

    # 1. Initialize MongoDB connection
    await db_manager.connect()

    # 2. Load persisted accounts from MongoDB into in-memory cache
    persisted_accounts = await db_manager.load_all_accounts()
    for doc in persisted_accounts:
        uid = doc["_id"]
        kw_trig = doc.get("keyword_triggers")
        if kw_trig is None:
            kw_trig = not doc.get("trigger_all", False)

        acc = ConnectedAccount(
            connection_id=doc.get("connection_id", ""),
            user_id=uid,
            username=doc.get("username", ""),
            first_name=doc.get("first_name", ""),
            can_reply=doc.get("can_reply", True),
            is_enabled=doc.get("is_enabled", True),
            keyword_triggers=kw_trig,
            trigger_all=not kw_trig,
            custom_message=doc.get("custom_message", DEFAULT_CUSTOM_MESSAGE),
            triggers_raw=doc.get("triggers_raw", DEFAULT_TRIGGERS_RAW),
            triggers=doc.get("triggers") or parse_triggers(DEFAULT_TRIGGERS_RAW),
            ai_fallback=doc.get("ai_fallback", True),
            model=doc.get("model", DEFAULT_MODEL),
            active=doc.get("active", True),
            processed_count=doc.get("processed_count", 0),
            away_mode=doc.get("away_mode", False),
            away_timeout_minutes=doc.get("away_timeout_minutes", 5),
            last_owner_activity=doc.get("last_owner_activity"),
        )
        accounts[uid] = acc
        if acc.connection_id:
            connection_to_user[acc.connection_id] = uid

    logger.info(
        f"[Init] Loaded {len(accounts)} persisted account(s) into memory cache."
    )

    # 3. Initialize HTTPX client
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
            bot_info["id"] = me.get("id")
            logger.info(
                f"Bot verified: @{bot_info['username']} (ID: {bot_info['id']}, Name: {bot_info['first_name']})"
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

    await db_manager.close()
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
            <p class="empty-note">Once linked, the account will instantly appear here and in your Telegram DM /settings wizard!</p>
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
            away_checked = "checked" if acc.away_mode else ""
            kw_checked = "checked" if acc.keyword_triggers else ""
            user_title = (
                f"@{html.escape(acc.username)}"
                if acc.username
                else f"Account #{acc.user_id}"
            )

            if not acc.last_owner_activity:
                presence_pill = "Presence: <strong style='color:#94a3b8;'>Offline</strong>"
            else:
                idle_s = int(
                    (datetime.now(timezone.utc) - acc.last_owner_activity).total_seconds()
                )
                if idle_s < (acc.away_timeout_minutes * 60):
                    presence_pill = f"Presence: <strong style='color:#4ade80;'>Online ({int(idle_s / 60)}m ago)</strong>"
                else:
                    presence_pill = f"Presence: <strong style='color:#94a3b8;'>Away ({int(idle_s / 60)}m ago)</strong>"

            if acc.keyword_triggers:
                mode_pill = '<span class="stat-pill" style="border-color: rgba(74, 222, 128, 0.4); color: #4ade80;">Mode: <strong>Keywords (Conditions)</strong></span>'
            else:
                mode_pill = '<span class="stat-pill" style="border-color: rgba(56, 189, 248, 0.4); color: #38bdf8;">Mode: <strong>Reply on All Msgs</strong></span>'

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
                    <span class="stat-pill">{presence_pill}</span>
                    {mode_pill}
                </div>

                <form method="POST" action="/account/{user_id}/update" class="account-form">
                    <div class="form-group">
                        <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 6px;">
                            <label style="margin-bottom:0; font-weight: 600; color: var(--accent-blue);">💬 Customized Message (Reply to All Mode)</label>
                            <span style="font-size: 11px; color: var(--text-muted);">Rich https://t.me/ previews</span>
                        </div>
                        <textarea name="custom_message" rows="5" class="form-control" style="min-height: 120px; font-size: 13px; line-height: 1.45;" placeholder="Enter custom message or invite links (e.g. Join our group: https://t.me/+...). Sent to all messages when Keyword Trigger is OFF!">{html.escape(acc.custom_message)}</textarea>
                        <div class="form-help">👉 <strong>Reply on All Mode:</strong> When Keyword Trigger is OFF, this message is sent to all incoming DMs (no matter what keyword). Supports rich Telegram cards for links.</div>
                    </div>

                    <div class="form-group">
                        <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 6px;">
                            <label style="margin-bottom:0; font-weight: 600;">📝 Keyword Conditions & Replies (When Keyword Trigger is ON)</label>
                            <span style="font-size: 11px; color: var(--text-muted);">keyword:response (1 per line)</span>
                        </div>
                        <textarea name="triggers" rows="5" class="form-control" style="min-height: 120px; font-size: 13px; font-family: monospace; line-height: 1.45;" placeholder="hello:Hello there!, pricing:Our plans start at $19/mo, group:Join here https://t.me/+...">{html.escape(acc.triggers_raw)}</textarea>
                        <div class="form-help">👉 <strong>Keyword Conditions Mode:</strong> When Keyword Trigger is ON, matches different keywords for different replies (with rich preview cards). If no keyword matches, bot stays quiet.</div>
                    </div>

                    <div class="form-row" style="flex-wrap: wrap; margin-bottom: 16px;">
                        <div class="form-group toggle-group" style="flex: 1; min-width: 115px;">
                            <label>Keyword Trigger</label>
                            <label class="switch">
                                <input type="checkbox" name="keyword_triggers" value="on" {kw_checked}>
                                <span class="slider"></span>
                            </label>
                            <span style="font-size: 10px; color: {'#4ade80' if acc.keyword_triggers else '#38bdf8'}; margin-top: 3px;">{'ON = Conditions' if acc.keyword_triggers else 'OFF = Reply All'}</span>
                        </div>
                        <div class="form-group toggle-group" style="flex: 1; min-width: 95px;">
                            <label>Smart Away</label>
                            <label class="switch">
                                <input type="checkbox" name="away_mode" value="on" {away_checked}>
                                <span class="slider"></span>
                            </label>
                            <span style="font-size: 10px; color: var(--text-muted); margin-top: 3px;">Auto-sleeps</span>
                        </div>
                        <div class="form-group toggle-group" style="flex: 1; min-width: 95px;">
                            <label>AI Fallback</label>
                            <label class="switch">
                                <input type="checkbox" name="ai_fallback" value="on" {ai_checked}>
                                <span class="slider"></span>
                            </label>
                            <span style="font-size: 10px; color: var(--text-muted); margin-top: 3px;">OpenRouter</span>
                        </div>
                        <div class="form-group" style="flex: 2; min-width: 170px;">
                            <label>AI Model String</label>
                            <input type="text" name="model" class="form-control" value="{html.escape(acc.model)}" placeholder="e.g. google/gemini-2.5-flash">
                        </div>
                    </div>

                    <div class="card-footer">
                        <button type="submit" class="btn btn-primary btn-full" style="padding: 12px; font-size: 14px; font-weight: 600; display: flex; align-items: center; justify-content: center; gap: 8px;">
                            💾 Save @{html.escape(acc.username or str(acc.user_id))} Settings & Messages
                        </button>
                    </div>
                </form>
            </div>
            """

    # Global Bulk Config values (reference from first account or default)
    bulk_triggers = (
        list(accounts.values())[0].triggers_raw if accounts else DEFAULT_TRIGGERS_RAW
    )
    bulk_custom_msg = (
        list(accounts.values())[0].custom_message if accounts else DEFAULT_CUSTOM_MESSAGE
    )
    bulk_model = list(accounts.values())[0].model if accounts else DEFAULT_MODEL

    db_badge_color = "#4ade80" if db_manager.connected else "#fbbf24"
    db_badge_text = "Connected (MongoDB)" if db_manager.connected else "Volatile (In-Memory)"

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
            min-height: 120px;
            font-family: inherit;
            line-height: 1.45;
            white-space: pre-wrap;
            word-break: break-word;
        }}
        .form-help {{
            font-size: 11px;
            color: var(--text-muted);
            margin-top: 4px;
            margin-bottom: 8px;
            line-height: 1.4;
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
                    <p>Multi-Account Manager with Telegram DM Wizard & MongoDB</p>
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
                    <span>Database:</span>
                    <span class="stat-value" style="color: {db_badge_color};">{db_badge_text}</span>
                </div>
                <div class="stat-chip">
                    <span>OpenRouter:</span>
                    <span class="stat-value" style="color: {'#4ade80' if OPENROUTER_API_KEY else '#fbbf24'}">{'Ready' if OPENROUTER_API_KEY else 'API Key Missing'}</span>
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
                Overwrite trigger keywords, custom auto-reply messages, and settings across ALL connected accounts in a single click.
            </div>
            <form method="POST" action="/bulk-update">
                <div class="form-group">
                    <label style="font-weight: 600; color: var(--accent-blue);">💬 Bulk Custom Message (Sent to all DMs when Keyword Trigger is OFF)</label>
                    <textarea name="custom_message" rows="4" class="form-control" style="min-height: 100px;" placeholder="Hello! Join our group: https://t.me/+...">{html.escape(bulk_custom_msg)}</textarea>
                </div>
                <div class="form-group">
                    <label style="font-weight: 600;">📝 Bulk Keyword Conditions (keyword:response text, 1 per line)</label>
                    <textarea name="triggers" rows="4" class="form-control" style="min-height: 100px;" placeholder="hello:Hi there!, pricing:Our pricing is $10/mo, group:https://t.me/+...">{html.escape(bulk_triggers)}</textarea>
                </div>
                <div class="form-row" style="flex-wrap: wrap; margin-bottom: 16px;">
                    <div class="form-group toggle-group" style="flex: 1; min-width: 115px;">
                        <label>Keyword Trigger</label>
                        <label class="switch">
                            <input type="checkbox" name="keyword_triggers" value="on" checked>
                            <span class="slider"></span>
                        </label>
                        <span style="font-size: 10px; color: var(--text-muted); margin-top: 3px;">ON=Rules, OFF=All</span>
                    </div>
                    <div class="form-group toggle-group" style="flex: 1; min-width: 95px;">
                        <label>Smart Away</label>
                        <label class="switch">
                            <input type="checkbox" name="away_mode" value="on">
                            <span class="slider"></span>
                        </label>
                        <span style="font-size: 10px; color: var(--text-muted); margin-top: 3px;">Auto-sleeps</span>
                    </div>
                    <div class="form-group toggle-group" style="flex: 1; min-width: 95px;">
                        <label>AI Fallback</label>
                        <label class="switch">
                            <input type="checkbox" name="ai_fallback" value="on" checked>
                            <span class="slider"></span>
                        </label>
                        <span style="font-size: 10px; color: var(--text-muted); margin-top: 3px;">OpenRouter</span>
                    </div>
                    <div class="form-group" style="flex: 2; min-width: 170px;">
                        <label>AI Model String for All Accounts</label>
                        <input type="text" name="model" class="form-control" value="{html.escape(bulk_model)}" placeholder="e.g. google/gemini-2.5-flash">
                    </div>
                </div>
                <button type="submit" class="btn btn-primary btn-full" style="padding: 12px; font-size: 14px; font-weight: 600;">⚡ Overwrite & Synchronize All Accounts</button>
            </form>
        </div>

        <!-- Connected Accounts Grid -->
        <div class="grid">
            {cards_html}
        </div>

        <footer>
            <div>Operational Health Endpoint: <a href="/health" target="_blank">/health</a> | Telegram Bot: <a href="https://t.me/{html.escape(bot_info.get('username',''))}" target="_blank">@{html.escape(bot_info.get('username',''))}</a> (/settings wizard)</div>
            <div>Powered by FastAPI + Telegram Business Bot API + MongoDB</div>
        </footer>
    </div>
</body>
</html>
"""


# ---------------------------------------------------------------------------
# HTTP Endpoints & Form Action Handlers
# ---------------------------------------------------------------------------
@app.api_route("/", methods=["GET", "HEAD"], response_class=HTMLResponse)
async def dashboard_view(msg: str = "", err: str = ""):
    """
    Renders the lightweight HTML UI dashboard.
    """
    return HTMLResponse(content=render_dashboard(msg=msg, err=err), status_code=200)


health_router = APIRouter(tags=["Health"])


@health_router.api_route("/health", methods=["GET", "HEAD"])
@health_router.api_route("/api/health", methods=["GET", "HEAD"])
async def health_check():
    """
    Detailed operational health and telemetry endpoint for Render and external monitors.
    Returns JSON payload matching the service status, database metrics, and bot states.
    """
    now = datetime.now(timezone.utc)
    delta = now - service_start_time
    uptime_seconds = int(delta.total_seconds())

    days, rem = divmod(uptime_seconds, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, seconds = divmod(rem, 60)

    if days > 0:
        uptime_str = f"{days}d {hours}h {minutes}m {seconds}s"
    elif hours > 0:
        uptime_str = f"{hours}h {minutes}m {seconds}s"
    elif minutes > 0:
        uptime_str = f"{minutes}m {seconds}s"
    else:
        uptime_str = f"{seconds}s"

    db_metrics = await db_manager.get_health_metrics()

    db_type = "In-Memory (Volatile)"
    if db_manager.connected:
        db_type = "MongoDB Atlas" if "mongodb+srv" in MONGO_URI else "MongoDB Local"

    is_bot_online = bool(polling_task and not polling_task.done() and bot_info.get("id"))
    active_count = sum(1 for a in accounts.values() if a.active and a.can_reply)
    total_triggers = sum(len(a.triggers) for a in accounts.values())

    health_payload = {
        "status": "UP" if is_bot_online or not BOT_TOKEN else "DEGRADED",
        "service": SERVICE_NAME,
        "telegram_bot": {
            "status": "ONLINE" if is_bot_online else "OFFLINE",
            "username": bot_info.get("username", "UnknownBot"),
            "id": bot_info.get("id"),
        },
        "database": {
            "type": db_type,
            "connected": db_manager.connected,
            "metrics": db_metrics,
        },
        "stats": {
            "connected_accounts": len(accounts),
            "active_auto_responders": active_count,
            "away_mode_active": sum(1 for a in accounts.values() if a.away_mode),
            "keyword_triggers_active": sum(1 for a in accounts.values() if a.keyword_triggers),
            "reply_all_active": sum(1 for a in accounts.values() if not a.keyword_triggers),
            "total_triggers_configured": total_triggers,
            "openrouter_status": "READY" if OPENROUTER_API_KEY else "NOT_CONFIGURED",
        },
        "uptime": uptime_str,
        "uptime_seconds": uptime_seconds,
        "timestamp": now.strftime("%Y-%m-%d %H:%M:%S UTC"),
    }
    return JSONResponse(content=health_payload, status_code=200)


app.include_router(health_router)


@app.post("/bulk-update")
async def bulk_update_accounts(request: Request):
    """
    Overwrites trigger rules, custom message, trigger_all state, AI fallback state, and model string for all connected accounts.
    """
    body = (await request.body()).decode("utf-8")
    data = urllib.parse.parse_qs(body)

    triggers_raw = data.get("triggers", [""])[0].strip()
    custom_message = (
        data.get("custom_message", [DEFAULT_CUSTOM_MESSAGE])[0].strip()
        or DEFAULT_CUSTOM_MESSAGE
    )
    keyword_triggers = "keyword_triggers" in data
    trigger_all = not keyword_triggers
    ai_fallback = "ai_fallback" in data
    away_mode = "away_mode" in data
    model = data.get("model", [DEFAULT_MODEL])[0].strip() or DEFAULT_MODEL
    parsed_rules = parse_triggers(triggers_raw)

    for acc in accounts.values():
        acc.triggers_raw = triggers_raw
        acc.triggers = parsed_rules
        acc.custom_message = custom_message
        acc.keyword_triggers = keyword_triggers
        acc.trigger_all = trigger_all
        acc.ai_fallback = ai_fallback
        acc.away_mode = away_mode
        acc.model = model
        # Persist to MongoDB
        await db_manager.save_account(acc)

    logger.info(
        f"Bulk update executed: applied {len(parsed_rules)} triggers, keyword_triggers={keyword_triggers}, AI fallback={ai_fallback}, model='{model}' across {len(accounts)} accounts."
    )
    return RedirectResponse(
        url="/?msg=All+connected+accounts+synchronized+successfully", status_code=303
    )


@app.post("/account/{user_id}/update")
async def update_account_config(user_id: int, request: Request):
    """
    Updates configuration and messages for an individual connected business account.
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
    acc.custom_message = (
        data.get("custom_message", [DEFAULT_CUSTOM_MESSAGE])[0].strip()
        or DEFAULT_CUSTOM_MESSAGE
    )
    acc.keyword_triggers = "keyword_triggers" in data
    acc.trigger_all = not acc.keyword_triggers
    acc.ai_fallback = "ai_fallback" in data
    acc.away_mode = "away_mode" in data
    acc.model = data.get("model", [DEFAULT_MODEL])[0].strip() or DEFAULT_MODEL

    # Persist to MongoDB
    await db_manager.save_account(acc)

    logger.info(
        f"[Account @{acc.username or user_id}] Updated: keyword_triggers={acc.keyword_triggers}, {len(acc.triggers)} triggers, AI fallback={acc.ai_fallback}, model='{acc.model}'"
    )
    return RedirectResponse(
        url=f"/?msg=Settings+and+messages+saved+successfully+for+@{html.escape(acc.username or str(user_id))}",
        status_code=303,
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
    # Persist to MongoDB
    await db_manager.save_account(acc)

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
