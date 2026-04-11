"""
Openbot - AI Chatbot powered by local LLM

Integrates with:
- LLM Proxy (:11435) for AI responses
- Essencem (:11445) for conversation memory
- Telegram Bot API (webhook)
- Twilio voice call-in (webhook)
"""

import asyncio
import base64
import hashlib
import hmac
import html
import io
import importlib
import json
import mimetypes
import logging
import os
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from typing import Optional
from contextlib import asynccontextmanager

import httpx
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from fastapi import BackgroundTasks, FastAPI, File, HTTPException, Request, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, Response
from pydantic import BaseModel
from dotenv import load_dotenv
import config

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"), override=False)

# import ugc as ugc_module  # noqa: E402
import session_store  # noqa: E402
# import trade_show  # noqa: E402
import referee  # noqa: E402
from log_sanitizer import mask_identifier, sanitize_text  # noqa: E402

log = logging.getLogger("openbot")

_ugc_cron_task: asyncio.Task | None = None
_inbox_watcher_task: asyncio.Task | None = None
STARTUP_WARNINGS: list[str] = []


async def _process_inbox_file(filename: str):
    """Process a single file from the inbox and move it to processed."""
    processed_dir = os.path.join(OPENBOT_INBOX_PATH, "processed")
    full_path = os.path.join(OPENBOT_INBOX_PATH, filename)
    
    if not os.path.isfile(full_path) or filename.startswith("."):
        return

    _agent_debug_log("H2", "main.py:_process_inbox_file", f"Processing file: {filename}")
    try:
        with open(full_path, "rb") as f:
            file_bytes = f.read()
        
        mime, _ = mimetypes.guess_type(full_path)
        ext = os.path.splitext(filename)[1].lower()
        
        # 1) Trade-show extraction
        extracted_ts = False
        if ext in (".jpg", ".jpeg", ".png", ".webp", ".heic"):
            if trade_show.GEMINI_API_KEY or trade_show.OPENROUTER_API_KEY:
                image_b64 = base64.b64encode(file_bytes).decode("utf-8")
                try:
                    result = await trade_show.process_photo(image_b64, trade_show="Inbox Ingest")
                    if result:
                        _agent_debug_log("H2", "main.py:_process_inbox_file", f"Trade-show extracted from {filename}")
                        extracted_ts = True
                except Exception as ts_err:
                    log.warning("Trade-show extraction failed for inbox file %s: %s", filename, ts_err)

        # 2) Extract text for Memory
        text, mode = _extract_text_from_document(file_bytes, filename, mime or "")
        if text:
            chunks = _chunk_text(text, KNOWLEDGE_CHUNK_CHARS, KNOWLEDGE_CHUNK_OVERLAP)
            total = min(len(chunks), 25)
            
            source = f"inbox:{filename}"
            meta_line = f"Inbox file: {filename} | size={len(file_bytes)} | mode={mode}"
            tags = ["inbox", "knowledge", (ext.lstrip(".") or "bin"), mode]

            await memory_save(
                category="knowledge_file",
                content=meta_line,
                tags=tags,
                source=source,
            )
            for idx, chunk in enumerate(chunks[:total], start=1):
                await memory_save(
                    category="knowledge",
                    content=f"{meta_line}\nChunk {idx}/{total}:\n{chunk}",
                    tags=tags + [f"chunk-{idx}"],
                    source=source,
                )
            _agent_debug_log("H2", "main.py:_process_inbox_file", f"Ingested {filename} to memory: {total} chunks")
        elif not extracted_ts:
            _agent_debug_log("H2", "main.py:_process_inbox_file", f"No data extracted from {filename}")

        # Move to processed
        dest = os.path.join(processed_dir, f"{int(time.time())}_{filename}")
        os.rename(full_path, dest)
    except Exception as e:
        _agent_debug_log("H1", "main.py:_process_inbox_file", f"Failed to process {filename}: {e}")


class InboxHandler(FileSystemEventHandler):
    def __init__(self, loop):
        self.loop = loop

    def on_created(self, event):
        if not event.is_directory:
            filename = os.path.basename(event.src_path)
            asyncio.run_coroutine_threadsafe(_process_inbox_file(filename), self.loop)


async def _inbox_watcher_cron():
    """Background task: watch openbot_inbox using event-driven watchdog."""
    os.makedirs(OPENBOT_INBOX_PATH, exist_ok=True)
    os.makedirs(os.path.join(OPENBOT_INBOX_PATH, "processed"), exist_ok=True)

    _agent_debug_log("H1", "main.py:_inbox_watcher_cron", f"Event-driven watcher started for {OPENBOT_INBOX_PATH}")

    # 1) Initial scan for existing files
    for filename in os.listdir(OPENBOT_INBOX_PATH):
        if os.path.isfile(os.path.join(OPENBOT_INBOX_PATH, filename)):
            await _process_inbox_file(filename)

    # 2) Setup Watchdog Observer
    loop = asyncio.get_running_loop()
    handler = InboxHandler(loop)
    observer = Observer()
    observer.schedule(handler, OPENBOT_INBOX_PATH, recursive=False)
    observer.start()

    try:
        while True:
            await asyncio.sleep(3600) # Keep task alive
    except asyncio.CancelledError:
        observer.stop()
    observer.join()
AUTOMATION_STATUS: dict[str, object] = {
    "last_memory_write_utc": None,
    "last_memory_write_ok": None,
    "last_memory_read_utc": None,
    "last_memory_read_ok": None,
    "last_error": None,
}


def _agent_debug_log(header: str, source: str, message: str, meta: dict | None = None):
    """Structured internal logging for agent operations."""
    ts = datetime.now(timezone.utc).isoformat()
    msg = f"[{ts}] [{header}] [{source}] {message}"
    if meta:
        msg += f" | {json.dumps(meta)}"
    log.info(msg)


async def _ugc_daily_cron():
    """Background task: scan new Immich uploads once per day and notify via Telegram."""
    while True:
        try:
            await asyncio.sleep(86400)  # 24 hours
            if not ugc_module.IMMICH_API_KEY or not ugc_module.OPENROUTER_API_KEY:
                continue
            immich = ugc_module.ImmichClient()
            chat_id = _ugc_notify_chat_id()

            async def notify(msg: str):
                if chat_id:
                    await telegram_send(chat_id, msg, bot_token=TELEGRAM_MYOSHE_BOT_TOKEN)

            results = await ugc_module.scan_new(immich, notify_fn=notify)
            if chat_id and results.get("scored", 0) > 0:
                summary = (
                    f"UGC daily scan complete:\n"
                    f"  Scored: {results['scored']}\n"
                    f"  Content Ready: {results.get('content_ready', 0)}\n"
                    f"  Needs Editing: {results.get('needs_editing', 0)}\n"
                    f"  Outreach: {results.get('outreach', 0)}"
                )
                await telegram_send(chat_id, summary, bot_token=TELEGRAM_MYOSHE_BOT_TOKEN)
        except asyncio.CancelledError:
            break
        except Exception as e:
            log.error("UGC cron error: %s", e)
            await asyncio.sleep(3600)


def _ugc_notify_chat_id() -> int | None:
    """Get the first allowed Telegram chat ID for UGC notifications."""
    if TELEGRAM_MYOSHE_ALLOWED_CHATS:
        ids = [s.strip() for s in TELEGRAM_MYOSHE_ALLOWED_CHATS.split(",") if s.strip()]
        if ids:
            return int(ids[0])
    return None


@asynccontextmanager
async def lifespan(app):
    yield
    if _ugc_cron_task:
        _ugc_cron_task.cancel()
        try:
            await _ugc_cron_task
        except asyncio.CancelledError:
            pass
    if _inbox_watcher_task:
        _inbox_watcher_task.cancel()
        try:
            await _inbox_watcher_task
        except asyncio.CancelledError:
            pass


app = FastAPI(title="Openbot", version="1.0.0", lifespan=lifespan)
app.include_router(referee.router)

# CORS for web UI
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


def _env_float(name: str, default: float) -> float:
    raw = (os.getenv(name, "") or "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


# Configuration
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY", "").strip()
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "").strip()
OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"
GEMINI_API_URL = "https://generativelanguage.googleapis.com/v1beta/models"

ESSENCEM_URL = os.getenv("ESSENCEM_URL", "http://127.0.0.1:11445")
ENABLE_WEB_UI = os.getenv("ENABLE_WEB_UI", "1").strip() == "1"
# Leave memory API disabled unless explicitly configured in environment.
AGENT_MEMORY_URL = os.getenv("AGENT_MEMORY_URL", "").strip().rstrip("/")
AGENT_MEMORY_TIMEOUT = int(os.getenv("AGENT_MEMORY_TIMEOUT", "15"))
KNOWLEDGE_UPLOAD_DIR = os.getenv("KNOWLEDGE_UPLOAD_DIR", os.path.join(BASE_DIR, "data", "knowledge_uploads")).strip()
if KNOWLEDGE_UPLOAD_DIR and not os.path.isabs(KNOWLEDGE_UPLOAD_DIR):
    KNOWLEDGE_UPLOAD_DIR = os.path.join(BASE_DIR, KNOWLEDGE_UPLOAD_DIR)
KNOWLEDGE_MAX_FILE_BYTES = int(os.getenv("KNOWLEDGE_MAX_FILE_BYTES", "15728640"))
KNOWLEDGE_MAX_EXTRACT_CHARS = int(os.getenv("KNOWLEDGE_MAX_EXTRACT_CHARS", "60000"))
KNOWLEDGE_CHUNK_CHARS = int(os.getenv("KNOWLEDGE_CHUNK_CHARS", "1200"))
KNOWLEDGE_CHUNK_OVERLAP = int(os.getenv("KNOWLEDGE_CHUNK_OVERLAP", "160"))
PORT = int(os.getenv("PORT", 8080))
LLM_TIMEOUT = int(os.getenv("LLM_TIMEOUT", "300"))
SHARED_SESSION_ID = os.getenv("SHARED_SESSION_ID", "peggens").strip() or "peggens"
BOT_TIER = config.BOT_TIER
APP_ENV = os.getenv("APP_ENV", "development").strip().lower()
LOG_VERBOSE_PAYLOADS = os.getenv("LOG_VERBOSE_PAYLOADS", "0").strip() == "1"
REQUIRE_ALLOWED_CHATS = os.getenv("REQUIRE_ALLOWED_CHATS", "0").strip() == "1"
REQUIRE_HTTPS_WEBHOOK = os.getenv("REQUIRE_HTTPS_WEBHOOK", "0").strip() == "1"
TELEGRAM_WEBHOOK_HOST = os.getenv("TELEGRAM_WEBHOOK_HOST", "").strip().lower()
TELEGRAM_ALERT_CHAT_ID = os.getenv("TELEGRAM_ALERT_CHAT_ID", "").strip()
WEBHOOK_RATE_LIMIT_ENABLED = os.getenv("WEBHOOK_RATE_LIMIT_ENABLED", "1").strip() == "1"
WEBHOOK_RATE_LIMIT_RPS = _env_float("WEBHOOK_RATE_LIMIT_RPS", 5.0)
WEBHOOK_RATE_LIMIT_BURST = _env_float("WEBHOOK_RATE_LIMIT_BURST", 20.0)
WEBHOOK_RATE_LIMIT_TRUST_XFF = os.getenv("WEBHOOK_RATE_LIMIT_TRUST_XFF", "1").strip() == "1"
CONTACT_LEAD_FORWARD_URL = os.getenv("CONTACT_LEAD_FORWARD_URL", "").strip()
CONTACT_LEAD_FORWARD_TIMEOUT = int(os.getenv("CONTACT_LEAD_FORWARD_TIMEOUT", "12"))
CONTACT_LEAD_TELEGRAM_ENABLED = os.getenv("CONTACT_LEAD_TELEGRAM_ENABLED", "1").strip() == "1"
TELEGRAM_UPDATE_DEDUP_ENABLED = os.getenv("TELEGRAM_UPDATE_DEDUP_ENABLED", "1").strip() == "1"
TELEGRAM_UPDATE_DEDUP_WINDOW_SECONDS = int(os.getenv("TELEGRAM_UPDATE_DEDUP_WINDOW_SECONDS", "600"))
EXPECTED_TRADE_SHOW_DB_PATH = os.getenv("EXPECTED_TRADE_SHOW_DB_PATH", "").strip()
NOTION_AUTOMATION_DATABASE_ID = os.getenv("NOTION_AUTOMATION_DATABASE_ID", "").strip()
EXPECTED_NOTION_AUTOMATION_DATABASE_ID = os.getenv("EXPECTED_NOTION_AUTOMATION_DATABASE_ID", "").strip()
NOTION_TRADE_SHOW_DATABASE_ID = os.getenv("NOTION_TRADE_SHOW_DATABASE_ID", "").strip()
EXPECTED_NOTION_TRADE_SHOW_DATABASE_ID = os.getenv("EXPECTED_NOTION_TRADE_SHOW_DATABASE_ID", "").strip()
NOTION_UGC_DATABASE_ID = os.getenv("NOTION_UGC_DATABASE_ID", "").strip()
EXPECTED_NOTION_UGC_DATABASE_ID = os.getenv("EXPECTED_NOTION_UGC_DATABASE_ID", "").strip()
NOTION_BUSINESS_CARDS_DATABASE_ID = os.getenv("NOTION_BUSINESS_CARDS_DATABASE_ID", "").strip()
EXPECTED_NOTION_BUSINESS_CARDS_DATABASE_ID = os.getenv("EXPECTED_NOTION_BUSINESS_CARDS_DATABASE_ID", "").strip()
AUTOMATION_MEMORY_SOURCE = os.getenv("AUTOMATION_MEMORY_SOURCE", f"openbot:{SHARED_SESSION_ID}").strip()
AUTOMATION_STRICT = os.getenv("AUTOMATION_STRICT", "0").strip() == "1"
UGC_CRON_ENABLED = os.getenv("UGC_CRON_ENABLED", "1").strip() == "1"
OPENBOT_INBOX_PATH = os.getenv("OPENBOT_INBOX_PATH", os.path.join(BASE_DIR, "openbot_inbox")).strip()
OPENBOT_INBOX_INTERVAL = int(os.getenv("OPENBOT_INBOX_INTERVAL", "60"))


def _telegram_session_id(chat_id: int) -> str:
    """Keep Telegram conversations isolated per chat for cleaner voice continuity."""
    return f"telegram:{chat_id}"

OPENROUTER_MODEL = config.resolve_setting(
    "chat_model",
    "OPENROUTER_MODEL",
    "nvidia/nemotron-3-nano-30b-a3b:free",
)
OPENROUTER_FALLBACK_MODELS = config.resolve_setting(
    "chat_fallbacks",
    "OPENROUTER_FALLBACK_MODELS",
    "",
)
OPENROUTER_VISION_MODEL = config.resolve_setting(
    "vision_model",
    "OPENROUTER_VISION_MODEL",
    "nvidia/nemotron-nano-12b-v2-vl:free",
)
OPENROUTER_VISION_FALLBACK_MODELS = config.resolve_setting(
    "vision_fallbacks",
    "OPENROUTER_VISION_FALLBACK_MODELS",
    "",
)

# Telegram configuration
TELEGRAM_MYOSHE_BOT_TOKEN = os.getenv(
    "TELEGRAM_MYOSHE_BOT_TOKEN",
    os.getenv("TELEGRAM_BOT_TOKEN", ""),
)
TELEGRAM_CITADELLE_BOT_TOKEN = os.getenv("TELEGRAM_CITADELLE_BOT_TOKEN", "")
# Comma-separated list of allowed chat IDs (empty = allow all, unless require flag is enabled)
TELEGRAM_MYOSHE_ALLOWED_CHATS = os.getenv(
    "TELEGRAM_MYOSHE_ALLOWED_CHATS",
    os.getenv("TELEGRAM_ALLOWED_CHATS", ""),
)
TELEGRAM_CITADELLE_ALLOWED_CHATS = os.getenv(
    "TELEGRAM_CITADELLE_ALLOWED_CHATS",
    os.getenv("TELEGRAM_ADMIN_ALLOWED_CHATS", ""),
)
REQUIRE_MYOSHE_ALLOWED_CHATS = os.getenv(
    "REQUIRE_MYOSHE_ALLOWED_CHATS",
    os.getenv("REQUIRE_ALLOWED_CHATS", "0"),
).strip() == "1"
REQUIRE_CITADELLE_ALLOWED_CHATS = os.getenv("REQUIRE_CITADELLE_ALLOWED_CHATS", "1").strip() == "1"

# Twilio configuration
TWILIO_ACCOUNT_SID = os.getenv("TWILIO_ACCOUNT_SID", "")
TWILIO_AUTH_TOKEN = os.getenv("TWILIO_AUTH_TOKEN", "")
TWILIO_PHONE_NUMBER = os.getenv("TWILIO_PHONE_NUMBER", "")
OPENBOT_PUBLIC_BASE_URL = os.getenv("OPENBOT_PUBLIC_BASE_URL", "").strip().rstrip("/")
if not OPENBOT_PUBLIC_BASE_URL and TELEGRAM_WEBHOOK_HOST:
    OPENBOT_PUBLIC_BASE_URL = f"https://{TELEGRAM_WEBHOOK_HOST}".rstrip("/")

# Twilio <Say> copy (override without editing code)
TWILIO_OUTBOUND_VOICE_INTRO = os.getenv("TWILIO_OUTBOUND_VOICE_INTRO", "Hi, this is Openbot.").strip()
TWILIO_INBOUND_VOICE_GREETING = os.getenv(
    "TWILIO_INBOUND_VOICE_GREETING",
    "Hey, this is Openbot. What can I help you with?",
).strip()
TWILIO_DEFAULT_OUTBOUND_SPOKEN_MESSAGE = os.getenv(
    "TWILIO_DEFAULT_OUTBOUND_SPOKEN_MESSAGE",
    "Hello from Openbot.",
).strip()

# Owner timezone (used for time-aware greetings and scheduling)
OWNER_TZ = ZoneInfo(os.getenv("OWNER_TIMEZONE", "America/New_York"))


def _collect_startup_warnings() -> list[str]:
    return []


def _startup_alert_chat_id() -> int | None:
    """Resolve a chat id for startup alerts."""
    if TELEGRAM_ALERT_CHAT_ID and re.fullmatch(r"-?\d+", TELEGRAM_ALERT_CHAT_ID):
        return int(TELEGRAM_ALERT_CHAT_ID)
    if TELEGRAM_MYOSHE_ALLOWED_CHATS:
        ids = [s.strip() for s in TELEGRAM_MYOSHE_ALLOWED_CHATS.split(",") if s.strip()]
        if ids and re.fullmatch(r"-?\d+", ids[0]):
            return int(ids[0])
    return None


async def _twilio_runtime_check() -> dict:
    """Validate Twilio credential/number consistency for outbound calls."""
    if not TWILIO_ACCOUNT_SID or not TWILIO_AUTH_TOKEN or not TWILIO_PHONE_NUMBER:
        return {
            "ok": False,
            "reason": "missing one or more TWILIO_* env values",
            "account_sid": TWILIO_ACCOUNT_SID,
            "from_number": TWILIO_PHONE_NUMBER,
        }
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            acct = await client.get(
                f"https://api.twilio.com/2010-04-01/Accounts/{TWILIO_ACCOUNT_SID}.json",
                auth=(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN),
            )
            if acct.status_code >= 400:
                return {
                    "ok": False,
                    "reason": f"account auth failed ({acct.status_code})",
                    "account_sid": TWILIO_ACCOUNT_SID,
                    "from_number": TWILIO_PHONE_NUMBER,
                }
            number_resp = await client.get(
                f"https://api.twilio.com/2010-04-01/Accounts/{TWILIO_ACCOUNT_SID}/IncomingPhoneNumbers.json",
                params={"PhoneNumber": TWILIO_PHONE_NUMBER},
                auth=(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN),
            )
            if number_resp.status_code >= 400:
                return {
                    "ok": False,
                    "reason": f"number lookup failed ({number_resp.status_code})",
                    "account_sid": TWILIO_ACCOUNT_SID,
                    "from_number": TWILIO_PHONE_NUMBER,
                }
            data = number_resp.json()
            owned = len(data.get("incoming_phone_numbers", [])) > 0
            return {
                "ok": owned,
                "reason": "ok" if owned else "TWILIO_PHONE_NUMBER not owned by configured account",
                "account_sid": TWILIO_ACCOUNT_SID,
                "from_number": TWILIO_PHONE_NUMBER,
            }
    except Exception as exc:
        return {
            "ok": False,
            "reason": f"twilio check exception: {type(exc).__name__}",
            "account_sid": TWILIO_ACCOUNT_SID,
            "from_number": TWILIO_PHONE_NUMBER,
        }


async def _agent_memory_health() -> dict:
    if not AGENT_MEMORY_URL:
        return {"ok": False, "reason": "AGENT_MEMORY_URL missing"}
    try:
        async with httpx.AsyncClient(timeout=AGENT_MEMORY_TIMEOUT) as client:
            resp = await client.get(f"{AGENT_MEMORY_URL}/memory", params={"limit": 1})
            if resp.status_code >= 400:
                return {"ok": False, "reason": f"memory api status {resp.status_code}"}
        return {"ok": True, "reason": "ok"}
    except Exception as exc:
        return {"ok": False, "reason": f"memory api exception: {type(exc).__name__}"}


def _memory_tags_csv(tags: list[str] | None = None) -> str:
    vals = ["myoshee", "openbot"]
    for t in tags or []:
        cleaned = (t or "").strip().lower()
        if cleaned and cleaned not in vals:
            vals.append(cleaned)
    return ",".join(vals)


async def memory_save(category: str, content: str, tags: list[str] | None = None, source: str = "telegram") -> dict:
    if not AGENT_MEMORY_URL:
        raise RuntimeError("Agent memory API is not configured.")
    payload = {
        "category": (category or "fact").strip() or "fact",
        "content": (content or "").strip(),
        "tags": _memory_tags_csv(tags),
        "source": source,
    }
    if not payload["content"]:
        raise RuntimeError("Memory content is empty.")
    async with httpx.AsyncClient(timeout=AGENT_MEMORY_TIMEOUT) as client:
        resp = await client.post(f"{AGENT_MEMORY_URL}/memory", json=payload)
        if resp.status_code >= 400:
            raise RuntimeError(f"Memory save failed ({resp.status_code}): {(resp.text or '')[:200]}")
        return resp.json() if resp.content else {}


async def memory_search(query: str, limit: int = 5) -> list[dict]:
    if not AGENT_MEMORY_URL:
        raise RuntimeError("Agent memory API is not configured.")
    q = (query or "").strip()
    if not q:
        return []
    async with httpx.AsyncClient(timeout=AGENT_MEMORY_TIMEOUT) as client:
        resp = await client.get(f"{AGENT_MEMORY_URL}/memory/search", params={"q": q, "limit": max(1, min(limit, 20))})
        if resp.status_code >= 400:
            raise RuntimeError(f"Memory search failed ({resp.status_code}): {(resp.text or '')[:200]}")
        data = resp.json() if resp.content else {}
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            items = data.get("items")
            return items if isinstance(items, list) else []
        return []


async def memory_recent(limit: int = 8, category: str = "", tag: str = "") -> list[dict]:
    if not AGENT_MEMORY_URL:
        raise RuntimeError("Agent memory API is not configured.")
    params: dict[str, object] = {"limit": max(1, min(limit, 50))}
    if category:
        params["category"] = category
    if tag:
        params["tag"] = tag
    async with httpx.AsyncClient(timeout=AGENT_MEMORY_TIMEOUT) as client:
        resp = await client.get(f"{AGENT_MEMORY_URL}/memory", params=params)
        if resp.status_code >= 400:
            raise RuntimeError(f"Memory list failed ({resp.status_code}): {(resp.text or '')[:200]}")
        data = resp.json() if resp.content else {}
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            items = data.get("items")
            return items if isinstance(items, list) else []
        return []


def _format_memory_items(items: list[dict], title: str) -> str:
    if not items:
        return f"{title}\n(no matches)"
    lines = [title]
    for item in items[:12]:
        mid = item.get("id")
        category = (item.get("category") or "fact").strip()
        content = (item.get("content") or "").strip().replace("\n", " ")
        if len(content) > 180:
            content = content[:177] + "..."
        tags = (item.get("tags") or "").strip()
        row = f"- #{mid} [{category}] {content}" if mid is not None else f"- [{category}] {content}"
        if tags:
            row += f" | tags: {tags}"
        lines.append(row)
    return "\n".join(lines)


def _parse_memory_intent(text: str) -> tuple[str, str]:
    raw = (text or "").strip()
    if not raw:
        return "", ""
    save_patterns = [
        r"^\s*(?:remember this|remember|save this for later|save this)\s*[:,-]?\s*(.+)$",
    ]
    recall_patterns = [
        r"^\s*(?:recall|what do you remember about|what did i tell you about|do you remember)\s+(.+)$",
    ]
    for pattern in save_patterns:
        m = re.match(pattern, raw, flags=re.IGNORECASE)
        if m:
            payload = (m.group(1) or "").strip()
            if payload:
                return "save", payload
    for pattern in recall_patterns:
        m = re.match(pattern, raw, flags=re.IGNORECASE)
        if m:
            payload = (m.group(1) or "").strip()
            if payload:
                return "recall", payload
    return "", ""


def _parse_triage_intent(text: str) -> str:
    raw = (text or "").strip()
    if not raw:
        return ""
    patterns = [
        r"^\s*(?:triage this|break this down|plan this)\s*[:,-]?\s*(.+)$",
        r"^\s*can you\s+(?:triage|break down|plan)\s+(.+)$",
    ]
    for pattern in patterns:
        m = re.match(pattern, raw, flags=re.IGNORECASE)
        if m:
            payload = (m.group(1) or "").strip()
            if payload:
                return payload
    return ""


async def _triage_text(request_text: str) -> str:
    brief = (request_text or "").strip()
    if not brief:
        raise RuntimeError("Missing triage request.")
    triage_prompt = (
        "You are Myoshee's task triage engine. "
        "Turn the user request into a practical execution plan.\n\n"
        "Output exactly these sections:\n"
        "Priority: <Critical|High|Medium|Low>\n"
        "Outcome: <one sentence>\n"
        "Next actions:\n"
        "1) ...\n2) ...\n3) ...\n"
        "Risks:\n"
        "- ...\n"
        "Do not include extra commentary."
    )
    messages = [
        {"role": "system", "content": triage_prompt},
        {"role": "user", "content": brief},
    ]
    result = await call_llm(messages)
    return result.strip()


async def _handle_triage_command(chat_id: int, text: str, bot_token: str):
    payload = text.strip()[len("/triage"):].strip()
    if not payload:
        await telegram_send(chat_id, "Usage: /triage <request>", bot_token=bot_token)
        return
    try:
        await telegram_send_action(chat_id, bot_token=bot_token)
        result = await _triage_text(payload)
        await telegram_send(chat_id, result, bot_token=bot_token)
        try:
            await memory_save(
                category="task",
                content=f"Triage request: {payload}\n\n{result}",
                tags=["triage", "telegram", "auto"],
                source=f"telegram:{chat_id}",
            )
        except Exception:
            # Non-fatal; triage response should still succeed when memory is down.
            pass
    except Exception as exc:
        log.error("Triage command failed: %s", exc)
        await telegram_send(chat_id, f"Triage failed: {exc}", bot_token=bot_token)


async def _build_diag_report() -> str:
    h = health()
    twilio = await _twilio_runtime_check()
    memory_health = await _agent_memory_health()
    lines = [
        "Openbot diag:",
        f"  status: {h.get('status')}",
        f"  bot_tier: {h.get('bot_tier')}",
        f"  model: {h.get('model')}",
        f"  warnings_count: {h.get('warnings_count')}",
        f"  twilio_ok: {str(bool(twilio.get('ok'))).lower()}",
        f"  twilio_reason: {twilio.get('reason')}",
        f"  twilio_from: {twilio.get('from_number') or '(missing)'}",
        f"  agent_memory_ok: {str(bool(memory_health.get('ok'))).lower()}",
        f"  agent_memory_reason: {memory_health.get('reason')}",
        f"  myoshe_token_set: {str(bool(TELEGRAM_MYOSHE_BOT_TOKEN)).lower()}",
        f"  citadelle_token_set: {str(bool(TELEGRAM_CITADELLE_BOT_TOKEN)).lower()}",
    ]
    return "\n".join(lines)


async def _build_memory_diag_report() -> str:
    health = await _agent_memory_health()
    lines = [
        "Memory diag:",
        f"  url: {AGENT_MEMORY_URL or '(missing)'}",
        f"  ok: {str(bool(health.get('ok'))).lower()}",
        f"  reason: {health.get('reason')}",
    ]
    if not health.get("ok"):
        return "\n".join(lines)
    try:
        items = await memory_recent(limit=1)
        lines.append("  sample_query_ok: true")
        lines.append(f"  sample_items: {len(items)}")
    except Exception as exc:
        lines.append("  sample_query_ok: false")
        lines.append(f"  sample_query_error: {type(exc).__name__}")
    return "\n".join(lines)


async def _maybe_send_startup_twilio_alert():
    """Notify operator in Telegram if Twilio outbound config is inconsistent."""
    check = await _twilio_runtime_check()
    if check.get("ok"):
        return
    chat_id = _startup_alert_chat_id()
    if not chat_id or not TELEGRAM_MYOSHE_BOT_TOKEN:
        log.warning("Twilio startup check failed but no alert chat/token configured: %s", check.get("reason"))
        return
    msg = (
        "Openbot startup alert:\n"
        f"Twilio config issue: {check.get('reason')}\n"
        f"Account SID: {check.get('account_sid') or '(missing)'}\n"
        f"From number: {check.get('from_number') or '(missing)'}"
    )
    try:
        await telegram_send(chat_id, msg, bot_token=TELEGRAM_MYOSHE_BOT_TOKEN)
    except Exception as exc:
        log.error("Failed to send startup Twilio alert: %s", exc)


async def _maybe_send_startup_memory_alert():
    """Notify operator in Telegram if agent-memory is unavailable at startup."""
    if not AGENT_MEMORY_URL:
        return
    check = await _agent_memory_health()
    if check.get("ok"):
        return
    chat_id = _startup_alert_chat_id()
    if not chat_id or not TELEGRAM_MYOSHE_BOT_TOKEN:
        log.warning("Memory startup check failed but no alert chat/token configured: %s", check.get("reason"))
        return
    msg = (
        "Openbot startup alert:\n"
        f"Memory API issue: {check.get('reason')}\n"
        f"Memory URL: {AGENT_MEMORY_URL or '(missing)'}"
    )
    try:
        await telegram_send(chat_id, msg, bot_token=TELEGRAM_MYOSHE_BOT_TOKEN)
    except Exception as exc:
        log.error("Failed to send startup memory alert: %s", exc)


def _model_chain(primary: str, fallback_csv: str) -> list[str]:
    chain = [primary]
    extras = [m.strip() for m in fallback_csv.split(",") if m.strip()]
    for model in extras:
        if model not in chain:
            chain.append(model)
    return chain


def _safe_snippet(text: str, limit: int = 80) -> str:
    """Redact/log-lite helper to avoid leaking user payloads in production logs."""
    if LOG_VERBOSE_PAYLOADS:
        return sanitize_text(text[:limit])
    return f"<len={len(text)}>"

# System prompt - loaded from SYSTEM_PROMPT_FILE env var or system_prompt.txt
_PROMPT_FILE = os.getenv(
    "SYSTEM_PROMPT_FILE",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "system_prompt.txt"),
)
if os.path.exists(_PROMPT_FILE):
    with open(_PROMPT_FILE) as _f:
        _BASE_SYSTEM_PROMPT = _f.read().strip()
else:
    _BASE_SYSTEM_PROMPT = "You are Openbot, a concise AI assistant. Keep responses brief."


def get_system_prompt(country_code: str = "") -> str:
    """Return system prompt with current date/time and visitor geo context."""
    now = datetime.now(OWNER_TZ)
    time_ctx = f"\n\nCurrent date/time: {now.strftime('%A, %B %d, %Y %I:%M %p %Z')}"
    geo_ctx = ""
    if country_code and country_code not in ("XX", "T1"):
        geo_ctx = f"\nVisitor country: {country_code}"
    return _BASE_SYSTEM_PROMPT + time_ctx + geo_ctx

# In-memory conversation store (keyed by session_id)
conversations: dict[str, list[dict]] = {SHARED_SESSION_ID: []}

# In-memory call log (list of call records)
call_log: list[dict] = []
# Per-call conversation threads (keyed by CallSid)
call_threads: dict[str, list[dict]] = {}
# Outbound call notifications (keyed by Twilio CallSid)
outbound_call_watchers: dict[str, dict] = {}
# Outbound call context archive for follow-up actions.
outbound_call_archive: dict[str, dict] = {}
# Last contact search results per chat (for /callpick).
contact_search_cache: dict[int, list[dict]] = {}
# Pending call drafts (tap contact, then send message text).
pending_call_drafts: dict[int, dict] = {}
telegram_brief_mode: dict[int, bool] = {}
retry_tasks: set[asyncio.Task] = set()

MYOSHE_HELP_TEXT = (
    "Myoshee Daily Driver:\n"
    "  /about\n"
    "  /call +1XXXXXXXXXX Your message\n"
    "  call/ Name | Your message\n"
    "  /call Name @ Company | Your message\n"
    "  /addcontact Name | +15551234567\n"
    "  /addcontact Name | +15551234567 | Company\n"
    "  /contacts\n"
    "  /contacts <name or destination>\n"
    "  /callpick <#> | Your message\n"
    "  /cancelcall\n"
    "  /calls\n"
    "  /callstatus <CallSid>\n"
    "  /remember <text>\n"
    "  /recall <query>\n"
    "  /memories [N]\n"
    "  /memorydiag\n"
    "  /brief [on|off]\n"
    "  /verbose [on|off]\n"
    "  /budget [key=value...]\n"
    "  /debt [key=value...]\n"
    "  /savings [key=value...]\n"
    "  /credit [key=value...]\n"
    "  /stocks [key=value...]\n"
    "  /crypto [key=value...]\n"
    "  /triage <request>\n"
    "  Send files (PDF/DOCX/EPUB/TXT/CSV/JSON/XLSX) to store + ingest knowledge\n"
    "  /ugc new\n"
    "  /ugc ready [N]\n"
    "  /ugc stats\n"
    "  /clear\n"
    "  You can also say: dial +15169843733 and say ..."
)

CALL_MESSAGE_TEMPLATES: dict[str, str] = {
    "followup": "Hi, this is Peggs from FlyWithPeggs. Quick follow-up regarding your latest update.",
    "availability": "Hi, this is Peggs from FlyWithPeggs. Checking current availability and best options for my client.",
    "urgent": "Hi, this is Peggs from FlyWithPeggs. This is time-sensitive; please call me back as soon as you can.",
}

FINANCE_COMMAND_HELP = (
    "Finance command examples:\n"
    "  /budget income=5000 fixed=2200 variable=900 debt=300\n"
    "  /debt balance=12000 apr=19.9 payment=450\n"
    "  /savings target=6000 months=12\n"
    "  /credit score=680 utilization=42\n"
    "  /stocks ticker=AAPL horizon=5y risk=medium\n"
    "  /crypto asset=BTC horizon=3y risk=high"
)


def _is_brief_mode(chat_id: int) -> bool:
    """Brief mode defaults to on for mobile-friendly Telegram use."""
    return telegram_brief_mode.get(chat_id, True)


def _set_brief_mode(chat_id: int, enabled: bool) -> None:
    telegram_brief_mode[chat_id] = bool(enabled)


def _brief_footer(chat_id: int) -> str:
    if _is_brief_mode(chat_id):
        return "\n\n_tip: send `/verbose on` for full detail._"
    return ""


def _parse_key_value_args(text: str) -> dict[str, str]:
    pairs: dict[str, str] = {}
    for match in re.finditer(r"([A-Za-z_][A-Za-z0-9_]*)=([^\s]+)", text or ""):
        key = match.group(1).strip().lower()
        val = match.group(2).strip()
        if key and val:
            pairs[key] = val
    return pairs


async def _handle_finance_command(chat_id: int, text: str, user_name: str, bot_token: str):
    """Handle Telegram finance commands with optional key=value inputs."""
    del user_name
    parts = text.strip().split(maxsplit=1)
    cmd = parts[0].lower()
    raw_args = parts[1].strip() if len(parts) > 1 else ""
    topic = cmd.lstrip("/")
    if topic == "finance":
        await telegram_send(chat_id, FINANCE_COMMAND_HELP, bot_token=bot_token)
        return

    session_id = _telegram_session_id(chat_id)
    if session_id not in conversations:
        conversations[session_id] = []

    kv = _parse_key_value_args(raw_args)
    style_ctx = (
        "Reply in this structure:\n"
        "1) one-line summary\n"
        "2) three short bullets\n"
        "3) one concrete next step question\n"
    )
    topic_ctx = (
        f"User requested finance topic: {topic}.\n"
        "Provide educational guidance only; do not provide personalized legal/tax/investment advice."
    )
    user_payload = raw_args if raw_args else "(no structured inputs provided)"
    if kv:
        structured = ", ".join(f"{k}={v}" for k, v in kv.items())
        user_payload += f"\nParsed inputs: {structured}"

    _append_conversation_entry(
        session_id=session_id,
        role="user",
        content=text,
        source="telegram",
    )

    try:
        messages = build_messages(
            session_id,
            response_style_ctx=f"[Response style]\n{style_ctx}\n\n[Finance context]\n{topic_ctx}",
        )
        messages.append(
            {
                "role": "user",
                "content": (
                    f"Finance task `{topic}`.\n"
                    f"User inputs:\n{user_payload}\n"
                    "If key inputs are missing, ask for only the minimum next fields needed."
                ),
            }
        )
        response = await call_llm(messages)
    except HTTPException as exc:
        await telegram_send(chat_id, f"Finance command error: {exc.detail}", bot_token=bot_token)
        return
    except Exception as exc:
        log.error("Finance command error for chat %s: %s", chat_id, exc)
        await telegram_send(chat_id, "Sorry, finance processing failed. Try again.", bot_token=bot_token)
        return

    _append_conversation_entry(
        session_id=session_id,
        role="assistant",
        content=response,
        source="telegram",
    )
    await store_memory(session_id, "user", text)
    await store_memory(session_id, "assistant", response)
    await telegram_send(chat_id, response + _brief_footer(chat_id), bot_token=bot_token)


class ChatRequest(BaseModel):
    message: str
    session_id: Optional[str] = None


class ChatResponse(BaseModel):
    response: str
    session_id: str


class MacWhisperPayload(BaseModel):
    title: str
    transcript: str


def get_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat()


def _record_automation_write(ok: bool, error: str = "") -> None:
    AUTOMATION_STATUS["last_memory_write_utc"] = get_timestamp()
    AUTOMATION_STATUS["last_memory_write_ok"] = bool(ok)
    if error:
        AUTOMATION_STATUS["last_error"] = error


def _record_automation_read(ok: bool, error: str = "") -> None:
    AUTOMATION_STATUS["last_memory_read_utc"] = get_timestamp()
    AUTOMATION_STATUS["last_memory_read_ok"] = bool(ok)
    if error:
        AUTOMATION_STATUS["last_error"] = error


def _append_conversation_entry(
    session_id: str,
    role: str,
    content: str,
    source: str = "",
    country: str = "",
):
    entry = {
        "role": role,
        "content": content,
        "timestamp": get_timestamp(),
        "source": source,
    }
    if country:
        entry["country"] = country
    conversations.setdefault(session_id, []).append(entry)
    session_store.append_session_message(
        session_id=session_id,
        role=role,
        content=content,
        source=source,
        country=country,
        timestamp=entry["timestamp"],
    )
    return entry


def _get_country(request: Request) -> str:
    """Extract visitor country code from Cloudflare's CF-IPCountry header."""
    return request.headers.get("CF-IPCountry", "")


def _build_trade_show_context(text: str) -> str:
    """Search the trade show DB for anything matching the user's message.
    Returns a context string to inject, or empty string if no matches."""
    try:
        contacts = trade_show.search_contacts(text, limit=5)
        promos = trade_show.search_promos(text, limit=5)
    except Exception:
        return ""

    if not contacts and not promos:
        return ""

    parts = ["[Trade show context]"]
    if contacts:
        parts.append("Supplier contacts:")
        for c in contacts:
            line = f"- {c['name']}, {c.get('title', '')} @ {c['company']}"
            if c.get("phone"):
                line += f" | {c['phone']}"
            if c.get("email"):
                line += f" | {c['email']}"
            if c.get("destinations"):
                line += f" | destinations: {c['destinations']}"
            if c.get("trade_show"):
                line += f" | met at {c['trade_show']}"
            parts.append(line)
    if promos:
        parts.append("Active promos:")
        for p in promos:
            line = f"- {p.get('promo_name', 'Promo')} from {p['supplier_name']}"
            if p.get("destinations"):
                line += f" | {p['destinations']}"
            if p.get("pricing"):
                line += f" | {p['pricing']}"
            if p.get("end_date"):
                line += f" | expires {p['end_date']}"
            if p.get("booking_code"):
                line += f" | code: {p['booking_code']}"
            parts.append(line)
    return "\n".join(parts)


def build_messages(
    session_id: str,
    country_code: str = "",
    trade_show_ctx: str = "",
    response_style_ctx: str = "",
) -> list[dict]:
    """Build chat messages with conversation history and optional context."""
    history = conversations.get(session_id, [])

    messages = [{"role": "system", "content": get_system_prompt(country_code)}]

    for msg in history[-8:]:
        messages.append({"role": msg["role"], "content": msg["content"]})

    if trade_show_ctx:
        last_user = next((m for m in reversed(messages) if m["role"] == "user"), None)
        if last_user:
            last_user["content"] += f"\n\n{trade_show_ctx}"

    if response_style_ctx:
        last_user = next((m for m in reversed(messages) if m["role"] == "user"), None)
        if last_user:
            last_user["content"] += f"\n\n{response_style_ctx}"

    return messages


OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434").strip().rstrip("/")


async def _ollama_chat(messages: list[dict], model: str) -> str:
    """Call local Ollama API (100% free local processing)."""
    url = f"{OLLAMA_BASE_URL}/api/chat"
    payload = {
        "model": model,
        "messages": messages,
        "stream": False
    }
    async with httpx.AsyncClient(timeout=LLM_TIMEOUT) as client:
        resp = await client.post(url, json=payload)
        resp.raise_for_status()
        data = resp.json()
        return data["message"]["content"].strip()


async def _gemini_chat(messages: list[dict], model: str) -> str:
    """Call Google Gemini API directly (bypassing OpenRouter/credits)."""
    if not GEMINI_API_KEY:
        raise RuntimeError("GEMINI_API_KEY missing")
    
    # Simple conversion from OpenAI message format to Gemini format
    contents = []
    for m in messages:
        role = "model" if m["role"] == "assistant" else m["role"]
        content = m["content"]
        if isinstance(content, list):
            parts = []
            for p in content:
                if p["type"] == "text":
                    parts.append({"text": p["text"]})
                elif p["type"] == "image_url":
                    # Handle base64 images
                    img_data = p["image_url"]["url"].split(",", 1)[1]
                    parts.append({"inline_data": {"mime_type": "image/jpeg", "data": img_data}})
            contents.append({"role": role, "parts": parts})
        else:
            contents.append({"role": role, "parts": [{"text": content}]})

    url = f"{GEMINI_API_URL}/{model}:generateContent?key={GEMINI_API_KEY}"
    payload = {"contents": contents}
    
    async with httpx.AsyncClient(timeout=LLM_TIMEOUT) as client:
        resp = await client.post(url, json=payload)
        resp.raise_for_status()
        data = resp.json()
        return data["candidates"][0]["content"]["parts"][0]["text"].strip()


async def _openrouter_chat(messages: list[dict], model: str | None = None) -> str:
    """Call OpenRouter chat completions API."""
    payload = {
        "model": model or OPENROUTER_MODEL,
        "messages": messages,
    }
    headers = {
        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
        "Content-Type": "application/json",
    }
    async with httpx.AsyncClient(timeout=LLM_TIMEOUT) as client:
        resp = await client.post(OPENROUTER_URL, headers=headers, json=payload)
        resp.raise_for_status()
        data = resp.json()
        return data["choices"][0]["message"]["content"].strip()


async def call_llm(messages: list[dict]) -> str:
    """Hybrid Intelligence: Ollama (if lid open) -> Direct Gemini (Plan) -> OpenRouter."""
    chain = _model_chain(OPENROUTER_MODEL, OPENROUTER_FALLBACK_MODELS)
    last_error: Exception | None = None
    
    for idx, model in enumerate(chain, start=1):
        try:
            # 1) Try local Ollama with a very short timeout (2s)
            # This handles the "lid closed" or "on the go" scenario
            if ":" in model or model in {"llama3", "qwen3", "gemma3", "altidor-bot", "travel-pro"}:
                try:
                    async with httpx.AsyncClient(timeout=2.0) as check_client:
                        # Fast heartbeat check
                        await check_client.get(f"{OLLAMA_BASE_URL}/api/tags")
                    return await _ollama_chat(messages, model)
                except (httpx.ConnectError, httpx.TimeoutException):
                    log.info("Ollama unreachable (lid likely closed), falling back to Cloud Plan.")
                    # Continue to next model in chain (likely Gemini)
                    continue

            # 2) Try direct Google API (Free/Plan Tokens)
            if GEMINI_API_KEY and ("gemini" in model.lower()):
                direct_model = model.split("/")[-1] if "/" in model else model
                if direct_model.startswith("gemini-"):
                    try:
                        return await _gemini_chat(messages, direct_model)
                    except Exception as g_err:
                        log.warning("Direct Gemini failed, falling back: %s", g_err)

            # 3) Fallback to OpenRouter (Credits)
            if not model.startswith("local:"):
                reply = await _openrouter_chat(messages, model=model)
                if idx > 1:
                    log.warning("LLM fallback used: model=%s attempt=%d", model, idx)
                return reply
        except (httpx.TimeoutException, httpx.HTTPError) as exc:
            last_error = exc
            log.warning("LLM model failed: model=%s attempt=%d error=%s", model, idx, type(exc).__name__)
            continue
        except Exception as exc:
            raise HTTPException(status_code=502, detail=f"LLM connection error: {type(exc).__name__}")

    if isinstance(last_error, httpx.TimeoutException):
        raise HTTPException(status_code=504, detail="LLM timeout across model chain")
    raise HTTPException(status_code=502, detail="LLM request failed across model chain")


async def call_vision_llm(image_base64: str, prompt: str = "Describe what you see in this image.") -> str:
    """Call vision model with preference: Ollama (if available) -> Direct Gemini -> OpenRouter."""
    messages = [
        {"role": "system", "content": get_system_prompt()},
        {
            "role": "user",
            "content": [
                {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{image_base64}"}},
                {"type": "text", "text": prompt},
            ],
        },
    ]
    
    chain = _model_chain(OPENROUTER_VISION_MODEL, OPENROUTER_VISION_FALLBACK_MODELS)
    last_error: Exception | None = None
    
    for idx, model in enumerate(chain, start=1):
        try:
            # 1) Try local Ollama Vision if supported
            if "vision" in model.lower() and ":" in model:
                try:
                    return await _ollama_chat(messages, model)
                except Exception as o_err:
                    log.warning("Local Ollama Vision failed: %s", o_err)

            # 2) Try direct Gemini Vision
            if GEMINI_API_KEY and ("gemini" in model.lower()):
                direct_model = model.split("/")[-1] if "/" in model else model
                if direct_model.startswith("gemini-"):
                    try:
                        return await _gemini_chat(messages, direct_model)
                    except Exception as g_err:
                        log.warning("Direct Gemini Vision failed: %s", g_err)

            # 3) Fallback to OpenRouter
            reply = await _openrouter_chat(messages, model=model)
            if idx > 1:
                log.warning("Vision fallback used: model=%s attempt=%d", model, idx)
            return reply
        except (httpx.TimeoutException, httpx.HTTPError) as exc:
            last_error = exc
            log.warning("Vision model failed: model=%s attempt=%d error=%s", model, idx, type(exc).__name__)
            continue
        except Exception as exc:
            raise HTTPException(status_code=502, detail=f"Vision LLM connection error: {type(exc).__name__}")

    if isinstance(last_error, httpx.TimeoutException):
        raise HTTPException(status_code=504, detail="Vision LLM timeout across model chain")
    raise HTTPException(status_code=502, detail="Vision LLM request failed across model chain")


def _telegram_api(bot_token: str) -> str:
    return f"https://api.telegram.org/bot{bot_token}"


async def telegram_download_photo(file_id: str, bot_token: str) -> str:
    """Download a photo from Telegram and return it as a base64 string."""
    async with httpx.AsyncClient(timeout=30) as client:
        file_resp = await client.get(f"{_telegram_api(bot_token)}/getFile", params={"file_id": file_id})
        file_resp.raise_for_status()
        file_path = file_resp.json()["result"]["file_path"]

        photo_resp = await client.get(
            f"https://api.telegram.org/file/bot{bot_token}/{file_path}"
        )
        photo_resp.raise_for_status()
        return base64.b64encode(photo_resp.content).decode("utf-8")


async def telegram_download_file(file_id: str, bot_token: str) -> tuple[bytes, str]:
    """Download a Telegram file and return (bytes, file_path)."""
    async with httpx.AsyncClient(timeout=45) as client:
        file_resp = await client.get(f"{_telegram_api(bot_token)}/getFile", params={"file_id": file_id})
        file_resp.raise_for_status()
        file_path = file_resp.json()["result"]["file_path"]
        data_resp = await client.get(f"https://api.telegram.org/file/bot{bot_token}/{file_path}")
        data_resp.raise_for_status()
        return data_resp.content, file_path


def _safe_file_name(name: str) -> str:
    base = os.path.basename((name or "upload.bin").strip()) or "upload.bin"
    return re.sub(r"[^A-Za-z0-9._-]+", "_", base)


def _chunk_text(text: str, size: int, overlap: int) -> list[str]:
    cleaned = (text or "").strip()
    if not cleaned:
        return []
    step = max(1, size - max(0, overlap))
    chunks: list[str] = []
    i = 0
    while i < len(cleaned):
        part = cleaned[i:i + size].strip()
        if part:
            chunks.append(part)
        i += step
    return chunks


def _extract_text_from_document(file_bytes: bytes, file_name: str, mime_type: str) -> tuple[str, str]:
    ext = os.path.splitext((file_name or "").lower())[1]
    mime = (mime_type or "").lower()
    # region agent log
    _agent_debug_log(
        "H2",
        "main.py:_extract_text_from_document",
        "Document extraction entry",
        {"ext": ext, "mime": mime, "byte_len": len(file_bytes)},
    )
    # endregion

    text_like_exts = {
        ".txt", ".md", ".markdown", ".csv", ".json", ".jsonl", ".xml", ".html", ".htm",
        ".py", ".js", ".ts", ".tsx", ".jsx", ".yml", ".yaml", ".toml", ".ini", ".log",
    }
    if mime.startswith("text/") or ext in text_like_exts:
        try:
            return file_bytes.decode("utf-8", errors="replace"), "text"
        except Exception:
            return "", "text-decode-failed"

    if ext == ".pdf" or mime == "application/pdf":
        try:
            PdfReader = importlib.import_module("pypdf").PdfReader
            # region agent log
            _agent_debug_log(
                "H3",
                "main.py:_extract_text_from_document:pdf",
                "Loaded PDF parser module",
                {"module": "pypdf"},
            )
            # endregion
            reader = PdfReader(io.BytesIO(file_bytes))
            pages = [(p.extract_text() or "") for p in reader.pages]
            return "\n\n".join(pages), "pdf"
        except Exception as e:
            # region agent log
            _agent_debug_log(
                "H3",
                "main.py:_extract_text_from_document:pdf",
                "PDF parser unavailable or failed",
                {"error_type": type(e).__name__},
            )
            # endregion
            return "", "pdf-extract-unavailable"

    if ext == ".docx" or mime in ("application/vnd.openxmlformats-officedocument.wordprocessingml.document",):
        try:
            Document = importlib.import_module("docx").Document
            # region agent log
            _agent_debug_log(
                "H4",
                "main.py:_extract_text_from_document:docx",
                "Loaded DOCX parser module",
                {"module": "docx"},
            )
            # endregion
            doc = Document(io.BytesIO(file_bytes))
            lines = [p.text for p in doc.paragraphs if (p.text or "").strip()]
            return "\n".join(lines), "docx"
        except Exception as e:
            # region agent log
            _agent_debug_log(
                "H4",
                "main.py:_extract_text_from_document:docx",
                "DOCX parser unavailable or failed",
                {"error_type": type(e).__name__},
            )
            # endregion
            return "", "docx-extract-unavailable"

    if ext == ".epub" or mime == "application/epub+zip":
        try:
            epub = importlib.import_module("ebooklib.epub")
            ITEM_DOCUMENT = importlib.import_module("ebooklib").ITEM_DOCUMENT
            BeautifulSoup = importlib.import_module("bs4").BeautifulSoup
            # region agent log
            _agent_debug_log(
                "H5",
                "main.py:_extract_text_from_document:epub",
                "Loaded EPUB parser modules",
                {"modules": "ebooklib,bs4"},
            )
            # endregion
            book = epub.read_epub(io.BytesIO(file_bytes))
            texts: list[str] = []
            for item in book.get_items_of_type(ITEM_DOCUMENT):
                soup = BeautifulSoup(item.get_content(), "html.parser")
                t = soup.get_text(" ", strip=True)
                if t:
                    texts.append(t)
            return "\n\n".join(texts), "epub"
        except Exception as e:
            # region agent log
            _agent_debug_log(
                "H5",
                "main.py:_extract_text_from_document:epub",
                "EPUB parser unavailable or failed",
                {"error_type": type(e).__name__},
            )
            # endregion
            return "", "epub-extract-unavailable"

    if ext in (".xlsx", ".xlsm") or mime in (
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "application/vnd.ms-excel.sheet.macroenabled.12",
    ):
        try:
            load_workbook = importlib.import_module("openpyxl").load_workbook
            wb = load_workbook(io.BytesIO(file_bytes), data_only=True, read_only=True)
            rows: list[str] = []
            for ws in wb.worksheets:
                rows.append(f"[Sheet: {ws.title}]")
                for row in ws.iter_rows(values_only=True):
                    vals = [str(v).strip() for v in row if v is not None and str(v).strip()]
                    if vals:
                        rows.append(" | ".join(vals))
            return "\n".join(rows), "xlsx"
        except Exception:
            return "", "xlsx-extract-unavailable"

    return "", "unsupported"


async def _handle_telegram_document(chat_id: int, document: dict, caption: str, user_name: str, bot_token: str):
    """Store and ingest a non-image Telegram document into agent memory."""
    del user_name
    file_id = (document.get("file_id") or "").strip()
    if not file_id:
        await telegram_send(chat_id, "I couldn't access that file. Please try again.", bot_token=bot_token)
        return

    original_name = _safe_file_name(str(document.get("file_name") or "upload.bin"))
    mime_type = str(document.get("mime_type") or "")
    size_hint = int(document.get("file_size") or 0)
    if size_hint > KNOWLEDGE_MAX_FILE_BYTES:
        await telegram_send(
            chat_id,
            f"File too large ({size_hint} bytes). Max is {KNOWLEDGE_MAX_FILE_BYTES} bytes.",
            bot_token=bot_token,
        )
        return

    try:
        await telegram_send_action(chat_id, bot_token=bot_token, action="typing")
        file_bytes, remote_path = await telegram_download_file(file_id, bot_token=bot_token)
    except Exception as exc:
        log.error("Failed downloading document from chat %s: %s", chat_id, exc)
        await telegram_send(chat_id, "Sorry, I couldn't download that file.", bot_token=bot_token)
        return

    if len(file_bytes) > KNOWLEDGE_MAX_FILE_BYTES:
        await telegram_send(
            chat_id,
            f"File too large after download ({len(file_bytes)} bytes). Max is {KNOWLEDGE_MAX_FILE_BYTES} bytes.",
            bot_token=bot_token,
        )
        return

    os.makedirs(KNOWLEDGE_UPLOAD_DIR, exist_ok=True)
    ext = os.path.splitext(original_name)[1].lower() or mimetypes.guess_extension(mime_type or "") or ""
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    digest = hashlib.sha256(file_bytes).hexdigest()[:12]
    stored_name = _safe_file_name(f"{stamp}_{digest}_{os.path.splitext(original_name)[0]}{ext}")
    stored_path = os.path.join(KNOWLEDGE_UPLOAD_DIR, stored_name)
    with open(stored_path, "wb") as f:
        f.write(file_bytes)

    extracted, mode = _extract_text_from_document(file_bytes, original_name, mime_type)
    extracted = (extracted or "").strip()
    if len(extracted) > KNOWLEDGE_MAX_EXTRACT_CHARS:
        extracted = extracted[:KNOWLEDGE_MAX_EXTRACT_CHARS]

    source = f"telegram:{chat_id}"
    meta_line = (
        f"Knowledge file: {original_name} | mime={mime_type or 'unknown'} "
        f"| stored={stored_name} | bytes={len(file_bytes)}"
    )
    if caption:
        meta_line += f" | caption={caption.strip()}"
    if remote_path:
        meta_line += f" | tg_path={remote_path}"

    tags = ["telegram", "file", "knowledge", (ext.lstrip(".") or "bin"), mode]

    try:
        if extracted:
            chunks = _chunk_text(extracted, KNOWLEDGE_CHUNK_CHARS, KNOWLEDGE_CHUNK_OVERLAP)
            total = min(len(chunks), 12)
            await memory_save(
                category="knowledge_file",
                content=meta_line,
                tags=tags,
                source=source,
            )
            for idx, chunk in enumerate(chunks[:total], start=1):
                await memory_save(
                    category="knowledge",
                    content=f"{meta_line}\nChunk {idx}/{total}:\n{chunk}",
                    tags=tags + [f"chunk-{idx}"],
                    source=source,
                )
            await telegram_send(
                chat_id,
                f"Stored and ingested `{original_name}`.\nSaved {total} searchable knowledge chunks.",
                bot_token=bot_token,
            )
        else:
            await memory_save(
                category="knowledge_file",
                content=f"{meta_line}\nNo text extracted ({mode}).",
                tags=tags,
                source=source,
            )
            await telegram_send(
                chat_id,
                f"Stored `{original_name}` and indexed metadata.\nText extraction is not available for this file type yet.",
                bot_token=bot_token,
            )
    except Exception as exc:
        await telegram_send(
            chat_id,
            f"Stored `{original_name}`, but knowledge ingestion failed: {exc}",
            bot_token=bot_token,
        )


def _xml_escape(value: str) -> str:
    return html.escape(value or "", quote=True)


def _parse_call_command(text: str) -> tuple[str, str]:
    """Parse call command formats.

    Supported:
    - call/ +15551234567 Your message here
    - /call +15551234567 Your message here
    - call/ location | Your message here
    - /call location | Your message here
    """
    raw = text.strip()
    if raw.lower().startswith("call/"):
        payload = raw[5:].strip()
    elif raw.lower().startswith("/call"):
        payload = raw[5:].strip()
    else:
        return "", ""

    if not payload:
        return "", ""

    if "|" in payload:
        left, right = payload.split("|", 1)
        return left.strip(), right.strip()

    parts = payload.split(maxsplit=1)
    if len(parts) < 2:
        return "", ""
    return parts[0].strip(), parts[1].strip()


def _parse_natural_call_request(text: str) -> tuple[str, str]:
    """Parse natural-language call requests.

    Examples:
    - "dial +15169843733 hello there"
    - "call +1 516-984-3733 and say this is Openbot"
    """
    raw = text.strip()
    m = re.search(
        r"\b(?:dial|call)\b\s+(\+?\d[\d\-\s\(\)]{7,}\d)\s*(?:,?\s*(?:and\s+say|say|message|with)\s+)?(.*)",
        raw,
        flags=re.IGNORECASE,
    )
    if not m:
        return "", ""
    number = re.sub(r"[^\d+]", "", m.group(1))
    if number and not number.startswith("+"):
        number = f"+{number}"
    message = (m.group(2) or "").strip()
    if not message:
        message = TWILIO_DEFAULT_OUTBOUND_SPOKEN_MESSAGE
    return number, message


def _parse_natural_named_call_request(text: str) -> tuple[str, str]:
    """Parse natural-language name/company call requests.

    Examples:
    - "call Maria @ Sandals and say quick follow-up on rates"
    - "dial John at Delta Vacations about the contract"
    """
    raw = text.strip()
    m = re.search(
        r"^\s*(?:dial|call)\s+(.+?)\s+(?:,?\s*(?:and\s+say|say|about|re|regarding)\s+)(.+)\s*$",
        raw,
        flags=re.IGNORECASE,
    )
    if not m:
        return "", ""
    target = (m.group(1) or "").strip()
    message = (m.group(2) or "").strip()
    if not target or not message:
        return "", ""
    # Avoid colliding with number-only flow.
    if re.fullmatch(r"\+?\d[\d\-\s\(\)]{7,}\d", target):
        return "", ""
    # Normalize " at " to existing "Name @ Company" format.
    target = re.sub(r"\s+at\s+", " @ ", target, flags=re.IGNORECASE).strip()
    return target, message


async def _twilio_place_outbound_call(
    to_number: str,
    spoken_message: str,
    location_hint: str = "",
) -> str:
    if not (TWILIO_ACCOUNT_SID and TWILIO_AUTH_TOKEN and TWILIO_PHONE_NUMBER):
        raise RuntimeError("Twilio outbound calling is not configured.")

    if not to_number.startswith("+"):
        raise RuntimeError("Destination number must be in E.164 format, e.g. +15551234567.")

    safe_msg = _xml_escape(spoken_message)
    intro = _xml_escape(TWILIO_OUTBOUND_VOICE_INTRO)
    if location_hint:
        intro += f" Message context: {_xml_escape(location_hint)}."

    twiml = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        f"<Response><Say voice=\"Polly.Joanna\">{intro} {safe_msg}</Say></Response>"
    )

    url = f"https://api.twilio.com/2010-04-01/Accounts/{TWILIO_ACCOUNT_SID}/Calls.json"
    payload = {
        "To": to_number,
        "From": TWILIO_PHONE_NUMBER,
        "Twiml": twiml,
    }
    if OPENBOT_PUBLIC_BASE_URL:
        payload["StatusCallback"] = f"{OPENBOT_PUBLIC_BASE_URL}/webhook/outbound/status"
        payload["StatusCallbackMethod"] = "POST"
        payload["StatusCallbackEvent"] = "initiated ringing answered completed"

    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(url, auth=(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN), data=payload)
        if resp.status_code >= 400:
            raise RuntimeError(f"Twilio call failed ({resp.status_code}): {resp.text[:300]}")
        data = resp.json()
        return data.get("sid", "")


async def _handle_call_command(chat_id: int, text: str, bot_token: str):
    target, message = _parse_call_command(text)
    if not target or not message:
        await telegram_send(
            chat_id,
            "Usage:\n"
            "  call/ +15551234567 Your message\n"
            "  or\n"
            "  /call +15551234567 Your message\n"
            "  or\n"
            "  call/ Name | Your message\n"
            "  or\n"
            "  /call Name @ Company | Your message",
            bot_token=bot_token,
        )
        return

    to_number = target
    location_hint = ""

    # Contact lookup when target is not a direct E.164 number.
    if not target.startswith("+"):
        target_name = target
        target_company = ""
        if "@" in target:
            target_name, target_company = [p.strip() for p in target.split("@", 1)]

        matches = await asyncio.to_thread(trade_show.search_contacts, target_name, 10)
        filtered = []
        for item in matches:
            name = (item.get("name") or "").strip()
            company = (item.get("company") or "").strip()
            phone = (item.get("phone") or "").strip()
            if not phone:
                continue
            if name.lower() != target_name.lower():
                continue
            if target_company and company.lower() != target_company.lower():
                continue
            filtered.append(item)

        # If exact-name matching produced no rows, fallback to any rows with phone.
        if not filtered:
            filtered = [m for m in matches if (m.get("phone") or "").strip()]

        if not filtered:
            await telegram_send(
                chat_id,
                f"No phone found for \"{target}\".\n"
                "Use `/contacts <name>` to find options, then call with `/call +15551234567 ...`.",
                bot_token=bot_token,
            )
            return

        # Never auto-decide when more than one matching contact has a phone.
        if len(filtered) > 1:
            lines = [f"Multiple contacts match \"{target}\". I won't choose for you.", ""]
            for c in filtered[:8]:
                lines.append(
                    f"- {c.get('name', 'Unknown')} @ {c.get('company', 'Unknown')} | {c.get('phone', 'no phone')}"
                )
            lines.append("")
            lines.append("Use one of these:")
            lines.append("  /call +15551234567 Your message")
            lines.append("  /call Name @ Company | Your message")
            await telegram_send(chat_id, "\n".join(lines), bot_token=bot_token)
            return

        chosen = filtered[0]
        to_number = (chosen.get("phone") or "").strip()
        location_hint = f"{chosen.get('name', '').strip()} @ {chosen.get('company', '').strip()}".strip(" @")

    await _place_and_notify_outbound_call(
        chat_id=chat_id,
        bot_token=bot_token,
        to_number=to_number,
        message=message,
        location_hint=location_hint,
    )


async def _handle_addcontact_command(chat_id: int, text: str, bot_token: str):
    """Add/update a direct-dial contact: /addcontact Name | +15551234567 [| Company]."""
    payload = text.strip()[len("/addcontact"):].strip()
    if not payload:
        await telegram_send(
            chat_id,
            "Usage:\n"
            "  /addcontact Name | +15551234567\n"
            "  /addcontact Name | +15551234567 | Company",
            bot_token=bot_token,
        )
        return

    parts = [p.strip() for p in payload.split("|")]
    if len(parts) < 2:
        await telegram_send(
            chat_id,
            "Usage:\n"
            "  /addcontact Name | +15551234567\n"
            "  /addcontact Name | +15551234567 | Company",
            bot_token=bot_token,
        )
        return

    name = parts[0]
    phone = parts[1]
    company = parts[2] if len(parts) > 2 and parts[2] else "Personal"

    digits = re.sub(r"[^\d+]", "", phone)
    if digits and not digits.startswith("+"):
        digits = f"+{digits}"
    if not re.fullmatch(r"\+\d{8,15}", digits or ""):
        await telegram_send(chat_id, "Phone must be E.164, e.g. +15551234567", bot_token=bot_token)
        return

    contact_data = {
        "name": name,
        "company": company,
        "phone": digits,
        "supplier_type": "personal",
        "destinations": "",
        "region": "",
        "notes": "Added via /addcontact",
    }
    try:
        contact_id, updated = await asyncio.to_thread(trade_show.upsert_contact, contact_data, "", "")
        status = "updated" if updated else "saved"
        await telegram_send(
            chat_id,
            f"Contact {status}: {name} @ {company} | {digits} (id {contact_id})",
            bot_token=bot_token,
        )
    except Exception as exc:
        log.error("Addcontact failed: %s", exc)
        await telegram_send(chat_id, f"Addcontact failed: {exc}", bot_token=bot_token)


def _format_contact_picker_line(index: int, contact: dict) -> str:
    name = (contact.get("name") or "Unknown").strip()
    company = (contact.get("company") or "Unknown").strip()
    phone = (contact.get("phone") or "").strip() or "no phone"
    destinations = (contact.get("destinations") or "").strip()
    base = f"{index}. {name} @ {company} | {phone}"
    if destinations:
        return f"{base} | {destinations}"
    return base


async def _handle_callpick_command(chat_id: int, text: str, bot_token: str):
    """Call a contact selected from the latest /contacts list.

    Usage: /callpick 2 | Your message
    """
    payload = text.strip()[len("/callpick"):].strip()
    if not payload or "|" not in payload:
        await telegram_send(
            chat_id,
            "Usage:\n"
            "  /contacts [query]\n"
            "  /callpick <#> | Your message",
            bot_token=bot_token,
        )
        return

    index_text, message = [p.strip() for p in payload.split("|", 1)]
    if not index_text.isdigit():
        await telegram_send(chat_id, "Pick must be a number, e.g. `/callpick 2 | hello`", bot_token=bot_token)
        return
    if not message:
        await telegram_send(chat_id, "Please include the message after `|`.", bot_token=bot_token)
        return

    picks = contact_search_cache.get(chat_id) or []
    if not picks:
        await telegram_send(
            chat_id,
            "No contact list is loaded yet. Run `/contacts` or `/contacts <query>` first.",
            bot_token=bot_token,
        )
        return

    idx = int(index_text)
    if idx < 1 or idx > len(picks):
        await telegram_send(chat_id, f"Pick out of range. Choose 1 to {len(picks)}.", bot_token=bot_token)
        return

    chosen = picks[idx - 1]
    raw_phone = (chosen.get("phone") or "").strip()
    if not raw_phone:
        await telegram_send(chat_id, "That contact has no phone number. Pick another.", bot_token=bot_token)
        return

    to_number = re.sub(r"[^\d+]", "", raw_phone)
    if to_number and not to_number.startswith("+"):
        to_number = f"+{to_number}"
    if not re.fullmatch(r"\+\d{8,15}", to_number or ""):
        await telegram_send(chat_id, f"Invalid phone on selected contact: {raw_phone}", bot_token=bot_token)
        return

    location_hint = f"{(chosen.get('name') or '').strip()} @ {(chosen.get('company') or '').strip()}".strip(" @")

    await _place_and_notify_outbound_call(
        chat_id=chat_id,
        bot_token=bot_token,
        to_number=to_number,
        message=message,
        location_hint=location_hint,
    )


async def _place_and_notify_outbound_call(
    *,
    chat_id: int,
    bot_token: str,
    to_number: str,
    message: str,
    location_hint: str = "",
):
    try:
        call_started_at = get_timestamp()
        call_sid = await _twilio_place_outbound_call(
            to_number=to_number,
            spoken_message=message,
            location_hint=location_hint,
        )
        if call_sid:
            outbound_call_watchers[call_sid] = {
                "chat_id": chat_id,
                "bot_token": bot_token,
                "to_number": to_number,
                "message": message,
                "location_hint": location_hint,
                "created_at": call_started_at,
            }
            outbound_call_archive[call_sid] = {
                "chat_id": chat_id,
                "bot_token": bot_token,
                "to_number": to_number,
                "message": message,
                "location_hint": location_hint,
                "created_at": call_started_at,
            }
            _upsert_call_runtime_record(
                call_sid=call_sid,
                status="initiated",
                caller=to_number,
                direction="outbound-api",
                duration=0,
            )
        await telegram_send(
            chat_id,
            f"Calling {to_number} now. Call SID: {call_sid or 'created'}\n"
            "I will send you the final call outcome (answered/no-answer/busy/failed).\n"
            "Use `/callstatus <CallSid>` anytime for live status.",
            bot_token=bot_token,
        )
        if not OPENBOT_PUBLIC_BASE_URL:
            await telegram_send(
                chat_id,
                "Note: webhook callback URL is not configured, so automatic final status updates may be delayed.\n"
                "Set `OPENBOT_PUBLIC_BASE_URL` or `TELEGRAM_WEBHOOK_HOST` to enable push status callbacks.",
                bot_token=bot_token,
            )
    except Exception as exc:
        log.error("Outbound call failed: %s", exc)
        await telegram_send(chat_id, f"Call failed: {exc}", bot_token=bot_token)


def _schedule_retry_call(
    *,
    chat_id: int,
    bot_token: str,
    to_number: str,
    message: str,
    location_hint: str,
    delay_seconds: int,
):
    async def _runner():
        await asyncio.sleep(max(1, int(delay_seconds)))
        await _place_and_notify_outbound_call(
            chat_id=chat_id,
            bot_token=bot_token,
            to_number=to_number,
            message=message,
            location_hint=location_hint,
        )

    task = asyncio.create_task(_runner())
    retry_tasks.add(task)
    task.add_done_callback(lambda t: retry_tasks.discard(t))


def _build_contact_picker_markup(results: list[dict], max_buttons: int = 8) -> dict:
    rows = []
    for idx, contact in enumerate(results[:max_buttons], start=1):
        label = f"{idx}) {(contact.get('name') or 'Unknown').strip()}"
        company = (contact.get("company") or "").strip()
        if company:
            label += f" @ {company[:20]}"
        rows.append([{"text": label[:64], "callback_data": f"pickcall:{idx}"}])
    if rows:
        rows.append([{"text": "Cancel", "callback_data": "cancelcalldraft"}])
    return {"inline_keyboard": rows}


def _build_calldraft_actions_markup() -> dict:
    return {
        "inline_keyboard": [
            [
                {"text": "Use Template", "callback_data": "calldraft:template_menu"},
                {"text": "Call Now", "callback_data": "calldraft:now"},
            ],
            [{"text": "Cancel", "callback_data": "cancelcalldraft"}],
        ]
    }


def _build_calldraft_templates_markup() -> dict:
    return {
        "inline_keyboard": [
            [{"text": "Quick Follow-up", "callback_data": "calldraft:template:followup"}],
            [{"text": "Availability Check", "callback_data": "calldraft:template:availability"}],
            [{"text": "Urgent Callback", "callback_data": "calldraft:template:urgent"}],
            [{"text": "Back", "callback_data": "calldraft:actions"}],
            [{"text": "Cancel", "callback_data": "cancelcalldraft"}],
        ]
    }


def _build_retry_markup(call_sid: str) -> dict:
    sid = (call_sid or "").strip()
    return {
        "inline_keyboard": [
            [
                {"text": "Retry in 15m", "callback_data": f"retry:15m:{sid}"},
                {"text": "Retry Tomorrow", "callback_data": f"retry:tmr:{sid}"},
            ],
            [{"text": "Mark Done", "callback_data": f"retry:done:{sid}"}],
        ]
    }


def _render_recent_calls(limit: int = 5) -> str:
    rows = session_store.list_calls(limit=limit)
    if not rows:
        return "No recent calls yet."
    lines = [f"Recent calls ({len(rows)}):"]
    for item in rows:
        sid = (item.get("call_sid") or "").strip()
        direction = (item.get("direction") or "unknown").strip()
        status = (item.get("status") or "unknown").strip()
        caller = (item.get("caller") or "unknown").strip()
        duration = int(item.get("duration") or 0)
        started = (item.get("started_at") or "").strip()
        short_sid = sid[:12] + "..." if len(sid) > 15 else sid
        lines.append(f"- {direction} {caller} | {status} | {duration}s | {short_sid or 'no-sid'}")
        if started:
            lines.append(f"  started: {started}")
    if OPENBOT_PUBLIC_BASE_URL:
        lines.append("")
        lines.append(f"Web log: {OPENBOT_PUBLIC_BASE_URL}/calls")
    return "\n".join(lines)


def _seconds_until_tomorrow_9am() -> int:
    """Seconds until the next 09:00 in OWNER_TZ (tomorrow morning if already past 9am today)."""
    now = datetime.now(OWNER_TZ)
    target = now.replace(hour=9, minute=0, second=0, microsecond=0)
    if now >= target:
        target = target + timedelta(days=1)
    return max(60, int((target - now).total_seconds()))


async def _twilio_fetch_call_status(call_sid: str) -> dict:
    if not (TWILIO_ACCOUNT_SID and TWILIO_AUTH_TOKEN):
        raise RuntimeError("Twilio account credentials are not configured.")
    sid = (call_sid or "").strip()
    if not sid:
        raise RuntimeError("CallSid is required.")
    if not re.fullmatch(r"CA[0-9a-fA-F]{32}", sid):
        raise RuntimeError("Invalid CallSid format.")
    url = f"https://api.twilio.com/2010-04-01/Accounts/{TWILIO_ACCOUNT_SID}/Calls/{sid}.json"
    async with httpx.AsyncClient(timeout=20) as client:
        resp = await client.get(url, auth=(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN))
        if resp.status_code >= 400:
            raise RuntimeError(f"Twilio status lookup failed ({resp.status_code}): {resp.text[:200]}")
        return resp.json()


def _upsert_call_runtime_record(call_sid: str, status: str, *, caller: str = "", direction: str = "outbound-api", duration: int = 0):
    if not call_sid:
        return
    now = get_timestamp()
    for entry in call_log:
        if entry.get("call_sid") == call_sid:
            entry["status"] = status or entry.get("status", "")
            entry["duration"] = int(duration or 0)
            entry["ended_at"] = now if status in {"completed", "busy", "failed", "no-answer", "canceled"} else entry.get("ended_at", "")
            session_store.upsert_call_record(
                call_sid=call_sid,
                caller=entry.get("caller", caller),
                direction=entry.get("direction", direction),
                status=entry.get("status", status),
                started_at=entry.get("started_at", now),
                ended_at=entry.get("ended_at", ""),
                duration=int(entry.get("duration", duration) or 0),
            )
            return
    call_entry = {
        "call_sid": call_sid,
        "caller": caller,
        "direction": direction,
        "status": status,
        "started_at": now,
        "ended_at": now if status in {"completed", "busy", "failed", "no-answer", "canceled"} else "",
        "duration": int(duration or 0),
        "turns": [],
    }
    call_log.append(call_entry)
    session_store.upsert_call_record(
        call_sid=call_sid,
        caller=caller,
        direction=direction,
        status=status,
        started_at=call_entry["started_at"],
        ended_at=call_entry["ended_at"],
        duration=call_entry["duration"],
    )


async def _handle_callback_query(
    body: dict,
    *,
    bot_token: str,
    allowed_chats_csv: str,
    require_allowed_chats: bool,
):
    cb = body.get("callback_query") or {}
    cb_id = cb.get("id", "")
    data = str(cb.get("data") or "").strip()
    msg = cb.get("message") or {}
    chat_id = ((msg.get("chat") or {}).get("id"))
    if not isinstance(chat_id, int):
        if cb_id:
            await telegram_answer_callback(cb_id, bot_token, "Invalid callback context")
        return
    if not _is_chat_allowed(chat_id, allowed_chats_csv=allowed_chats_csv, require_allowed_chats=require_allowed_chats):
        if cb_id:
            await telegram_answer_callback(cb_id, bot_token, "Chat not allowed")
        return

    if data.startswith("retry:"):
        parts = data.split(":", 2)
        if len(parts) < 3:
            if cb_id:
                await telegram_answer_callback(cb_id, bot_token, "Invalid retry action")
            return
        action = parts[1].strip()
        call_sid = parts[2].strip()
        ctx = outbound_call_archive.get(call_sid) or {}
        if action == "done":
            if cb_id:
                await telegram_answer_callback(cb_id, bot_token, "Done")
            await telegram_send(chat_id, f"Marked done for {call_sid}.", bot_token=bot_token)
            return
        if not ctx:
            if cb_id:
                await telegram_answer_callback(cb_id, bot_token, "Call context expired")
            await telegram_send(chat_id, "Retry context expired. Please start a new call.", bot_token=bot_token)
            return
        if action == "15m":
            delay = 15 * 60
            label = "15 minutes"
        elif action == "tmr":
            delay = _seconds_until_tomorrow_9am()
            label = "tomorrow at 9:00 AM"
        else:
            if cb_id:
                await telegram_answer_callback(cb_id, bot_token, "Unknown retry action")
            return

        _schedule_retry_call(
            chat_id=int(ctx.get("chat_id") or chat_id),
            bot_token=str(ctx.get("bot_token") or bot_token),
            to_number=str(ctx.get("to_number") or ""),
            message=str(ctx.get("message") or CALL_MESSAGE_TEMPLATES["followup"]),
            location_hint=str(ctx.get("location_hint") or ""),
            delay_seconds=delay,
        )
        if cb_id:
            await telegram_answer_callback(cb_id, bot_token, "Retry scheduled")
        await telegram_send(
            chat_id,
            f"Retry scheduled for {label}.\nTarget: {ctx.get('to_number', 'unknown')}",
            bot_token=bot_token,
        )
        return

    if data == "cancelcalldraft":
        pending_call_drafts.pop(chat_id, None)
        if cb_id:
            await telegram_answer_callback(cb_id, bot_token, "Canceled")
        await telegram_send(chat_id, "Canceled call draft.", bot_token=bot_token)
        return

    if data == "calldraft:actions":
        draft = pending_call_drafts.get(chat_id)
        if not draft:
            if cb_id:
                await telegram_answer_callback(cb_id, bot_token, "No active draft")
            await telegram_send(chat_id, "No active call draft. Run `/contacts` again.", bot_token=bot_token)
            return
        if cb_id:
            await telegram_answer_callback(cb_id, bot_token, "Draft options")
        await telegram_send(
            chat_id,
            f"Selected {draft.get('location_hint') or draft.get('to_number')}. Choose next step:",
            bot_token=bot_token,
            reply_markup=_build_calldraft_actions_markup(),
        )
        return

    if data == "calldraft:template_menu":
        draft = pending_call_drafts.get(chat_id)
        if not draft:
            if cb_id:
                await telegram_answer_callback(cb_id, bot_token, "No active draft")
            await telegram_send(chat_id, "No active call draft. Run `/contacts` again.", bot_token=bot_token)
            return
        if cb_id:
            await telegram_answer_callback(cb_id, bot_token, "Pick a template")
        await telegram_send(
            chat_id,
            "Pick a call template:",
            bot_token=bot_token,
            reply_markup=_build_calldraft_templates_markup(),
        )
        return

    if data == "calldraft:now":
        draft = pending_call_drafts.pop(chat_id, None)
        if not draft:
            if cb_id:
                await telegram_answer_callback(cb_id, bot_token, "No active draft")
            await telegram_send(chat_id, "No active call draft. Run `/contacts` again.", bot_token=bot_token)
            return
        default_message = CALL_MESSAGE_TEMPLATES["followup"]
        if cb_id:
            await telegram_answer_callback(cb_id, bot_token, "Placing call")
        asyncio.create_task(
            _place_and_notify_outbound_call(
                chat_id=chat_id,
                bot_token=bot_token,
                to_number=draft["to_number"],
                message=default_message,
                location_hint=draft.get("location_hint", ""),
            )
        )
        return

    if data.startswith("calldraft:template:"):
        key = data.split(":", 2)[2].strip()
        draft = pending_call_drafts.pop(chat_id, None)
        if not draft:
            if cb_id:
                await telegram_answer_callback(cb_id, bot_token, "No active draft")
            await telegram_send(chat_id, "No active call draft. Run `/contacts` again.", bot_token=bot_token)
            return
        template = CALL_MESSAGE_TEMPLATES.get(key)
        if not template:
            if cb_id:
                await telegram_answer_callback(cb_id, bot_token, "Unknown template")
            await telegram_send(chat_id, "Unknown template. Try again from `/contacts`.", bot_token=bot_token)
            return
        if cb_id:
            await telegram_answer_callback(cb_id, bot_token, "Template selected")
        asyncio.create_task(
            _place_and_notify_outbound_call(
                chat_id=chat_id,
                bot_token=bot_token,
                to_number=draft["to_number"],
                message=template,
                location_hint=draft.get("location_hint", ""),
            )
        )
        return

    if data.startswith("pickcall:"):
        raw_idx = data.split(":", 1)[1].strip()
        if not raw_idx.isdigit():
            if cb_id:
                await telegram_answer_callback(cb_id, bot_token, "Invalid selection")
            return
        picks = contact_search_cache.get(chat_id) or []
        idx = int(raw_idx)
        if idx < 1 or idx > len(picks):
            if cb_id:
                await telegram_answer_callback(cb_id, bot_token, "Selection expired")
            await telegram_send(chat_id, "Selection expired. Run `/contacts` again.", bot_token=bot_token)
            return
        chosen = picks[idx - 1]
        raw_phone = (chosen.get("phone") or "").strip()
        to_number = re.sub(r"[^\d+]", "", raw_phone)
        if to_number and not to_number.startswith("+"):
            to_number = f"+{to_number}"
        if not re.fullmatch(r"\+\d{8,15}", to_number or ""):
            if cb_id:
                await telegram_answer_callback(cb_id, bot_token, "No valid phone")
            await telegram_send(chat_id, "That contact does not have a valid dialable phone.", bot_token=bot_token)
            return
        location_hint = f"{(chosen.get('name') or '').strip()} @ {(chosen.get('company') or '').strip()}".strip(" @")
        pending_call_drafts[chat_id] = {
            "to_number": to_number,
            "location_hint": location_hint,
            "picked_at": get_timestamp(),
        }
        if cb_id:
            await telegram_answer_callback(cb_id, bot_token, "Selected")
        await telegram_send(
            chat_id,
            f"Selected {location_hint or to_number}.\nChoose next step or just type the call message.\nSend `/cancelcall` to cancel.",
            bot_token=bot_token,
            reply_markup=_build_calldraft_actions_markup(),
        )
        return

    if cb_id:
        await telegram_answer_callback(cb_id, bot_token, "Unsupported action")


async def store_memory(session_id: str, role: str, content: str):
    """Store conversation turn in Essencem."""
    payload = {
        "source": f"openbot:{session_id}",
        "timestamp": get_timestamp(),
        "text": f"[{role}] {content}",
        "tags": "openbot,chat",
    }
    
    async with httpx.AsyncClient(timeout=10) as client:
        try:
            await client.post(f"{ESSENCEM_URL}/memories", json=payload)
            _record_automation_write(True)
        except httpx.HTTPError:
            _record_automation_write(False, "memory_write_failed")
            pass  # Non-critical, don't fail chat if memory store fails


async def get_recent_memories(session_id: str, limit: int = 10) -> list[dict]:
    """Retrieve recent memories for a session."""
    async with httpx.AsyncClient(timeout=10) as client:
        try:
            resp = await client.get(
                f"{ESSENCEM_URL}/memories",
                params={"source": f"openbot:{session_id}", "limit": limit}
            )
            resp.raise_for_status()
            _record_automation_read(True)
            return resp.json().get("items", [])
        except httpx.HTTPError:
            _record_automation_read(False, "memory_read_failed")
            return []


def _contact_email_ok(email: str) -> bool:
    if not email or len(email) > 254 or email.count("@") != 1:
        return False
    local, domain = email.split("@", 1)
    if not local or not domain or "." not in domain:
        return False
    return True


N8N_URL = os.getenv("N8N_URL", "").strip().rstrip("/")
N8N_API_KEY = os.getenv("N8N_API_KEY", "").strip()


async def notify_n8n(event_type: str, data: dict):
    """Send an event to n8n for cross-platform synchronization."""
    if not N8N_URL:
        return

    # Use a specific webhook path if configured, or a default one
    url = f"{N8N_URL}/webhook/myoshee-event"
    headers = {"Content-Type": "application/json"}
    if N8N_API_KEY:
        headers["X-N8N-API-KEY"] = N8N_API_KEY

    payload = {
        "event": event_type,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "data": data,
    }

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(url, json=payload, headers=headers)
            if resp.status_code >= 400:
                log.warning("n8n notification failed: HTTP %s", resp.status_code)
    except Exception as e:
        log.warning("n8n notification error: %s", e)


@app.post("/webhook/n8n-trigger")
async def n8n_trigger(request: Request):
    """Incoming endpoint for n8n to trigger actions in MyOshee."""
    # Basic security check for the n8n API key if provided
    auth_header = request.headers.get("X-N8N-API-KEY")
    if N8N_API_KEY and auth_header != N8N_API_KEY:
        log.warning("Unauthorized n8n trigger attempt")
        raise HTTPException(status_code=401, detail="Unauthorized")

    try:
        payload = await request.json()
        action = payload.get("action")
        data = payload.get("data", {})

        _agent_debug_log("H2", "main.py:n8n_trigger", f"Action received: {action}", data)

        if action == "send_telegram":
            chat_id = data.get("chat_id")
            text = data.get("text")
            if chat_id and text:
                await telegram_send(int(chat_id), text, bot_token=TELEGRAM_MYOSHE_BOT_TOKEN)
                return {"ok": True, "message": "Telegram message sent"}

        elif action == "remember":
            category = data.get("category", "n8n_fact")
            content = data.get("content")
            tags = data.get("tags", ["n8n"])
            if content:
                await memory_save(category, content, tags, source="n8n")
                return {"ok": True, "message": "Fact saved to memory"}

        elif action == "sync_notion":
            # Manual trigger for a Notion sync task
            pass

        return {"ok": False, "error": f"Unknown action: {action}"}

    except Exception as e:
        log.error("n8n trigger error: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


async def _deliver_contact_lead(body: dict) -> None:
    """Telegram ping + optional n8n/secondary POST; errors logged, never raised to client."""
    lines = [
        "New contact (flywithpeggs.com)",
        f"Name: {(body.get('name') or '').strip() or '(none)'}",
        f"Email: {(body.get('email') or '').strip()}",
        f"Inquiry: {(body.get('inquiry_type') or body.get('category') or '').strip() or '(none)'}",
        f"Goal: {(body.get('goal') or '').strip() or '(none)'}",
        f"Timeline: {(body.get('timeline') or '').strip() or '(none)'}",
        f"Preferred: {(body.get('preferred_contact') or '').strip() or '(none)'}",
        f"Details: {(body.get('preferred_contact_details') or '').strip() or '(none)'}",
    ]
    msg_body = (body.get("message") or "").strip()
    if msg_body:
        cap = 1200
        lines.append("Message:")
        lines.append(msg_body if len(msg_body) <= cap else msg_body[: cap - 3] + "...")
    lines.append(f"Source: {(body.get('source') or '').strip() or '(none)'}")
    lines.append(f"Submitted: {(body.get('submitted_at') or '').strip() or '(none)'}")
    telegram_text = "\n".join(lines)

    if CONTACT_LEAD_TELEGRAM_ENABLED:
        chat_id = _startup_alert_chat_id()
        if chat_id and TELEGRAM_MYOSHE_BOT_TOKEN:
            try:
                await telegram_send(chat_id, telegram_text, bot_token=TELEGRAM_MYOSHE_BOT_TOKEN)
            except Exception as exc:
                log.warning("contact lead Telegram notify failed: %s", exc)

    if CONTACT_LEAD_FORWARD_URL:
        try:
            async with httpx.AsyncClient(timeout=CONTACT_LEAD_FORWARD_TIMEOUT) as client:
                r = await client.post(
                    CONTACT_LEAD_FORWARD_URL,
                    json=body,
                    headers={"Content-Type": "application/json"},
                )
                if r.status_code >= 400:
                    log.warning(
                        "contact lead forward HTTP %s (len=%s)",
                        r.status_code,
                        len(r.text or ""),
                    )
        except httpx.HTTPError as exc:
            log.warning("contact lead forward request failed: %s", exc)


@app.post("/new-lead")
async def contact_new_lead(request: Request, background_tasks: BackgroundTasks):
    """Accept JSON from flywithpeggs.com contact form; rate-limited; 200 {ok:true} on success."""
    await _enforce_webhook_rate_limit(request)
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")
    if not isinstance(body, dict):
        raise HTTPException(status_code=400, detail="Expected a JSON object")

    if (str(body.get("website") or "")).strip():
        return {"ok": True}

    email = (str(body.get("email") or "")).strip()
    if not _contact_email_ok(email):
        raise HTTPException(status_code=400, detail="Valid email required")

    background_tasks.add_task(_deliver_contact_lead, body)
    return {"ok": True}


@app.get("/health")
def health():
    persistence = {
        "trade_show": trade_show.get_db_fingerprint(),
        "session_store": session_store.get_fingerprint(),
        "memory_source": AUTOMATION_MEMORY_SOURCE,
        "in_memory_state": {
            "session_messages": len(conversations.get(SHARED_SESSION_ID, [])),
            "call_records": len(call_log),
            "call_threads": len(call_threads),
        },
    }
    return {
        "status": "ok",
        "provider": "openrouter",
        "bot_tier": BOT_TIER,
        "model": OPENROUTER_MODEL,
        "vision_model": OPENROUTER_VISION_MODEL,
        "warnings_count": len(STARTUP_WARNINGS),
        "warnings": STARTUP_WARNINGS,
        "persistence": persistence,
    }


@app.get("/automation/status")
async def automation_status():
    # Fetch recent memory items for the explorer
    recent_memories = []
    try:
        recent_memories = await memory_search("", limit=10)
    except Exception:
        pass

    # Fetch basic counts from trade show DB
    ts_counts = {}
    try:
        import sqlite3
        conn = sqlite3.connect(trade_show.DB_PATH)
        cur = conn.cursor()
        for table in ("supplier_contacts", "promos"):
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            ts_counts[table] = cur.fetchone()[0]
        conn.close()
    except Exception:
        pass

    return {
        "status": "ok",
        "strict_mode": AUTOMATION_STRICT,
        "configured": {
            "notion_automation_database_id": NOTION_AUTOMATION_DATABASE_ID,
            "expected_notion_automation_database_id": EXPECTED_NOTION_AUTOMATION_DATABASE_ID,
            "notion_trade_show_database_id": NOTION_TRADE_SHOW_DATABASE_ID,
            "expected_notion_trade_show_database_id": EXPECTED_NOTION_TRADE_SHOW_DATABASE_ID,
            "notion_ugc_database_id": NOTION_UGC_DATABASE_ID,
            "expected_notion_ugc_database_id": EXPECTED_NOTION_UGC_DATABASE_ID,
            "notion_business_cards_database_id": NOTION_BUSINESS_CARDS_DATABASE_ID,
            "expected_notion_business_cards_database_id": EXPECTED_NOTION_BUSINESS_CARDS_DATABASE_ID,
            "memory_source": AUTOMATION_MEMORY_SOURCE,
            "trade_show_db_path": trade_show.DB_PATH,
            "expected_trade_show_db_path": EXPECTED_TRADE_SHOW_DB_PATH,
        },
        "runtime": AUTOMATION_STATUS,
        "trade_show_db": trade_show.get_db_fingerprint(),
        "session_db": session_store.get_fingerprint(),
        "stats": ts_counts,
        "recent_memory": recent_memories
    }


@app.post("/chat", response_model=ChatResponse)
async def chat(req: Request, request: ChatRequest):
    """Send a message and get an AI response."""
    session_id = SHARED_SESSION_ID
    country = _get_country(req)

    if session_id not in conversations:
        conversations[session_id] = []

    _append_conversation_entry(
        session_id=session_id,
        role="user",
        content=request.message,
        source="web",
        country=country,
    )

    messages = build_messages(session_id, country)
    response = await call_llm(messages)

    _append_conversation_entry(
        session_id=session_id,
        role="assistant",
        content=response,
        source="web",
    )
    
    await store_memory(session_id, "user", request.message)
    await store_memory(session_id, "assistant", response)
    
    return ChatResponse(response=response, session_id=session_id)


@app.post("/chat/image", response_model=ChatResponse)
async def chat_image(
    req: Request,
    file: UploadFile | None = File(None),
    files: list[UploadFile] | None = File(None),
    caption: str = "",
):
    """Send one or more images and get a vision AI response.

    If Gemini trade-show extraction is configured, run structured extraction first
    (business cards/flyers), which writes to SQLite and best-effort syncs cards to Notion.
    """
    session_id = SHARED_SESSION_ID
    country = _get_country(req)

    if session_id not in conversations:
        conversations[session_id] = []

    upload_files: list[UploadFile] = []
    if files:
        upload_files.extend(files)
    if file:
        upload_files.append(file)
    if not upload_files:
        raise HTTPException(status_code=400, detail="No image file uploaded")

    prompt = caption if caption else "What's in this image? If it's a business card, extract all contact info. If it's a flyer or promo, extract all details."
    if len(upload_files) == 1:
        user_desc = f"[sent a photo] {caption}" if caption else "[sent a photo]"
    else:
        user_desc = f"[sent {len(upload_files)} photos] {caption}" if caption else f"[sent {len(upload_files)} photos]"

    _append_conversation_entry(
        session_id=session_id,
        role="user",
        content=user_desc,
        source="web",
        country=country,
    )

    responses: list[str] = []
    ts_name = ""
    ts_date = ""
    if caption:
        low = caption.lower()
        if low.startswith("show:") or low.startswith("tradeshow:"):
            ts_name = caption.split(":", 1)[1].strip()
            ts_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    for idx, upload in enumerate(upload_files, start=1):
        image_data = await upload.read()
        image_b64 = base64.b64encode(image_data).decode("utf-8")
        result = ""
        
        # 1) Try trade-show extraction (business cards/flyers)
        if trade_show.GEMINI_API_KEY or trade_show.OPENROUTER_API_KEY:
            try:
                extracted = await trade_show.process_photo(
                    image_b64,
                    trade_show=ts_name,
                    trade_show_date=ts_date,
                    my_location=country,
                )
                if extracted:
                    result = trade_show.format_results_summary(extracted)
            except Exception as exc:
                log.warning("Web image trade-show extraction failed: %s", exc)

        # 2) Perform UGC scoring and sync to Notion (for all photos)
        try:
            # We don't have an Immich asset ID here since it's a direct upload, 
            # but we can still score it and sync to Notion.
            ugc_score = await ugc_module.score_photo(image_data)
            if ugc_score:
                await ugc_module.sync_ugc_to_notion(ugc_score, f"web-upload-{int(time.time())}-{idx}")
                if not result:
                    # If not a business card, use the UGC description
                    result = f"UGC Score: {ugc_score.get('overall_score')}/10\nCategory: {ugc_score.get('category')}\nDescription: {ugc_score.get('description')}"
        except Exception as exc:
            log.warning("UGC scoring/sync failed for web upload: %s", exc)

        # 3) Fallback to generic vision if no specific result yet
        if not result:
            result = await call_vision_llm(image_b64, prompt)

        if len(upload_files) == 1:
            responses.append(result)
        else:
            file_label = upload.filename or f"image_{idx}"
            responses.append(f"[{idx}/{len(upload_files)}] {file_label}\n{result}")
    response = "\n\n".join(responses)

    _append_conversation_entry(
        session_id=session_id,
        role="assistant",
        content=response,
        source="web",
    )

    await store_memory(session_id, "user", user_desc)
    await store_memory(session_id, "assistant", response)

    # Notify n8n of the new image(s) processed
    await notify_n8n("web_image_processed", {
        "session_id": session_id,
        "count": len(upload_files),
        "response": response
    })

    return ChatResponse(response=response, session_id=session_id)


# ---------------------------------------------------------------------------
# Telegram webhook
# ---------------------------------------------------------------------------

async def telegram_send(chat_id: int, text: str, bot_token: str, reply_markup: dict | None = None):
    """Send a message back to a Telegram chat."""
    async with httpx.AsyncClient(timeout=30) as client:
        if reply_markup is not None:
            # reply_markup is only supported on a single message payload.
            chunk = (text or "")[:4096]
            resp = await client.post(
                f"{_telegram_api(bot_token)}/sendMessage",
                json={
                    "chat_id": chat_id,
                    "text": chunk,
                    "parse_mode": "Markdown",
                    "reply_markup": reply_markup,
                },
            )
            if resp.status_code >= 400:
                fallback = await client.post(
                    f"{_telegram_api(bot_token)}/sendMessage",
                    json={"chat_id": chat_id, "text": chunk, "reply_markup": reply_markup},
                )
                if fallback.status_code >= 400:
                    log.error(
                        "Telegram send failed (chat=%s status=%s body=%s)",
                        chat_id,
                        fallback.status_code,
                        (fallback.text or "")[:200],
                    )
            return

        # Telegram limits messages to 4096 chars; split if needed.
        chunks = [text[i : i + 4096] for i in range(0, len(text), 4096)]
        for chunk in chunks:
            resp = await client.post(
                f"{_telegram_api(bot_token)}/sendMessage",
                json={"chat_id": chat_id, "text": chunk, "parse_mode": "Markdown"},
            )
            # Some content (symbols/underscores/etc.) can fail Markdown parsing.
            # Retry plain text so users still get a response instead of silence.
            if resp.status_code >= 400:
                fallback = await client.post(
                    f"{_telegram_api(bot_token)}/sendMessage",
                    json={"chat_id": chat_id, "text": chunk},
                )
                if fallback.status_code >= 400:
                    log.error(
                        "Telegram send failed (chat=%s status=%s body=%s)",
                        chat_id,
                        fallback.status_code,
                        (fallback.text or "")[:200],
                    )


async def telegram_answer_callback(callback_query_id: str, bot_token: str, text: str = ""):
    async with httpx.AsyncClient(timeout=10) as client:
        await client.post(
            f"{_telegram_api(bot_token)}/answerCallbackQuery",
            json={
                "callback_query_id": callback_query_id,
                "text": text[:180] if text else "",
                "show_alert": False,
            },
        )


async def telegram_send_action(chat_id: int, bot_token: str, action: str = "typing"):
    """Show 'typing...' indicator in Telegram while the LLM thinks."""
    async with httpx.AsyncClient(timeout=10) as client:
        await client.post(
            f"{_telegram_api(bot_token)}/sendChatAction",
            json={"chat_id": chat_id, "action": action},
        )


def _is_chat_allowed(chat_id: int, allowed_chats_csv: str, require_allowed_chats: bool) -> bool:
    """Check whether a chat ID is in the allow-list (if one is set)."""
    if not allowed_chats_csv:
        if require_allowed_chats:
            return False
        return True  # no allow-list → open to all
    allowed = {s.strip() for s in allowed_chats_csv.split(",") if s.strip()}
    return str(chat_id) in allowed


_SYSTEM_MSG_PATTERNS = re.compile(
    r"HEARTBEAT|heartbeat|"
    r"scheduled reminder has been triggered|"
    r"conversation_label|"
    r"Read HEARTBEAT\.md|"
    r"HEARTBEAT_OK|"
    r"untrusted metadata|"
    r"OpenClaw gateway status",
    re.IGNORECASE,
)

_telegram_lock = asyncio.Lock()
_webhook_rate_lock = asyncio.Lock()
_webhook_rate_state: dict[str, tuple[float, float]] = {}
_telegram_update_dedup_lock = asyncio.Lock()
_telegram_update_seen: dict[int, float] = {}


def _is_system_message(text: str) -> bool:
    """Detect automated system/heartbeat/reminder messages that shouldn't reach the LLM."""
    return bool(_SYSTEM_MSG_PATTERNS.search(text))


def _client_ip_for_rate_limit(request: Request) -> str:
    if WEBHOOK_RATE_LIMIT_TRUST_XFF:
        xff = (request.headers.get("x-forwarded-for") or "").strip()
        if xff:
            return xff.split(",")[0].strip()
    if request.client and request.client.host:
        return request.client.host
    return "unknown"


async def _enforce_webhook_rate_limit(request: Request):
    if not WEBHOOK_RATE_LIMIT_ENABLED:
        return
    ip = _client_ip_for_rate_limit(request)
    now = time.monotonic()
    async with _webhook_rate_lock:
        tokens, last = _webhook_rate_state.get(ip, (WEBHOOK_RATE_LIMIT_BURST, now))
        elapsed = max(0.0, now - last)
        tokens = min(WEBHOOK_RATE_LIMIT_BURST, tokens + (elapsed * WEBHOOK_RATE_LIMIT_RPS))
        if tokens < 1.0:
            _webhook_rate_state[ip] = (tokens, now)
            raise HTTPException(status_code=429, detail="Too Many Requests")
        _webhook_rate_state[ip] = (tokens - 1.0, now)
        # Keep memory bounded under noisy traffic patterns.
        if len(_webhook_rate_state) > 5000:
            cutoff = now - 3600.0
            stale = [k for k, (_, ts) in _webhook_rate_state.items() if ts < cutoff]
            for key in stale[:2000]:
                _webhook_rate_state.pop(key, None)


async def _mark_update_seen(update_id: int | None) -> bool:
    """Return True if this update should be processed, False if duplicate."""
    if not TELEGRAM_UPDATE_DEDUP_ENABLED or update_id is None:
        return True

    now = time.monotonic()
    cutoff = now - float(TELEGRAM_UPDATE_DEDUP_WINDOW_SECONDS)
    async with _telegram_update_dedup_lock:
        stale = [k for k, ts in _telegram_update_seen.items() if ts < cutoff]
        for k in stale[:5000]:
            _telegram_update_seen.pop(k, None)
        if update_id in _telegram_update_seen:
            return False
        _telegram_update_seen[update_id] = now
        return True


async def _handle_telegram_message(chat_id: int, text: str, user_name: str, bot_token: str):
    """Process a Telegram message in the background (called via asyncio.create_task)."""
    if _is_system_message(text):
        log.info("Ignoring system/heartbeat message from chat %s: %s", chat_id, _safe_snippet(text))
        return

    async with _telegram_lock:
        session_id = _telegram_session_id(chat_id)

    if session_id not in conversations:
        conversations[session_id] = []

    # Show typing indicator
    try:
        await telegram_send_action(chat_id, bot_token=bot_token)
    except httpx.HTTPError:
        pass  # non-critical

    # Store user message in local history (also persisted).
    _append_conversation_entry(
        session_id=session_id,
        role="user",
        content=text,
        source="telegram",
    )

    # Search trade show DB for relevant context
    ts_ctx = await asyncio.to_thread(_build_trade_show_context, text)

    # Search agent-memory (Lean RAG) for fact-based context
    memory_ctx = ""
    try:
        memories = await memory_search(text, limit=5)
        if memories:
            memory_ctx = _format_memory_items(memories, "[Relevant memories/facts]")
    except Exception as exc:
        log.warning("Memory search failed during chat: %s", exc)

    response_style_ctx = (
        "[Response style]\n"
        "Keep this Telegram reply concise and mobile-friendly. "
        "Prefer short paragraphs or bullets and avoid large code blocks unless explicitly requested."
        if _is_brief_mode(chat_id)
        else "[Response style]\n"
        "Verbose mode enabled. You can provide deeper detail when useful."
    )

    # Build prompt and call LLM
    try:
        # Combine contexts
        full_extra_ctx = (ts_ctx or "").strip()
        if memory_ctx:
            full_extra_ctx += f"\n\n{memory_ctx}"

        messages = build_messages(session_id, trade_show_ctx=full_extra_ctx, response_style_ctx=response_style_ctx)
        response = await call_llm(messages)
    except HTTPException as exc:
        await telegram_send(chat_id, f"Error from LLM: {exc.detail}", bot_token=bot_token)
        return
    except Exception as exc:
        log.error("LLM error for chat %s: %s", chat_id, exc)
        await telegram_send(chat_id, "Sorry, something went wrong with the LLM.", bot_token=bot_token)
        return

    # Store assistant response (also persisted).
    _append_conversation_entry(
        session_id=session_id,
        role="assistant",
        content=response,
        source="telegram",
    )

    # Persist to Essencem (non-blocking, non-critical)
    await store_memory(session_id, "user", text)
    await store_memory(session_id, "assistant", response)

    # Notify n8n
    await notify_n8n("telegram_message", {
        "chat_id": chat_id,
        "user_text": text,
        "assistant_response": response,
        "user_name": user_name
    })

    # Send reply back to Telegram
    try:
        await telegram_send(chat_id, response + _brief_footer(chat_id), bot_token=bot_token)
    except httpx.HTTPError as exc:
        log.error("Failed to send Telegram reply: %s", exc)


async def _handle_telegram_photo(chat_id: int, photo_file_id: str, caption: str, user_name: str, bot_token: str):
    """Process a Telegram photo message in the background.

    If Gemini is configured, tries the trade-show extraction pipeline first.
    Falls back to local vision LLM for general photos.
    """
    session_id = _telegram_session_id(chat_id)

    if session_id not in conversations:
        conversations[session_id] = []

    try:
        await telegram_send_action(chat_id, bot_token=bot_token)
    except httpx.HTTPError:
        pass

    user_desc = f"[sent a photo] {caption}" if caption else "[sent a photo]"
    _append_conversation_entry(
        session_id=session_id,
        role="user",
        content=user_desc,
        source="telegram",
    )

    try:
        image_b64 = await telegram_download_photo(photo_file_id, bot_token=bot_token)
    except Exception as exc:
        log.error("Failed to download photo from chat %s: %s", chat_id, exc)
        await telegram_send(chat_id, "Sorry, I couldn't download that image.", bot_token=bot_token)
        return

    # Try trade-show extraction pipeline (Gemini) first
    if trade_show.GEMINI_API_KEY or trade_show.OPENROUTER_API_KEY:
        try:
            ts_name = ""
            ts_date = ""
            if caption:
                low = caption.lower()
                if low.startswith("show:") or low.startswith("tradeshow:"):
                    ts_name = caption.split(":", 1)[1].strip()
                    ts_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

            results = await trade_show.process_photo(
                image_b64, trade_show=ts_name, trade_show_date=ts_date, my_location="",
            )
            if results:
                response = trade_show.format_results_summary(results)
                _append_conversation_entry(
                    session_id=session_id,
                    role="assistant",
                    content=response,
                    source="telegram",
                )
                await store_memory(session_id, "user", user_desc)
                await store_memory(session_id, "assistant", response)
                
                # Notify n8n
                await notify_n8n("telegram_photo_tradeshow", {
                    "chat_id": chat_id,
                    "caption": caption,
                    "extraction": results
                })

                await telegram_send(chat_id, response, bot_token=bot_token)
                return
        except Exception as exc:
            log.warning("Trade show pipeline failed, falling back to vision LLM: %s", exc)

    # Fallback: local vision LLM
    prompt = caption if caption else "What's in this image? If it's a business card, extract all contact info. If it's a flyer or promo, extract all details."

    try:
        response = await call_vision_llm(image_b64, prompt)
    except HTTPException as exc:
        await telegram_send(chat_id, f"Error from vision model: {exc.detail}", bot_token=bot_token)
        return
    except Exception as exc:
        log.error("Vision LLM error for chat %s: %s", chat_id, exc)
        await telegram_send(chat_id, "Sorry, something went wrong analyzing that image.", bot_token=bot_token)
        return

    _append_conversation_entry(
        session_id=session_id,
        role="assistant",
        content=response,
        source="telegram",
    )

    await store_memory(session_id, "user", user_desc)
    await store_memory(session_id, "assistant", response)

    try:
        await telegram_send(chat_id, response, bot_token=bot_token)
    except httpx.HTTPError as exc:
        log.error("Failed to send Telegram reply: %s", exc)


async def _handle_ugc_command(chat_id: int, text: str, bot_token: str):
    """Handle /ugc subcommands in a background task."""
    parts = text.strip().split()
    subcommand = parts[1].lower() if len(parts) > 1 else "help"

    if not ugc_module.IMMICH_API_KEY:
        await telegram_send(chat_id, "UGC not configured: IMMICH_API_KEY missing", bot_token=bot_token)
        return
    if not ugc_module.OPENROUTER_API_KEY and subcommand not in ("stats", "ready", "help"):
        await telegram_send(chat_id, "UGC not configured: OPENROUTER_API_KEY missing", bot_token=bot_token)
        return

    immich = ugc_module.ImmichClient()

    async def notify(msg: str):
        await telegram_send(chat_id, msg, bot_token=bot_token)

    try:
        if subcommand == "scan":
            await telegram_send(chat_id, "Starting full UGC scan...", bot_token=bot_token)
            results = await ugc_module.scan_all(immich, notify_fn=notify)
            summary = (
                f"UGC scan complete:\n"
                f"  Scored: {results['scored']}\n"
                f"  Failed: {results['failed']}\n"
                f"  Content Ready: {results.get('content_ready', 0)}\n"
                f"  Needs Editing: {results.get('needs_editing', 0)}\n"
                f"  Outreach: {results.get('outreach', 0)}\n"
                f"  Archive: {results.get('archive', 0)}\n"
                f"  Prompt tokens: {results.get('prompt_tokens', 0)}\n"
                f"  Completion tokens: {results.get('completion_tokens', 0)}\n"
                f"  Estimated cost (USD): ${results.get('estimated_cost_usd', 0.0):.6f}"
            )
            await telegram_send(chat_id, summary, bot_token=bot_token)

        elif subcommand == "new":
            await telegram_send(chat_id, "Scanning new uploads...", bot_token=bot_token)
            results = await ugc_module.scan_new(immich, notify_fn=notify)
            if results.get("scored", 0) == 0:
                await telegram_send(chat_id, results.get("message", "No new photos to score"), bot_token=bot_token)
            else:
                summary = (
                    f"New scan complete:\n"
                    f"  Scored: {results['scored']}\n"
                    f"  Content Ready: {results.get('content_ready', 0)}\n"
                    f"  Needs Editing: {results.get('needs_editing', 0)}\n"
                    f"  Prompt tokens: {results.get('prompt_tokens', 0)}\n"
                    f"  Completion tokens: {results.get('completion_tokens', 0)}\n"
                    f"  Estimated cost (USD): ${results.get('estimated_cost_usd', 0.0):.6f}"
                )
                await telegram_send(chat_id, summary, bot_token=bot_token)

        elif subcommand == "ready":
            limit = 5
            if len(parts) > 2:
                try:
                    limit = max(1, min(100, int(parts[2])))
                except ValueError:
                    await telegram_send(chat_id, "Usage: /ugc ready [1-100]", bot_token=bot_token)
                    return
            assets = await ugc_module.get_ready_assets(immich, limit=limit)
            if not assets:
                await telegram_send(chat_id, "No content-ready photos yet. Run /ugc scan first.", bot_token=bot_token)
            else:
                lines = [f"Content Ready ({len(assets)} shown):"]
                for a in assets:
                    name = a.get("originalFileName", a.get("id", "?")[:8])
                    created = a.get("createdAt", "")[:10]
                    lines.append(f"  - {name} ({created})")
                lines.append(f"\nBrowse in Immich: {ugc_module.IMMICH_URL}")
                await telegram_send(chat_id, "\n".join(lines), bot_token=bot_token)

        elif subcommand == "stats":
            stats = await ugc_module.get_stats(immich)
            lines = [
                "UGC Stats:",
                f"  Total images: {stats['total_assets']}",
                f"  Last scan: {stats['last_scan']}",
            ]
            for name, count in stats.get("albums", {}).items():
                lines.append(f"  {name}: {count}")
            if stats.get("last_results"):
                lr = stats["last_results"]
                lines.append(f"  Last run scored: {lr.get('scored', 0)}")
                lines.append(f"  Last run prompt tokens: {lr.get('prompt_tokens', 0)}")
                lines.append(f"  Last run completion tokens: {lr.get('completion_tokens', 0)}")
                lines.append(f"  Last run est. cost: ${lr.get('estimated_cost_usd', 0.0):.6f}")
            await telegram_send(chat_id, "\n".join(lines), bot_token=bot_token)

        else:
            await telegram_send(
                chat_id,
                "UGC commands:\n"
                "  /ugc scan - Score all unscored photos\n"
                "  /ugc new - Score only new uploads\n"
                "  /ugc ready [N] - Show content-ready photos (default 5, max 100)\n"
                "  /ugc stats - Scoring statistics",
                bot_token=bot_token,
            )
    except Exception as e:
        log.error("UGC command error: %s", e)
        await telegram_send(chat_id, f"UGC error: {e}", bot_token=bot_token)


async def _handle_tradeshow_command(chat_id: int, text: str, bot_token: str):
    """Handle /contacts, /promos, /tradeshow commands."""
    try:
        parts = text.split(maxsplit=1)
        cmd = parts[0].lower()
        query = parts[1].strip() if len(parts) > 1 else ""

        if cmd == "/contacts":
            if not query:
                results = await asyncio.to_thread(trade_show.search_contacts, "", 15)
                if not results:
                    stats = await asyncio.to_thread(trade_show.get_stats)
                    await telegram_send(
                        chat_id,
                        f"Supplier contacts: {stats['total_contacts']}\n"
                        f"Trade shows: {', '.join(stats['trade_shows']) or 'none yet'}\n\n"
                        "Search: `/contacts <destination or company>`",
                        bot_token=bot_token,
                    )
                    return
                contact_search_cache[chat_id] = results
                lines = [f"Recent contacts ({len(results)}):"]
                for idx, c in enumerate(results, start=1):
                    lines.append(_format_contact_picker_line(idx, c))
                lines.append("")
                lines.append("Call from this list:")
                lines.append("  /callpick <#> | Your message")
                lines.append("Or direct:")
                lines.append("  /call Name @ Company | Your message")
                await telegram_send(
                    chat_id,
                    "\n".join(lines),
                    bot_token=bot_token,
                    reply_markup=_build_contact_picker_markup(results),
                )
                return
            results = await asyncio.to_thread(trade_show.search_contacts, query, 15)
            if not results:
                await telegram_send(chat_id, f"No contacts found for \"{query}\"", bot_token=bot_token)
                return
            contact_search_cache[chat_id] = results
            lines = [f"Contacts matching \"{query}\" ({len(results)}):"]
            for idx, c in enumerate(results, start=1):
                lines.append(_format_contact_picker_line(idx, c))
            lines.append("")
            lines.append("Call from this list:")
            lines.append("  /callpick <#> | Your message")
            await telegram_send(
                chat_id,
                "\n".join(lines),
                bot_token=bot_token,
                reply_markup=_build_contact_picker_markup(results),
            )

        elif cmd == "/promos":
            if not query:
                stats = await asyncio.to_thread(trade_show.get_stats)
                expiring = await asyncio.to_thread(trade_show.get_expiring_promos, 14)
                msg = f"Active promos: {stats['active_promos']} (total: {stats['total_promos']})\n"
                if expiring:
                    msg += f"\nExpiring within 2 weeks ({len(expiring)}):\n"
                    for p in expiring[:5]:
                        msg += f"  • {p.get('promo_name', 'Untitled')} — {p['supplier_name']} (expires {p['end_date']})\n"
                msg += "\nSearch: `/promos <destination or supplier>`"
                await telegram_send(chat_id, msg, bot_token=bot_token)
                return
            results = await asyncio.to_thread(trade_show.search_promos, query)
            if not results:
                await telegram_send(chat_id, f"No promos found for \"{query}\"", bot_token=bot_token)
                return
            lines = [f"Promos matching \"{query}\" ({len(results)}):"]
            for p in results:
                lines.append("")
                lines.append(trade_show.format_promo(p))
            await telegram_send(chat_id, "\n".join(lines), bot_token=bot_token)

        elif cmd == "/tradeshow":
            if not query or query == "help":
                await telegram_send(
                    chat_id,
                    "Trade Show Intel:\n"
                    "  Send a photo of a business card or flyer to scan it.\n"
                    "  Add caption `show:CruiseWorld 2026` to tag the show.\n\n"
                    "  /contacts — list or search supplier contacts\n"
                    "  /contacts China — find contacts for a destination\n"
                    "  /promos — list or search active promos\n"
                    "  /promos Caribbean — find promos for a destination\n"
                    "  /tradeshow stats — database stats",
                    bot_token=bot_token,
                )
                return
            if query == "stats":
                stats = await asyncio.to_thread(trade_show.get_stats)
                await telegram_send(
                    chat_id,
                    f"Trade Show DB:\n"
                    f"  Contacts: {stats['total_contacts']}\n"
                    f"  Promos: {stats['total_promos']} ({stats['active_promos']} active)\n"
                    f"  Shows: {', '.join(stats['trade_shows']) or 'none yet'}",
                    bot_token=bot_token,
                )
                return

    except Exception as e:
        log.error("Trade show command error: %s", e)
        await telegram_send(chat_id, f"Trade show error: {e}", bot_token=bot_token)


def _build_about_report() -> str:
    twilio_ready = bool(TWILIO_ACCOUNT_SID and TWILIO_AUTH_TOKEN and TWILIO_PHONE_NUMBER)
    agent_memory_ready = bool(AGENT_MEMORY_URL)
    lines = [
        "Myoshee Runtime:",
        "  Core: Openbot (Kondo stack)",
        f"  Model: {OPENROUTER_MODEL}",
        f"  Vision: {OPENROUTER_VISION_MODEL}",
        f"  Web UI: {'on' if ENABLE_WEB_UI else 'off'}",
        f"  Telegram: {'on' if bool(TELEGRAM_MYOSHE_BOT_TOKEN) else 'off'} (@Myoshee_bot)",
        f"  Twilio voice: {'on' if twilio_ready else 'off'}",
        f"  Memory (Essencem): {'on' if bool(ESSENCEM_URL) else 'off'}",
        f"  Agent memory API: {'on' if agent_memory_ready else 'off'}",
        f"  Trade-show DB: {trade_show.DB_PATH}",
    ]
    if OPENBOT_PUBLIC_BASE_URL:
        lines.append(f"  Public URL: {OPENBOT_PUBLIC_BASE_URL}")
    lines.append("")
    lines.append("Try:")
    lines.append("  /help")
    lines.append("  /contacts")
    lines.append("  /remember")
    lines.append("  /recall")
    lines.append("  /memories")
    lines.append("  /brief")
    lines.append("  /verbose")
    lines.append("  /finance")
    lines.append("  /triage")
    lines.append("  /diag")
    return "\n".join(lines)


async def _handle_citadelle_command(chat_id: int, text: str, bot_token: str):
    cmd = text.strip().split(maxsplit=1)[0].lower()
    if cmd in ("/start", "/help"):
        await telegram_send(
            chat_id,
            "Citadelle admin commands:\n"
            "  /health - runtime health\n"
            "  /status - model + warning summary\n"
            "  /clear - clear shared session memory",
            bot_token=bot_token,
        )
        return
    if cmd == "/clear":
        conversations[SHARED_SESSION_ID] = []
        session_store.clear_session(SHARED_SESSION_ID)
        await telegram_send(chat_id, "Shared session cleared.", bot_token=bot_token)
        return
    if cmd in ("/health", "/status"):
        h = health()
        await telegram_send(
            chat_id,
            f"OpenBot status:\n"
            f"  status: {h['status']}\n"
            f"  bot_tier: {h['bot_tier']}\n"
            f"  model: {h['model']}\n"
            f"  vision_model: {h['vision_model']}\n"
            f"  warnings_count: {h['warnings_count']}",
            bot_token=bot_token,
        )
        return
    await telegram_send(
        chat_id,
        "Citadelle is admin-only. Use /help for supported commands.",
        bot_token=bot_token,
    )


async def _telegram_webhook_common(
    request: Request,
    *,
    bot_name: str,
    bot_token: str,
    allowed_chats_csv: str,
    require_allowed_chats: bool,
    admin_only: bool,
):
    if not bot_token:
        if bot_name == "citadelle":
            # Keep endpoint quiet when Citadelle bot is not configured.
            return {"ok": True, "disabled": True}
        raise HTTPException(status_code=503, detail=f"{bot_name} bot token not configured")
    if require_allowed_chats and not allowed_chats_csv:
        raise HTTPException(status_code=503, detail=f"{bot_name} allow-list required but empty")

    if REQUIRE_HTTPS_WEBHOOK:
        proto = (request.headers.get("x-forwarded-proto") or request.url.scheme or "").lower()
        host = (
            request.headers.get("x-forwarded-host")
            or request.headers.get("host")
            or request.url.hostname
            or ""
        ).split(":")[0].lower()
        if proto != "https":
            raise HTTPException(status_code=403, detail="Webhook requires HTTPS")
        if TELEGRAM_WEBHOOK_HOST and host != TELEGRAM_WEBHOOK_HOST:
            raise HTTPException(status_code=403, detail="Webhook host mismatch")
    await _enforce_webhook_rate_limit(request)

    body = await request.json()
    if isinstance(body, dict) and body.get("callback_query"):
        await _handle_callback_query(
            body,
            bot_token=bot_token,
            allowed_chats_csv=allowed_chats_csv,
            require_allowed_chats=require_allowed_chats,
        )
        return {"ok": True}
    update_id = body.get("update_id") if isinstance(body, dict) else None
    if isinstance(update_id, int):
        if not await _mark_update_seen(update_id):
            # Telegram retries are common; dedupe to avoid duplicate replies/token burn.
            return {"ok": True, "duplicate": True}
    message = body.get("message") or body.get("edited_message")
    if not message:
        return {"ok": True}

    chat_id: int = message["chat"]["id"]
    user_name: str = (message.get("from") or {}).get("first_name", "user")
    if not _is_chat_allowed(chat_id, allowed_chats_csv=allowed_chats_csv, require_allowed_chats=require_allowed_chats):
        log.warning("%s message from disallowed chat %s (%s)", bot_name, chat_id, sanitize_text(user_name))
        return {"ok": True}

    if "photo" in message or ("document" in message and str((message["document"]).get("mime_type", "")).startswith("image/")):
        if admin_only:
            await telegram_send(chat_id, "Citadelle does not process media. Use Myoshe for UGC/trade-show workflows.", bot_token=bot_token)
            return {"ok": True}
        photo_file_id = message["photo"][-1]["file_id"] if "photo" in message else message["document"]["file_id"]
        caption = message.get("caption", "")
        asyncio.create_task(_handle_telegram_photo(chat_id, photo_file_id, caption, user_name, bot_token=bot_token))
        return {"ok": True}

    if "document" in message:
        if admin_only:
            await telegram_send(chat_id, "Citadelle does not process files. Use Myoshe for knowledge ingestion.", bot_token=bot_token)
            return {"ok": True}
        caption = message.get("caption", "")
        asyncio.create_task(_handle_telegram_document(chat_id, message["document"], caption, user_name, bot_token=bot_token))
        return {"ok": True}

    if "text" not in message:
        return {"ok": True}

    text: str = message["text"]
    if admin_only:
        asyncio.create_task(_handle_citadelle_command(chat_id, text, bot_token=bot_token))
        return {"ok": True}

    if text.strip() == "/start":
        await telegram_send(
            chat_id,
            "Hey! I'm Myoshee (Leann). Send me a message and I'll reply.\nUse /help for command shortcuts.",
            bot_token=bot_token,
        )
        return {"ok": True}
    if text.strip() == "/help":
        await telegram_send(chat_id, MYOSHE_HELP_TEXT, bot_token=bot_token)
        return {"ok": True}
    if text.strip().startswith("/brief"):
        parts = text.strip().split(maxsplit=1)
        if len(parts) == 1:
            status = "on" if _is_brief_mode(chat_id) else "off"
            await telegram_send(chat_id, f"Brief mode is currently {status}. Use `/brief on` or `/brief off`.", bot_token=bot_token)
            return {"ok": True}
        val = parts[1].strip().lower()
        if val not in ("on", "off"):
            await telegram_send(chat_id, "Usage: /brief on|off", bot_token=bot_token)
            return {"ok": True}
        _set_brief_mode(chat_id, val == "on")
        await telegram_send(chat_id, f"Brief mode set to {val}.", bot_token=bot_token)
        return {"ok": True}
    if text.strip().startswith("/verbose"):
        parts = text.strip().split(maxsplit=1)
        if len(parts) == 1:
            status = "off" if _is_brief_mode(chat_id) else "on"
            await telegram_send(chat_id, f"Verbose mode is currently {status}. Use `/verbose on` or `/verbose off`.", bot_token=bot_token)
            return {"ok": True}
        val = parts[1].strip().lower()
        if val not in ("on", "off"):
            await telegram_send(chat_id, "Usage: /verbose on|off", bot_token=bot_token)
            return {"ok": True}
        _set_brief_mode(chat_id, val != "on")
        await telegram_send(chat_id, f"Verbose mode set to {val}.", bot_token=bot_token)
        return {"ok": True}
    if text.strip() == "/about":
        await telegram_send(chat_id, _build_about_report(), bot_token=bot_token)
        return {"ok": True}
    if text.strip() == "/diag":
        await telegram_send(chat_id, await _build_diag_report(), bot_token=bot_token)
        return {"ok": True}
    if text.strip() == "/memorydiag":
        await telegram_send(chat_id, await _build_memory_diag_report(), bot_token=bot_token)
        return {"ok": True}
    if text.strip().startswith("/remember"):
        payload = text.strip()[len("/remember"):].strip()
        if not payload:
            await telegram_send(chat_id, "Usage: /remember <text>", bot_token=bot_token)
            return {"ok": True}
        try:
            saved = await memory_save(
                category="fact",
                content=payload,
                tags=["telegram", "manual"],
                source=f"telegram:{chat_id}",
            )
            mem_id = saved.get("id")
            msg = f"Saved to memory{f' (#{mem_id})' if mem_id is not None else ''}."
            await telegram_send(chat_id, msg, bot_token=bot_token)
        except Exception as exc:
            await telegram_send(chat_id, f"Memory save unavailable: {exc}", bot_token=bot_token)
        return {"ok": True}
    if text.strip().startswith("/recall"):
        payload = text.strip()[len("/recall"):].strip()
        if not payload:
            await telegram_send(chat_id, "Usage: /recall <query>", bot_token=bot_token)
            return {"ok": True}
        try:
            items = await memory_search(payload, limit=6)
            await telegram_send(
                chat_id,
                _format_memory_items(items, f"Memory results for \"{payload}\":"),
                bot_token=bot_token,
            )
        except Exception as exc:
            await telegram_send(chat_id, f"Memory recall unavailable: {exc}", bot_token=bot_token)
        return {"ok": True}
    if text.strip().startswith("/memories"):
        payload = text.strip()[len("/memories"):].strip()
        limit = 8
        if payload and payload.isdigit():
            limit = max(1, min(int(payload), 20))
        try:
            items = await memory_recent(limit=limit)
            await telegram_send(chat_id, _format_memory_items(items, f"Recent memories ({limit}):"), bot_token=bot_token)
        except Exception as exc:
            await telegram_send(chat_id, f"Memory list unavailable: {exc}", bot_token=bot_token)
        return {"ok": True}
    if text.strip().startswith("/triage"):
        asyncio.create_task(_handle_triage_command(chat_id, text.strip(), bot_token=bot_token))
        return {"ok": True}
    if text.strip().startswith("/finance") or text.strip().startswith("/budget") or text.strip().startswith("/debt") or text.strip().startswith("/savings") or text.strip().startswith("/credit") or text.strip().startswith("/stocks") or text.strip().startswith("/crypto"):
        asyncio.create_task(_handle_finance_command(chat_id, text.strip(), user_name, bot_token=bot_token))
        return {"ok": True}
    if text.strip() == "/calls":
        await telegram_send(chat_id, _render_recent_calls(limit=8), bot_token=bot_token)
        return {"ok": True}
    if text.strip().startswith("/callstatus"):
        parts = text.strip().split(maxsplit=1)
        if len(parts) < 2:
            await telegram_send(chat_id, "Usage: /callstatus <CallSid>", bot_token=bot_token)
            return {"ok": True}
        sid = parts[1].strip()
        try:
            call = await _twilio_fetch_call_status(sid)
            status = (call.get("status") or "unknown").strip()
            to_number = (call.get("to") or "").strip()
            from_number = (call.get("from") or "").strip()
            direction = (call.get("direction") or "").strip()
            duration = (call.get("duration") or "0").strip()
            await telegram_send(
                chat_id,
                f"Call {sid}:\n"
                f"  status: {status}\n"
                f"  direction: {direction}\n"
                f"  from: {from_number}\n"
                f"  to: {to_number}\n"
                f"  duration: {duration}s",
                bot_token=bot_token,
            )
            _upsert_call_runtime_record(
                call_sid=sid,
                status=status,
                caller=to_number or from_number,
                direction=direction or "outbound-api",
                duration=int(duration or 0),
            )
        except Exception as exc:
            await telegram_send(chat_id, f"Call status lookup failed: {exc}", bot_token=bot_token)
        return {"ok": True}
    if text.strip() == "/cancelcall":
        if pending_call_drafts.pop(chat_id, None):
            await telegram_send(chat_id, "Canceled call draft.", bot_token=bot_token)
        else:
            await telegram_send(chat_id, "No active call draft.", bot_token=bot_token)
        return {"ok": True}
    if text.strip() == "/clear":
        session_id = _telegram_session_id(chat_id)
        conversations[session_id] = []
        session_store.clear_session(session_id)
        await telegram_send(chat_id, "Conversation cleared.", bot_token=bot_token)
        return {"ok": True}
    if text.strip().startswith("/ugc"):
        asyncio.create_task(_handle_ugc_command(chat_id, text.strip(), bot_token=bot_token))
        return {"ok": True}
    if text.strip().startswith("/contacts") or text.strip().startswith("/promos") or text.strip().startswith("/tradeshow"):
        asyncio.create_task(_handle_tradeshow_command(chat_id, text.strip(), bot_token=bot_token))
        return {"ok": True}
    if text.strip().startswith("/addcontact"):
        asyncio.create_task(_handle_addcontact_command(chat_id, text.strip(), bot_token=bot_token))
        return {"ok": True}
    if text.strip().startswith("/callpick"):
        asyncio.create_task(_handle_callpick_command(chat_id, text.strip(), bot_token=bot_token))
        return {"ok": True}
    if text.strip().lower().startswith("call/") or text.strip().lower().startswith("/call"):
        asyncio.create_task(_handle_call_command(chat_id, text.strip(), bot_token=bot_token))
        return {"ok": True}
    natural_number, natural_message = _parse_natural_call_request(text)
    if natural_number:
        synthetic = f"/call {natural_number} {natural_message}"
        asyncio.create_task(_handle_call_command(chat_id, synthetic, bot_token=bot_token))
        return {"ok": True}
    natural_target, named_message = _parse_natural_named_call_request(text)
    if natural_target:
        synthetic = f"/call {natural_target} | {named_message}"
        asyncio.create_task(_handle_call_command(chat_id, synthetic, bot_token=bot_token))
        return {"ok": True}

    memory_action, memory_payload = _parse_memory_intent(text)
    if memory_action == "save":
        try:
            saved = await memory_save(
                category="fact",
                content=memory_payload,
                tags=["telegram", "auto"],
                source=f"telegram:{chat_id}",
            )
            mem_id = saved.get("id")
            await telegram_send(
                chat_id,
                f"Saved to memory{f' (#{mem_id})' if mem_id is not None else ''}.",
                bot_token=bot_token,
            )
        except Exception as exc:
            await telegram_send(chat_id, f"Memory save unavailable: {exc}", bot_token=bot_token)
        return {"ok": True}
    if memory_action == "recall":
        try:
            items = await memory_search(memory_payload, limit=6)
            await telegram_send(
                chat_id,
                _format_memory_items(items, f"Memory results for \"{memory_payload}\":"),
                bot_token=bot_token,
            )
        except Exception as exc:
            await telegram_send(chat_id, f"Memory recall unavailable: {exc}", bot_token=bot_token)
    return {"ok": True}

    draft = pending_call_drafts.pop(chat_id, None)
    if draft:
        message_text = text.strip()
        if not message_text:
            await telegram_send(chat_id, "Please send the call message text or `/cancelcall`.", bot_token=bot_token)
            return {"ok": True}
        asyncio.create_task(
            _place_and_notify_outbound_call(
                chat_id=chat_id,
                bot_token=bot_token,
                to_number=draft["to_number"],
                message=message_text,
                location_hint=draft.get("location_hint", ""),
            )
        )
        return {"ok": True}

    triage_payload = _parse_triage_intent(text)
    if triage_payload:
        synthetic = f"/triage {triage_payload}"
        asyncio.create_task(_handle_triage_command(chat_id, synthetic, bot_token=bot_token))
        return {"ok": True}

    asyncio.create_task(_handle_telegram_message(chat_id, text, user_name, bot_token=bot_token))
    return {"ok": True}


@app.post("/telegram-webhook")
@app.post("/telegram-webhook/myoshe")
async def telegram_webhook_myoshe(request: Request):
    return await _telegram_webhook_common(
        request,
        bot_name="myoshe",
        bot_token=TELEGRAM_MYOSHE_BOT_TOKEN,
        allowed_chats_csv=TELEGRAM_MYOSHE_ALLOWED_CHATS,
        require_allowed_chats=REQUIRE_MYOSHE_ALLOWED_CHATS,
        admin_only=False,
    )


@app.post("/telegram-webhook/citadelle")
async def telegram_webhook_citadelle(request: Request):
    return await _telegram_webhook_common(
        request,
        bot_name="citadelle",
        bot_token=TELEGRAM_CITADELLE_BOT_TOKEN,
        allowed_chats_csv=TELEGRAM_CITADELLE_ALLOWED_CHATS,
        require_allowed_chats=REQUIRE_CITADELLE_ALLOWED_CHATS,
        admin_only=True,
    )


@app.get("/sessions/{session_id}/history")
async def get_history(session_id: str):
    """Get conversation history for a session."""
    history = conversations.get(session_id)
    if history is None:
        history = session_store.get_session_messages(session_id, limit=500)
    return {"session_id": session_id, "messages": history}


@app.delete("/sessions/{session_id}")
async def clear_session(session_id: str):
    """Clear a conversation session."""
    if session_id == SHARED_SESSION_ID:
        conversations[session_id] = []
        session_store.clear_session(session_id)
        return {"status": "cleared", "session_id": session_id}
    raise HTTPException(status_code=404, detail="Session not found")


@app.get("/court", response_class=HTMLResponse)
async def court_dashboard():
    """Serve the NBA-styled Asphalt Live dashboard."""
    with open("templates/court_dashboard.html", "r") as f:
        return f.read()


@app.get("/", response_class=HTMLResponse)
async def web_ui():
    """Web UI with text and image chat support."""
    if not ENABLE_WEB_UI:
        raise HTTPException(status_code=404, detail="Web UI is disabled")
    return """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Openbot Chat</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: #1a1a1a;
            color: #e0e0e0;
            block-size: 100vh;
            display: flex;
            flex-direction: column;
        }
        header {
            background: #252525;
            padding: 1rem;
            border-block-end: 1px solid #333;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }
        header h1 { font-size: 1.5rem; color: #fff; }
        .header-links { display: flex; gap: 0.75rem; }
        .header-links a {
            color: #58a6ff;
            text-decoration: none;
            padding: 0.5rem 1rem;
            border: 1px solid #58a6ff;
            border-radius: 6px;
            transition: all 0.2s;
            font-size: 0.9rem;
        }
        .header-links a:hover { background: #58a6ff; color: #fff; }
        #chat-container {
            flex: 1;
            overflow-y: auto;
            padding: 2rem;
            display: flex;
            flex-direction: column;
            gap: 1rem;
        }
        .message {
            max-inline-size: 70%;
            padding: 1rem;
            border-radius: 12px;
            word-wrap: break-word;
            white-space: pre-wrap;
            line-height: 1.5;
        }
        .message.user {
            align-self: flex-end;
            background: #0d419d;
            color: #fff;
        }
        .message.assistant {
            align-self: flex-start;
            background: #2d2d2d;
            border: 1px solid #404040;
        }
        .message.typing {
            align-self: flex-start;
            background: #2d2d2d;
            border: 1px solid #404040;
            color: #888;
            font-style: italic;
        }
        .photo-badge {
            display: inline-block;
            background: #3a3a5c;
            padding: 0.2rem 0.6rem;
            border-radius: 4px;
            font-size: 0.8rem;
            margin-block-end: 0.4rem;
        }
        #image-preview {
            display: none;
            background: #252525;
            padding: 0.5rem 1rem;
            border-block-start: 1px solid #333;
            align-items: center;
            gap: 0.75rem;
        }
        #image-preview img {
            max-block-size: 60px;
            border-radius: 4px;
        }
        #image-preview .remove-img {
            background: none;
            border: none;
            color: #f44;
            cursor: pointer;
            font-size: 1.2rem;
            padding: 0.25rem;
        }
        #image-preview .img-name {
            color: #aaa;
            font-size: 0.85rem;
            flex: 1;
            overflow: hidden;
            text-overflow: ellipsis;
            white-space: nowrap;
        }
        #input-area {
            display: flex;
            gap: 0.5rem;
            padding: 1rem;
            background: #252525;
            border-block-start: 1px solid #333;
            align-items: center;
        }
        #image-upload-btn {
            background: none;
            border: 1px solid #404040;
            border-radius: 6px;
            color: #aaa;
            cursor: pointer;
            padding: 0.75rem;
            font-size: 1.1rem;
            transition: all 0.2s;
            flex-shrink: 0;
        }
        #image-upload-btn:hover { border-color: #58a6ff; color: #58a6ff; }
        #message-input {
            flex: 1;
            padding: 0.75rem;
            background: #1a1a1a;
            border: 1px solid #404040;
            border-radius: 6px;
            color: #e0e0e0;
            font-size: 1rem;
        }
        #send-btn {
            padding: 0.75rem 1.5rem;
            background: #58a6ff;
            color: #fff;
            border: none;
            border-radius: 6px;
            cursor: pointer;
            font-weight: 600;
            transition: background 0.2s;
            flex-shrink: 0;
        }
        #send-btn:hover { background: #4493f8; }
        #send-btn:disabled { background: #404040; cursor: not-allowed; }
        .message-meta {
            font-size: 0.75rem;
            opacity: 0.7;
            margin-block-start: 0.5rem;
        }
    </style>
</head>
<body>
    <header>
        <h1>Openbot</h1>
        <div class="header-links">
            <a href="/hub">Hub</a>
            <a href="/calls">Calls</a>
        </div>
    </header>

    <div id="chat-container"></div>

    <div id="image-preview">
        <img id="preview-thumb" src="" alt="preview">
        <span class="img-name" id="preview-name"></span>
        <button class="remove-img" id="remove-img-btn" title="Remove image">✕</button>
    </div>

    <div id="input-area">
        <input type="file" id="image-input" accept="image/*" multiple hidden>
        <button id="image-upload-btn" title="Attach image">📷</button>
        <input type="text" id="message-input" placeholder="Type your message..." autocomplete="off">
        <button id="send-btn">Send</button>
    </div>
    <script>
        const sessionId = "peggens";
        const chatContainer = document.getElementById('chat-container');
        const messageInput = document.getElementById('message-input');
        const sendBtn = document.getElementById('send-btn');
        const imageInput = document.getElementById('image-input');
        const imageUploadBtn = document.getElementById('image-upload-btn');
        const imagePreview = document.getElementById('image-preview');
        const previewThumb = document.getElementById('preview-thumb');
        const previewName = document.getElementById('preview-name');
        const removeImgBtn = document.getElementById('remove-img-btn');

        let pendingImages = [];

        imageUploadBtn.addEventListener('click', () => imageInput.click());

        imageInput.addEventListener('change', (e) => {
            const selected = Array.from(e.target.files || []);
            if (!selected.length) return;
            pendingImages = selected;
            previewThumb.src = URL.createObjectURL(selected[0]);
            previewName.textContent = selected.length === 1
                ? selected[0].name
                : `${selected.length} files selected (${selected[0].name} +${selected.length - 1} more)`;
            imagePreview.style.display = 'flex';
            messageInput.placeholder = 'Add a caption (optional)...';
            messageInput.focus();
        });

        removeImgBtn.addEventListener('click', clearImage);

        function clearImage() {
            pendingImages = [];
            imageInput.value = '';
            imagePreview.style.display = 'none';
            previewThumb.src = '';
            previewName.textContent = '';
            messageInput.placeholder = 'Type your message...';
        }

        async function loadHistory() {
            try {
                const response = await fetch('/sessions/peggens/history');
                const data = await response.json();
                data.messages.forEach(msg => {
                    addMessage(msg.content, msg.role, msg.timestamp, msg.source);
                });
                scrollToBottom();
            } catch (error) {
                console.error('Failed to load history:', error);
            }
        }

        function addMessage(content, role, timestamp = null, source = null) {
            removeTyping();
            const msgDiv = document.createElement('div');
            msgDiv.className = `message ${role}`;

            const contentDiv = document.createElement('div');
            if (content.startsWith('[sent a photo]') || /^\\[sent \\d+ photos\\]/.test(content)) {
                const badge = document.createElement('span');
                badge.className = 'photo-badge';
                badge.textContent = '📷 Photo Upload';
                contentDiv.appendChild(badge);
                const caption = content
                    .replace(/^\\[sent a photo\\]/, '')
                    .replace(/^\\[sent \\d+ photos\\]/, '')
                    .trim();
                if (caption) {
                    contentDiv.appendChild(document.createElement('br'));
                    contentDiv.appendChild(document.createTextNode(caption));
                }
            } else {
                contentDiv.textContent = content;
            }
            msgDiv.appendChild(contentDiv);

            if (timestamp) {
                const metaDiv = document.createElement('div');
                metaDiv.className = 'message-meta';
                metaDiv.textContent = `${timestamp} • ${source || 'web'}`;
                msgDiv.appendChild(metaDiv);
            }

            chatContainer.appendChild(msgDiv);
        }

        function showTyping() {
            const existing = document.getElementById('typing-indicator');
            if (existing) return;
            const msgDiv = document.createElement('div');
            msgDiv.className = 'message typing';
            msgDiv.id = 'typing-indicator';
            msgDiv.textContent = 'Openbot is thinking...';
            chatContainer.appendChild(msgDiv);
            scrollToBottom();
        }

        function removeTyping() {
            const el = document.getElementById('typing-indicator');
            if (el) el.remove();
        }

        function scrollToBottom() {
            chatContainer.scrollTop = chatContainer.scrollHeight;
        }

        async function sendMessage() {
            const message = messageInput.value.trim();
            if (!message && !pendingImages.length) return;

            sendBtn.disabled = true;
            messageInput.value = '';

            if (pendingImages.length) {
                const caption = message;
                const displayText = pendingImages.length === 1
                    ? (caption ? `[sent a photo] ${caption}` : '[sent a photo]')
                    : (caption ? `[sent ${pendingImages.length} photos] ${caption}` : `[sent ${pendingImages.length} photos]`);
                addMessage(displayText, 'user');
                scrollToBottom();
                showTyping();

                const formData = new FormData();
                pendingImages.forEach((img) => formData.append('files', img));
                formData.append('caption', caption);
                clearImage();

                try {
                    const res = await fetch('/chat/image', { method: 'POST', body: formData });
                    const data = await res.json();
                    addMessage(data.response, 'assistant');
                    scrollToBottom();
                } catch (err) {
                    addMessage('Error: Failed to analyze image', 'assistant');
                }
            } else {
                addMessage(message, 'user');
                scrollToBottom();
                showTyping();

                try {
                    const res = await fetch('/chat', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ message, session_id: sessionId })
                    });
                    const data = await res.json();
                    addMessage(data.response, 'assistant');
                    scrollToBottom();
                } catch (err) {
                    addMessage('Error: Failed to get response', 'assistant');
                }
            }

            sendBtn.disabled = false;
            messageInput.focus();
        }

        sendBtn.addEventListener('click', sendMessage);
        messageInput.addEventListener('keypress', (e) => {
            if (e.key === 'Enter') sendMessage();
        });

        loadHistory();
    </script>
</body>
</html>
"""


@app.get("/hub", response_class=HTMLResponse)
async def hub_page():
    """Unified conversation hub - read-only view."""
    if not ENABLE_WEB_UI:
        raise HTTPException(status_code=404, detail="Web UI is disabled")
    return """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Openbot Hub</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: #1a1a1a;
            color: #e0e0e0;
            min-block-size: 100vh;
        }
        header {
            background: #252525;
            padding: 1rem 2rem;
            border-block-end: 1px solid #333;
            display: flex;
            justify-content: space-between;
            align-items: center;
            position: sticky;
            inset-block-start: 0;
            z-index: 100;
        }
        header h1 { font-size: 1.5rem; color: #fff; }
        .header-actions {
            display: flex;
            gap: 1rem;
        }
        .header-actions button, .header-actions a {
            color: #58a6ff;
            background: transparent;
            text-decoration: none;
            padding: 0.5rem 1rem;
            border: 1px solid #58a6ff;
            border-radius: 6px;
            cursor: pointer;
            transition: all 0.2s;
            font-size: 0.9rem;
        }
        .header-actions button:hover, .header-actions a:hover {
            background: #58a6ff;
            color: #fff;
        }
        #hub-container {
            max-inline-size: 1200px;
            margin: 2rem auto;
            padding: 0 2rem;
        }
        .message-item {
            background: #252525;
            border: 1px solid #333;
            border-radius: 8px;
            padding: 1.5rem;
            margin-block-end: 1rem;
        }
        .message-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-block-end: 1rem;
        }
        .role-badge {
            display: inline-block;
            padding: 0.25rem 0.75rem;
            border-radius: 4px;
            font-size: 0.85rem;
            font-weight: 600;
            text-transform: uppercase;
        }
        .role-badge.user { background: #0d419d; color: #fff; }
        .role-badge.assistant { background: #2d6a4f; color: #fff; }
        .source-badge {
            display: inline-block;
            padding: 0.25rem 0.75rem;
            border-radius: 4px;
            font-size: 0.75rem;
            font-weight: 500;
            margin-inline-start: 0.5rem;
        }
        .source-badge.telegram { background: #0088cc; color: #fff; }
        .source-badge.web { background: #6366f1; color: #fff; }
        .photo-badge {
            display: inline-block;
            background: #3a3a5c;
            padding: 0.2rem 0.6rem;
            border-radius: 4px;
            font-size: 0.8rem;
            margin-inline-end: 0.5rem;
        }
        .message-timestamp {
            font-size: 0.85rem;
            color: #888;
        }
        .message-content {
            line-height: 1.6;
            white-space: pre-wrap;
            word-wrap: break-word;
        }
        .empty-state {
            text-align: center;
            padding: 4rem 2rem;
            color: #888;
        }
        .empty-state h2 { margin-block-end: 1rem; }
    </style>
</head>
<body>
    <header>
        <h1>📊 Conversation Hub</h1>
        <div class="header-actions">
            <button id="refresh-btn">🔄 Refresh</button>
            <a href="/">← Back to Chat</a>
        </div>
    </header>

    <div id="hub-container"></div>

    <script>
        const hubContainer = document.getElementById('hub-container');
        const refreshBtn = document.getElementById('refresh-btn');

        async function loadConversation() {
            try {
                const response = await fetch('/sessions/peggens/history');
                const data = await response.json();

                if (data.messages.length === 0) {
                    hubContainer.innerHTML = `
                        <div class="empty-state">
                            <h2>No messages yet</h2>
                            <p>Start a conversation in the chat UI or via Telegram</p>
                        </div>
                    `;
                    return;
                }

                hubContainer.innerHTML = '';
                data.messages.forEach(msg => {
                    const msgDiv = document.createElement('div');
                    msgDiv.className = 'message-item';

                    const header = document.createElement('div');
                    header.className = 'message-header';

                    const badges = document.createElement('div');
                    badges.innerHTML = `
                        <span class="role-badge ${msg.role}">${msg.role}</span>
                        <span class="source-badge ${msg.source}">${msg.source}</span>
                    `;

                    const timestamp = document.createElement('div');
                    timestamp.className = 'message-timestamp';
                    timestamp.textContent = msg.timestamp || '';

                    header.appendChild(badges);
                    header.appendChild(timestamp);

                    const content = document.createElement('div');
                    content.className = 'message-content';
                    if (msg.content.startsWith('[sent a photo]')) {
                        const badge = document.createElement('span');
                        badge.className = 'photo-badge';
                        badge.textContent = '📷 Photo';
                        content.appendChild(badge);
                        const caption = msg.content.replace('[sent a photo]', '').trim();
                        if (caption) content.appendChild(document.createTextNode(caption));
                    } else {
                        content.textContent = msg.content;
                    }

                    msgDiv.appendChild(header);
                    msgDiv.appendChild(content);
                    hubContainer.appendChild(msgDiv);
                });
            } catch (error) {
                hubContainer.innerHTML = `
                    <div class="empty-state">
                        <h2>Error loading conversation</h2>
                        <p>${error.message}</p>
                    </div>
                `;
            }
        }

        refreshBtn.addEventListener('click', loadConversation);

        // Auto-refresh every 10 seconds
        setInterval(loadConversation, 10000);

        // Initial load
        loadConversation();
    </script>
</body>
</html>
"""


# ---------------------------------------------------------------------------
# Twilio voice call-in webhooks
# ---------------------------------------------------------------------------

def _validate_twilio_signature(request: Request, form_data: dict) -> bool:
    """Verify the X-Twilio-Signature header to ensure the request came from Twilio."""
    if not TWILIO_AUTH_TOKEN:
        return True  # skip validation when token not configured
    signature = request.headers.get("X-Twilio-Signature", "")
    url = str(request.url)
    data = url + "".join(f"{k}{v}" for k, v in sorted(form_data.items()))
    expected = base64.b64encode(
        hmac.new(TWILIO_AUTH_TOKEN.encode(), data.encode(), hashlib.sha1).digest()
    ).decode()
    return hmac.compare_digest(signature, expected)


@app.post("/webhook/voice")
async def handle_inbound_call(request: Request):
    """Twilio webhook: answer an inbound voice call.

    Only runs when the Twilio number's **Voice** webhook URL is this server's
    ``/webhook/voice``. If inbound uses **Twilio SIP** to sip-bridge (or Studio /
    TwiML Bin), that path never hits this handler; set the greeting in sip-bridge
    ``SYSTEM_PROMPT`` / ``VOICE_REALTIME_OPENING_LINE`` (or in Twilio for Studio).
    """
    form = await request.form()
    form_dict = dict(form)
    caller = form_dict.get("From", "unknown")
    call_sid = form_dict.get("CallSid", "")

    log.info(
        "Inbound call from %s (CallSid: %s)",
        mask_identifier(caller),
        mask_identifier(call_sid),
    )

    call_threads[call_sid] = []
    call_entry = {
        "call_sid": call_sid,
        "caller": caller,
        "direction": "inbound",
        "started_at": get_timestamp(),
        "status": "in-progress",
        "turns": [],
    }
    call_log.append(call_entry)
    session_store.upsert_call_record(
        call_sid=call_sid,
        caller=caller,
        direction="inbound",
        status="in-progress",
        started_at=call_entry["started_at"],
    )

    safe_greeting = _xml_escape(TWILIO_INBOUND_VOICE_GREETING)
    twiml = f"""<?xml version="1.0" encoding="UTF-8"?>
<Response>
    <Say voice="Polly.Joanna">{safe_greeting}</Say>
    <Gather input="speech" action="/webhook/voice/respond" speechTimeout="auto" />
    <Say voice="Polly.Joanna">I didn't catch that. Goodbye!</Say>
</Response>"""
    return Response(content=twiml, media_type="application/xml")


@app.post("/webhook/voice/respond")
async def handle_voice_response(request: Request):
    """Twilio webhook: process caller speech and respond via LLM."""
    form = await request.form()
    form_dict = dict(form)
    speech_result = form_dict.get("SpeechResult", "")
    call_sid = form_dict.get("CallSid", "")
    caller = form_dict.get("From", "unknown")

    log.info(
        "Voice input from %s: %s",
        mask_identifier(caller),
        _safe_snippet(speech_result, limit=120),
    )

    thread = call_threads.get(call_sid, [])
    thread.append({"role": "user", "content": speech_result})

    messages = [{"role": "system", "content": get_system_prompt()}]
    for msg in thread[-8:]:
        messages.append({"role": msg["role"], "content": msg["content"]})

    try:
        response_text = await call_llm(messages)
    except Exception as exc:
        log.error("LLM error during voice call %s: %s", call_sid, exc)
        response_text = "Sorry, I had trouble thinking about that. Could you try again?"

    thread.append({"role": "assistant", "content": response_text})
    call_threads[call_sid] = thread

    for entry in call_log:
        if entry["call_sid"] == call_sid:
            turn_ts = get_timestamp()
            entry["turns"].append({"user": speech_result, "assistant": response_text, "ts": turn_ts})
            session_store.append_call_turn(
                call_sid=call_sid,
                user_text=speech_result,
                assistant_text=response_text,
                timestamp=turn_ts,
            )
            break

    await store_memory(f"call:{caller}", "user", speech_result)
    await store_memory(f"call:{caller}", "assistant", response_text)

    safe_text = (
        response_text
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&apos;")
    )

    twiml = f"""<?xml version="1.0" encoding="UTF-8"?>
<Response>
    <Say voice="Polly.Joanna">{safe_text}</Say>
    <Gather input="speech" action="/webhook/voice/respond" speechTimeout="auto" />
    <Say voice="Polly.Joanna">Are you still there? Goodbye!</Say>
</Response>"""
    return Response(content=twiml, media_type="application/xml")


@app.post("/webhook/voice/status")
async def handle_voice_status(request: Request):
    """Twilio webhook: call status callback (completed, no-answer, etc.)."""
    form = await request.form()
    form_dict = dict(form)
    call_sid = form_dict.get("CallSid", "")
    status = form_dict.get("CallStatus", "")
    duration = form_dict.get("CallDuration", "0")

    log.info("Call %s status: %s (duration: %ss)", call_sid, status, duration)

    for entry in call_log:
        if entry["call_sid"] == call_sid:
            entry["status"] = status
            entry["duration"] = int(duration)
            entry["ended_at"] = get_timestamp()
            session_store.upsert_call_record(
                call_sid=call_sid,
                caller=entry.get("caller", ""),
                direction=entry.get("direction", ""),
                status=status,
                started_at=entry.get("started_at", ""),
                ended_at=entry.get("ended_at", ""),
                duration=int(duration or 0),
            )
            break

    return {"ok": True}


@app.post("/webhook/outbound/status")
async def handle_outbound_call_status(request: Request):
    """Twilio status callback for outbound /call actions."""
    form = await request.form()
    form_dict = dict(form)
    call_sid = (form_dict.get("CallSid") or "").strip()
    status = (form_dict.get("CallStatus") or "").strip().lower()
    to_number = (form_dict.get("To") or "").strip()
    duration = (form_dict.get("CallDuration") or "0").strip()

    _upsert_call_runtime_record(
        call_sid=call_sid,
        status=status or "unknown",
        caller=to_number,
        direction="outbound-api",
        duration=int(duration or 0),
    )

    watcher = outbound_call_watchers.get(call_sid)
    if not watcher:
        return {"ok": True}

    chat_id = watcher.get("chat_id")
    if not chat_id:
        return {"ok": True}
    watcher_bot_token = watcher.get("bot_token") or TELEGRAM_MYOSHE_BOT_TOKEN

    terminal_statuses = {"completed", "busy", "failed", "no-answer", "canceled"}
    if status in terminal_statuses:
        outcome = {
            "completed": "answered",
            "busy": "busy",
            "failed": "failed",
            "no-answer": "no answer",
            "canceled": "canceled",
        }.get(status, status)
        reply_markup = None
        if status in {"busy", "failed", "no-answer", "canceled"} and call_sid:
            reply_markup = _build_retry_markup(call_sid)
        try:
            await telegram_send(
                int(chat_id),
                f"Call result for {to_number or watcher.get('to_number', 'unknown')}: {outcome}."
                + (f" Duration: {duration}s." if duration and duration != "0" else ""),
                bot_token=watcher_bot_token,
                reply_markup=reply_markup,
            )
        except Exception as exc:
            log.error("Failed to send outbound call status update: %s", exc)
        finally:
            outbound_call_watchers.pop(call_sid, None)
    elif status in {"initiated", "ringing", "in-progress"}:
        # Optional progress ping for visibility while waiting.
        try:
            await telegram_send(
                int(chat_id),
                f"Call update for {to_number or watcher.get('to_number', 'unknown')}: {status}.",
                bot_token=watcher_bot_token,
            )
        except Exception:
            pass

    return {"ok": True}


# ---------------------------------------------------------------------------
# Call log API + view
# ---------------------------------------------------------------------------

@app.get("/api/calls")
async def get_calls():
    """Return persistent call logs."""
    return {"calls": session_store.list_calls(limit=200)}


@app.get("/calls", response_class=HTMLResponse)
async def calls_view():
    """Web UI showing voice call history."""
    if not ENABLE_WEB_UI:
        raise HTTPException(status_code=404, detail="Web UI is disabled")
    return """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Openbot Calls</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: #1a1a1a;
            color: #e0e0e0;
            min-block-size: 100vh;
        }
        header {
            background: #252525;
            padding: 1rem 2rem;
            border-block-end: 1px solid #333;
            display: flex;
            justify-content: space-between;
            align-items: center;
            position: sticky;
            inset-block-start: 0;
            z-index: 100;
        }
        header h1 { font-size: 1.5rem; color: #fff; }
        .header-actions { display: flex; gap: 1rem; }
        .header-actions button, .header-actions a {
            color: #58a6ff;
            background: transparent;
            text-decoration: none;
            padding: 0.5rem 1rem;
            border: 1px solid #58a6ff;
            border-radius: 6px;
            cursor: pointer;
            transition: all 0.2s;
            font-size: 0.9rem;
        }
        .header-actions button:hover, .header-actions a:hover {
            background: #58a6ff;
            color: #fff;
        }
        #calls-container {
            max-inline-size: 1000px;
            margin: 2rem auto;
            padding: 0 2rem;
        }
        .call-card {
            background: #252525;
            border: 1px solid #333;
            border-radius: 8px;
            padding: 1.5rem;
            margin-block-end: 1rem;
        }
        .call-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-block-end: 1rem;
        }
        .call-caller { font-weight: 600; font-size: 1.1rem; }
        .call-meta { font-size: 0.85rem; color: #888; }
        .call-status {
            display: inline-block;
            padding: 0.2rem 0.6rem;
            border-radius: 4px;
            font-size: 0.75rem;
            font-weight: 600;
            text-transform: uppercase;
        }
        .call-status.completed { background: #2d6a4f; color: #fff; }
        .call-status.in-progress { background: #0d419d; color: #fff; }
        .call-status.no-answer { background: #7c3aed; color: #fff; }
        .call-status.failed { background: #dc2626; color: #fff; }
        .turn {
            margin-block-start: 0.75rem;
            padding: 0.75rem;
            background: #1a1a1a;
            border-radius: 6px;
        }
        .turn-user { font-weight: 500; color: #58a6ff; margin-block-end: 0.25rem; }
        .turn-bot { color: #ccc; line-height: 1.5; }
        .turn-ts { font-size: 0.7rem; color: #666; margin-block-start: 0.25rem; }
        .empty-state { text-align: center; padding: 4rem 2rem; color: #888; }
        .empty-state h2 { margin-block-end: 1rem; }
    </style>
</head>
<body>
    <header>
        <h1>Phone Calls</h1>
        <div class="header-actions">
            <button id="refresh-btn">Refresh</button>
            <a href="/">Back to Chat</a>
        </div>
    </header>
    <div id="calls-container"></div>
    <script>
        const container = document.getElementById('calls-container');
        document.getElementById('refresh-btn').addEventListener('click', loadCalls);

        function escapeHtml(s) {
            const d = document.createElement('div');
            d.textContent = s || '';
            return d.innerHTML;
        }

        async function loadCalls() {
            try {
                const res = await fetch('/api/calls');
                const data = await res.json();
                const calls = data.calls || [];
                if (calls.length === 0) {
                    container.innerHTML = '<div class="empty-state"><h2>No calls yet</h2><p>Configure your Twilio number to point to /webhook/voice</p></div>';
                    return;
                }
                container.innerHTML = '';
                calls.forEach(c => {
                    const card = document.createElement('div');
                    card.className = 'call-card';
                    const statusClass = (c.status || 'unknown').replace(/\\s+/g, '-').toLowerCase();
                    const duration = c.duration ? c.duration + 's' : '';
                    let turnsHtml = '';
                    (c.turns || []).forEach(t => {
                        turnsHtml += '<div class="turn">'
                            + '<div class="turn-user">Caller: ' + escapeHtml(t.user) + '</div>'
                            + '<div class="turn-bot">Openbot: ' + escapeHtml(t.assistant) + '</div>'
                            + '<div class="turn-ts">' + (t.ts || '') + '</div></div>';
                    });
                    card.innerHTML = '<div class="call-header">'
                        + '<div><span class="call-caller">' + escapeHtml(c.caller) + '</span>'
                        + ' <span class="call-status ' + statusClass + '">' + escapeHtml(c.status || 'unknown') + '</span></div>'
                        + '<div class="call-meta">' + (c.started_at || '') + (duration ? ' &middot; ' + duration : '') + '</div></div>'
                        + turnsHtml;
                    container.appendChild(card);
                });
            } catch (err) {
                container.innerHTML = '<div class="empty-state"><h2>Error loading calls</h2><p>' + err.message + '</p></div>';
            }
        }

        setInterval(loadCalls, 15000);
        loadCalls();
    </script>
</body>
</html>
"""


@app.post("/webhook/macwhisper")
async def handle_macwhisper_webhook(payload: MacWhisperPayload):
    """Receive transcriptions from MacWhisper Pro."""
    log.info("MacWhisper transcript received: %s", payload.title)
    
    # Store in a global "voice notes" session
    session_id = "macwhisper:global"
    content = f"Title: {payload.title}\n\nTranscript: {payload.transcript}"
    
    session_store.append_session_message(
        session_id=session_id,
        role="user",
        content=content,
        source="macwhisper"
    )
    
    # Also notify Telegram if an alert chat is configured
    if TELEGRAM_ALERT_CHAT_ID:
        msg = f"🎙 **MacWhisper Note Received**\n\n**{payload.title}**\n\n{payload.transcript[:500]}..."
        await telegram_send(int(TELEGRAM_ALERT_CHAT_ID), msg, bot_token=TELEGRAM_MYOSHE_BOT_TOKEN)
        
    return {"status": "ok", "session_id": session_id}


app = app # Ensure app is exposed

if __name__ == "__main__":
    import uvicorn
    import os
    port = int(os.environ.get("PORT", 8080))
    uvicorn.run("main:app", host="0.0.0.0", port=port, log_level="info")
