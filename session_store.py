"""
Durable store for Openbot runtime session history.

This keeps conversation and call history persistent across process restarts.
"""

import json
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


SESSION_DB_PATH = os.getenv("OPENBOT_SESSION_DB_PATH", "/data/openbot_sessions.db")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _get_db() -> sqlite3.Connection:
    Path(SESSION_DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(SESSION_DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db() -> None:
    conn = _get_db()
    try:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS session_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT NOT NULL,
                role TEXT NOT NULL,
                content TEXT NOT NULL,
                source TEXT DEFAULT '',
                country TEXT DEFAULT '',
                timestamp TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_session_messages_session_ts
                ON session_messages(session_id, id);

            CREATE TABLE IF NOT EXISTS call_records (
                call_sid TEXT PRIMARY KEY,
                caller TEXT DEFAULT '',
                direction TEXT DEFAULT '',
                status TEXT DEFAULT '',
                started_at TEXT DEFAULT '',
                ended_at TEXT DEFAULT '',
                duration INTEGER DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS call_turns (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                call_sid TEXT NOT NULL,
                user_text TEXT DEFAULT '',
                assistant_text TEXT DEFAULT '',
                timestamp TEXT NOT NULL,
                FOREIGN KEY(call_sid) REFERENCES call_records(call_sid)
            );
            CREATE INDEX IF NOT EXISTS idx_call_turns_call_sid
                ON call_turns(call_sid, id);
            """
        )
    finally:
        conn.close()


def append_session_message(
    session_id: str,
    role: str,
    content: str,
    source: str = "",
    country: str = "",
    timestamp: str | None = None,
) -> None:
    ts = timestamp or _utc_now()
    conn = _get_db()
    try:
        conn.execute(
            """
            INSERT INTO session_messages (session_id, role, content, source, country, timestamp)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (session_id, role, content, source, country, ts),
        )
        conn.commit()
    finally:
        conn.close()


def get_session_messages(session_id: str, limit: int = 200) -> list[dict[str, Any]]:
    conn = _get_db()
    try:
        rows = conn.execute(
            """
            SELECT role, content, source, country, timestamp
            FROM session_messages
            WHERE session_id = ?
            ORDER BY id DESC
            LIMIT ?
            """,
            (session_id, max(1, int(limit))),
        ).fetchall()
    finally:
        conn.close()
    # API/UI expects oldest -> newest ordering.
    return [dict(r) for r in reversed(rows)]


def clear_session(session_id: str) -> None:
    conn = _get_db()
    try:
        conn.execute("DELETE FROM session_messages WHERE session_id = ?", (session_id,))
        conn.commit()
    finally:
        conn.close()


def upsert_call_record(
    call_sid: str,
    caller: str = "",
    direction: str = "",
    status: str = "",
    started_at: str = "",
    ended_at: str = "",
    duration: int = 0,
) -> None:
    conn = _get_db()
    try:
        conn.execute(
            """
            INSERT INTO call_records (call_sid, caller, direction, status, started_at, ended_at, duration)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(call_sid) DO UPDATE SET
                caller=excluded.caller,
                direction=excluded.direction,
                status=excluded.status,
                started_at=excluded.started_at,
                ended_at=excluded.ended_at,
                duration=excluded.duration
            """,
            (call_sid, caller, direction, status, started_at, ended_at, int(duration or 0)),
        )
        conn.commit()
    finally:
        conn.close()


def append_call_turn(call_sid: str, user_text: str, assistant_text: str, timestamp: str | None = None) -> None:
    ts = timestamp or _utc_now()
    conn = _get_db()
    try:
        conn.execute(
            """
            INSERT INTO call_turns (call_sid, user_text, assistant_text, timestamp)
            VALUES (?, ?, ?, ?)
            """,
            (call_sid, user_text or "", assistant_text or "", ts),
        )
        conn.commit()
    finally:
        conn.close()


def list_calls(limit: int = 100) -> list[dict[str, Any]]:
    conn = _get_db()
    try:
        records = conn.execute(
            """
            SELECT call_sid, caller, direction, status, started_at, ended_at, duration
            FROM call_records
            ORDER BY started_at DESC
            LIMIT ?
            """,
            (max(1, int(limit)),),
        ).fetchall()
        out: list[dict[str, Any]] = []
        for rec in records:
            entry = dict(rec)
            turns = conn.execute(
                """
                SELECT user_text, assistant_text, timestamp
                FROM call_turns
                WHERE call_sid = ?
                ORDER BY id ASC
                """,
                (entry["call_sid"],),
            ).fetchall()
            entry["turns"] = [
                {"user": t["user_text"], "assistant": t["assistant_text"], "ts": t["timestamp"]}
                for t in turns
            ]
            out.append(entry)
    finally:
        conn.close()
    return out


def get_fingerprint() -> dict[str, Any]:
    path = Path(SESSION_DB_PATH)
    exists = path.exists()
    size = path.stat().st_size if exists else 0
    counts = {
        "session_messages": 0,
        "call_records": 0,
        "call_turns": 0,
    }
    if exists:
        conn = _get_db()
        try:
            for table in counts:
                try:
                    counts[table] = int(
                        conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                    )
                except sqlite3.Error:
                    counts[table] = -1
        finally:
            conn.close()
    return {
        "session_db_path": str(path),
        "session_db_exists": exists,
        "session_db_file_size_bytes": int(size),
        "row_counts": counts,
    }


def export_session_state(session_id: str, call_sid: str | None = None) -> str:
    """Small helper for automation diagnostics."""
    payload = {
        "session_id": session_id,
        "messages": get_session_messages(session_id, limit=50),
        "calls": list_calls(limit=20),
    }
    if call_sid:
        payload["call_sid"] = call_sid
    return json.dumps(payload)

