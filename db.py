"""YATB - Yet Another Backup Tool - Database module.

Handles all SQLite operations for profiles, runs, users, and settings.
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Iterable, Optional

DB_PATH = Path(__file__).parent / "data" / "yatb.sqlite"


class ManagedConnection(sqlite3.Connection):
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        try:
            super().__exit__(exc_type, exc_val, exc_tb)
        finally:
            self.close()


def connect_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, factory=ManagedConnection)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with connect_db() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                role TEXT NOT NULL,
                created_at TEXT NOT NULL,
                last_login TEXT
            );

            CREATE TABLE IF NOT EXISTS profiles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL,
                source_path TEXT NOT NULL,
                dest_path TEXT NOT NULL,
                exclude_patterns TEXT NOT NULL,
                schedule_time TEXT,
                schedule_frequency TEXT NOT NULL DEFAULT 'day',
                schedule_enabled INTEGER NOT NULL DEFAULT 0,
                retention_count INTEGER NOT NULL DEFAULT 7,
                verify_mode TEXT NOT NULL DEFAULT 'size',
                last_scheduled_date TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                profile_id INTEGER NOT NULL,
                triggered_by TEXT NOT NULL,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                status TEXT NOT NULL,
                message TEXT,
                log_text TEXT,
                FOREIGN KEY(profile_id) REFERENCES profiles(id)
            );

            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS system_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_type TEXT NOT NULL,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                status TEXT NOT NULL,
                message TEXT,
                log_text TEXT
            );
            """
        )

        profile_columns = {
            row["name"] for row in conn.execute("PRAGMA table_info(profiles)").fetchall()
        }
        if "schedule_frequency" not in profile_columns:
            conn.execute(
                "ALTER TABLE profiles ADD COLUMN schedule_frequency TEXT NOT NULL DEFAULT 'day'"
            )


def now_iso() -> str:
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"


def get_setting(key: str, default: Optional[str] = None) -> Optional[str]:
    with connect_db() as conn:
        row = conn.execute("SELECT value FROM settings WHERE key = ?", (key,)).fetchone()
        if row is None:
            return default
        return row["value"]


def set_setting(key: str, value: str) -> None:
    with connect_db() as conn:
        conn.execute(
            "INSERT INTO settings (key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, value),
        )


def get_setting_json(key: str, default: Optional[dict] = None) -> dict:
    raw = get_setting(key)
    if raw is None:
        return default or {}
    try:
        data = json.loads(raw)
        return data if isinstance(data, dict) else (default or {})
    except json.JSONDecodeError:
        return default or {}


def set_setting_json(key: str, value: dict) -> None:
    set_setting(key, json.dumps(value, indent=2))


def create_user(username: str, password_hash: str, role: str) -> None:
    with connect_db() as conn:
        conn.execute(
            "INSERT INTO users (username, password_hash, role, created_at) VALUES (?, ?, ?, ?)",
            (username, password_hash, role, now_iso()),
        )


def update_user(user_id: int, username: str, role: str, password_hash: Optional[str]) -> None:
    with connect_db() as conn:
        if password_hash:
            conn.execute(
                "UPDATE users SET username = ?, role = ?, password_hash = ? WHERE id = ?",
                (username, role, password_hash, user_id),
            )
        else:
            conn.execute(
                "UPDATE users SET username = ?, role = ? WHERE id = ?",
                (username, role, user_id),
            )


def delete_user(user_id: int) -> None:
    with connect_db() as conn:
        conn.execute("DELETE FROM users WHERE id = ?", (user_id,))


def list_users() -> list[sqlite3.Row]:
    with connect_db() as conn:
        return conn.execute(
            "SELECT id, username, role, created_at, last_login FROM users ORDER BY username"
        ).fetchall()


def find_user_by_username(username: str) -> Optional[sqlite3.Row]:
    with connect_db() as conn:
        return conn.execute(
            "SELECT * FROM users WHERE username = ?",
            (username,),
        ).fetchone()


def find_user_by_id(user_id: int) -> Optional[sqlite3.Row]:
    with connect_db() as conn:
        return conn.execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()


def set_last_login(user_id: int) -> None:
    with connect_db() as conn:
        conn.execute("UPDATE users SET last_login = ? WHERE id = ?", (now_iso(), user_id))


def create_profile(
    name: str,
    source_path: str,
    dest_path: str,
    exclude_patterns: Iterable[str],
    schedule_time: Optional[str],
    schedule_frequency: str,
    schedule_enabled: bool,
    retention_count: int,
    verify_mode: str,
) -> None:
    with connect_db() as conn:
        conn.execute(
            """
            INSERT INTO profiles (
                name, source_path, dest_path, exclude_patterns,
                schedule_time, schedule_frequency, schedule_enabled, retention_count, verify_mode,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                name,
                source_path,
                dest_path,
                json.dumps(list(exclude_patterns)),
                schedule_time,
                schedule_frequency,
                1 if schedule_enabled else 0,
                retention_count,
                verify_mode,
                now_iso(),
                now_iso(),
            ),
        )


def update_profile(
    profile_id: int,
    name: str,
    source_path: str,
    dest_path: str,
    exclude_patterns: Iterable[str],
    schedule_time: Optional[str],
    schedule_frequency: str,
    schedule_enabled: bool,
    retention_count: int,
    verify_mode: str,
) -> None:
    with connect_db() as conn:
        conn.execute(
            """
            UPDATE profiles
            SET name = ?, source_path = ?, dest_path = ?, exclude_patterns = ?,
                schedule_time = ?, schedule_frequency = ?, schedule_enabled = ?, retention_count = ?,
                verify_mode = ?, updated_at = ?
            WHERE id = ?
            """,
            (
                name,
                source_path,
                dest_path,
                json.dumps(list(exclude_patterns)),
                schedule_time,
                schedule_frequency,
                1 if schedule_enabled else 0,
                retention_count,
                verify_mode,
                now_iso(),
                profile_id,
            ),
        )


def delete_profile(profile_id: int) -> None:
    with connect_db() as conn:
        conn.execute("DELETE FROM profiles WHERE id = ?", (profile_id,))


def get_profile(profile_id: int) -> Optional[sqlite3.Row]:
    with connect_db() as conn:
        return conn.execute("SELECT * FROM profiles WHERE id = ?", (profile_id,)).fetchone()


def list_profiles() -> list[sqlite3.Row]:
    with connect_db() as conn:
        return conn.execute(
            "SELECT * FROM profiles ORDER BY name"
        ).fetchall()


def set_profile_last_scheduled(profile_id: int, date_str: str) -> None:
    with connect_db() as conn:
        conn.execute(
            "UPDATE profiles SET last_scheduled_date = ? WHERE id = ?",
            (date_str, profile_id),
        )


def create_run(profile_id: int, triggered_by: str) -> int:
    with connect_db() as conn:
        cur = conn.execute(
            "INSERT INTO runs (profile_id, triggered_by, started_at, status) VALUES (?, ?, ?, ?)",
            (profile_id, triggered_by, now_iso(), "running"),
        )
        return int(cur.lastrowid)


def finish_run(run_id: int, status: str, message: str, log_text: str) -> None:
    with connect_db() as conn:
        conn.execute(
            "UPDATE runs SET finished_at = ?, status = ?, message = ?, log_text = ? WHERE id = ?",
            (now_iso(), status, message, log_text, run_id),
        )


def list_runs(limit: int = 50) -> list[sqlite3.Row]:
    with connect_db() as conn:
        return conn.execute(
            """
            SELECT runs.*, profiles.name AS profile_name
            FROM runs
            JOIN profiles ON profiles.id = runs.profile_id
            ORDER BY runs.started_at DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()


def get_run(run_id: int) -> Optional[sqlite3.Row]:
    with connect_db() as conn:
        return conn.execute(
            """
            SELECT runs.*, profiles.name AS profile_name
            FROM runs
            JOIN profiles ON profiles.id = runs.profile_id
            WHERE runs.id = ?
            """,
            (run_id,),
        ).fetchone()


def get_last_run_for_profile(profile_id: int) -> Optional[sqlite3.Row]:
    with connect_db() as conn:
        return conn.execute(
            "SELECT * FROM runs WHERE profile_id = ? ORDER BY started_at DESC LIMIT 1",
            (profile_id,),
        ).fetchone()


def create_system_run(task_type: str) -> int:
    with connect_db() as conn:
        cur = conn.execute(
            "INSERT INTO system_runs (task_type, started_at, status) VALUES (?, ?, ?)",
            (task_type, now_iso(), "running"),
        )
        return int(cur.lastrowid)


def finish_system_run(run_id: int, status: str, message: str, log_text: str) -> None:
    with connect_db() as conn:
        conn.execute(
            "UPDATE system_runs SET finished_at = ?, status = ?, message = ?, log_text = ? WHERE id = ?",
            (now_iso(), status, message, log_text, run_id),
        )


def list_system_runs(limit: int = 50) -> list[sqlite3.Row]:
    with connect_db() as conn:
        return conn.execute(
            "SELECT * FROM system_runs ORDER BY started_at DESC LIMIT ?",
            (limit,),
        ).fetchall()


def get_system_run(run_id: int) -> Optional[sqlite3.Row]:
    with connect_db() as conn:
        return conn.execute(
            "SELECT * FROM system_runs WHERE id = ?",
            (run_id,),
        ).fetchone()


def get_last_system_run_for_task(task_type: str) -> Optional[sqlite3.Row]:
    with connect_db() as conn:
        return conn.execute(
            "SELECT * FROM system_runs WHERE task_type = ? ORDER BY started_at DESC LIMIT 1",
            (task_type,),
        ).fetchone()
