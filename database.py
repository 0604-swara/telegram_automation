import json
import logging
import os
import sqlite3
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

from config import ConfigurationError, resolve_db_path
from dotenv import load_dotenv

load_dotenv()

try:
    DB_NAME = resolve_db_path()
except ConfigurationError as exc:
    raise ValueError(str(exc)) from exc

# Claim lease duration used when recovering after crashes/restarts.
DEFAULT_STALE_CLAIM_SECONDS = int(os.getenv("STALE_CLAIM_SECONDS", "900"))

# Schema version stored in bot_state after successful migration.
SCHEMA_VERSION = "6"
GO_LIVE_PENDING = "pending"
GO_LIVE_CAPTURING = "capturing"
GO_LIVE_WEBHOOK_CONFLICT = "webhook_conflict"
GO_LIVE_READY = "ready"

# Additive claim and publish-evidence columns.
PHASE3_COLUMNS = {
    "claimed_at": "TIMESTAMP",
    "claim_owner": "TEXT",
    "destination_message_ids": "TEXT",
    "cycle_id": "INTEGER",
}

NEW_POST_COLUMNS = {
    "live_eligible": "INTEGER NOT NULL DEFAULT 1",
}


class DatabaseError(Exception):
    """Base error for database operations."""


class ClaimError(DatabaseError):
    """Raised when an all-or-nothing claim cannot be completed."""


class PublishOwnershipError(DatabaseError):
    """Raised when publish finalization fails ownership checks."""


def _resolve_connection_path(db_path=None):
    try:
        resolved = resolve_db_path(DB_NAME if db_path is None else db_path)
    except ConfigurationError as exc:
        raise DatabaseError(str(exc)) from exc

    path = Path(resolved)
    if path.exists() and path.is_dir():
        raise DatabaseError(
            f"DB_PATH points to a directory, not a database file: {path}"
        )
    if not path.parent.exists():
        raise DatabaseError(
            f"Database directory does not exist: {path.parent}. "
            "Create it or correct DB_PATH."
        )
    if not path.parent.is_dir():
        raise DatabaseError(
            f"Database parent is not a directory: {path.parent}"
        )
    return str(path)


def _configure_connection(conn):
    """Apply production-safe SQLite connection settings."""
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = 30000")
    conn.execute("PRAGMA foreign_keys = ON")
    _preflight_schema(conn)
    # WAL improves concurrent listener + scheduler access.
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    return conn


@contextmanager
def get_connection(db_path=None):
    """
    Yield a configured SQLite connection and always close it.

    Explicit BEGIN / COMMIT / ROLLBACK remain the caller's responsibility.
    The built-in sqlite3 connection context manager is intentionally not used
    because it commits/rollbacks on exit but does not close the connection.
    """
    path = _resolve_connection_path(db_path)
    conn = sqlite3.connect(path, timeout=30, isolation_level=None)
    try:
        _configure_connection(conn)
        yield conn
    finally:
        conn.close()


def _transaction_active(conn):
    """Return True when an explicit transaction is open."""
    return bool(getattr(conn, "in_transaction", False))


def _safe_rollback(conn):
    """
    Roll back only if a transaction is active.

    Rollback failures are logged and do not replace the original exception.
    Callers must re-raise the original exception themselves.
    """
    if not _transaction_active(conn):
        return
    try:
        conn.execute("ROLLBACK")
    except sqlite3.Error as rollback_error:
        logging.error(
            "SQLite rollback failed while preserving original error: %s",
            rollback_error,
        )


def _begin_immediate(conn):
    conn.execute("BEGIN IMMEDIATE")


def _commit(conn):
    conn.execute("COMMIT")


def _column_names(conn, table_name):
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return {row["name"] for row in rows}


def _preflight_schema(conn):
    """Reject unknown databases before enabling WAL or applying migrations."""
    objects = {
        row["name"]: row["type"]
        for row in conn.execute(
            """
            SELECT name, type FROM sqlite_master
            WHERE type IN ('table', 'view', 'trigger')
              AND name NOT LIKE 'sqlite_%'
            """
        )
    }
    if not objects:
        return "empty"

    tables = {name for name, kind in objects.items() if kind == "table"}
    expected_tables = {"incoming_posts", "bot_state"}
    if not expected_tables.issubset(tables):
        raise DatabaseError(
            "Existing database is not a recognized Telegram application "
            "database; no migration was applied."
        )

    if objects != {name: "table" for name in tables}:
        raise DatabaseError(
            "Existing database contains unsupported schema objects; "
            "no migration was applied."
        )

    if tables - expected_tables - {"publish_history"}:
        raise DatabaseError(
            "Existing database contains unsupported tables; "
            "no migration was applied."
        )

    state_columns = _column_names(conn, "bot_state")
    if not {"key", "value"}.issubset(state_columns):
        raise DatabaseError(
            "Existing database has an unsupported bot_state schema; "
            "no migration was applied."
        )

    version = _get_state(conn, "schema_version")
    if version not in {"3", "4", "5", SCHEMA_VERSION}:
        raise DatabaseError(
            "Existing database schema version is unsupported "
            f"({version or 'unversioned'}); expected version 3, 4, 5, or "
            f"{SCHEMA_VERSION}. No migration was applied."
        )

    post_columns = _column_names(conn, "incoming_posts")
    required_post_columns = {
        "id",
        "channel_id",
        "message_id",
        "media_group_id",
        "message_type",
        "message_data",
        "received_at",
        "status",
        "retry_count",
        "last_error",
        "published_at",
    }
    if not required_post_columns.issubset(post_columns):
        raise DatabaseError(
            "Existing database has an unsupported incoming_posts schema; "
            "no migration was applied."
        )

    has_history = "publish_history" in tables
    if (version in {"4", "5", SCHEMA_VERSION}) != has_history:
        raise DatabaseError(
            "Existing database schema version does not match its tables; "
            "no migration was applied."
        )
    if version in {"5", SCHEMA_VERSION}:
        if "live_eligible" not in post_columns:
            raise DatabaseError(
                "Existing database is missing live-post eligibility; "
                "no migration was applied."
            )
    if version == SCHEMA_VERSION:
        bootstrap_status = _get_state(conn, "go_live_bootstrap_status")
        first_update_id = _get_state(conn, "go_live_first_update_id")
        if bootstrap_status not in {
            GO_LIVE_PENDING,
            GO_LIVE_CAPTURING,
            GO_LIVE_WEBHOOK_CONFLICT,
            GO_LIVE_READY,
        }:
            raise DatabaseError(
                "Existing database has an invalid go-live bootstrap state; "
                "no migration was applied."
            )
        if first_update_id is not None and first_update_id != "":
            try:
                int(first_update_id)
            except (TypeError, ValueError) as exc:
                raise DatabaseError(
                    "Existing database has an invalid go-live update "
                    "boundary; no migration was applied."
                ) from exc
        if bootstrap_status == GO_LIVE_READY and first_update_id is None:
            raise DatabaseError(
                "Existing database is missing its go-live update boundary; "
                "no migration was applied."
            )
    if has_history and not {
        "id",
        "cycle_id",
        "incoming_post_ids",
        "source_message_ids",
        "media_group_id",
        "destination_message_ids",
        "published_at",
    }.issubset(_column_names(conn, "publish_history")):
        raise DatabaseError(
            "Existing database has an unsupported publish_history schema; "
            "no migration was applied."
        )
    return version


def _set_state(conn, key, value):
    conn.execute(
        """
        INSERT INTO bot_state (key, value)
        VALUES (?, ?)
        ON CONFLICT(key)
        DO UPDATE SET value = excluded.value
        """,
        (key, str(value)),
    )


def _get_state(conn, key, default=None):
    row = conn.execute(
        "SELECT value FROM bot_state WHERE key = ?",
        (key,),
    ).fetchone()
    return row["value"] if row else default


def _normalize_post_ids(post_ids):
    """Return ordered unique post ids; reject duplicates and null values."""
    if not post_ids:
        return []

    normalized = []
    seen = set()
    for post_id in post_ids:
        if post_id is None:
            raise DatabaseError("Post id list contains a null id")
        if post_id in seen:
            raise DatabaseError(
                f"Post id list contains duplicate id: {post_id}"
            )
        seen.add(post_id)
        normalized.append(post_id)
    return normalized


def _destination_value(destination_message_ids):
    if destination_message_ids is None:
        return None
    if isinstance(destination_message_ids, str):
        return destination_message_ids
    return json.dumps(list(destination_message_ids))


def backup_database(reason="manual", db_path=None):
    """
    Create a consistent verified snapshot using sqlite3.Connection.backup().

    Preserves the original database and all existing backups.
    Does not delete files. Does not manually copy WAL/SHM sidecars.
    """
    source_path = db_path or DB_NAME
    if not os.path.exists(source_path):
        raise FileNotFoundError(f"Database not found: {source_path}")

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    backup_path = f"telegram_queue_backup_{reason}_{timestamp}.db"
    if os.path.exists(backup_path):
        raise RuntimeError(
            f"Refusing to overwrite existing backup file: {backup_path}"
        )

    with get_connection(source_path) as source_conn:
        pre_posts = source_conn.execute(
            "SELECT COUNT(*) AS c FROM incoming_posts"
        ).fetchone()["c"]
        pre_offset = _get_state(source_conn, "update_offset")
        pre_rows = source_conn.execute(
            """
            SELECT id, channel_id, message_id, status, retry_count
            FROM incoming_posts
            ORDER BY id
            """
        ).fetchall()
        pre_state = source_conn.execute(
            "SELECT key, value FROM bot_state ORDER BY key"
        ).fetchall()
        has_publish_history = source_conn.execute(
            """
            SELECT 1 FROM sqlite_master
            WHERE type='table' AND name='publish_history'
            """
        ).fetchone() is not None
        pre_history = (
            source_conn.execute(
                """
                SELECT id, cycle_id, incoming_post_ids, source_message_ids,
                       media_group_id, destination_message_ids, published_at
                FROM publish_history
                ORDER BY id
                """
            ).fetchall()
            if has_publish_history
            else []
        )

        # Consistent online backup into a new file (includes WAL content).
        backup_conn = sqlite3.connect(backup_path, timeout=30)
        try:
            source_conn.backup(backup_conn)
        finally:
            backup_conn.close()

    verify_conn = sqlite3.connect(backup_path, timeout=30)
    try:
        verify_conn.row_factory = sqlite3.Row
        integrity = verify_conn.execute("PRAGMA integrity_check").fetchone()[0]
        if integrity != "ok":
            raise RuntimeError(f"Backup integrity check failed: {integrity}")

        post_count = verify_conn.execute(
            "SELECT COUNT(*) AS c FROM incoming_posts"
        ).fetchone()["c"]
        offset_row = verify_conn.execute(
            "SELECT value FROM bot_state WHERE key = 'update_offset'"
        ).fetchone()
        offset_value = offset_row["value"] if offset_row else None
        rows = verify_conn.execute(
            """
            SELECT id, channel_id, message_id, status, retry_count
            FROM incoming_posts
            ORDER BY id
            """
        ).fetchall()
        state = verify_conn.execute(
            "SELECT key, value FROM bot_state ORDER BY key"
        ).fetchall()
        has_verified_history = verify_conn.execute(
            """
            SELECT 1 FROM sqlite_master
            WHERE type='table' AND name='publish_history'
            """
        ).fetchone() is not None
        history = (
            verify_conn.execute(
                """
                SELECT id, cycle_id, incoming_post_ids, source_message_ids,
                       media_group_id, destination_message_ids, published_at
                FROM publish_history
                ORDER BY id
                """
            ).fetchall()
            if has_verified_history
            else []
        )
    finally:
        verify_conn.close()

    if post_count != pre_posts:
        raise RuntimeError("Backup verification failed: post count mismatch")
    if offset_value != pre_offset:
        raise RuntimeError("Backup verification failed: offset mismatch")
    if [tuple(r) for r in rows] != [tuple(r) for r in pre_rows]:
        raise RuntimeError("Backup verification failed: row mismatch")
    if [tuple(r) for r in state] != [tuple(r) for r in pre_state]:
        raise RuntimeError("Backup verification failed: bot_state mismatch")
    if [tuple(r) for r in history] != [tuple(r) for r in pre_history]:
        raise RuntimeError("Backup verification failed: publish history mismatch")

    logging.info(
        "Database backup created and verified via Connection.backup(): "
        "%s (posts=%s, offset=%s)",
        backup_path,
        post_count,
        offset_value,
    )
    return {
        "backup_path": backup_path,
        "integrity": integrity,
        "post_count": post_count,
        "update_offset": offset_value,
        "bot_state_keys": [row["key"] for row in state],
    }


def _migrate_schema_on_connection(conn):
    """Apply additive schema upgrades on an open connection."""
    previous_version = _preflight_schema(conn)
    _begin_immediate(conn)
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS incoming_posts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                channel_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                media_group_id TEXT,
                message_type TEXT,
                message_data TEXT NOT NULL,
                received_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                status TEXT DEFAULT 'pending',
                retry_count INTEGER DEFAULT 0,
                last_error TEXT,
                published_at TIMESTAMP,
                claimed_at TIMESTAMP,
                claim_owner TEXT,
                destination_message_ids TEXT,
                cycle_id INTEGER,
                live_eligible INTEGER NOT NULL DEFAULT 1,
                UNIQUE(channel_id, message_id)
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS bot_state (
                key TEXT PRIMARY KEY,
                value TEXT
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS publish_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                cycle_id INTEGER NOT NULL,
                incoming_post_ids TEXT NOT NULL,
                source_message_ids TEXT NOT NULL,
                media_group_id TEXT,
                destination_message_ids TEXT,
                published_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_publish_history_cycle
            ON publish_history (cycle_id, id)
            """
        )

        existing_columns = _column_names(conn, "incoming_posts")
        added_columns = []

        for column_name, column_type in {
            **PHASE3_COLUMNS,
            **NEW_POST_COLUMNS,
        }.items():
            if column_name not in existing_columns:
                conn.execute(
                    f"ALTER TABLE incoming_posts "
                    f"ADD COLUMN {column_name} {column_type}"
                )
                added_columns.append(column_name)

        if previous_version != SCHEMA_VERSION:
            conn.execute(
                "UPDATE incoming_posts SET live_eligible = 0"
            )
        if previous_version != SCHEMA_VERSION:
            conn.execute(
                "DELETE FROM bot_state WHERE key IN "
                "('go_live_update_id', 'go_live_first_update_id')"
            )
            _set_state(conn, "go_live_bootstrap_status", GO_LIVE_PENDING)
        elif _get_state(conn, "go_live_bootstrap_status") is None:
            _set_state(conn, "go_live_bootstrap_status", GO_LIVE_PENDING)
        if _get_state(conn, "go_live_first_update_id") is None:
            _set_state(conn, "go_live_first_update_id", "")

        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_incoming_posts_queue
            ON incoming_posts (status, retry_count, message_id DESC)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_incoming_posts_media_group
            ON incoming_posts (media_group_id)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_incoming_posts_claim
            ON incoming_posts (status, claimed_at)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_incoming_posts_cycle
            ON incoming_posts (cycle_id)
            """
        )

        if _get_state(conn, "publish_cycle_id") is None:
            _set_state(conn, "publish_cycle_id", "1")

        if _get_state(conn, "cycle_post_number") is None:
            _set_state(conn, "cycle_post_number", "0")

        _set_state(conn, "schema_version", SCHEMA_VERSION)
        _set_state(
            conn,
            "schema_migrated_at",
            datetime.now(timezone.utc).isoformat(),
        )

        _commit(conn)

        logging.info(
            "Schema migration complete. Added columns: %s",
            added_columns or "none (already up to date)",
        )
        return {
            "added_columns": added_columns,
            "schema_version": SCHEMA_VERSION,
        }
    except Exception:
        _safe_rollback(conn)
        raise


def migrate_schema(conn=None):
    """
    Apply additive, backward-compatible schema upgrades.

    - Does not delete tables, rows, or database files
    - Preserves update_offset and all existing post records
    - Safe to run repeatedly
    """
    if conn is not None:
        return _migrate_schema_on_connection(conn)

    with get_connection() as owned_conn:
        return _migrate_schema_on_connection(owned_conn)


def initialize_database():
    """Create tables if needed and apply additive migrations."""
    with get_connection() as conn:
        migrate_schema(conn)
    logging.info("Database initialized successfully.")


def verify_schema():
    """
    Verify Phase 3 schema expectations.

    Returns a dict of checks. Raises RuntimeError if critical checks fail.
    """
    required_columns = {
        "id",
        "channel_id",
        "message_id",
        "media_group_id",
        "message_type",
        "message_data",
        "received_at",
        "status",
        "retry_count",
        "last_error",
        "published_at",
        "claimed_at",
        "claim_owner",
        "destination_message_ids",
        "cycle_id",
        "live_eligible",
    }
    required_indexes = {
        "idx_incoming_posts_queue",
        "idx_incoming_posts_media_group",
        "idx_incoming_posts_claim",
        "idx_incoming_posts_cycle",
        "idx_publish_history_cycle",
        "sqlite_autoindex_incoming_posts_1",
    }

    with get_connection() as conn:
        columns = _column_names(conn, "incoming_posts")
        indexes = {
            row["name"]
            for row in conn.execute("PRAGMA index_list(incoming_posts)")
        }
        journal_mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
        schema_version = _get_state(conn, "schema_version")
        update_offset = _get_state(conn, "update_offset")
        publish_cycle_id = _get_state(conn, "publish_cycle_id")
        tables = {
            row["name"]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        history_indexes = {
            row["name"]
            for row in conn.execute("PRAGMA index_list(publish_history)")
        } if "publish_history" in tables else set()
        indexes.update(history_indexes)
        post_count = conn.execute(
            "SELECT COUNT(*) AS c FROM incoming_posts"
        ).fetchone()["c"]
        integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]

    missing_columns = sorted(required_columns - columns)
    missing_indexes = sorted(required_indexes - indexes)

    result = {
        "integrity": integrity,
        "journal_mode": journal_mode,
        "schema_version": schema_version,
        "update_offset": update_offset,
        "publish_cycle_id": publish_cycle_id,
        "post_count": post_count,
        "missing_columns": missing_columns,
        "missing_indexes": missing_indexes,
        "ok": (
            integrity == "ok"
            and not missing_columns
            and not missing_indexes
            and schema_version == SCHEMA_VERSION
            and "publish_history" in tables
            and str(journal_mode).lower() == "wal"
        ),
    }

    if not result["ok"]:
        raise RuntimeError(f"Schema verification failed: {result}")

    return result


def identify_message_type(message):
    for message_type in (
        "photo",
        "video",
        "animation",
        "document",
        "audio",
        "voice",
        "text",
    ):
        if message_type in message:
            return message_type
    return "other"


def _insert_incoming_post(conn, message):
    """
    Insert one incoming post using INSERT OR IGNORE.

    Returns (inserted: bool, message_type: str).
    """
    chat = message.get("chat") or {}
    channel_id = chat.get("id")
    message_id = message.get("message_id")
    if channel_id is None or message_id is None:
        raise DatabaseError(
            "Incoming Telegram post is missing chat.id or message_id"
        )

    message_type = identify_message_type(message)
    cursor = conn.execute(
        """
        INSERT OR IGNORE INTO incoming_posts (
            channel_id,
            message_id,
            media_group_id,
            message_type,
            message_data
        )
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            channel_id,
            message_id,
            message.get("media_group_id"),
            message_type,
            json.dumps(message),
        ),
    )
    return cursor.rowcount == 1, message_type


def save_post(message):
    """Reject inserts that bypass the update-id and offset transaction."""
    raise DatabaseError(
        "save_post() is unsafe for listener ingestion; use "
        "process_telegram_update() with the Telegram update_id."
    )


def get_offset():
    """Retrieve the last processed Telegram update offset."""
    with get_connection() as conn:
        value = _get_state(conn, "update_offset")
    return int(value) if value is not None else None


def get_go_live_state(conn=None):
    """Return persisted bootstrap status and exclusive first eligible ID."""
    def read_state(connection):
        status = _get_state(connection, "go_live_bootstrap_status")
        first_update_id = _get_state(
            connection,
            "go_live_first_update_id",
        )
        if first_update_id == "":
            first_update_id = None
        elif first_update_id is not None:
            first_update_id = int(first_update_id)
        return {
            "status": status,
            "first_update_id": first_update_id,
        }

    if conn is not None:
        return read_state(conn)
    with get_connection() as connection:
        return read_state(connection)


def begin_go_live_bootstrap():
    """Persist intent before the external boundary request; never auto-reset."""
    with get_connection() as conn:
        _begin_immediate(conn)
        try:
            status = _get_state(conn, "go_live_bootstrap_status")
            first_update_id = _get_state(conn, "go_live_first_update_id")
            if status == GO_LIVE_READY:
                _commit(conn)
                return {
                    "status": status,
                    "first_update_id": (
                        int(first_update_id)
                        if first_update_id not in (None, "")
                        else None
                    ),
                }
            if status != GO_LIVE_PENDING:
                raise DatabaseError(
                    "Go-live bootstrap is ambiguous or blocked "
                    f"(state={status or 'missing'}); manual recovery is "
                    "required."
                )
            _set_state(conn, "go_live_bootstrap_status", GO_LIVE_CAPTURING)
            _commit(conn)
            return {"status": GO_LIVE_CAPTURING, "first_update_id": None}
        except Exception:
            _safe_rollback(conn)
            raise


def complete_go_live_bootstrap(boundary_update_id):
    """Persist the first eligible update ID and listener cursor atomically."""
    first_update_id = (
        None if boundary_update_id is None else int(boundary_update_id) + 1
    )
    with get_connection() as conn:
        _begin_immediate(conn)
        try:
            status = _get_state(conn, "go_live_bootstrap_status")
            if status != GO_LIVE_CAPTURING:
                raise DatabaseError(
                    "Go-live bootstrap completion requires an active "
                    "capture state."
                )
            _set_state(
                conn,
                "go_live_first_update_id",
                "" if first_update_id is None else first_update_id,
            )
            conn.execute(
                "DELETE FROM bot_state WHERE key = 'update_offset'"
            )
            if first_update_id is not None:
                _set_state(conn, "update_offset", first_update_id)
            _set_state(conn, "go_live_bootstrap_status", GO_LIVE_READY)
            _commit(conn)
        except Exception:
            _safe_rollback(conn)
            raise
    return first_update_id


def mark_go_live_webhook_conflict():
    """Persist a confirmed Telegram webhook conflict; operator action needed."""
    with get_connection() as conn:
        _begin_immediate(conn)
        try:
            if _get_state(conn, "go_live_bootstrap_status") != GO_LIVE_CAPTURING:
                raise DatabaseError("No active go-live bootstrap to block.")
            _set_state(
                conn,
                "go_live_bootstrap_status",
                GO_LIVE_WEBHOOK_CONFLICT,
            )
            _commit(conn)
        except Exception:
            _safe_rollback(conn)
            raise


def recover_go_live_bootstrap(boundary_update_id, operator_confirmed=False):
    """Manually finalize an ambiguous bootstrap using an operator-reviewed ID."""
    if operator_confirmed is not True:
        raise DatabaseError(
            "Manual go-live recovery requires explicit operator confirmation."
        )
    first_update_id = (
        None if boundary_update_id is None else int(boundary_update_id) + 1
    )
    with get_connection() as conn:
        _begin_immediate(conn)
        try:
            status = _get_state(conn, "go_live_bootstrap_status")
            if status not in {GO_LIVE_CAPTURING, GO_LIVE_WEBHOOK_CONFLICT}:
                raise DatabaseError(
                    "Manual go-live recovery is only valid for an ambiguous "
                    "or webhook-blocked bootstrap."
                )
            _set_state(
                conn,
                "go_live_first_update_id",
                "" if first_update_id is None else first_update_id,
            )
            conn.execute(
                "DELETE FROM bot_state WHERE key = 'update_offset'"
            )
            if first_update_id is not None:
                _set_state(conn, "update_offset", first_update_id)
            _set_state(conn, "go_live_bootstrap_status", GO_LIVE_READY)
            _commit(conn)
        except Exception:
            _safe_rollback(conn)
            raise
    return first_update_id


def require_go_live_ready():
    state = get_go_live_state()
    if state["status"] != GO_LIVE_READY:
        raise DatabaseError(
            "Go-live update boundary is not initialized; refusing to poll."
        )
    return state


def save_offset(offset):
    """Save the Telegram update offset."""
    with get_connection() as conn:
        _begin_immediate(conn)
        try:
            _set_state(conn, "update_offset", offset)
            _commit(conn)
        except Exception:
            _safe_rollback(conn)
            raise


def process_telegram_update(update, source_channel_id):
    """
    Atomically process one Telegram getUpdates result item.

    Rules:
    - Always persist next_offset = update_id + 1 when processing succeeds.
    - For matching source-channel posts, INSERT OR IGNORE the post and
      advance the offset in the same transaction.
    - If post insertion fails, the offset is not advanced.
    - Unrelated updates only advance the offset.
    - Reprocessing never creates duplicate rows (UNIQUE channel_id, message_id).

    Returns a result dict for listener logging.
    """
    if "update_id" not in update:
        raise DatabaseError("Telegram update is missing update_id")

    next_offset = int(update["update_id"]) + 1
    update_id = int(update["update_id"])
    channel_post = update.get("channel_post")

    result = {
        "next_offset": next_offset,
        "inserted": False,
        "duplicate": False,
        "ignored": True,
        "message_id": None,
        "message_type": None,
        "media_group_id": None,
        "channel_id": None,
        "pre_live_ignored": False,
    }

    with get_connection() as conn:
        _begin_immediate(conn)
        try:
            bootstrap_status = _get_state(
                conn,
                "go_live_bootstrap_status",
            )
            if bootstrap_status != GO_LIVE_READY:
                raise DatabaseError(
                    "Go-live update boundary is not ready; refusing to "
                    "process or acknowledge Telegram updates."
                )
            first_update_id_value = _get_state(
                conn,
                "go_live_first_update_id",
            )
            first_update_id = (
                None
                if first_update_id_value in (None, "")
                else int(first_update_id_value)
            )
            pre_go_live = (
                first_update_id is not None
                and update_id < first_update_id
            )

            if channel_post is not None:
                chat = channel_post.get("chat") or {}
                chat_id = chat.get("id")
                result["channel_id"] = chat_id

                if str(chat_id) == str(source_channel_id) and not pre_go_live:
                    result["message_id"] = channel_post.get("message_id")
                    result["media_group_id"] = channel_post.get(
                        "media_group_id"
                    )

                    result["ignored"] = False
                    inserted, message_type = _insert_incoming_post(
                        conn,
                        channel_post,
                    )
                    result["message_type"] = message_type
                    result["inserted"] = inserted
                    result["duplicate"] = not inserted
            if pre_go_live:
                result["pre_live_ignored"] = True
                result["ignored"] = True

            current_offset = _get_state(conn, "update_offset")
            if current_offset is None or next_offset > int(current_offset):
                _set_state(conn, "update_offset", next_offset)
            _commit(conn)
        except Exception:
            _safe_rollback(conn)
            raise

    return result


def get_publish_cycle_id():
    """Return the current publishing cycle id."""
    with get_connection() as conn:
        value = _get_state(conn, "publish_cycle_id", "1")
    return int(value)


def get_post_number():
    """Return the current in-cycle publishing sequence number."""
    with get_connection() as conn:
        value = _get_state(conn, "cycle_post_number", "0")
    return int(value)


def update_post_number(number):
    """Persist the in-cycle publishing sequence number."""
    with get_connection() as conn:
        _begin_immediate(conn)
        try:
            _set_state(conn, "cycle_post_number", number)
            _commit(conn)
        except Exception:
            _safe_rollback(conn)
            raise


STALE_CLAIM_ERROR = (
    "STALE_CLAIM_MANUAL_REVIEW: Publishing claim expired while status was "
    "'publishing'. Telegram may already have accepted the copy. Automatic "
    "republish is disabled; inspect the destination channel and "
    "destination_message_ids before manual reset."
)


def release_stale_claims(stale_seconds=DEFAULT_STALE_CLAIM_SECONDS):
    """
    Move stale publishing claims to failed for manual review.

    Telegram may have accepted a copy before the scheduler crashed. Stale
    claims are therefore NOT returned to pending and are never auto-retried.

    - status -> failed
    - claimed_at / claim_owner cleared
    - destination_message_ids preserved
    - descriptive last_error recorded
    """
    with get_connection() as conn:
        _begin_immediate(conn)
        try:
            cursor = conn.execute(
                """
                UPDATE incoming_posts
                SET status = 'failed',
                    claimed_at = NULL,
                    claim_owner = NULL,
                    last_error = ?
                WHERE status = 'publishing'
                  AND (
                        claimed_at IS NULL
                        OR claimed_at <= datetime('now', ?)
                  )
                """,
                (
                    STALE_CLAIM_ERROR,
                    f"-{int(stale_seconds)} seconds",
                ),
            )
            released = cursor.rowcount
            _commit(conn)
        except Exception:
            _safe_rollback(conn)
            raise

    if released:
        logging.warning(
            "Moved %s stale publishing claim(s) older than %s seconds "
            "to failed for manual review (no automatic republish)",
            released,
            stale_seconds,
        )
    return released


def claim_posts(post_ids, claim_owner):
    """
    Atomically claim pending posts for publishing (all-or-nothing).

    If any requested id cannot be claimed, the entire transaction is rolled
    back and ClaimError is raised. Partial album claims never succeed.
    """
    if not claim_owner:
        raise ClaimError("claim_owner is required")

    requested_ids = _normalize_post_ids(post_ids)
    if not requested_ids:
        return []

    placeholders = ",".join("?" for _ in requested_ids)

    with get_connection() as conn:
        _begin_immediate(conn)
        try:
            cycle_id = int(_get_state(conn, "publish_cycle_id", "1"))

            eligible_rows = conn.execute(
                f"""
                SELECT id
                FROM incoming_posts
                WHERE id IN ({placeholders})
                  AND status = 'pending'
                  AND live_eligible = 1
                ORDER BY id
                """,
                requested_ids,
            ).fetchall()
            eligible_ids = {row["id"] for row in eligible_rows}

            missing_or_unclaimable = [
                post_id
                for post_id in requested_ids
                if post_id not in eligible_ids
            ]
            if missing_or_unclaimable:
                raise ClaimError(
                    "All-or-nothing claim failed; unclaimable ids: "
                    f"{missing_or_unclaimable}"
                )

            cursor = conn.execute(
                f"""
                UPDATE incoming_posts
                SET status = 'publishing',
                    claimed_at = CURRENT_TIMESTAMP,
                    claim_owner = ?,
                    cycle_id = ?
                WHERE id IN ({placeholders})
                  AND status = 'pending'
                  AND live_eligible = 1
                """,
                [claim_owner, cycle_id, *requested_ids],
            )

            if cursor.rowcount != len(requested_ids):
                raise ClaimError(
                    "All-or-nothing claim failed during update; "
                    f"requested={len(requested_ids)} "
                    f"updated={cursor.rowcount}"
                )

            claimed_rows = conn.execute(
                f"""
                SELECT id
                FROM incoming_posts
                WHERE id IN ({placeholders})
                  AND status = 'publishing'
                  AND claim_owner = ?
                ORDER BY id
                """,
                [*requested_ids, claim_owner],
            ).fetchall()
            claimed_ids = [row["id"] for row in claimed_rows]

            if len(claimed_ids) != len(requested_ids):
                raise ClaimError(
                    "All-or-nothing claim verification failed; "
                    f"requested={requested_ids} claimed={claimed_ids}"
                )

            _commit(conn)
        except Exception:
            _safe_rollback(conn)
            raise

    return claimed_ids


def release_claim(
    post_ids,
    claim_owner=None,
    error=None,
    increment_retry=False,
    max_retries=5,
):
    """
    Release a publishing claim back to pending/failed after a failed attempt.

    When claim_owner is provided, only that owner's claims are released.
    """
    requested_ids = _normalize_post_ids(post_ids)
    if not requested_ids:
        return 0

    placeholders = ",".join("?" for _ in requested_ids)
    owner_clause = ""
    params_prefix = []
    if claim_owner is not None:
        owner_clause = " AND claim_owner = ?"
        params_prefix = [claim_owner]

    with get_connection() as conn:
        _begin_immediate(conn)
        try:
            if increment_retry:
                cursor = conn.execute(
                    f"""
                    UPDATE incoming_posts
                    SET retry_count = retry_count + 1,
                        last_error = ?,
                        claimed_at = NULL,
                        claim_owner = NULL,
                        status = CASE
                            WHEN retry_count + 1 >= ?
                            THEN 'failed'
                            ELSE 'pending'
                        END
                    WHERE id IN ({placeholders})
                      AND status = 'publishing'
                      {owner_clause}
                    """,
                    [
                        str(error) if error is not None else None,
                        max_retries,
                        *requested_ids,
                        *params_prefix,
                    ],
                )
            else:
                cursor = conn.execute(
                    f"""
                    UPDATE incoming_posts
                    SET status = 'pending',
                        claimed_at = NULL,
                        claim_owner = NULL,
                        last_error = COALESCE(?, last_error)
                    WHERE id IN ({placeholders})
                      AND status = 'publishing'
                      {owner_clause}
                    """,
                    [
                        str(error) if error is not None else None,
                        *requested_ids,
                        *params_prefix,
                    ],
                )
            released = cursor.rowcount
            _commit(conn)
        except Exception:
            _safe_rollback(conn)
            raise

    return released


def mark_as_published(post_ids, claim_owner, destination_message_ids=None):
    """
    Finalize only posts claimed by this owner with status='publishing'.

    Pending rows cannot be marked published.
    Claim ownership is verified and cleared after success.
    All requested ids must finalize or the transaction rolls back.
    """
    if not claim_owner:
        raise PublishOwnershipError("claim_owner is required")

    requested_ids = _normalize_post_ids(post_ids)
    if not requested_ids:
        return 0

    destination_value = _destination_value(destination_message_ids)
    placeholders = ",".join("?" for _ in requested_ids)

    with get_connection() as conn:
        _begin_immediate(conn)
        try:
            cycle_id = int(_get_state(conn, "publish_cycle_id", "1"))

            owned = conn.execute(
                f"""
                SELECT id, message_id, media_group_id
                FROM incoming_posts
                WHERE id IN ({placeholders})
                  AND status = 'publishing'
                  AND claim_owner = ?
                """,
                [*requested_ids, claim_owner],
            ).fetchall()
            owned_ids = {row["id"] for row in owned}

            if len(owned_ids) != len(requested_ids):
                missing = [
                    post_id
                    for post_id in requested_ids
                    if post_id not in owned_ids
                ]
                raise PublishOwnershipError(
                    "Publish ownership check failed for ids: "
                    f"{missing} (owner={claim_owner})"
                )

            owned_by_id = {row["id"]: row for row in owned}
            ordered_rows = [owned_by_id[post_id] for post_id in requested_ids]
            media_group_ids = {
                row["media_group_id"]
                for row in ordered_rows
                if row["media_group_id"] is not None
            }
            media_group_id = (
                next(iter(media_group_ids))
                if len(media_group_ids) == 1
                else None
            )
            conn.execute(
                """
                INSERT INTO publish_history (
                    cycle_id,
                    incoming_post_ids,
                    source_message_ids,
                    media_group_id,
                    destination_message_ids
                )
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    cycle_id,
                    json.dumps(requested_ids),
                    json.dumps([row["message_id"] for row in ordered_rows]),
                    media_group_id,
                    destination_value,
                ),
            )

            cursor = conn.execute(
                f"""
                UPDATE incoming_posts
                SET status = 'published',
                    published_at = CURRENT_TIMESTAMP,
                    last_error = NULL,
                    claimed_at = NULL,
                    claim_owner = NULL,
                    destination_message_ids = COALESCE(
                        ?,
                        destination_message_ids
                    ),
                    cycle_id = ?
                WHERE id IN ({placeholders})
                  AND status = 'publishing'
                  AND claim_owner = ?
                """,
                [
                    destination_value,
                    cycle_id,
                    *requested_ids,
                    claim_owner,
                ],
            )

            if cursor.rowcount != len(requested_ids):
                raise PublishOwnershipError(
                    "Publish finalization updated unexpected row count: "
                    f"requested={len(requested_ids)} "
                    f"updated={cursor.rowcount}"
                )

            _commit(conn)
        except Exception:
            _safe_rollback(conn)
            raise

    return len(requested_ids)


def mark_as_uncertain(
    post_ids,
    claim_owner,
    destination_message_ids,
    error,
):
    """Quarantine posts accepted by Telegram but not finalized locally."""
    if not claim_owner:
        raise PublishOwnershipError("claim_owner is required")

    requested_ids = _normalize_post_ids(post_ids)
    if not requested_ids:
        return 0

    destination_value = _destination_value(destination_message_ids)
    placeholders = ",".join("?" for _ in requested_ids)
    with get_connection() as conn:
        _begin_immediate(conn)
        try:
            rows = conn.execute(
                f"""
                SELECT id
                FROM incoming_posts
                WHERE id IN ({placeholders})
                  AND status = 'publishing'
                  AND claim_owner = ?
                """,
                [*requested_ids, claim_owner],
            ).fetchall()
            if len(rows) != len(requested_ids):
                raise PublishOwnershipError(
                    "Uncertain publish ownership check failed for ids: "
                    f"{requested_ids}"
                )

            cursor = conn.execute(
                f"""
                UPDATE incoming_posts
                SET status = 'uncertain',
                    last_error = ?,
                    destination_message_ids = COALESCE(
                        ?,
                        destination_message_ids
                    )
                WHERE id IN ({placeholders})
                  AND status = 'publishing'
                  AND claim_owner = ?
                """,
                [
                    str(error),
                    destination_value,
                    *requested_ids,
                    claim_owner,
                ],
            )
            if cursor.rowcount != len(requested_ids):
                raise PublishOwnershipError(
                    "Uncertain publish update changed an unexpected number "
                    f"of rows: expected={len(requested_ids)} "
                    f"updated={cursor.rowcount}"
                )
            _commit(conn)
        except Exception:
            _safe_rollback(conn)
            raise

    return len(requested_ids)


def reconcile_uncertain_posts(
    post_ids,
    resolution,
    destination_message_ids=None,
):
    """
    Manually reconcile quarantined posts as published or safe to retry.

    For ``published``, an operator must supply one destination message id per
    requested source post after verifying Telegram's destination state.
    For ``not_published``, posts return to pending only after confirming no
    copy was accepted. Complete albums must be reconciled as a whole.
    """
    requested_ids = _normalize_post_ids(post_ids)
    if not requested_ids:
        return 0
    if resolution not in {"published", "not_published"}:
        raise ValueError(
            "resolution must be 'published' or 'not_published'"
        )

    destination_ids = None
    if resolution == "published":
        if not isinstance(destination_message_ids, (list, tuple)):
            raise ValueError(
                "destination_message_ids is required when resolving "
                "posts as published"
            )
        destination_ids = list(destination_message_ids)
        if (
            len(destination_ids) != len(requested_ids)
            or any(message_id is None for message_id in destination_ids)
        ):
            raise ValueError(
                "Provide one non-null destination message id per source post"
            )
    elif destination_message_ids is not None:
        raise ValueError(
            "destination_message_ids are only valid for a published "
            "resolution"
        )

    placeholders = ",".join("?" for _ in requested_ids)
    with get_connection() as conn:
        _begin_immediate(conn)
        try:
            rows = conn.execute(
                f"""
                SELECT id, message_id, media_group_id, status
                FROM incoming_posts
                WHERE id IN ({placeholders})
                ORDER BY id
                """,
                requested_ids,
            ).fetchall()
            if (
                len(rows) != len(requested_ids)
                or any(row["status"] != "uncertain" for row in rows)
            ):
                raise DatabaseError(
                    "Only existing uncertain posts can be reconciled"
                )

            selected = {row["id"] for row in rows}
            for media_group_id in {
                row["media_group_id"]
                for row in rows
                if row["media_group_id"] is not None
            }:
                album_ids = {
                    row["id"]
                    for row in conn.execute(
                        """
                        SELECT id FROM incoming_posts
                        WHERE media_group_id = ?
                        """,
                        (media_group_id,),
                    ).fetchall()
                }
                if not album_ids.issubset(selected):
                    raise DatabaseError(
                        "Uncertain albums must be reconciled as a whole"
                    )

            if resolution == "not_published":
                cursor = conn.execute(
                    f"""
                    UPDATE incoming_posts
                    SET status = 'pending',
                        retry_count = 0,
                        last_error = NULL,
                        claimed_at = NULL,
                        claim_owner = NULL
                    WHERE id IN ({placeholders})
                      AND status = 'uncertain'
                    """,
                    requested_ids,
                )
                if cursor.rowcount != len(requested_ids):
                    raise DatabaseError(
                        "Uncertain-post retry reconciliation was incomplete"
                    )
            else:
                rows_by_id = {row["id"]: row for row in rows}
                destination_by_id = dict(zip(requested_ids, destination_ids))
                cycle_id = int(_get_state(conn, "publish_cycle_id", "1"))
                groups = {}
                for post_id in requested_ids:
                    row = rows_by_id[post_id]
                    key = (
                        ("album", row["media_group_id"])
                        if row["media_group_id"] is not None
                        else ("single", post_id)
                    )
                    groups.setdefault(key, []).append(row)

                for group in groups.values():
                    ordered_group = sorted(
                        group,
                        key=lambda row: row["message_id"],
                    )
                    group_ids = [row["id"] for row in ordered_group]
                    group_message_ids = [
                        row["message_id"] for row in ordered_group
                    ]
                    group_destination_ids = [
                        destination_by_id[post_id] for post_id in group_ids
                    ]
                    media_group_id = ordered_group[0]["media_group_id"]
                    conn.execute(
                        """
                        INSERT INTO publish_history (
                            cycle_id, incoming_post_ids, source_message_ids,
                            media_group_id, destination_message_ids
                        )
                        VALUES (?, ?, ?, ?, ?)
                        """,
                        (
                            cycle_id,
                            json.dumps(group_ids),
                            json.dumps(group_message_ids),
                            media_group_id,
                            json.dumps(group_destination_ids),
                        ),
                    )

                cursor = conn.execute(
                    f"""
                    UPDATE incoming_posts
                    SET status = 'published',
                        published_at = CURRENT_TIMESTAMP,
                        last_error = NULL,
                        claimed_at = NULL,
                        claim_owner = NULL,
                        destination_message_ids = ?,
                        cycle_id = ?
                    WHERE id IN ({placeholders})
                      AND status = 'uncertain'
                    """,
                    [
                        json.dumps(destination_ids),
                        cycle_id,
                        *requested_ids,
                    ],
                )
                if cursor.rowcount != len(requested_ids):
                    raise DatabaseError(
                        "Uncertain-post published reconciliation was "
                        "incomplete"
                    )

            _commit(conn)
        except Exception:
            _safe_rollback(conn)
            raise

    logging.warning(
        "Manually reconciled %s uncertain post(s) as %s",
        len(requested_ids),
        resolution,
    )
    return len(requested_ids)


def mark_as_failed(post_ids, error, max_retries=5, claim_owner=None):
    """
    Record publishing errors and manage retries.

    When claim_owner is supplied (scheduler path):
    - every requested row must already be status='publishing'
    - every requested row must have claim_owner exactly equal to the supplied
      owner (NULL ownership is rejected)
    - validation happens before any mutation
    - the update is all-or-nothing; partial album updates are rejected

    When claim_owner is omitted (manual/operator path), rows are updated
    without ownership checks.

    Permanently failed posts stay failed until manually reviewed and reset.
    """
    requested_ids = _normalize_post_ids(post_ids)
    if not requested_ids:
        return 0

    placeholders = ",".join("?" for _ in requested_ids)

    with get_connection() as conn:
        _begin_immediate(conn)
        try:
            if claim_owner is not None:
                if claim_owner == "":
                    raise PublishOwnershipError(
                        "claim_owner must be a non-empty string when provided"
                    )

                rows = conn.execute(
                    f"""
                    SELECT id, status, claim_owner
                    FROM incoming_posts
                    WHERE id IN ({placeholders})
                    """,
                    requested_ids,
                ).fetchall()
                found = {row["id"]: row for row in rows}

                invalid_ids = []
                for post_id in requested_ids:
                    row = found.get(post_id)
                    if row is None:
                        invalid_ids.append(post_id)
                        continue
                    if row["status"] != "publishing":
                        invalid_ids.append(post_id)
                        continue
                    if row["claim_owner"] is None or row["claim_owner"] != claim_owner:
                        invalid_ids.append(post_id)

                if invalid_ids:
                    raise PublishOwnershipError(
                        "mark_as_failed ownership check failed for ids: "
                        f"{invalid_ids} (owner={claim_owner})"
                    )

                cursor = conn.execute(
                    f"""
                    UPDATE incoming_posts
                    SET retry_count = retry_count + 1,
                        last_error = ?,
                        claimed_at = NULL,
                        claim_owner = NULL,
                        status = CASE
                            WHEN retry_count + 1 >= ?
                            THEN 'failed'
                            ELSE 'pending'
                        END
                    WHERE id IN ({placeholders})
                      AND status = 'publishing'
                      AND claim_owner = ?
                    """,
                    [str(error), max_retries, *requested_ids, claim_owner],
                )
                if cursor.rowcount != len(requested_ids):
                    raise PublishOwnershipError(
                        "mark_as_failed updated unexpected row count: "
                        f"requested={len(requested_ids)} "
                        f"updated={cursor.rowcount}"
                    )
                updated = cursor.rowcount
            else:
                cursor = conn.execute(
                    f"""
                    UPDATE incoming_posts
                    SET retry_count = retry_count + 1,
                        last_error = ?,
                        claimed_at = NULL,
                        claim_owner = NULL,
                        status = CASE
                            WHEN retry_count + 1 >= ?
                            THEN 'failed'
                            ELSE 'pending'
                        END
                    WHERE id IN ({placeholders})
                    """,
                    [str(error), max_retries, *requested_ids],
                )
                updated = cursor.rowcount

            _commit(conn)
        except Exception:
            _safe_rollback(conn)
            raise

    return updated


def reset_failed_posts(post_ids=None):
    """
    Manually reset permanently failed posts back to pending.

    If post_ids is None, resets all failed posts after operator review.
    """
    with get_connection() as conn:
        _begin_immediate(conn)
        try:
            if post_ids is not None:
                requested_ids = _normalize_post_ids(post_ids)
                placeholders = ",".join("?" for _ in requested_ids)
                cursor = conn.execute(
                    f"""
                    UPDATE incoming_posts
                    SET status = 'pending',
                        retry_count = 0,
                        last_error = NULL,
                        claimed_at = NULL,
                        claim_owner = NULL
                    WHERE status = 'failed'
                      AND id IN ({placeholders})
                    """,
                    requested_ids,
                )
            else:
                cursor = conn.execute(
                    """
                    UPDATE incoming_posts
                    SET status = 'pending',
                        retry_count = 0,
                        last_error = NULL,
                        claimed_at = NULL,
                        claim_owner = NULL
                    WHERE status = 'failed'
                    """
                )
            reset_count = cursor.rowcount
            _commit(conn)
        except Exception:
            _safe_rollback(conn)
            raise

    logging.info("Manually reset %s failed post(s) to pending", reset_count)
    return reset_count


def restart_posting_cycle():
    """
    Restart the publishing cycle when no eligible pending posts remain.

    Business rules:
    - Reset published posts to pending
    - Continuously restart from the newest eligible post
    - Do NOT automatically requeue permanently failed posts (Option A)
    - Preserve destination_message_ids as last-known publish evidence
      (not cleared; see DATABASE.md for publish_history recommendation)
    """
    with get_connection() as conn:
        _begin_immediate(conn)
        try:
            pending = conn.execute(
                """
                SELECT COUNT(*) AS c
                FROM incoming_posts
                WHERE status = 'pending'
                  AND retry_count < 5
                  AND live_eligible = 1
                """
            ).fetchone()["c"]

            publishing = conn.execute(
                """
                SELECT COUNT(*) AS c
                FROM incoming_posts
                WHERE status = 'publishing'
                  AND live_eligible = 1
                """
            ).fetchone()["c"]

            uncertain = conn.execute(
                """
                SELECT COUNT(*) AS c
                FROM incoming_posts
                WHERE status = 'uncertain'
                  AND live_eligible = 1
                """
            ).fetchone()["c"]

            published = conn.execute(
                """
                SELECT COUNT(*) AS c
                FROM incoming_posts
                WHERE status = 'published'
                  AND live_eligible = 1
                """
            ).fetchone()["c"]

            # Do not restart while claims are in flight.
            if (
                pending == 0
                and publishing == 0
                and uncertain == 0
                and published > 0
            ):
                current_cycle = int(_get_state(conn, "publish_cycle_id", "1"))
                next_cycle = current_cycle + 1

                conn.execute(
                    """
                    UPDATE incoming_posts
                    SET status = 'pending',
                        retry_count = 0,
                        last_error = NULL,
                        claimed_at = NULL,
                        claim_owner = NULL
                    WHERE status = 'published'
                      AND live_eligible = 1
                    """
                )

                _set_state(conn, "publish_cycle_id", next_cycle)
                _set_state(conn, "cycle_post_number", 0)
                _set_state(
                    conn,
                    "last_cycle_restart_at",
                    datetime.now(timezone.utc).isoformat(),
                )
                _commit(conn)

                logging.info(
                    "Publishing cycle restarted. New cycle_id=%s "
                    "(failed posts unchanged; destination_message_ids preserved)",
                    next_cycle,
                )
                return True

            _commit(conn)
        except Exception:
            _safe_rollback(conn)
            raise

    return False


def get_queue_stats():
    """Return simple queue counters for health/monitoring."""
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT status, COUNT(*) AS c
            FROM incoming_posts
            WHERE live_eligible = 1
            GROUP BY status
            """
        ).fetchall()
        ineligible_count = conn.execute(
            "SELECT COUNT(*) FROM incoming_posts WHERE live_eligible = 0"
        ).fetchone()[0]
        offset = _get_state(conn, "update_offset")
        cycle_id = _get_state(conn, "publish_cycle_id", "1")
        post_number = _get_state(conn, "cycle_post_number", "0")
        schema_version = _get_state(conn, "schema_version")

    counts = {row["status"]: row["c"] for row in rows}
    return {
        "counts": counts,
        "ineligible_count": ineligible_count,
        "update_offset": offset,
        "publish_cycle_id": int(cycle_id),
        "cycle_post_number": int(post_number),
        "schema_version": schema_version,
    }


def record_heartbeat(component):
    """Persist an ISO UTC heartbeat for listener or scheduler."""
    if component not in ("listener", "scheduler"):
        raise DatabaseError("component must be 'listener' or 'scheduler'")
    key = f"{component}_heartbeat"
    stamp = datetime.now(timezone.utc).isoformat()
    with get_connection() as conn:
        _begin_immediate(conn)
        try:
            _set_state(conn, key, stamp)
            _commit(conn)
        except Exception:
            _safe_rollback(conn)
            raise
    return stamp


def restore_database_to_path(backup_path, target_path):
    """
    Restore a verified backup into a NEW target path using Connection.backup().

    Refuses to write over the configured live DB_PATH. Refuses to overwrite an
    existing target file. Does not delete the backup or the live database.
    """
    live_path = os.path.abspath(DB_NAME)
    target_abs = os.path.abspath(target_path)
    backup_abs = os.path.abspath(backup_path)

    if not os.path.exists(backup_abs):
        raise FileNotFoundError(f"Backup not found: {backup_abs}")
    if target_abs == live_path:
        raise RuntimeError(
            "Refusing to restore over the configured live database path. "
            "Restore only into a separate test/recovery file."
        )
    if os.path.exists(target_abs):
        raise RuntimeError(
            f"Refusing to overwrite existing target database: {target_abs}"
        )

    source = sqlite3.connect(backup_abs, timeout=30)
    dest = sqlite3.connect(target_abs, timeout=30)
    try:
        source.backup(dest)
    finally:
        dest.close()
        source.close()

    verify = sqlite3.connect(target_abs, timeout=30)
    try:
        integrity = verify.execute("PRAGMA integrity_check").fetchone()[0]
        if integrity != "ok":
            raise RuntimeError(f"Restored database integrity check failed: {integrity}")
        post_count = verify.execute(
            "SELECT COUNT(*) FROM incoming_posts"
        ).fetchone()[0]
        offset_row = verify.execute(
            "SELECT value FROM bot_state WHERE key='update_offset'"
        ).fetchone()
        offset_value = offset_row[0] if offset_row else None
    finally:
        verify.close()

    logging.info(
        "Database restored to %s from %s (posts=%s, offset=%s)",
        target_abs,
        backup_abs,
        post_count,
        offset_value,
    )
    return {
        "target_path": target_abs,
        "backup_path": backup_abs,
        "integrity": integrity,
        "post_count": post_count,
        "update_offset": offset_value,
    }


def get_health_report(stale_heartbeat_seconds=600):
    """
    Lightweight read-only operational health snapshot.

    Does not create tables, migrate schema, or write any rows.
    """
    db_accessible = False
    schema_ok = False
    integrity = None
    error = None
    listener_hb = None
    scheduler_hb = None
    manual_review = 0
    counts = {}
    ineligible_count = 0
    offset = None

    if not os.path.exists(DB_NAME):
        error = f"Database file not found: {DB_NAME}"
    else:
        # Read-only URI connection: no WAL pragma, no schema writes.
        abs_path = os.path.abspath(DB_NAME).replace("\\", "/")
        uri = f"file:{abs_path}?mode=ro"
        conn = None
        try:
            conn = sqlite3.connect(uri, uri=True, timeout=30)
            conn.row_factory = sqlite3.Row
            db_accessible = True
            integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
            tables = {
                row[0]
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
            required_tables = {
                "incoming_posts",
                "bot_state",
                "publish_history",
            }
            if not required_tables.issubset(tables):
                schema_ok = False
                error = (
                    "Required schema unavailable "
                    "(incoming_posts / bot_state / publish_history missing)"
                )
            else:
                post_columns = _column_names(conn, "incoming_posts")
                schema_version = _get_state(conn, "schema_version")
                schema_ok = (
                    "live_eligible" in post_columns
                    and schema_version == SCHEMA_VERSION
                )
                if not schema_ok:
                    error = (
                        f"New-post-only schema version {SCHEMA_VERSION} "
                        "is not available"
                    )
                else:
                    rows = conn.execute(
                        """
                        SELECT status, COUNT(*) AS c
                        FROM incoming_posts
                        WHERE live_eligible = 1
                        GROUP BY status
                        """
                    ).fetchall()
                    counts = {row["status"]: row["c"] for row in rows}
                    ineligible_count = conn.execute(
                        """
                        SELECT COUNT(*) FROM incoming_posts
                        WHERE live_eligible = 0
                        """
                    ).fetchone()[0]
                    offset = _get_state(conn, "update_offset")
                    listener_hb = _get_state(conn, "listener_heartbeat")
                    scheduler_hb = _get_state(conn, "scheduler_heartbeat")
                    manual_review = conn.execute(
                        """
                        SELECT COUNT(*) AS c
                        FROM incoming_posts
                        WHERE live_eligible = 1
                          AND status = 'failed'
                          AND (
                                last_error LIKE '%MANUAL_REVIEW%'
                                OR last_error LIKE '%STALE_CLAIM%'
                                OR last_error LIKE '%AMBIGUOUS%'
                          )
                        """
                    ).fetchone()["c"]
        except Exception as exc:
            error = str(exc)
        finally:
            if conn is not None:
                conn.close()

    def _component_status(heartbeat):
        if not heartbeat:
            return "unknown"
        try:
            stamp = datetime.fromisoformat(heartbeat)
            if stamp.tzinfo is None:
                stamp = stamp.replace(tzinfo=timezone.utc)
            age = (datetime.now(timezone.utc) - stamp).total_seconds()
            if age <= stale_heartbeat_seconds:
                return "ok"
            return "stale"
        except ValueError:
            return "unknown"

    return {
        "database_accessible": db_accessible,
        "schema_ok": schema_ok,
        "integrity": integrity,
        "error": error,
        "update_offset": offset,
        "pending_posts": counts.get("pending", 0),
        "publishing_posts": counts.get("publishing", 0),
        "published_posts": counts.get("published", 0),
        "failed_posts": counts.get("failed", 0),
        "uncertain_posts": counts.get("uncertain", 0),
        "ineligible_posts": ineligible_count,
        "manual_review_failed_posts": manual_review,
        "listener_heartbeat": listener_hb,
        "scheduler_heartbeat": scheduler_hb,
        "listener_status": _component_status(listener_hb),
        "scheduler_status": _component_status(scheduler_hb),
        "db_path": DB_NAME,
    }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    print("Creating verified backup via Connection.backup()...")
    if os.path.exists(DB_NAME):
        result = backup_database(reason="phase3_corrections")
        print(f"Backup verified: {result}")
    else:
        print("No existing database file; a new one will be created.")

    print("Initializing / migrating database...")
    initialize_database()

    print("Verifying schema...")
    schema = verify_schema()
    for key, value in schema.items():
        print(f"  {key}: {value}")

    stats = get_queue_stats()
    print("Queue stats:", stats)
    print("Database ready.")
