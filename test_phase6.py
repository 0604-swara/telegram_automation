"""
Phase 6 verification tests.

Uses an isolated temporary database. Never touches the live DB or Telegram API.
"""

from __future__ import annotations

import importlib
import logging
import os
import sqlite3
from logging.handlers import RotatingFileHandler
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parent
LIVE_DB = ROOT / "telegram_queue.db"
TEST_DB = ROOT / "telegram_queue_phase6_test.db"


def _live_snapshot():
    conn = sqlite3.connect(LIVE_DB)
    try:
        offset = conn.execute(
            "SELECT value FROM bot_state WHERE key='update_offset'"
        ).fetchone()[0]
        rows = conn.execute(
            "SELECT id, message_id, status FROM incoming_posts ORDER BY id"
        ).fetchall()
        return {"offset": offset, "rows": rows}
    finally:
        conn.close()


def _cleanup(path: Path):
    for suffix in ("", "-wal", "-shm"):
        p = Path(str(path) + suffix)
        if p.exists():
            p.unlink()


def main():
    before = _live_snapshot()
    print("LIVE_BEFORE", before)

    _cleanup(TEST_DB)
    os.environ["DB_PATH"] = str(TEST_DB)
    os.environ["TELEGRAM_BOT_TOKEN"] = "123456:PHASE6-SECRET-TOKEN"
    os.environ["SOURCE_CHANNEL_ID"] = "-100111222333"
    os.environ["DESTINATION_CHANNEL_ID"] = "-100444555666"

    import config
    import database
    import telegram_handler
    import scheduler
    import healthcheck

    importlib.reload(config)
    importlib.reload(database)
    importlib.reload(telegram_handler)
    importlib.reload(scheduler)
    importlib.reload(healthcheck)

    results = []

    def check(name, cond, detail=""):
        results.append((name, bool(cond)))
        print(f"[{'PASS' if cond else 'FAIL'}] {name} {detail}")
        if not cond:
            raise AssertionError(f"{name}: {detail}")

    # --- config validation ---
    ok_cfg = config.validate_config(require_destination=True, require_token=True)
    check("config_valid", ok_cfg["SOURCE_CHANNEL_ID"] == "-100111222333")
    check("config_no_token_in_result", "TELEGRAM_BOT_TOKEN" not in ok_cfg)

    with patch.dict(os.environ, {"SOURCE_CHANNEL_ID": "not-a-channel"}, clear=False):
        importlib.reload(config)
        raised = False
        try:
            config.validate_config(require_destination=False, require_token=True)
        except config.ConfigurationError as exc:
            raised = True
            check("config_error_no_secret", "PHASE6-SECRET-TOKEN" not in str(exc))
        check("config_rejects_bad_channel", raised)

    # restore env after patch side effects
    os.environ["SOURCE_CHANNEL_ID"] = "-100111222333"
    importlib.reload(config)

    # --- database init / logging / sanitization ---
    database.initialize_database()
    telegram_handler.BOT_TOKEN = "123456:PHASE6-SECRET-TOKEN"
    dirty = "https://api.telegram.org/bot123456:PHASE6-SECRET-TOKEN/getUpdates boom"
    clean = telegram_handler.sanitize_telegram_error(dirty)
    check("token_sanitized", "PHASE6-SECRET-TOKEN" not in clean and "***REDACTED***" in clean)

    scheduler.BOT_TOKEN = "123456:PHASE6-SECRET-TOKEN"
    clean2 = scheduler.sanitize_telegram_error(dirty)
    check("scheduler_token_sanitized", "PHASE6-SECRET-TOKEN" not in clean2)

    # rotating handlers present
    telegram_handler._configure_logging()
    scheduler._configure_logging()
    check(
        "listener_rotating_log",
        any(isinstance(h, RotatingFileHandler) for h in telegram_handler._logger.handlers),
    )
    check(
        "scheduler_rotating_log",
        any(isinstance(h, RotatingFileHandler) for h in scheduler._logger.handlers),
    )

    # --- graceful shutdown flags ---
    telegram_handler._shutdown_requested = False
    telegram_handler.request_shutdown(2)
    check("listener_shutdown_flag", telegram_handler._shutdown_requested is True)
    scheduler._shutdown_requested = False
    scheduler.request_shutdown(15)
    check("scheduler_shutdown_flag", scheduler._shutdown_requested is True)

    # --- heartbeat + health ---
    database.record_heartbeat("listener")
    database.record_heartbeat("scheduler")
    report = database.get_health_report()
    check("health_db_accessible", report["database_accessible"] is True)
    check("health_schema_ok", report["schema_ok"] is True)
    check("health_integrity_ok", report["integrity"] == "ok")
    check("health_listener_ok", report["listener_status"] == "ok")
    check("health_scheduler_ok", report["scheduler_status"] == "ok")
    check("health_counts_present", "pending_posts" in report and "failed_posts" in report)

    # healthcheck CLI: healthy when DB + listener + scheduler are ok
    with patch("healthcheck.get_health_report", return_value=report):
        rc = healthcheck.main()
    check("healthcheck_cli_ok", rc == 0)

    # unknown heartbeat is not healthy
    unknown_report = dict(report)
    unknown_report["listener_status"] = "unknown"
    unknown_report["listener_heartbeat"] = None
    with patch("healthcheck.get_health_report", return_value=unknown_report):
        rc_unknown = healthcheck.main()
    check("healthcheck_unknown_not_healthy", rc_unknown == 1)
    check(
        "is_healthy_rejects_unknown",
        healthcheck.is_healthy(unknown_report) is False,
    )

    # stale listener/scheduler statuses are not healthy
    stale_report = dict(report)
    stale_report["listener_status"] = "stale"
    stale_report["scheduler_status"] = "ok"
    check("stale_listener_not_healthy", healthcheck.is_healthy(stale_report) is False)
    stale_sched = dict(report)
    stale_sched["scheduler_status"] = "stale"
    check("stale_scheduler_not_healthy", healthcheck.is_healthy(stale_sched) is False)

    # get_health_report marks missing heartbeat as unknown
    with database.get_connection() as conn:
        database._begin_immediate(conn)
        conn.execute("DELETE FROM bot_state WHERE key='listener_heartbeat'")
        conn.execute("COMMIT")
    cleared = database.get_health_report()
    check("missing_listener_heartbeat_unknown", cleared["listener_status"] == "unknown")
    # restore listener heartbeat for later checks
    database.record_heartbeat("listener")

    # stale age detection
    with database.get_connection() as conn:
        database._begin_immediate(conn)
        conn.execute(
            "INSERT INTO bot_state(key,value) VALUES('scheduler_heartbeat', ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            ("2000-01-01T00:00:00+00:00",),
        )
        conn.execute("COMMIT")
    aged = database.get_health_report(stale_heartbeat_seconds=60)
    check("old_scheduler_heartbeat_stale", aged["scheduler_status"] == "stale")
    database.record_heartbeat("scheduler")

    # read-only healthcheck: missing DB file must not create/migrate
    missing_path = ROOT / "telegram_queue_phase6_missing.db"
    _cleanup(missing_path)
    original_db = database.DB_NAME
    database.DB_NAME = str(missing_path)
    try:
        missing_report = database.get_health_report()
        check(
            "readonly_missing_db_reports_error",
            missing_report["database_accessible"] is False
            and missing_report["error"]
            and "not found" in missing_report["error"].lower(),
        )
        check("readonly_missing_db_no_file_created", not missing_path.exists())
        with patch("healthcheck.get_health_report", return_value=missing_report):
            rc_missing = healthcheck.main()
        check("readonly_missing_db_nonzero_exit", rc_missing == 1)
    finally:
        database.DB_NAME = original_db

    # heartbeat refreshed during long interruptible scheduler wait
    hb_calls = []
    scheduler._shutdown_requested = False
    scheduler.HEARTBEAT_DURING_WAIT_SECONDS = 1

    def _capture_hb(component):
        hb_calls.append(component)
        return database.record_heartbeat(component)

    with patch.object(scheduler, "record_heartbeat", side_effect=_capture_hb):
        scheduler._sleep_interruptible(3.2)
    check(
        "scheduler_wait_refreshes_heartbeat",
        hb_calls.count("scheduler") >= 2,
        f"calls={hb_calls}",
    )
    # ensure wait remains interruptible by shutdown
    scheduler._shutdown_requested = False
    hb_calls.clear()

    def _stop_soon(component):
        hb_calls.append(component)
        scheduler.request_shutdown()
        return "hb"

    with patch.object(scheduler, "record_heartbeat", side_effect=_stop_soon):
        started = __import__("time").monotonic()
        scheduler._sleep_interruptible(30)
        elapsed = __import__("time").monotonic() - started
    check("scheduler_wait_shutdown_still_works", elapsed < 3, f"elapsed={elapsed}")
    scheduler._shutdown_requested = False

    # --- backup integrity + no overwrite ---
    backup = database.backup_database(reason="phase6_test")
    check("backup_integrity", backup["integrity"] == "ok")
    check("backup_file_exists", Path(backup["backup_path"]).exists())

    # Backup overwrite refusal via colliding timestamped path
    from datetime import datetime as real_datetime, timezone as real_timezone

    class _FixedDateTime:
        timezone = real_timezone

        @classmethod
        def now(cls, tz=None):
            stamp = Path(backup["backup_path"]).stem.split("phase6_test_")[-1]
            return real_datetime.strptime(stamp, "%Y%m%d_%H%M%S").replace(
                tzinfo=real_timezone.utc
            )

    overwrite_refused = False
    original_datetime = database.datetime
    database.datetime = _FixedDateTime
    try:
        database.backup_database(reason="phase6_test")
    except RuntimeError as exc:
        overwrite_refused = "Refusing to overwrite existing backup file" in str(exc)
    finally:
        database.datetime = original_datetime
    check("backup_refuses_overwrite", overwrite_refused)

    live_refuse = False
    try:
        database.restore_database_to_path(backup["backup_path"], database.DB_NAME)
    except RuntimeError as exc:
        live_refuse = "Refusing to restore over the configured live database path" in str(exc)
    check("restore_refuses_live_db_path", live_refuse)

    restore_target = ROOT / "telegram_queue_phase6_restore_target.db"
    _cleanup(restore_target)
    restored = database.restore_database_to_path(backup["backup_path"], str(restore_target))
    check("restore_integrity", restored["integrity"] == "ok")
    check("restore_file_exists", restore_target.exists())

    overwrite_target = False
    try:
        database.restore_database_to_path(backup["backup_path"], str(restore_target))
    except RuntimeError as exc:
        overwrite_target = "Refusing to overwrite existing target" in str(exc)
    check("restore_refuses_existing_target", overwrite_target)

    # stale claim remains failed policy still present
    with database.get_connection() as conn:
        database._begin_immediate(conn)
        conn.execute(
            """
            INSERT INTO incoming_posts (
                channel_id, message_id, message_type, message_data,
                status, claimed_at, claim_owner, destination_message_ids
            ) VALUES (-1001, 1, 'text', '{}', 'publishing',
                      datetime('now', '-2000 seconds'), 'dead', '[9]')
            """
        )
        conn.execute("COMMIT")
    n = database.release_stale_claims(stale_seconds=900)
    with database.get_connection() as conn:
        row = conn.execute(
            "SELECT status, claim_owner, destination_message_ids, last_error "
            "FROM incoming_posts WHERE message_id=1"
        ).fetchone()
    check("stale_still_failed", n == 1 and row["status"] == "failed")
    check("stale_dest_preserved", row["destination_message_ids"] == "[9]")
    check("stale_owner_cleared", row["claim_owner"] is None)

    after = _live_snapshot()
    check("live_offset_unchanged", after["offset"] == before["offset"])
    check("live_rows_unchanged", after["rows"] == before["rows"])

    # cleanup isolated artifacts
    _cleanup(TEST_DB)
    _cleanup(restore_target)
    for p in ROOT.glob("telegram_queue_backup_phase6_test_*.db"):
        try:
            p.unlink()
        except OSError:
            pass

    print("ALL_PHASE6_TESTS_PASSED")
    print("LIVE_AFTER", after)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
