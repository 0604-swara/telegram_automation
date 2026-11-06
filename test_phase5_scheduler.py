"""
Isolated Phase 5 scheduler tests.

Uses a temporary database and mocked Telegram HTTP responses.
Does not touch the live database or real destination channel.
"""

from __future__ import annotations

import importlib
import json
import os
import sqlite3
import sys
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

ROOT = Path(__file__).resolve().parent
LIVE_DB = ROOT / "telegram_queue.db"
TEST_DB = ROOT / "telegram_queue_phase5_scheduler_test.db"
LOCK_A = ROOT / "scheduler_phase5_test_a.lock"
LOCK_B = ROOT / "scheduler_phase5_test_b.lock"


def _cleanup_sidecars(path: Path):
    for suffix in ("", "-wal", "-shm"):
        candidate = Path(str(path) + suffix)
        if candidate.exists():
            candidate.unlink()


def _live_snapshot():
    conn = sqlite3.connect(LIVE_DB)
    try:
        offset = conn.execute(
            "SELECT value FROM bot_state WHERE key='update_offset'"
        ).fetchone()[0]
        count = conn.execute("SELECT COUNT(*) FROM incoming_posts").fetchone()[0]
        rows = conn.execute(
            "SELECT id, message_id, status FROM incoming_posts ORDER BY id"
        ).fetchall()
        return {"offset": offset, "count": count, "rows": rows}
    finally:
        conn.close()


def _reload_modules():
    os.environ["DB_PATH"] = str(TEST_DB)
    os.environ["TELEGRAM_BOT_TOKEN"] = "123456:PHASE5-TEST-TOKEN-SECRET"
    os.environ["SOURCE_CHANNEL_ID"] = "-100111"
    os.environ["DESTINATION_CHANNEL_ID"] = "-100222"
    os.environ["PUBLISH_INTERVAL_SECONDS"] = "300"
    os.environ["EMPTY_QUEUE_CHECK_SECONDS"] = "30"
    os.environ["ALBUM_WAIT_SECONDS"] = "0"
    os.environ["STALE_CLAIM_SECONDS"] = "900"
    os.environ["PUBLISH_TRANSIENT_ATTEMPTS"] = "3"
    os.environ["SCHEDULER_LOCK_FILE"] = str(LOCK_A)

    import database
    import scheduler

    importlib.reload(database)
    importlib.reload(scheduler)
    return database, scheduler


def _insert_post(database, message_id, message_type="text", media_group_id=None, status="pending", retry_count=0):
    payload = {
        "message_id": message_id,
        "chat": {"id": -100111},
        "date": 1,
    }
    if message_type == "text":
        payload["text"] = f"text-{message_id}"
    elif message_type == "photo":
        payload["photo"] = [{"file_id": f"file-{message_id}", "width": 1, "height": 1}]
        payload["caption"] = f"caption-{message_id}"
    if media_group_id:
        payload["media_group_id"] = media_group_id

    with database.get_connection() as conn:
        database._begin_immediate(conn)
        conn.execute(
            """
            INSERT INTO incoming_posts (
                channel_id, message_id, media_group_id, message_type,
                message_data, status, retry_count, received_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now', '-60 seconds'))
            """,
            (
                -100111,
                message_id,
                media_group_id,
                message_type,
                json.dumps(payload),
                status,
                retry_count,
            ),
        )
        conn.execute("COMMIT")


def _status_map(database):
    with database.get_connection() as conn:
        rows = conn.execute(
            "SELECT message_id, status, claim_owner, destination_message_ids, retry_count "
            "FROM incoming_posts ORDER BY message_id"
        ).fetchall()
    return {
        row["message_id"]: {
            "status": row["status"],
            "claim_owner": row["claim_owner"],
            "destination_message_ids": row["destination_message_ids"],
            "retry_count": row["retry_count"],
        }
        for row in rows
    }


def _ok_copy_message(dest_id=9001):
    response = MagicMock()
    response.status_code = 200
    response.ok = True
    response.headers = {}
    response.text = "ok"
    response.json.return_value = {
        "ok": True,
        "result": {"message_id": dest_id, "chat": {"id": -100222}},
    }
    return response


def _ok_copy_messages(dest_ids):
    response = MagicMock()
    response.status_code = 200
    response.ok = True
    response.headers = {}
    response.text = "ok"
    response.json.return_value = {
        "ok": True,
        "result": [{"message_id": dest_id} for dest_id in dest_ids],
    }
    return response


def _http_error(status, description, retry_after=None, ok_flag=False):
    response = MagicMock()
    response.status_code = status
    response.ok = status < 400
    response.headers = {}
    payload = {"ok": ok_flag, "description": description}
    if retry_after is not None:
        response.headers["Retry-After"] = str(retry_after)
        payload["parameters"] = {"retry_after": retry_after}
    response.text = description
    response.json.return_value = payload
    return response


def main():
    assert LIVE_DB.exists(), "Live database missing"
    before = _live_snapshot()
    print("LIVE_BEFORE", before)

    _cleanup_sidecars(TEST_DB)
    for lock in (LOCK_A, LOCK_B):
        if lock.exists():
            lock.unlink()

    database, scheduler = _reload_modules()
    database.initialize_database()
    scheduler._configure_logging()
    scheduler._shutdown_requested = False
    scheduler._claim_owner = "scheduler:test:1:aaaa"
    scheduler.SOURCE_CHANNEL_ID = "-100111"
    scheduler.DESTINATION_CHANNEL_ID = "-100222"
    scheduler.BOT_TOKEN = "123456:PHASE5-TEST-TOKEN-SECRET"
    scheduler.ALBUM_WAIT_SECONDS = 0
    scheduler.TRANSIENT_ATTEMPTS = 3
    scheduler.MAX_RETRIES = 5
    scheduler.INTERVAL = 300

    results = []

    def check(name, condition, detail=""):
        results.append((name, bool(condition), detail))
        status = "PASS" if condition else "FAIL"
        print(f"[{status}] {name} {detail}")
        if not condition:
            raise AssertionError(f"FAILED: {name} {detail}")

    # ------------------------------------------------------------------
    # 1) Text publishing
    # ------------------------------------------------------------------
    _insert_post(database, 101, "text")
    session = MagicMock()
    session.post.return_value = _ok_copy_message(8001)
    with patch.object(scheduler, "_sleep_interruptible", lambda s: None):
        published = scheduler.process_next_post(
            claim_owner=scheduler._claim_owner,
            session=session,
        )
    check("text_publishing", published is True)
    row = _status_map(database)[101]
    check("text_destination_ids", row["destination_message_ids"] == "[8001]")
    check("text_status_published", row["status"] == "published")
    check("text_claim_cleared", row["claim_owner"] is None)

    # ------------------------------------------------------------------
    # 2) Single-photo publishing
    # ------------------------------------------------------------------
    _insert_post(database, 102, "photo")
    session.post.return_value = _ok_copy_message(8002)
    with patch.object(scheduler, "_sleep_interruptible", lambda s: None):
        published = scheduler.process_next_post(
            claim_owner=scheduler._claim_owner,
            session=session,
        )
    check("single_photo_publishing", published is True)
    check(
        "photo_destination_ids",
        _status_map(database)[102]["destination_message_ids"] == "[8002]",
    )

    # ------------------------------------------------------------------
    # 3) Three-photo album
    # ------------------------------------------------------------------
    for mid in (201, 202, 203):
        _insert_post(database, mid, "photo", media_group_id="album-5")
    session.post.return_value = _ok_copy_messages([8101, 8102, 8103])
    with patch.object(scheduler, "_sleep_interruptible", lambda s: None):
        published = scheduler.process_next_post(
            claim_owner=scheduler._claim_owner,
            session=session,
        )
    check("album_publishing", published is True)
    statuses = _status_map(database)
    check(
        "album_all_published",
        all(statuses[m]["status"] == "published" for m in (201, 202, 203)),
    )
    check(
        "album_destination_ids",
        statuses[201]["destination_message_ids"] == "[8101, 8102, 8103]",
    )

    # ------------------------------------------------------------------
    # 4) New-post priority (newer message_id first)
    # ------------------------------------------------------------------
    _insert_post(database, 301, "text")
    _insert_post(database, 399, "text")
    unit = scheduler.select_next_publish_unit(album_wait_seconds=0)
    check(
        "new_post_priority",
        unit is not None and unit["posts"][0]["message_id"] == 399,
        f"selected={unit}",
    )

    # Publish newest then older
    session.post.return_value = _ok_copy_message(8201)
    with patch.object(scheduler, "_sleep_interruptible", lambda s: None):
        scheduler.process_next_post(claim_owner=scheduler._claim_owner, session=session)
    check("priority_published_newest", _status_map(database)[399]["status"] == "published")
    check("priority_older_still_pending", _status_map(database)[301]["status"] == "pending")

    # ------------------------------------------------------------------
    # 5) Five-minute scheduling logic without waiting
    # ------------------------------------------------------------------
    sleeps = []

    def capture_sleep(seconds):
        sleeps.append(seconds)

    session.post.return_value = _ok_copy_message(8301)
    # Ensure one pending exists
    if _status_map(database)[301]["status"] != "pending":
        _insert_post(database, 302, "text")

    scheduler._shutdown_requested = False

    def stop_after_first_sleep(seconds):
        sleeps.append(seconds)
        scheduler.request_shutdown()

    with patch.object(scheduler, "_sleep_interruptible", side_effect=stop_after_first_sleep), \
         patch.object(scheduler, "process_next_post", return_value=True):
        scheduler.run_scheduler_loop(
            claim_owner=scheduler._claim_owner,
            session=session,
        )
    check(
        "five_minute_slot_waits_in_interruptible_chunks",
        (
            scheduler.INTERVAL == 300
            and sleeps
            and sleeps[0]
            == min(
                scheduler.EMPTY_QUEUE_CHECK_SECONDS,
                scheduler.INTERVAL,
            )
        ),
        f"sleeps={sleeps}",
    )
    scheduler._shutdown_requested = False

    # ------------------------------------------------------------------
    # 6) Cycle restart
    # ------------------------------------------------------------------
    with database.get_connection() as conn:
        database._begin_immediate(conn)
        conn.execute("UPDATE incoming_posts SET status='published', claim_owner=NULL, claimed_at=NULL")
        conn.execute("COMMIT")
    restarted = database.restart_posting_cycle()
    check("cycle_restart", restarted is True)
    stats = database.get_queue_stats()
    check("cycle_restart_pending", stats["counts"].get("pending", 0) > 0)
    check("cycle_restart_no_failed_auto", stats["counts"].get("failed", 0) == 0)

    # ------------------------------------------------------------------
    # 7 + 8) Failed-post exclusion + manual reset
    # ------------------------------------------------------------------
    with database.get_connection() as conn:
        database._begin_immediate(conn)
        conn.execute("DELETE FROM incoming_posts")
        conn.execute("COMMIT")
    _insert_post(database, 401, "text", status="failed", retry_count=5)
    _insert_post(database, 402, "text", status="published")
    unit = scheduler.select_next_publish_unit(album_wait_seconds=0)
    check("failed_excluded_from_selection", unit is None or all(
        p["message_id"] != 401 for p in unit["posts"]
    ))
    # Only published exists eligible for restart path; failed stays failed
    restarted = database.restart_posting_cycle()
    check("failed_excluded_from_cycle_restart", restarted is True)
    check("failed_still_failed", _status_map(database)[401]["status"] == "failed")
    with database.get_connection() as conn:
        failed_id = conn.execute(
            "SELECT id FROM incoming_posts WHERE message_id=401"
        ).fetchone()["id"]
    reset_count = database.reset_failed_posts([failed_id])
    check("manual_failed_reset", reset_count == 1)
    check("manual_failed_now_pending", _status_map(database)[401]["status"] == "pending")

    # ------------------------------------------------------------------
    # 9) Atomic album claims
    # ------------------------------------------------------------------
    with database.get_connection() as conn:
        database._begin_immediate(conn)
        conn.execute("DELETE FROM incoming_posts")
        conn.execute("COMMIT")
    for mid in (501, 502, 503):
        _insert_post(database, mid, "photo", media_group_id="album-atomic")
    with database.get_connection() as conn:
        ids = [
            row["id"]
            for row in conn.execute(
                "SELECT id FROM incoming_posts WHERE media_group_id='album-atomic' ORDER BY message_id"
            ).fetchall()
        ]
    # Claim first two with owner A
    database.claim_posts(ids[:2], "owner-A")
    raised = False
    try:
        database.claim_posts(ids, "owner-B")
    except database.ClaimError:
        raised = True
    check("atomic_album_claim_rejects_partial", raised)
    with database.get_connection() as conn:
        rows = conn.execute(
            "SELECT status, claim_owner FROM incoming_posts WHERE media_group_id='album-atomic' ORDER BY message_id"
        ).fetchall()
    check(
        "atomic_album_third_unclaimed",
        rows[2]["status"] == "pending" and rows[2]["claim_owner"] is None,
    )
    database.release_claim(ids[:2], claim_owner="owner-A")

    # ------------------------------------------------------------------
    # 10) Wrong claim-owner rejection
    # ------------------------------------------------------------------
    database.claim_posts(ids[:1], "owner-A")
    wrong = False
    try:
        database.mark_as_published(ids[:1], claim_owner="owner-B", destination_message_ids=[1])
    except database.PublishOwnershipError:
        wrong = True
    check("wrong_claim_owner_rejected", wrong)
    database.release_claim(ids[:1], claim_owner="owner-A")

    # ------------------------------------------------------------------
    # 11) Telegram 429 handling
    # ------------------------------------------------------------------
    with database.get_connection() as conn:
        database._begin_immediate(conn)
        conn.execute("DELETE FROM incoming_posts")
        conn.execute("COMMIT")
    _insert_post(database, 601, "text")
    session = MagicMock()
    session.post.side_effect = [
        _http_error(429, "Too Many Requests", retry_after=2),
        _ok_copy_message(8601),
    ]
    waited = []
    with patch.object(scheduler, "_sleep_interruptible", side_effect=lambda s: waited.append(s)):
        published = scheduler.process_next_post(
            claim_owner="owner-429",
            session=session,
        )
    check("rate_limit_handling", published is True and 2 in waited, f"waited={waited}")
    check("rate_limit_eventually_published", _status_map(database)[601]["status"] == "published")

    # ------------------------------------------------------------------
    # 12) Connection loss after transmission may have started -> no retry
    # ------------------------------------------------------------------
    _insert_post(database, 602, "text")
    session = MagicMock()
    session.post.side_effect = requests.exceptions.ConnectionError(
        "connection aborted mid-request"
    )
    waited_net = []
    with patch.object(
        scheduler,
        "_sleep_interruptible",
        side_effect=lambda s: waited_net.append(s),
    ):
        published = scheduler.process_next_post(
            claim_owner="owner-net",
            session=session,
        )
    check("connection_loss_no_retry", published is False and waited_net == [])
    check(
        "connection_loss_marked_failed",
        _status_map(database)[602]["status"] == "failed",
    )
    check("connection_loss_single_attempt", session.post.call_count == 1)

    # ------------------------------------------------------------------
    # 12b) HTTP 5xx -> ambiguous, no retry
    # ------------------------------------------------------------------
    _insert_post(database, 604, "text")
    session = MagicMock()
    session.post.return_value = _http_error(502, "Bad Gateway")
    waited_5xx = []
    with patch.object(
        scheduler,
        "_sleep_interruptible",
        side_effect=lambda s: waited_5xx.append(s),
    ):
        published = scheduler.process_next_post(
            claim_owner="owner-5xx",
            session=session,
        )
    check("http_5xx_no_retry", published is False and waited_5xx == [])
    check("http_5xx_marked_failed", _status_map(database)[604]["status"] == "failed")
    check("http_5xx_single_attempt", session.post.call_count == 1)

    # ------------------------------------------------------------------
    # 12c) Unreadable response -> failed for manual review
    # ------------------------------------------------------------------
    _insert_post(database, 605, "text")
    session = MagicMock()
    bad_body = MagicMock()
    bad_body.status_code = 200
    bad_body.ok = True
    bad_body.headers = {}
    bad_body.text = "not-json"
    bad_body.json.side_effect = ValueError("No JSON")
    session.post.return_value = bad_body
    with patch.object(scheduler, "_sleep_interruptible", lambda s: None):
        published = scheduler.process_next_post(
            claim_owner="owner-badjson",
            session=session,
        )
    check("unreadable_response_failed", published is False)
    check(
        "unreadable_response_manual_review",
        _status_map(database)[605]["status"] == "failed",
    )

    # ------------------------------------------------------------------
    # 13) Permanent Telegram errors
    # ------------------------------------------------------------------
    _insert_post(database, 603, "text")
    session = MagicMock()
    session.post.return_value = _http_error(400, "Bad Request: message to copy not found")
    with patch.object(scheduler, "_sleep_interruptible", lambda s: None):
        published = scheduler.process_next_post(
            claim_owner="owner-perm",
            session=session,
        )
    check("permanent_error_not_published", published is False)
    check("permanent_error_retry_bumped", _status_map(database)[603]["retry_count"] >= 1)

    # ------------------------------------------------------------------
    # 14) Partial album response (ambiguous, atomic failure)
    # ------------------------------------------------------------------
    for mid in (701, 702, 703):
        _insert_post(database, mid, "photo", media_group_id="album-partial")
    session = MagicMock()
    partial = MagicMock()
    partial.status_code = 200
    partial.ok = True
    partial.headers = {}
    partial.text = "ok"
    partial.json.return_value = {
        "ok": True,
        "result": [{"message_id": 1}, {"message_id": 2}],  # missing third
    }
    session.post.return_value = partial
    with patch.object(scheduler, "_sleep_interruptible", lambda s: None):
        published = scheduler.process_next_post(
            claim_owner="owner-partial",
            session=session,
        )
    check("partial_album_not_auto_retried", published is False)
    statuses = _status_map(database)
    check(
        "partial_album_manual_review_failed",
        all(statuses[m]["status"] == "failed" for m in (701, 702, 703)),
    )
    check(
        "partial_album_failure_atomic",
        session.post.call_count == 1
        and all(statuses[m]["claim_owner"] is None for m in (701, 702, 703)),
    )

    # ------------------------------------------------------------------
    # 15) Crash recovery / stale claims -> failed, never pending
    # ------------------------------------------------------------------
    _insert_post(database, 801, "text")
    with database.get_connection() as conn:
        post_id = conn.execute(
            "SELECT id FROM incoming_posts WHERE message_id=801"
        ).fetchone()["id"]
        database._begin_immediate(conn)
        conn.execute(
            """
            UPDATE incoming_posts
            SET status='publishing',
                claim_owner='dead-owner',
                claimed_at=datetime('now', '-1000 seconds'),
                destination_message_ids='[7777]'
            WHERE id=?
            """,
            (post_id,),
        )
        conn.execute("COMMIT")
    released = database.release_stale_claims(stale_seconds=900)
    stale_row = _status_map(database)[801]
    check("stale_claim_recovery_count", released == 1)
    check("stale_claim_becomes_failed", stale_row["status"] == "failed")
    check("stale_claim_never_pending", stale_row["status"] != "pending")
    check("stale_claim_owner_cleared", stale_row["claim_owner"] is None)
    check(
        "stale_claim_preserves_destination_ids",
        stale_row["destination_message_ids"] == "[7777]",
    )
    with database.get_connection() as conn:
        err = conn.execute(
            "SELECT last_error FROM incoming_posts WHERE message_id=801"
        ).fetchone()["last_error"]
    check(
        "stale_claim_error_mentions_manual_review",
        err is not None
        and "STALE_CLAIM_MANUAL_REVIEW" in err
        and "may already have accepted" in err.lower(),
    )

    # Fresh live claim must not be treated as stale
    _insert_post(database, 802, "text")
    with database.get_connection() as conn:
        live_id = conn.execute(
            "SELECT id FROM incoming_posts WHERE message_id=802"
        ).fetchone()["id"]
    database.claim_posts([live_id], "live-owner")
    released_live = database.release_stale_claims(stale_seconds=900)
    check("live_claim_not_released", released_live == 0)
    check("live_claim_still_publishing", _status_map(database)[802]["status"] == "publishing")
    database.release_claim([live_id], claim_owner="live-owner")

    # ------------------------------------------------------------------
    # 15b) Strict mark_as_failed ownership
    # ------------------------------------------------------------------
    _insert_post(database, 811, "text")
    _insert_post(database, 812, "text")
    with database.get_connection() as conn:
        ids_own = [
            row["id"]
            for row in conn.execute(
                "SELECT id FROM incoming_posts WHERE message_id IN (811,812) ORDER BY message_id"
            ).fetchall()
        ]
    database.claim_posts(ids_own, "owner-right")
    wrong_fail = False
    try:
        database.mark_as_failed(
            ids_own,
            error="should-not-apply",
            max_retries=5,
            claim_owner="owner-wrong",
        )
    except database.PublishOwnershipError:
        wrong_fail = True
    check("wrong_owner_mark_as_failed_rejected", wrong_fail)
    check(
        "wrong_owner_left_publishing",
        all(_status_map(database)[m]["status"] == "publishing" for m in (811, 812)),
    )

    # NULL claim_owner rows cannot be modified by a scheduler owner
    with database.get_connection() as conn:
        database._begin_immediate(conn)
        conn.execute(
            """
            UPDATE incoming_posts
            SET claim_owner=NULL
            WHERE id=?
            """,
            (ids_own[0],),
        )
        conn.execute("COMMIT")
    null_owner_rejected = False
    try:
        database.mark_as_failed(
            ids_own,
            error="null-owner-should-fail",
            max_retries=0,
            claim_owner="owner-right",
        )
    except database.PublishOwnershipError:
        null_owner_rejected = True
    check("null_owner_mark_as_failed_rejected", null_owner_rejected)
    # Restore ownership and release cleanly for later tests
    with database.get_connection() as conn:
        database._begin_immediate(conn)
        conn.execute(
            """
            UPDATE incoming_posts
            SET status='publishing', claim_owner='owner-right'
            WHERE id IN (?, ?)
            """,
            ids_own,
        )
        conn.execute("COMMIT")
    database.release_claim(ids_own, claim_owner="owner-right")

    # Album failure updates are atomic under ownership rules
    for mid in (821, 822, 823):
        _insert_post(database, mid, "photo", media_group_id="album-fail-atomic")
    with database.get_connection() as conn:
        album_fail_ids = [
            row["id"]
            for row in conn.execute(
                "SELECT id FROM incoming_posts WHERE media_group_id='album-fail-atomic' ORDER BY message_id"
            ).fetchall()
        ]
    database.claim_posts(album_fail_ids, "owner-album")
    # Break ownership on one member, then require all-or-nothing failure update
    with database.get_connection() as conn:
        database._begin_immediate(conn)
        conn.execute(
            "UPDATE incoming_posts SET claim_owner='other' WHERE id=?",
            (album_fail_ids[1],),
        )
        conn.execute("COMMIT")
    album_atomic_rejected = False
    try:
        database.mark_as_failed(
            album_fail_ids,
            error="partial-should-roll-back",
            max_retries=0,
            claim_owner="owner-album",
        )
    except database.PublishOwnershipError:
        album_atomic_rejected = True
    check("album_mark_as_failed_atomic_rejection", album_atomic_rejected)
    with database.get_connection() as conn:
        rows = conn.execute(
            """
            SELECT status, claim_owner FROM incoming_posts
            WHERE media_group_id='album-fail-atomic'
            ORDER BY message_id
            """
        ).fetchall()
    check(
        "album_mark_as_failed_no_partial_update",
        rows[0]["status"] == "publishing"
        and rows[0]["claim_owner"] == "owner-album"
        and rows[1]["status"] == "publishing"
        and rows[1]["claim_owner"] == "other"
        and rows[2]["status"] == "publishing"
        and rows[2]["claim_owner"] == "owner-album",
    )
    # Cleanup claims without using broken ownership path
    with database.get_connection() as conn:
        database._begin_immediate(conn)
        conn.execute(
            """
            UPDATE incoming_posts
            SET status='pending', claim_owner=NULL, claimed_at=NULL
            WHERE media_group_id='album-fail-atomic'
            """
        )
        conn.execute("COMMIT")

    # ------------------------------------------------------------------
    # 16) Two scheduler instances / lock
    # ------------------------------------------------------------------
    if LOCK_A.exists():
        try:
            LOCK_A.unlink()
        except OSError:
            pass
    lock1 = scheduler.SingleInstanceLock(str(LOCK_A))
    lock1.acquire()
    raised_lock = False
    try:
        lock2 = scheduler.SingleInstanceLock(str(LOCK_A))
        try:
            lock2.acquire()
        except scheduler.SchedulerError:
            raised_lock = True
        except (OSError, PermissionError):
            raised_lock = True
    finally:
        lock1.release()
    check("single_instance_lock_blocks_second", raised_lock)

    # ------------------------------------------------------------------
    # 17) Ambiguous publishing outcomes (timeout) -> no retry
    # ------------------------------------------------------------------
    _insert_post(database, 901, "text")
    session = MagicMock()
    session.post.side_effect = requests.exceptions.Timeout("read timed out")
    waited_timeout = []
    with patch.object(
        scheduler,
        "_sleep_interruptible",
        side_effect=lambda s: waited_timeout.append(s),
    ):
        published = scheduler.process_next_post(
            claim_owner="owner-ambiguous",
            session=session,
        )
    check(
        "timeout_does_not_retry",
        published is False and waited_timeout == [] and session.post.call_count == 1,
    )
    check("ambiguous_marked_failed", _status_map(database)[901]["status"] == "failed")

    # ------------------------------------------------------------------
    # 18) Destination message ID persistence
    # ------------------------------------------------------------------
    _insert_post(database, 902, "text")
    session = MagicMock()
    session.post.return_value = _ok_copy_message(8902)
    with patch.object(scheduler, "_sleep_interruptible", lambda s: None):
        scheduler.process_next_post(claim_owner="owner-dest", session=session)
    check(
        "destination_message_id_persisted",
        _status_map(database)[902]["destination_message_ids"] == "[8902]",
    )

    # ------------------------------------------------------------------
    # 19) No duplicate concurrent publishing (claim exclusivity)
    # ------------------------------------------------------------------
    _insert_post(database, 903, "text")
    with database.get_connection() as conn:
        pid = conn.execute(
            "SELECT id FROM incoming_posts WHERE message_id=903"
        ).fetchone()["id"]
    database.claim_posts([pid], "owner-1")
    conflict = False
    try:
        database.claim_posts([pid], "owner-2")
    except database.ClaimError:
        conflict = True
    check("no_duplicate_concurrent_claim", conflict)

    # ------------------------------------------------------------------
    # 20) Dedicated ConnectTimeout retry (pre-transmission, retryable)
    # ------------------------------------------------------------------
    _insert_post(database, 910, "text")
    session = MagicMock()
    session.post.side_effect = [
        requests.exceptions.ConnectTimeout("connect timed out"),
        _ok_copy_message(8910),
    ]
    waited_connect = []
    with patch.object(
        scheduler,
        "_sleep_interruptible",
        side_effect=lambda s: waited_connect.append(s),
    ):
        published = scheduler.process_next_post(
            claim_owner="owner-connect-timeout",
            session=session,
        )
    check(
        "connect_timeout_retries_then_succeeds",
        published is True
        and session.post.call_count == 2
        and len(waited_connect) == 1,
        f"calls={session.post.call_count} waited={waited_connect}",
    )
    check(
        "connect_timeout_final_status_published",
        _status_map(database)[910]["status"] == "published",
    )

    # ------------------------------------------------------------------
    # 21) Transient retry exhaustion -> failed, claims cleared, no republish
    # ------------------------------------------------------------------
    # Seed retry_count near the permanent threshold so the exhaustion path's
    # mark_as_failed(..., max_retries=MAX_RETRIES) lands on status='failed'
    # without changing production code.
    _insert_post(database, 911, "text", retry_count=scheduler.MAX_RETRIES - 1)
    session = MagicMock()
    session.post.side_effect = [
        requests.exceptions.ConnectTimeout("connect timed out"),
        requests.exceptions.ConnectTimeout("connect timed out"),
        requests.exceptions.ConnectTimeout("connect timed out"),
        requests.exceptions.ConnectTimeout("should-not-be-called"),
    ]
    waited_exhaust = []
    original_attempts = scheduler.TRANSIENT_ATTEMPTS
    scheduler.TRANSIENT_ATTEMPTS = 3
    try:
        with patch.object(
            scheduler,
            "_sleep_interruptible",
            side_effect=lambda s: waited_exhaust.append(s),
        ):
            published = scheduler.process_next_post(
                claim_owner="owner-exhaust",
                session=session,
            )
    finally:
        scheduler.TRANSIENT_ATTEMPTS = original_attempts

    exhaust_row = _status_map(database)[911]
    check(
        "retry_exhaustion_respects_limit",
        published is False and session.post.call_count == 3,
        f"calls={session.post.call_count} waited={waited_exhaust}",
    )
    check("retry_exhaustion_status_failed", exhaust_row["status"] == "failed")
    check("retry_exhaustion_claim_cleared", exhaust_row["claim_owner"] is None)
    with database.get_connection() as conn:
        claimed_at = conn.execute(
            "SELECT claimed_at FROM incoming_posts WHERE message_id=911"
        ).fetchone()["claimed_at"]
    check("retry_exhaustion_claimed_at_cleared", claimed_at is None)

    # Failed posts must not be selected again for automatic republish.
    unit_after_exhaust = scheduler.select_next_publish_unit(album_wait_seconds=0)
    selected_ids = []
    if unit_after_exhaust is not None:
        selected_ids = [p["message_id"] for p in unit_after_exhaust["posts"]]
    check(
        "retry_exhaustion_not_auto_republished",
        911 not in selected_ids,
        f"selected={selected_ids}",
    )

    # Token sanitization
    dirty = f"https://api.telegram.org/bot{scheduler.BOT_TOKEN}/copyMessage failed"
    clean = scheduler.sanitize_telegram_error(dirty)
    check("token_not_logged", "PHASE5-TEST-TOKEN-SECRET" not in clean and "***REDACTED***" in clean)

    after = _live_snapshot()
    check("live_db_preserved_offset", after["offset"] == before["offset"])
    check("live_db_preserved_count", after["count"] == before["count"])
    check("live_db_preserved_rows", after["rows"] == before["rows"])

    print("ALL_PHASE5_TESTS_PASSED")
    print("LIVE_AFTER", after)
    return 0


if __name__ == "__main__":
    # Ensure requests is importable inside test helpers
    import requests  # noqa: F401

    try:
        raise SystemExit(main())
    except Exception:
        import traceback

        traceback.print_exc()
        # Still report live DB safety if possible
        try:
            print("LIVE_SNAPSHOT_ON_ERROR", _live_snapshot())
        except Exception:
            pass
        raise SystemExit(1)
