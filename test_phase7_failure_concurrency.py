"""Isolated Phase 7 tests for failure handling and concurrency."""

from __future__ import annotations

import importlib
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import MagicMock, patch

import requests


ROOT = Path(__file__).resolve().parent
TEST_SETTINGS = {
    "TELEGRAM_TEST_MODE": "1",
    "TELEGRAM_BOT_TOKEN": "000000:PHASE7_D_TEST_PLACEHOLDER",
    "SOURCE_CHANNEL_ID": "-1004344475194",
    "DESTINATION_CHANNEL_ID": "-1004456077151",
    "SCHEDULER_LOCK_FILE": "scheduler_test.lock",
    "PUBLISH_INTERVAL_SECONDS": "30",
    "EMPTY_QUEUE_CHECK_SECONDS": "7",
    "ALBUM_WAIT_SECONDS": "0",
    "PUBLISH_TRANSIENT_ATTEMPTS": "3",
    "MAX_PUBLISH_RETRIES": "5",
    "STALE_CLAIM_SECONDS": "900",
}


class Phase7FailureConcurrencyTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        source_db = ROOT / "telegram_test.db"
        if not source_db.is_file():
            raise FileNotFoundError(source_db)

        cls.original_env = {
            key: os.environ.get(key)
            for key in set(TEST_SETTINGS) | {"DB_PATH"}
        }
        cls.temp_dir = tempfile.TemporaryDirectory()
        cls.db_path = Path(cls.temp_dir.name) / "phase7_d_test.db"
        shutil.copy2(source_db, cls.db_path)
        os.environ.update(TEST_SETTINGS)
        os.environ["DB_PATH"] = str(cls.db_path)

        import config
        import database
        import scheduler
        import telegram_handler

        cls.config = importlib.reload(config)
        cls.original_db_name = database.DB_NAME
        cls.database = importlib.reload(database)
        cls.scheduler = importlib.reload(scheduler)
        cls.handler = importlib.reload(telegram_handler)
        cls.database.DB_NAME = str(cls.db_path)
        cls.database.initialize_database()

    @classmethod
    def tearDownClass(cls):
        cls.database.DB_NAME = cls.original_db_name
        for key, value in cls.original_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        cls.temp_dir.cleanup()

    def setUp(self):
        self.scheduler = importlib.reload(self.scheduler)
        self.handler = importlib.reload(self.handler)
        with self.database.get_connection() as conn:
            self.database._begin_immediate(conn)
            conn.execute("DELETE FROM incoming_posts")
            conn.execute(
                "DELETE FROM bot_state WHERE key = 'update_offset'"
            )
            self.database._set_state(conn, "publish_cycle_id", 1)
            self.database._set_state(conn, "cycle_post_number", 0)
            self.database._set_state(conn, "go_live_cutoff", 0)
            self.database._commit(conn)

    def _insert_post(
        self,
        message_id,
        *,
        status="pending",
        retry_count=0,
        media_group_id=None,
        claimed_at=None,
        claim_owner=None,
        destination_message_ids="[41]",
    ):
        payload = {
            "message_id": message_id,
            "chat": {"id": -1004344475194},
            "date": 1,
            "text": f"phase7-post-{message_id}",
        }
        with self.database.get_connection() as conn:
            self.database._begin_immediate(conn)
            cursor = conn.execute(
                """
                INSERT INTO incoming_posts (
                    channel_id, message_id, media_group_id, message_type,
                    message_data, status, retry_count, claimed_at,
                    claim_owner, destination_message_ids
                )
                VALUES (?, ?, ?, 'text', ?, ?, ?, ?, ?, ?)
                """,
                (
                    -1004344475194,
                    message_id,
                    media_group_id,
                    json.dumps(payload),
                    status,
                    retry_count,
                    claimed_at,
                    claim_owner,
                    destination_message_ids,
                ),
            )
            post_id = cursor.lastrowid
            self.database._commit(conn)
        return post_id

    def _state(self, post_id):
        with self.database.get_connection() as conn:
            row = conn.execute(
                """
                SELECT status, retry_count, last_error, claim_owner,
                       claimed_at, destination_message_ids
                FROM incoming_posts
                WHERE id = ?
                """,
                (post_id,),
            ).fetchone()
        return tuple(row)

    def _row_by_message(self, message_id):
        with self.database.get_connection() as conn:
            return conn.execute(
                "SELECT * FROM incoming_posts WHERE message_id = ?",
                (message_id,),
            ).fetchone()

    def _success_response(self, destination_id):
        response = MagicMock()
        response.status_code = 200
        response.ok = True
        response.headers = {}
        response.text = "ok"
        response.json.return_value = {
            "ok": True,
            "result": {"message_id": destination_id},
        }
        return response

    @staticmethod
    def _http_error(status, description, retry_after=None):
        response = MagicMock()
        response.status_code = status
        response.ok = status < 400
        response.headers = {}
        payload = {"ok": False, "description": description}
        if retry_after is not None:
            payload["parameters"] = {"retry_after": retry_after}
            response.headers["Retry-After"] = str(retry_after)
        response.text = description
        response.json.return_value = payload
        return response

    def _publish_one(self, message_id, session, owner="phase7-d-owner"):
        self._insert_post(message_id)
        return self.scheduler.process_next_post(
            claim_owner=owner,
            session=session,
        )

    def test_http_429_waits_for_retry_after_while_claim_is_owned(self):
        post_id = self._insert_post(1001)
        session = MagicMock()
        events = []

        def verify_wait(seconds):
            events.append(("wait", seconds))
            state = self._state(post_id)
            self.assertEqual(state[0], "publishing")
            self.assertEqual(state[3], "phase7-d-429")

        responses = [
            self._http_error(429, "Too Many Requests", retry_after=9),
            self._success_response(81001),
        ]

        def request(*args, **kwargs):
            events.append(("request", kwargs["json"]["message_id"]))
            return responses.pop(0)

        session.post.side_effect = request
        with patch.object(
            self.scheduler,
            "_sleep_interruptible",
            side_effect=verify_wait,
        ):
            published = self.scheduler.process_next_post(
                claim_owner="phase7-d-429",
                session=session,
            )

        self.assertTrue(published)
        self.assertEqual(events, [("request", 1001), ("wait", 9), ("request", 1001)])
        self.assertEqual(session.post.call_count, 2)
        self.assertEqual(self._state(post_id)[0], "published")
        self.assertEqual(self._state(post_id)[1], 0)

    def test_connect_timeout_uses_configured_retry_without_duplicate_success(self):
        post_id = self._insert_post(1002)
        session = MagicMock()
        session.post.side_effect = [
            requests.exceptions.ConnectTimeout("connection not established"),
            self._success_response(81002),
        ]
        waits = []

        def verify_wait(seconds):
            waits.append(seconds)
            state = self._state(post_id)
            self.assertEqual(state[0], "publishing")
            self.assertEqual(state[3], "phase7-d-connect")

        with (
            patch.object(self.scheduler, "TRANSIENT_ATTEMPTS", 2),
            patch.object(
                self.scheduler,
                "_sleep_interruptible",
                side_effect=verify_wait,
            ),
        ):
            published = self.scheduler.process_next_post(
                claim_owner="phase7-d-connect",
                session=session,
            )

        self.assertTrue(published)
        self.assertEqual(session.post.call_count, 2)
        self.assertEqual(waits, [5])
        self.assertEqual(self._state(post_id)[0], "published")
        self.assertEqual(self._state(post_id)[5], "[81002]")

    def test_generic_timeout_is_failed_without_retry(self):
        session = MagicMock()
        session.post.side_effect = requests.exceptions.Timeout(
            "read timed out"
        )
        waits = []

        with patch.object(
            self.scheduler,
            "_sleep_interruptible",
            side_effect=lambda seconds: waits.append(seconds),
        ):
            published = self._publish_one(1003, session)

        row = self._row_by_message(1003)
        self.assertFalse(published)
        self.assertEqual(session.post.call_count, 1)
        self.assertEqual(waits, [])
        self.assertEqual(row["status"], "failed")
        self.assertIsNone(row["claim_owner"])

    def test_connection_loss_is_failed_without_retry(self):
        session = MagicMock()
        session.post.side_effect = requests.exceptions.ConnectionError(
            "connection dropped mid-request"
        )
        waits = []

        with patch.object(
            self.scheduler,
            "_sleep_interruptible",
            side_effect=lambda seconds: waits.append(seconds),
        ):
            published = self._publish_one(1004, session)

        row = self._row_by_message(1004)
        self.assertFalse(published)
        self.assertEqual(session.post.call_count, 1)
        self.assertEqual(waits, [])
        self.assertEqual(row["status"], "failed")
        self.assertIsNone(row["claim_owner"])
        self.assertIn("AMBIGUOUS_PUBLISH_MANUAL_REVIEW", row["last_error"])

    def _assert_ambiguous_http_failure(self, status, message_id):
        session = MagicMock()
        session.post.return_value = self._http_error(
            status,
            f"HTTP {status} simulated response",
        )
        waits = []
        with patch.object(
            self.scheduler,
            "_sleep_interruptible",
            side_effect=lambda seconds: waits.append(seconds),
        ):
            published = self._publish_one(message_id, session)

        row = self._row_by_message(message_id)
        self.assertFalse(published)
        self.assertEqual(session.post.call_count, 1)
        self.assertEqual(waits, [])
        self.assertEqual(row["status"], "failed")
        self.assertIsNone(row["claim_owner"])
        self.assertIn("AMBIGUOUS_PUBLISH_MANUAL_REVIEW", row["last_error"])

    def test_http_500_ambiguous_failure_policy(self):
        self._assert_ambiguous_http_failure(500, 1005)

    def test_http_502_ambiguous_failure_policy(self):
        self._assert_ambiguous_http_failure(502, 1006)

    def test_http_503_ambiguous_failure_policy(self):
        self._assert_ambiguous_http_failure(503, 1007)

    def test_unreadable_success_response_fails_without_corrupting_row(self):
        post_id = self._insert_post(1008)
        session = MagicMock()
        response = MagicMock()
        response.status_code = 200
        response.ok = True
        response.headers = {}
        response.text = "unreadable"
        response.json.side_effect = ValueError("invalid JSON")
        session.post.return_value = response

        published = self.scheduler.process_next_post(
            claim_owner="phase7-d-invalid-json",
            session=session,
        )

        row = self._state(post_id)
        self.assertFalse(published)
        self.assertEqual(session.post.call_count, 1)
        self.assertEqual(row[0], "failed")
        self.assertIsNone(row[3])
        self.assertEqual(row[5], "[41]")

    def test_invalid_api_result_fails_without_corrupting_row(self):
        post_id = self._insert_post(1009)
        session = MagicMock()
        response = MagicMock()
        response.status_code = 200
        response.ok = True
        response.headers = {}
        response.text = "ok"
        response.json.return_value = {
            "ok": True,
            "result": {"unexpected": "shape"},
        }
        session.post.return_value = response

        published = self.scheduler.process_next_post(
            claim_owner="phase7-d-invalid-result",
            session=session,
        )

        row = self._state(post_id)
        self.assertFalse(published)
        self.assertEqual(session.post.call_count, 1)
        self.assertEqual(row[0], "failed")
        self.assertIsNone(row[3])
        self.assertEqual(row[5], "[41]")

    def test_first_permanent_failure_needs_explicit_manual_recovery(self):
        post_id = self._insert_post(1010)
        session = MagicMock()
        session.post.side_effect = [
            self._http_error(400, "Bad Request"),
            self._success_response(81010),
        ]

        published = self.scheduler.process_next_post(
            claim_owner="phase7-d-permanent",
            session=session,
        )
        failed_row = self._row_by_message(1010)
        second_result = self.scheduler.process_next_post(
            claim_owner="phase7-d-permanent",
            session=session,
        )

        self.assertFalse(published)
        self.assertEqual(failed_row["status"], "failed")
        self.assertEqual(failed_row["retry_count"], 1)
        self.assertFalse(second_result)
        self.assertEqual(session.post.call_count, 1)
        self.assertEqual(self._row_by_message(1010)["status"], "failed")

        self.assertEqual(
            self.database.reset_failed_posts([post_id]),
            1,
        )
        self.assertTrue(
            self.scheduler.process_next_post(
                claim_owner="phase7-d-permanent-recovery",
                session=session,
            )
        )
        self.assertEqual(session.post.call_count, 2)
        self.assertEqual(self._row_by_message(1010)["status"], "published")

    def test_successful_copy_finalization_failure_is_quarantined(self):
        post_id = self._insert_post(1012)
        published_id = self._insert_post(1014, status="published")
        session = MagicMock()
        session.post.return_value = self._success_response(81012)

        with patch.object(
            self.scheduler,
            "mark_as_published",
            side_effect=RuntimeError("simulated local commit failure"),
        ):
            published = self.scheduler.process_next_post(
                claim_owner="phase7-d-finalization",
                session=session,
            )

        row = self._row_by_message(1012)
        self.assertFalse(published)
        self.assertEqual(session.post.call_count, 1)
        self.assertEqual(row["status"], "uncertain")
        self.assertEqual(row["claim_owner"], "phase7-d-finalization")
        self.assertIsNotNone(row["claimed_at"])
        self.assertEqual(row["destination_message_ids"], "[81012]")
        self.assertIn(
            "TELEGRAM_ACCEPTED_LOCAL_FINALIZATION_FAILED",
            row["last_error"],
        )

        next_result = self.scheduler.process_next_post(
            claim_owner="phase7-d-finalization",
            session=session,
        )
        self.assertFalse(next_result)
        self.assertEqual(session.post.call_count, 1)
        self.assertEqual(self._row_by_message(1012)["status"], "uncertain")
        self.assertEqual(self._row_by_message(1014)["status"], "published")
        self.assertEqual(self.database.get_publish_cycle_id(), 1)

        self.assertEqual(
            self.database.reconcile_uncertain_posts(
                [post_id],
                "published",
                destination_message_ids=[81012],
            ),
            1,
        )
        reconciled = self._row_by_message(1012)
        self.assertEqual(reconciled["status"], "published")
        self.assertIsNone(reconciled["claim_owner"])
        with self.database.get_connection() as conn:
            history = conn.execute(
                """
                SELECT source_message_ids, destination_message_ids
                FROM publish_history
                WHERE incoming_post_ids = ?
                """,
                (json.dumps([post_id]),),
            ).fetchone()
        self.assertEqual(history["source_message_ids"], "[1012]")
        self.assertEqual(history["destination_message_ids"], "[81012]")

    def test_uncertain_posts_can_be_retried_only_after_manual_confirmation(self):
        post_id = self._insert_post(
            1013,
            status="uncertain",
            claim_owner="reconciliation-owner",
            claimed_at="2026-01-01 00:00:00",
        )
        self.assertIsNone(
            self.scheduler.select_next_publish_unit(album_wait_seconds=0)
        )
        self.assertEqual(
            self.database.reconcile_uncertain_posts(
                [post_id],
                "not_published",
            ),
            1,
        )
        row = self._row_by_message(1013)
        self.assertEqual(row["status"], "pending")
        self.assertIsNone(row["claim_owner"])
        self.assertIsNone(row["claimed_at"])
        self.assertEqual(
            self.scheduler.select_next_publish_unit(album_wait_seconds=0)[
                "posts"
            ][0]["message_id"],
            1013,
        )

    def test_stale_claim_requires_manual_recovery(self):
        post_id = self._insert_post(
            1011,
            status="publishing",
            claimed_at="2000-01-01 00:00:00",
            claim_owner="abandoned-worker",
            destination_message_ids="[8111, 8112]",
        )

        recovered = self.database.release_stale_claims(stale_seconds=900)
        state = self._state(post_id)
        selected = self.scheduler.select_next_publish_unit(
            album_wait_seconds=0
        )

        self.assertEqual(recovered, 1)
        self.assertEqual(state[0], "failed")
        self.assertIsNone(state[3])
        self.assertIsNone(state[4])
        self.assertEqual(state[5], "[8111, 8112]")
        self.assertIn("STALE_CLAIM_MANUAL_REVIEW", state[2])
        self.assertIsNone(selected)

        reset_count = self.database.reset_failed_posts([post_id])
        self.assertEqual(reset_count, 1)
        self.assertEqual(self._state(post_id)[0], "pending")
        self.assertEqual(self._state(post_id)[5], "[8111, 8112]")

    def _update(self, update_id, message_id, media_group_id=None):
        message = {
            "message_id": message_id,
            "chat": {"id": -1004344475194},
            "date": 1,
            "text": f"listener-{message_id}",
        }
        if media_group_id is not None:
            message["media_group_id"] = media_group_id
            message.pop("text")
            message["photo"] = [{"file_id": f"photo-{message_id}"}]
        return {"update_id": update_id, "channel_post": message}

    def test_repeated_update_id_does_not_insert_duplicate_message(self):
        update = self._update(2001, 1101)
        with patch.object(self.handler, "_log_processed_update"):
            self.handler.process_updates([update])
            self.handler.process_updates([update])

        with self.database.get_connection() as conn:
            row_count = conn.execute(
                """
                SELECT COUNT(*) AS c FROM incoming_posts
                WHERE channel_id = ? AND message_id = ?
                """,
                (-1004344475194, 1101),
            ).fetchone()["c"]

        self.assertEqual(row_count, 1)
        self.assertEqual(self.database.get_offset(), 2002)

    def test_duplicate_album_member_update_is_ignored(self):
        updates = [
            self._update(2101, 1201, "album-duplicate"),
            self._update(2102, 1202, "album-duplicate"),
            self._update(2102, 1202, "album-duplicate"),
            self._update(2103, 1203, "album-duplicate"),
        ]
        with patch.object(self.handler, "_log_processed_update"):
            self.handler.process_updates(updates)

        with self.database.get_connection() as conn:
            rows = conn.execute(
                """
                SELECT message_id, media_group_id
                FROM incoming_posts
                WHERE media_group_id = ?
                ORDER BY message_id
                """,
                ("album-duplicate",),
            ).fetchall()

        self.assertEqual([row["message_id"] for row in rows], [1201, 1202, 1203])
        self.assertEqual(len(rows), 3)
        self.assertEqual(self.database.get_offset(), 2104)

    def test_replayed_updates_after_listener_restart_do_not_duplicate(self):
        updates = [
            self._update(2201, 1301, "album-replay"),
            self._update(2202, 1302, "album-replay"),
        ]

        def run_poll(batch):
            requested_offsets = []

            def fake_fetch(offset, _session):
                requested_offsets.append(offset)
                self.handler._shutdown_requested = True
                return {"result": batch}

            with (
                patch.object(self.handler, "install_signal_handlers"),
                patch.object(self.handler, "fetch_updates", side_effect=fake_fetch),
                patch.object(
                    self.handler.requests,
                    "Session",
                    return_value=MagicMock(),
                ),
            ):
                self.handler.listen_to_channel()
            return requested_offsets

        first_offsets = run_poll(updates)
        self.assertEqual(first_offsets, [None])
        self.assertEqual(self.database.get_offset(), 2203)

        self.handler = importlib.reload(self.handler)
        replay_offsets = run_poll(updates)

        with self.database.get_connection() as conn:
            count = conn.execute(
                "SELECT COUNT(*) AS c FROM incoming_posts"
            ).fetchone()["c"]
        self.assertEqual(replay_offsets, [2203])
        self.assertEqual(self.database.get_offset(), 2203)
        self.assertEqual(count, 2)

    def test_insert_failure_rolls_back_without_advancing_update_offset(self):
        update = self._update(2301, 1401)

        with (
            patch.object(
                self.database,
                "_insert_incoming_post",
                side_effect=sqlite3.IntegrityError("simulated insert failure"),
            ),
            patch.object(self.handler, "_log_processed_update"),
        ):
            with self.assertRaisesRegex(
                sqlite3.IntegrityError,
                "simulated insert failure",
            ):
                self.handler.process_updates([update])

        self.assertIsNone(self.database.get_offset())
        with self.database.get_connection() as conn:
            count = conn.execute(
                "SELECT COUNT(*) AS c FROM incoming_posts"
            ).fetchone()["c"]
        self.assertEqual(count, 0)

    def test_commit_failure_rolls_back_post_and_offset_together(self):
        update = self._update(2302, 1402)
        with self.assertRaisesRegex(
            sqlite3.OperationalError,
            "simulated commit failure",
        ):
            with patch.object(
                self.database,
                "_commit",
                side_effect=sqlite3.OperationalError(
                    "simulated commit failure"
                ),
            ):
                self.database.process_telegram_update(
                    update,
                    "-1004344475194",
                )

        self.assertIsNone(self.database.get_offset())
        with self.database.get_connection() as conn:
            count = conn.execute(
                "SELECT COUNT(*) AS c FROM incoming_posts"
            ).fetchone()["c"]
        self.assertEqual(count, 0)

    def test_simultaneous_process_lock_allows_only_one_scheduler_owner(self):
        with tempfile.TemporaryDirectory() as lock_dir:
            lock_path = Path(lock_dir) / "isolated_scheduler.lock"
            parent_lock = self.scheduler.SingleInstanceLock(str(lock_path))
            parent_lock.acquire()
            try:
                env = os.environ.copy()
                env.update(TEST_SETTINGS)
                env["DB_PATH"] = str(self.db_path)
                child_code = (
                    "import sys\n"
                    "import scheduler\n"
                    "lock = scheduler.SingleInstanceLock(sys.argv[1])\n"
                    "try:\n"
                    "    lock.acquire()\n"
                    "except scheduler.SchedulerError:\n"
                    "    print('LOCK_BLOCKED')\n"
                    "    raise SystemExit(0)\n"
                    "else:\n"
                    "    lock.release()\n"
                    "    print('LOCK_ACQUIRED')\n"
                    "    raise SystemExit(3)\n"
                )
                child = subprocess.run(
                    [sys.executable, "-c", child_code, str(lock_path)],
                    cwd=ROOT,
                    env=env,
                    capture_output=True,
                    text=True,
                    timeout=30,
                    check=False,
                )
            finally:
                parent_lock.release()

        self.assertEqual(child.returncode, 0, child.stderr)
        self.assertIn("LOCK_BLOCKED", child.stdout)
        self.assertNotIn(TEST_SETTINGS["TELEGRAM_BOT_TOKEN"], child.stdout)
        self.assertNotIn(TEST_SETTINGS["TELEGRAM_BOT_TOKEN"], child.stderr)

    def test_two_workers_claim_same_post_once_and_enforce_ownership(self):
        post_id = self._insert_post(1501)
        barrier = threading.Barrier(2)

        def claim(owner):
            barrier.wait(timeout=10)
            try:
                return owner, self.database.claim_posts([post_id], owner), None
            except self.database.ClaimError as exc:
                return owner, None, exc

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(claim, ("worker-one", "worker-two")))

        winners = [result for result in results if result[1] == [post_id]]
        losers = [result for result in results if result[2] is not None]
        self.assertEqual(len(winners), 1)
        self.assertEqual(len(losers), 1)
        winning_owner = winners[0][0]
        state = self._state(post_id)
        self.assertEqual(state[0], "publishing")
        self.assertEqual(state[3], winning_owner)

        with self.assertRaises(self.database.PublishOwnershipError):
            self.database.mark_as_published(
                [post_id],
                claim_owner=losers[0][0],
                destination_message_ids=[91501],
            )
        self.assertEqual(self._state(post_id)[0], "publishing")
        self.assertEqual(self._state(post_id)[3], winning_owner)

    def test_failed_album_claim_leaves_other_members_unmodified(self):
        post_ids = [
            self._insert_post(
                message_id,
                media_group_id="album-atomic-failure",
            )
            for message_id in (1601, 1602, 1603)
        ]
        self.database.claim_posts([post_ids[1]], "prior-owner")

        with self.assertRaises(self.database.ClaimError):
            self.database.claim_posts(post_ids, "second-owner")

        with self.database.get_connection() as conn:
            rows = conn.execute(
                """
                SELECT id, status, claim_owner
                FROM incoming_posts
                WHERE id IN (?, ?, ?)
                ORDER BY message_id
                """,
                post_ids,
            ).fetchall()
        self.assertEqual(
            [(row["status"], row["claim_owner"]) for row in rows],
            [
                ("pending", None),
                ("publishing", "prior-owner"),
                ("pending", None),
            ],
        )

    def test_finalization_commit_failure_rolls_back_entire_album(self):
        post_ids = [
            self._insert_post(
                message_id,
                status="pending",
                media_group_id="album-commit-failure",
                destination_message_ids="[old]",
            )
            for message_id in (1701, 1702, 1703)
        ]
        self.database.claim_posts(post_ids, "album-owner")

        with self.assertRaisesRegex(
            sqlite3.OperationalError,
            "simulated commit failure",
        ):
            with patch.object(
                self.database,
                "_commit",
                side_effect=sqlite3.OperationalError(
                    "simulated commit failure"
                ),
            ):
                self.database.mark_as_published(
                    post_ids,
                    claim_owner="album-owner",
                    destination_message_ids=[91701, 91702, 91703],
                )

        with self.database.get_connection() as conn:
            rows = conn.execute(
                """
                SELECT status, claim_owner, destination_message_ids
                FROM incoming_posts
                WHERE id IN (?, ?, ?)
                ORDER BY message_id
                """,
                post_ids,
            ).fetchall()
        self.assertEqual(
            [tuple(row) for row in rows],
            [
                ("publishing", "album-owner", "[old]"),
                ("publishing", "album-owner", "[old]"),
                ("publishing", "album-owner", "[old]"),
            ],
        )


if __name__ == "__main__":
    unittest.main()
