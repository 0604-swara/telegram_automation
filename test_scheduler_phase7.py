"""Isolated Phase 7 tests for scheduler configuration and cycle behavior."""

from __future__ import annotations

import importlib
import json
import os
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch


ROOT = Path(__file__).resolve().parent
TEST_SETTINGS = {
    "TELEGRAM_TEST_MODE": "1",
    "TELEGRAM_BOT_TOKEN": "test:placeholder",
    "SOURCE_CHANNEL_ID": "-1004344475194",
    "DESTINATION_CHANNEL_ID": "-1004456077151",
    "SCHEDULER_LOCK_FILE": "scheduler_test.lock",
    "PUBLISH_INTERVAL_SECONDS": "30",
    "EMPTY_QUEUE_CHECK_SECONDS": "7",
    "ALBUM_WAIT_SECONDS": "3",
    "MAX_PUBLISH_RETRIES": "5",
    "STALE_CLAIM_SECONDS": "900",
    "PUBLISH_TRANSIENT_ATTEMPTS": "2",
}
_MISSING = object()


class SchedulerPhase7Tests(unittest.TestCase):
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
        cls.db_path = Path(cls.temp_dir.name) / "scheduler_phase7_test.db"
        shutil.copy2(source_db, cls.db_path)
        os.environ.update(TEST_SETTINGS)
        os.environ["DB_PATH"] = str(cls.db_path)

        import config
        import database
        import scheduler

        cls.config = importlib.reload(config)
        cls.original_db_name = database.DB_NAME
        cls.database = importlib.reload(database)
        cls.scheduler = importlib.reload(scheduler)
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
        os.environ.update(TEST_SETTINGS)
        os.environ["DB_PATH"] = str(self.db_path)
        self._reload_scheduler()

        with self.database.get_connection() as conn:
            self.database._begin_immediate(conn)
            conn.execute("DELETE FROM incoming_posts")
            self.database._set_state(conn, "publish_cycle_id", 1)
            self.database._set_state(conn, "cycle_post_number", 0)
            self.database._commit(conn)

    def _reload_scheduler(self, updates=None, remove=()):
        updates = updates or {}
        keys = set(updates) | set(remove)
        previous = {
            key: os.environ.get(key, _MISSING)
            for key in keys
        }
        os.environ.update(updates)
        for key in remove:
            os.environ.pop(key, None)
        try:
            with patch("dotenv.load_dotenv", return_value=False):
                self.scheduler = importlib.reload(self.scheduler)
        finally:
            for key, value in previous.items():
                if value is _MISSING:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value
        return self.scheduler

    def _insert_post(self, message_id, status="pending"):
        payload = {
            "message_id": message_id,
            "chat": {"id": -1004344475194},
            "text": f"post-{message_id}",
        }
        with self.database.get_connection() as conn:
            self.database._begin_immediate(conn)
            cursor = conn.execute(
                """
                INSERT INTO incoming_posts (
                    channel_id, message_id, message_type, message_data,
                    status, retry_count
                )
                VALUES (?, ?, 'text', ?, ?, 0)
                """,
                (
                    -1004344475194,
                    message_id,
                    json.dumps(payload),
                    status,
                ),
            )
            post_id = cursor.lastrowid
            self.database._commit(conn)
        return post_id

    def _statuses(self):
        with self.database.get_connection() as conn:
            rows = conn.execute(
                "SELECT message_id, status FROM incoming_posts ORDER BY message_id"
            ).fetchall()
        return {row["message_id"]: row["status"] for row in rows}

    @staticmethod
    def _success_response(destination_id):
        response = MagicMock()
        response.status_code = 200
        response.ok = True
        response.text = "ok"
        response.json.return_value = {
            "ok": True,
            "result": {"message_id": destination_id},
        }
        return response

    def _validate_test_environment(self):
        with patch.object(
            self.config,
            "TEST_DB_PATH",
            self.db_path.resolve(),
        ):
            self.scheduler.validate_environment()

    def test_test_mode_interval_settings_load_and_validate(self):
        self.assertEqual(self.scheduler.INTERVAL, 30)
        self.assertEqual(self.scheduler.EMPTY_QUEUE_CHECK_SECONDS, 7)
        self.assertEqual(self.scheduler.ALBUM_WAIT_SECONDS, 3)

        self._validate_test_environment()

    def test_production_interval_remains_300_seconds(self):
        scheduler = self._reload_scheduler(
            {
                "TELEGRAM_TEST_MODE": "0",
                "PUBLISH_INTERVAL_SECONDS": "300",
            }
        )
        self.assertEqual(scheduler.INTERVAL, 300)

    def test_missing_interval_settings_use_safe_defaults(self):
        scheduler = self._reload_scheduler(
            remove=(
                "PUBLISH_INTERVAL_SECONDS",
                "EMPTY_QUEUE_CHECK_SECONDS",
                "ALBUM_WAIT_SECONDS",
            )
        )
        self.assertEqual(scheduler.INTERVAL, 300)
        self.assertEqual(scheduler.EMPTY_QUEUE_CHECK_SECONDS, 30)
        self.assertEqual(scheduler.ALBUM_WAIT_SECONDS, 15)
        self._validate_test_environment()

    def test_invalid_interval_settings_are_rejected_safely(self):
        invalid_values = (
            ("PUBLISH_INTERVAL_SECONDS", "not-a-number"),
            ("PUBLISH_INTERVAL_SECONDS", "0"),
            ("PUBLISH_INTERVAL_SECONDS", "-1"),
            ("EMPTY_QUEUE_CHECK_SECONDS", "0"),
            ("EMPTY_QUEUE_CHECK_SECONDS", "-1"),
            ("ALBUM_WAIT_SECONDS", "not-a-number"),
            ("ALBUM_WAIT_SECONDS", "-1"),
        )
        for name, value in invalid_values:
            with self.subTest(setting=name, value=value):
                scheduler = self._reload_scheduler({name: value})
                with patch.object(
                    self.config,
                    "TEST_DB_PATH",
                    self.db_path.resolve(),
                ):
                    with self.assertRaises(
                        self.config.ConfigurationError
                    ) as raised:
                        scheduler.validate_environment()
                self.assertIn(name, str(raised.exception))
                self.assertNotIn(TEST_SETTINGS["TELEGRAM_BOT_TOKEN"], str(raised.exception))

    def test_zero_album_wait_is_allowed_for_controlled_tests(self):
        scheduler = self._reload_scheduler({"ALBUM_WAIT_SECONDS": "0"})
        self.assertEqual(scheduler.ALBUM_WAIT_SECONDS, 0)
        self._validate_test_environment()

    def test_missing_bot_token_is_rejected_without_exposing_a_value(self):
        with patch.dict(os.environ, {"TELEGRAM_BOT_TOKEN": ""}):
            with patch.object(
                self.config,
                "TEST_DB_PATH",
                self.db_path.resolve(),
            ):
                with self.assertRaises(
                    self.config.ConfigurationError
                ) as raised:
                    self.scheduler.validate_environment()
        self.assertIn("TELEGRAM_BOT_TOKEN", str(raised.exception))
        self.assertNotIn(
            TEST_SETTINGS["TELEGRAM_BOT_TOKEN"],
            str(raised.exception),
        )

    def test_scheduler_logs_configured_publish_and_empty_waits(self):
        self.scheduler._shutdown_requested = False

        def stop_after_empty_wait(_seconds):
            self.scheduler.request_shutdown()

        with (
            patch.object(
                self.scheduler,
                "process_next_post",
                return_value=False,
            ),
            patch.object(self.scheduler, "_sleep_interruptible", stop_after_empty_wait),
            patch.object(self.scheduler._logger, "info") as log_info,
        ):
            self.scheduler.run_scheduler_loop(
                claim_owner="scheduler-phase7-test",
                session=MagicMock(),
            )

        log_info.assert_any_call(
            "Scheduler loop starting. owner=%s interval=%ss empty_wait=%ss",
            "scheduler-phase7-test",
            30,
            7,
        )
        log_info.assert_any_call(
            "No publish this slot. queue=%s; next slot in %ss",
            self.database.get_queue_stats(),
            self.scheduler.INTERVAL,
        )

    def test_scheduler_uses_five_minute_slots_while_idle(self):
        scheduler = self._reload_scheduler(
            {
                "PUBLISH_INTERVAL_SECONDS": "300",
                "EMPTY_QUEUE_CHECK_SECONDS": "30",
            }
        )
        clock = {"now": 100.0}
        attempts = []
        waits = []

        def fake_sleep(seconds):
            waits.append(seconds)
            clock["now"] += seconds

        def attempt(*args, **kwargs):
            attempts.append(clock["now"])
            if len(attempts) == 2:
                scheduler._shutdown_requested = True
                return True
            return False

        scheduler._shutdown_requested = False
        with (
            patch.object(scheduler.time, "monotonic",
                         side_effect=lambda: clock["now"]),
            patch.object(scheduler, "_sleep_interruptible",
                         side_effect=fake_sleep),
            patch.object(scheduler, "process_next_post", side_effect=attempt),
            patch.object(scheduler, "record_heartbeat"),
            patch.object(scheduler, "get_queue_stats", return_value={}),
        ):
            scheduler.run_scheduler_loop(
                claim_owner="scheduler-fixed-slots",
                session=MagicMock(),
            )

        self.assertEqual(attempts, [100.0, 400.0])
        self.assertEqual(sum(waits), 300)
        self.assertTrue(all(wait <= 30 for wait in waits))

    def test_new_post_during_active_publish_is_selected_next(self):
        self._insert_post(200)
        self._insert_post(100)
        session = MagicMock()
        published_ids = []

        def copy_message(_url, *, json, timeout):
            message_id = json["message_id"]
            published_ids.append(message_id)
            if message_id == 200:
                self._insert_post(300)
            return self._success_response(9000 + message_id)

        session.post.side_effect = copy_message

        completed_first = self.scheduler.process_next_post(
            claim_owner="scheduler-priority-test",
            session=session,
        )
        self.assertTrue(completed_first)
        self.assertEqual(published_ids, [200])
        self.assertEqual(self._statuses(), {100: "pending", 200: "published", 300: "pending"})

        self.assertTrue(
            self.scheduler.process_next_post(
                claim_owner="scheduler-priority-test",
                session=session,
            )
        )
        self.assertEqual(published_ids, [200, 300])
        self.assertEqual(self._statuses(), {100: "pending", 200: "published", 300: "published"})

        self.assertTrue(
            self.scheduler.process_next_post(
                claim_owner="scheduler-priority-test",
                session=session,
            )
        )
        self.assertEqual(published_ids, [200, 300, 100])
        self.assertEqual(len(published_ids), len(set(published_ids)))

    def test_pre_live_rows_are_never_selected_claimed_or_cycle_reset(self):
        old_pending = self._insert_post(700)
        old_published = self._insert_post(600, status="published")
        with self.database.get_connection() as conn:
            self.database._begin_immediate(conn)
            conn.execute(
                """
                UPDATE incoming_posts
                SET live_eligible = 0
                WHERE id IN (?, ?)
                """,
                (old_pending, old_published),
            )
            self.database._commit(conn)

        self.assertIsNone(self.scheduler.select_next_publish_unit())
        self.assertFalse(self.database.restart_posting_cycle())
        with self.assertRaises(self.database.ClaimError):
            self.database.claim_posts([old_pending], "pre-live-claim")
        with self.database.get_connection() as conn:
            statuses = conn.execute(
                """
                SELECT id, status, live_eligible
                FROM incoming_posts
                WHERE id IN (?, ?)
                ORDER BY id
                """,
                (old_pending, old_published),
            ).fetchall()
        queue = self.database.get_queue_stats()
        self.assertEqual(queue["ineligible_count"], 2)
        self.assertEqual(
            [(row["status"], row["live_eligible"]) for row in statuses],
            [("pending", 0), ("published", 0)],
        )

    def test_restart_resumes_pending_without_republishing_published_posts(self):
        self._insert_post(200)
        self._insert_post(100)
        session = MagicMock()
        session.post.side_effect = [
            self._success_response(9200),
            self._success_response(9100),
        ]

        self.assertTrue(
            self.scheduler.process_next_post(
                claim_owner="scheduler-before-restart",
                session=session,
            )
        )
        self.assertEqual(self._statuses(), {100: "pending", 200: "published"})

        self.scheduler = importlib.reload(self.scheduler)
        self.assertTrue(
            self.scheduler.process_next_post(
                claim_owner="scheduler-after-restart",
                session=session,
            )
        )

        requested_messages = [
            call.kwargs["json"]["message_id"]
            for call in session.post.call_args_list
        ]
        self.assertEqual(requested_messages, [200, 100])
        self.assertEqual(self._statuses(), {100: "published", 200: "published"})
        self.assertEqual(self.database.get_post_number(), 2)

    def test_new_post_between_cycles_is_prioritized_without_resetting_published(self):
        self._insert_post(100, status="published")
        self._insert_post(200, status="published")
        self._insert_post(300)
        session = MagicMock()
        session.post.return_value = self._success_response(9300)

        self.assertTrue(
            self.scheduler.process_next_post(
                claim_owner="scheduler-between-cycles",
                session=session,
            )
        )

        self.assertEqual(
            session.post.call_args.kwargs["json"]["message_id"],
            300,
        )
        self.assertEqual(self._statuses(), {100: "published", 200: "published", 300: "published"})
        self.assertEqual(self.database.get_publish_cycle_id(), 1)

    def test_cycle_restart_resets_numbering_and_publishes_next_cycle(self):
        self._insert_post(100)
        self._insert_post(200)
        session = MagicMock()
        calls = []

        def copy_message(_url, *, json, timeout):
            message_id = json["message_id"]
            calls.append(message_id)
            return self._success_response(9400 + len(calls))

        session.post.side_effect = copy_message
        for _ in range(2):
            self.assertTrue(
                self.scheduler.process_next_post(
                    claim_owner="scheduler-cycle-test",
                    session=session,
                )
            )

        self.assertEqual(self.database.get_publish_cycle_id(), 1)
        self.assertEqual(self.database.get_post_number(), 2)
        self.assertEqual(self._statuses(), {100: "published", 200: "published"})

        self.assertTrue(
            self.scheduler.process_next_post(
                claim_owner="scheduler-cycle-test",
                session=session,
            )
        )
        self.assertEqual(self.database.get_publish_cycle_id(), 2)
        self.assertEqual(self.database.get_post_number(), 1)
        self.assertEqual(calls, [200, 100, 200])
        self.assertEqual(self._statuses(), {100: "pending", 200: "published"})

    def test_empty_queue_waits_for_configured_interval_and_detects_new_post(self):
        session = MagicMock()
        session.post.return_value = self._success_response(9501)
        waits = []
        post_inserted = False
        clock = {"now": 0.0}

        def wait_and_change_state(seconds):
            nonlocal post_inserted
            waits.append(seconds)
            clock["now"] += seconds
            if (
                seconds == self.scheduler.EMPTY_QUEUE_CHECK_SECONDS
                and not post_inserted
            ):
                self._insert_post(501)
                post_inserted = True
            else:
                if session.post.called:
                    self.scheduler.request_shutdown()

        self.scheduler._shutdown_requested = False
        with (
            patch.object(
                self.scheduler.time,
                "monotonic",
                side_effect=lambda: clock["now"],
            ),
            patch.object(
                self.scheduler,
                "_sleep_interruptible",
                side_effect=wait_and_change_state,
            ),
            patch.object(self.scheduler, "record_heartbeat"),
        ):
            self.scheduler.run_scheduler_loop(
                claim_owner="scheduler-empty-queue-test",
                session=session,
            )

        self.assertEqual(waits, [7, 7, 7, 7, 2, 7])
        self.assertEqual(session.post.call_count, 1)
        self.assertEqual(self._statuses(), {501: "published"})

    def test_empty_queue_does_not_busy_loop_and_shutdown_is_graceful(self):
        waits = []
        self.scheduler._shutdown_requested = False

        def wait_then_shutdown(seconds):
            waits.append(seconds)
            if len(waits) == 2:
                self.scheduler.request_shutdown()

        with (
            patch.object(
                self.scheduler,
                "process_next_post",
                return_value=False,
            ) as process_next,
            patch.object(
                self.scheduler,
                "_sleep_interruptible",
                side_effect=wait_then_shutdown,
            ),
            patch.object(self.scheduler, "record_heartbeat"),
        ):
            self.scheduler.run_scheduler_loop(
                claim_owner="scheduler-empty-shutdown-test",
                session=MagicMock(),
            )

        self.assertEqual(waits, [7, 7])
        self.assertEqual(process_next.call_count, 1)
        self.assertEqual(self._statuses(), {})


if __name__ == "__main__":
    unittest.main()
