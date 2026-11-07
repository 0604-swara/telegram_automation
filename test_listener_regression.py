"""Isolated listener and album regression tests."""

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
TEST_ENV = {
    "DB_PATH": None,
    "TELEGRAM_BOT_TOKEN": "000000:LISTENER_TEST_TOKEN",
    "SOURCE_CHANNEL_ID": "-100111",
    "DESTINATION_CHANNEL_ID": "-100222",
}


class ListenerRegressionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        source_db = ROOT / "telegram_test.db"
        if not source_db.is_file():
            raise FileNotFoundError(source_db)

        cls.original_env = {
            key: os.environ.get(key) for key in TEST_ENV
        }
        cls.temp_dir = tempfile.TemporaryDirectory()
        cls.db_path = Path(cls.temp_dir.name) / "listener_test.db"
        shutil.copy2(source_db, cls.db_path)
        os.environ.update(
            {
                **TEST_ENV,
                "DB_PATH": str(cls.db_path),
            }
        )

        import config
        import database
        import telegram_handler
        import scheduler

        cls.original_db_name = database.DB_NAME
        cls.config = importlib.reload(config)
        cls.database = importlib.reload(database)
        cls.handler = importlib.reload(telegram_handler)
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
        with self.database.get_connection() as conn:
            self.database._begin_immediate(conn)
            conn.execute("DELETE FROM incoming_posts")
            conn.execute(
                "DELETE FROM bot_state WHERE key = 'update_offset'"
            )
            self.database._set_state(
                conn,
                "go_live_bootstrap_status",
                self.database.GO_LIVE_READY,
            )
            self.database._set_state(
                conn,
                "go_live_first_update_id",
                0,
            )
            self.database._commit(conn)

    @staticmethod
    def _album_updates():
        updates = []
        for offset, message_id in enumerate((701, 702, 703), start=1001):
            message = {
                "message_id": message_id,
                "chat": {"id": -100111},
                "date": 1,
                "media_group_id": "album-listener-1",
                "photo": [
                    {
                        "file_id": f"file-{message_id}",
                        "width": 640,
                        "height": 480,
                    }
                ],
            }
            if message_id == 701:
                message["caption"] = "Album caption"
            updates.append(
                {
                    "update_id": offset,
                    "channel_post": message,
                }
            )
        return updates

    def _ingest_album(self):
        return self.handler.process_updates(self._album_updates())

    def _prepare_go_live_bootstrap(self):
        with self.database.get_connection() as conn:
            self.database._begin_immediate(conn)
            self.database._set_state(
                conn,
                "go_live_bootstrap_status",
                self.database.GO_LIVE_PENDING,
            )
            self.database._set_state(
                conn,
                "go_live_first_update_id",
                "",
            )
            conn.execute(
                "DELETE FROM bot_state WHERE key = 'update_offset'"
            )
            self.database._commit(conn)

    def test_album_caption_and_shared_media_group_are_stored(self):
        latest_offset = self._ingest_album()

        with self.database.get_connection() as conn:
            rows = conn.execute(
                """
                SELECT message_id, media_group_id, message_data
                FROM incoming_posts
                ORDER BY message_id
                """
            ).fetchall()

        self.assertEqual(latest_offset, 1004)
        self.assertEqual([row["message_id"] for row in rows], [701, 702, 703])
        self.assertEqual(
            {row["media_group_id"] for row in rows},
            {"album-listener-1"},
        )
        stored_messages = {
            row["message_id"]: json.loads(row["message_data"])
            for row in rows
        }
        self.assertEqual(stored_messages[701]["caption"], "Album caption")
        self.assertNotIn("caption", stored_messages[702])
        self.assertNotIn("caption", stored_messages[703])

    def test_new_text_and_single_photo_caption_are_stored(self):
        updates = [
            {
                "update_id": 1101,
                "channel_post": {
                    "message_id": 801,
                    "chat": {"id": -100111},
                    "date": 1,
                    "text": "New after go-live",
                },
            },
            {
                "update_id": 1102,
                "channel_post": {
                    "message_id": 802,
                    "chat": {"id": -100111},
                    "date": 1,
                    "photo": [{"file_id": "photo-new"}],
                    "caption": "Photo caption",
                },
            },
        ]

        self.handler.process_updates(updates)
        with self.database.get_connection() as conn:
            rows = conn.execute(
                """
                SELECT message_id, message_type, message_data,
                       live_eligible
                FROM incoming_posts
                ORDER BY message_id
                """
            ).fetchall()
        self.assertEqual(
            [(row["message_id"], row["message_type"]) for row in rows],
            [(801, "text"), (802, "photo")],
        )
        self.assertEqual([row["live_eligible"] for row in rows], [1, 1])
        self.assertEqual(
            json.loads(rows[1]["message_data"])["caption"],
            "Photo caption",
        )

    def test_pre_live_pending_update_is_ignored_but_offset_advances(self):
        with self.database.get_connection() as conn:
            self.database._begin_immediate(conn)
            self.database._set_state(
                conn,
                "go_live_first_update_id",
                1202,
            )
            self.database._commit(conn)

        result = self.database.process_telegram_update(
            {
                "update_id": 1201,
                "channel_post": {
                    "message_id": 901,
                    "chat": {"id": -100111},
                    "date": 100,
                    "text": "Historical pending update",
                },
            },
            "-100111",
        )
        self.assertTrue(result["pre_live_ignored"])
        self.assertTrue(result["ignored"])
        self.assertFalse(result["inserted"])
        self.assertEqual(result["next_offset"], 1202)
        self.assertEqual(self.database.get_offset(), 1202)
        with self.database.get_connection() as conn:
            count = conn.execute(
                "SELECT COUNT(*) FROM incoming_posts"
            ).fetchone()[0]
        self.assertEqual(count, 0)

    def test_new_same_second_update_after_boundary_is_accepted(self):
        with self.database.get_connection() as conn:
            self.database._begin_immediate(conn)
            self.database._set_state(
                conn,
                "go_live_first_update_id",
                1500,
            )
            self.database._commit(conn)

        result = self.database.process_telegram_update(
            {
                "update_id": 1500,
                "channel_post": {
                    "message_id": 950,
                    "chat": {"id": -100111},
                    "date": 100,
                    "text": "Same-second go-live post",
                    "media_group_id": None,
                },
            },
            "-100111",
        )
        self.assertTrue(result["inserted"])
        self.assertFalse(result["pre_live_ignored"])
        with self.database.get_connection() as conn:
            row = conn.execute(
                "SELECT message_id, live_eligible FROM incoming_posts"
            ).fetchone()
        self.assertEqual(tuple(row), (950, 1))

    def test_non_source_channel_update_is_ignored_and_offset_advances(self):
        result = self.database.process_telegram_update(
            {
                "update_id": 1550,
                "channel_post": {
                    "message_id": 951,
                    "chat": {"id": -100999},
                    "date": 100,
                    "text": "Not the configured source",
                },
            },
            "-100111",
        )
        self.assertTrue(result["ignored"])
        self.assertFalse(result["inserted"])
        self.assertEqual(self.database.get_offset(), 1551)
        with self.database.get_connection() as conn:
            count = conn.execute(
                "SELECT COUNT(*) FROM incoming_posts"
            ).fetchone()[0]
        self.assertEqual(count, 0)

    def test_duplicate_update_replay_does_not_duplicate_post(self):
        update = {
            "update_id": 1560,
            "channel_post": {
                "message_id": 952,
                "chat": {"id": -100111},
                "date": 100,
                "text": "Replay-safe source post",
            },
        }
        first = self.database.process_telegram_update(update, "-100111")
        second = self.database.process_telegram_update(update, "-100111")
        self.assertTrue(first["inserted"])
        self.assertTrue(second["duplicate"])
        self.assertEqual(self.database.get_offset(), 1561)
        with self.database.get_connection() as conn:
            count = conn.execute(
                "SELECT COUNT(*) FROM incoming_posts WHERE message_id=952"
            ).fetchone()[0]
        self.assertEqual(count, 1)

    def test_bootstrap_bounds_more_than_one_hundred_pending_updates(self):
        self._prepare_go_live_bootstrap()
        pending = [
            {"update_id": update_id}
            for update_id in range(5000, 5250)
        ]
        session = MagicMock()
        session.get.return_value.status_code = 200
        session.get.return_value.json.return_value = {
            "ok": True,
            "result": pending[-1:],
        }

        first_eligible = self.handler.bootstrap_go_live_boundary(session)

        self.assertEqual(first_eligible, 5250)
        self.assertEqual(session.get.call_count, 1)
        params = session.get.call_args.kwargs["params"]
        self.assertEqual(params["offset"], -1)
        self.assertEqual(params["timeout"], 0)
        self.assertEqual(self.database.get_offset(), 5250)

    def test_empty_bootstrap_accepts_updates_arriving_after_boundary(self):
        self._prepare_go_live_bootstrap()
        session = MagicMock()
        session.get.return_value.status_code = 200
        session.get.return_value.json.return_value = {
            "ok": True,
            "result": [],
        }

        first_eligible = self.handler.bootstrap_go_live_boundary(session)
        self.assertIsNone(first_eligible)
        self.assertIsNone(self.database.get_offset())

        result = self.database.process_telegram_update(
            {
                "update_id": 6300,
                "channel_post": {
                    "message_id": 960,
                    "chat": {"id": -100111},
                    "date": 100,
                    "text": "Arrived after empty boundary",
                },
            },
            "-100111",
        )
        self.assertTrue(result["inserted"])
        self.assertEqual(self.database.get_offset(), 6301)

    def test_update_arriving_during_startup_after_boundary_is_preserved(self):
        self._prepare_go_live_bootstrap()
        session = MagicMock()
        session.get.return_value.status_code = 200
        session.get.return_value.json.return_value = {
            "ok": True,
            "result": [{"update_id": 7000}],
        }

        first_eligible = self.handler.bootstrap_go_live_boundary(session)
        # Simulates the server queuing a post after its boundary response but
        # before the listener child starts polling.
        self.assertEqual(first_eligible, 7001)
        result = self.database.process_telegram_update(
            {
                "update_id": 7001,
                "channel_post": {
                    "message_id": 970,
                    "chat": {"id": -100111},
                    "date": 100,
                    "text": "Arrived during child startup",
                },
            },
            "-100111",
        )
        self.assertTrue(result["inserted"])

    def test_ambiguous_bootstrap_crash_requires_manual_recovery(self):
        self._prepare_go_live_bootstrap()
        self.database.begin_go_live_bootstrap()
        session = MagicMock()
        with self.assertRaises(self.database.DatabaseError):
            self.handler.bootstrap_go_live_boundary(session)
        session.get.assert_not_called()
        self.assertEqual(
            self.database.get_go_live_state()["status"],
            self.database.GO_LIVE_CAPTURING,
        )
        with self.assertRaisesRegex(
            self.database.DatabaseError,
            "explicit operator confirmation",
        ):
            self.database.recover_go_live_bootstrap(8000)
        self.database.recover_go_live_bootstrap(
            8000,
            operator_confirmed=True,
        )
        self.assertEqual(self.database.get_offset(), 8001)

    def test_successful_bootstrap_is_not_recalculated_after_restart(self):
        self._prepare_go_live_bootstrap()
        session = MagicMock()
        session.get.return_value.status_code = 200
        session.get.return_value.json.return_value = {
            "ok": True,
            "result": [{"update_id": 9000}],
        }
        self.assertEqual(
            self.handler.bootstrap_go_live_boundary(session),
            9001,
        )
        self.assertEqual(
            self.handler.bootstrap_go_live_boundary(session),
            9001,
        )
        session.get.assert_called_once()

    def test_webhook_conflict_blocks_bootstrap_until_manual_recovery(self):
        self._prepare_go_live_bootstrap()
        session = MagicMock()
        session.get.return_value.status_code = 409
        session.get.return_value.json.return_value = {
            "ok": False,
            "description": "webhook conflict",
        }
        with self.assertRaisesRegex(
            self.handler.GoLiveBootstrapError,
            "webhook",
        ):
            self.handler.bootstrap_go_live_boundary(session)
        self.assertEqual(
            self.database.get_go_live_state()["status"],
            self.database.GO_LIVE_WEBHOOK_CONFLICT,
        )
        with self.assertRaises(self.database.DatabaseError):
            self.handler.bootstrap_go_live_boundary(session)
        self.database.recover_go_live_bootstrap(
            9100,
            operator_confirmed=True,
        )
        self.assertEqual(self.database.get_offset(), 9101)

    def test_telegram_api_error_leaves_bootstrap_blocked(self):
        self._prepare_go_live_bootstrap()
        session = MagicMock()
        session.get.return_value.status_code = 400
        session.get.return_value.json.return_value = {
            "ok": False,
            "description": "invalid getUpdates request",
        }

        with self.assertRaisesRegex(
            self.handler.GoLiveBootstrapError,
            "ambiguous",
        ):
            self.handler.bootstrap_go_live_boundary(session)

        self.assertEqual(
            self.database.get_go_live_state()["status"],
            self.database.GO_LIVE_CAPTURING,
        )
        with self.assertRaises(self.database.DatabaseError):
            self.handler.bootstrap_go_live_boundary(session)
        self.assertEqual(session.get.call_count, 1)

    def test_listener_refuses_to_poll_before_bootstrap_is_ready(self):
        with (
            patch.object(self.handler, "install_signal_handlers"),
            patch.object(
                self.handler,
                "require_go_live_ready",
                side_effect=self.database.DatabaseError(
                    "bootstrap is not ready"
                ),
            ),
            patch.object(self.handler, "fetch_updates") as fetch_updates,
        ):
            with self.assertRaisesRegex(
                self.database.DatabaseError,
                "bootstrap is not ready",
            ):
                self.handler.listen_to_channel()

        fetch_updates.assert_not_called()

    def test_offset_failure_rolls_back_post_insert(self):
        original_set_state = self.database._set_state

        def fail_update_offset(conn, key, value):
            if key == "update_offset":
                raise self.database.DatabaseError(
                    "simulated offset persistence failure"
                )
            return original_set_state(conn, key, value)

        with patch.object(
            self.database,
            "_set_state",
            side_effect=fail_update_offset,
        ):
            with self.assertRaisesRegex(
                self.database.DatabaseError,
                "simulated offset persistence failure",
            ):
                self.database.process_telegram_update(
                    {
                        "update_id": 9200,
                        "channel_post": {
                            "message_id": 980,
                            "chat": {"id": -100111},
                            "date": 100,
                            "text": "Must rollback atomically",
                        },
                    },
                    "-100111",
                )
        with self.database.get_connection() as conn:
            count = conn.execute(
                "SELECT COUNT(*) FROM incoming_posts WHERE message_id=980"
            ).fetchone()[0]
        self.assertEqual(count, 0)
        self.assertIsNone(self.database.get_offset())

    def test_listener_polls_channel_posts_only_not_channel_history(self):
        response = MagicMock()
        response.status_code = 200
        response.json.return_value = {"ok": True, "result": []}
        session = MagicMock()
        session.get.return_value = response

        self.handler.fetch_updates(100, session)

        request_url = session.get.call_args.args[0]
        request_params = session.get.call_args.kwargs["params"]
        self.assertTrue(request_url.endswith("/getUpdates"))
        self.assertEqual(
            json.loads(request_params["allowed_updates"]),
            ["channel_post"],
        )
        self.assertEqual(request_params["offset"], 100)

    def test_album_is_published_as_one_group_not_individual_posts(self):
        self._ingest_album()

        unit = self.scheduler.select_next_publish_unit(
            album_wait_seconds=0
        )
        self.assertIsNotNone(unit)
        self.assertEqual(unit["kind"], "album")
        self.assertEqual(unit["media_group_id"], "album-listener-1")
        self.assertEqual(
            [post["message_id"] for post in unit["posts"]],
            [701, 702, 703],
        )
        with self.database.get_connection() as conn:
            self.database._begin_immediate(conn)
            conn.execute(
                """
                UPDATE incoming_posts
                SET received_at = datetime('now', '-60 seconds')
                WHERE media_group_id = 'album-listener-1'
                """
            )
            self.database._commit(conn)

        response = MagicMock()
        response.status_code = 200
        response.ok = True
        response.text = "ok"
        response.json.return_value = {
            "ok": True,
            "result": [
                {"message_id": 8701},
                {"message_id": 8702},
                {"message_id": 8703},
            ],
        }
        session = MagicMock()
        session.post.return_value = response

        published = self.scheduler.process_next_post(
            claim_owner="listener-album-test",
            session=session,
        )

        self.assertTrue(published)
        session.post.assert_called_once()
        request_url = session.post.call_args.args[0]
        request_body = session.post.call_args.kwargs["json"]
        self.assertTrue(request_url.endswith("/copyMessages"))
        self.assertEqual(request_body["message_ids"], [701, 702, 703])
        with self.database.get_connection() as conn:
            statuses = conn.execute(
                """
                SELECT status, destination_message_ids
                FROM incoming_posts
                ORDER BY message_id
                """
            ).fetchall()
        self.assertEqual(
            [row["status"] for row in statuses],
            ["published", "published", "published"],
        )

    def test_listener_restart_replay_does_not_duplicate_album(self):
        updates = self._album_updates()

        def run_one_poll(replayed_updates):
            requested_offsets = []

            def fake_fetch(offset, session):
                requested_offsets.append(offset)
                self.handler._shutdown_requested = True
                return {"result": replayed_updates}

            with (
                patch.object(self.handler, "install_signal_handlers"),
                patch.object(
                    self.handler,
                    "fetch_updates",
                    side_effect=fake_fetch,
                ),
                patch.object(
                    self.handler.requests,
                    "Session",
                    return_value=MagicMock(),
                ),
            ):
                self.handler.listen_to_channel()
            return requested_offsets

        first_offsets = run_one_poll(updates)
        with self.database.get_connection() as conn:
            first_rows = conn.execute(
                """
                SELECT message_id, media_group_id
                FROM incoming_posts
                ORDER BY message_id
                """
            ).fetchall()
        self.assertEqual(first_offsets, [None])
        self.assertEqual(self.database.get_offset(), 1004)
        self.assertEqual(len(first_rows), 3)

        self.handler = importlib.reload(self.handler)
        replay_offsets = run_one_poll(updates)

        with self.database.get_connection() as conn:
            replayed_rows = conn.execute(
                """
                SELECT message_id, media_group_id
                FROM incoming_posts
                ORDER BY message_id
                """
            ).fetchall()
        self.assertEqual(replay_offsets, [1004])
        self.assertEqual(self.database.get_offset(), 1004)
        self.assertEqual(
            [tuple(row) for row in replayed_rows],
            [tuple(row) for row in first_rows],
        )
        self.assertEqual(len(replayed_rows), 3)


if __name__ == "__main__":
    unittest.main()
