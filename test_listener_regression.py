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
            self.database._set_state(conn, "go_live_cutoff", 0)
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
            self.database._set_state(conn, "go_live_cutoff", 100)
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
