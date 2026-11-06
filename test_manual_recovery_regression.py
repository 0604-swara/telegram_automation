"""Isolated regression tests for manually resetting failed posts."""

from __future__ import annotations

import shutil
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import database


ROOT = Path(__file__).resolve().parent


class ResetFailedPostsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        source_db = ROOT / "telegram_test.db"
        if not source_db.is_file():
            raise FileNotFoundError(source_db)

        cls.temp_dir = tempfile.TemporaryDirectory()
        cls.db_path = Path(cls.temp_dir.name) / "manual_recovery_test.db"
        shutil.copy2(source_db, cls.db_path)
        cls.original_db_name = database.DB_NAME
        database.DB_NAME = str(cls.db_path)
        database.initialize_database()

    @classmethod
    def tearDownClass(cls):
        database.DB_NAME = cls.original_db_name
        cls.temp_dir.cleanup()

    def setUp(self):
        with database.get_connection() as conn:
            database._begin_immediate(conn)
            conn.execute("DELETE FROM incoming_posts")
            database._commit(conn)

    def _insert_post(
        self,
        message_id,
        status,
        retry_count=4,
        last_error="manual review required",
        destination_message_ids="[23]",
    ):
        with database.get_connection() as conn:
            database._begin_immediate(conn)
            cursor = conn.execute(
                """
                INSERT INTO incoming_posts (
                    channel_id, message_id, message_type, message_data,
                    status, retry_count, last_error, claimed_at, claim_owner,
                    destination_message_ids
                )
                VALUES (?, ?, 'text', '{}', ?, ?, ?, '2026-01-01', ?, ?)
                """,
                (
                    -100111,
                    message_id,
                    status,
                    retry_count,
                    last_error,
                    "manual-review-owner",
                    destination_message_ids,
                ),
            )
            post_id = cursor.lastrowid
            database._commit(conn)
        return post_id

    def _state(self, post_id):
        with database.get_connection() as conn:
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

    def test_explicit_ids_reset_only_selected_failed_posts(self):
        selected_id = self._insert_post(101, "failed")
        other_failed_id = self._insert_post(102, "failed")
        published_id = self._insert_post(103, "published")
        pending_id = self._insert_post(104, "pending")
        published_before = self._state(published_id)
        pending_before = self._state(pending_id)

        reset_count = database.reset_failed_posts([selected_id])

        self.assertEqual(reset_count, 1)
        self.assertEqual(
            self._state(selected_id),
            ("pending", 0, None, None, None, "[23]"),
        )
        self.assertEqual(
            self._state(other_failed_id),
            (
                "failed",
                4,
                "manual review required",
                "manual-review-owner",
                "2026-01-01",
                "[23]",
            ),
        )
        self.assertEqual(self._state(published_id), published_before)
        self.assertEqual(self._state(pending_id), pending_before)

    def test_empty_list_resets_nothing(self):
        failed_ids = [
            self._insert_post(201, "failed"),
            self._insert_post(202, "failed"),
        ]
        before = {post_id: self._state(post_id) for post_id in failed_ids}

        reset_count = database.reset_failed_posts([])

        self.assertEqual(reset_count, 0)
        self.assertEqual(
            {post_id: self._state(post_id) for post_id in failed_ids},
            before,
        )

    def test_none_resets_all_failed_posts_only(self):
        failed_ids = [
            self._insert_post(301, "failed"),
            self._insert_post(302, "failed", destination_message_ids="[31]"),
        ]
        published_id = self._insert_post(303, "published")
        pending_id = self._insert_post(304, "pending")
        published_before = self._state(published_id)
        pending_before = self._state(pending_id)

        reset_count = database.reset_failed_posts(None)

        self.assertEqual(reset_count, 2)
        self.assertEqual(
            [self._state(post_id) for post_id in failed_ids],
            [
                ("pending", 0, None, None, None, "[23]"),
                ("pending", 0, None, None, None, "[31]"),
            ],
        )
        self.assertEqual(self._state(published_id), published_before)
        self.assertEqual(self._state(pending_id), pending_before)

    def test_nonexistent_id_does_not_change_any_record(self):
        failed_id = self._insert_post(401, "failed")
        pending_id = self._insert_post(402, "pending")
        before = (self._state(failed_id), self._state(pending_id))

        reset_count = database.reset_failed_posts([999999999])

        self.assertEqual(reset_count, 0)
        self.assertEqual(
            (self._state(failed_id), self._state(pending_id)),
            before,
        )

    def test_published_and_pending_ids_are_never_reset(self):
        published_id = self._insert_post(501, "published")
        pending_id = self._insert_post(502, "pending")
        before = (self._state(published_id), self._state(pending_id))

        reset_count = database.reset_failed_posts([published_id, pending_id])

        self.assertEqual(reset_count, 0)
        self.assertEqual(
            (self._state(published_id), self._state(pending_id)),
            before,
        )

    def test_commit_failure_rolls_back_reset(self):
        post_id = self._insert_post(601, "failed")
        before = self._state(post_id)

        with patch.object(
            database,
            "_commit",
            side_effect=sqlite3.OperationalError("simulated commit failure"),
        ):
            with self.assertRaisesRegex(
                sqlite3.OperationalError, "simulated commit failure"
            ):
                database.reset_failed_posts([post_id])

        self.assertEqual(self._state(post_id), before)


if __name__ == "__main__":
    unittest.main()
