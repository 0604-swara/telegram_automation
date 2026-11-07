"""Isolated tests for Linux deployment preparation; never use live DB files."""

from __future__ import annotations

import os
import sqlite3
import sys
import tempfile
import threading
import unittest
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import backup_database
import config
import database
import process_manager
import telegram_handler


class DeploymentPreparationTests(unittest.TestCase):
    def test_db_path_resolution_and_validation(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            database_file = directory / "state" / "queue.db"
            database_file.parent.mkdir()
            env = {
                "DB_PATH": str(database_file),
                "SOURCE_CHANNEL_ID": "-1001234567890",
                "DESTINATION_CHANNEL_ID": "-1009876543210",
            }
            with patch.dict(os.environ, env, clear=False):
                settings = config.validate_config(
                    require_destination=False,
                    require_token=False,
                )
                self.assertEqual(
                    settings["DB_PATH"],
                    str(database_file.resolve()),
                )
                self.assertEqual(
                    config.resolve_db_path(),
                    str(database_file.resolve()),
                )
                previous_db_name = database.DB_NAME
                try:
                    database.DB_NAME = os.getenv("DB_PATH")
                    self.assertEqual(
                        database._resolve_connection_path(),
                        settings["DB_PATH"],
                    )
                finally:
                    database.DB_NAME = previous_db_name

                missing_parent = directory / "missing" / "queue.db"
                with patch.dict(
                    os.environ,
                    {"DB_PATH": str(missing_parent)},
                    clear=False,
                ):
                    with self.assertRaisesRegex(
                        config.ConfigurationError,
                        "parent directory does not exist",
                    ):
                        config.validate_config(
                            require_destination=False,
                            require_token=False,
                        )
                    with self.assertRaises(database.DatabaseError):
                        database._resolve_connection_path("")

                with patch.dict(
                    os.environ,
                    {"DB_PATH": "  "},
                    clear=False,
                ):
                    with self.assertRaisesRegex(
                        config.ConfigurationError,
                        "DB_PATH must not be empty",
                    ):
                        config.validate_config(
                            require_destination=False,
                            require_token=False,
                        )

    def test_production_config_rejects_test_channels_in_either_field(self):
        with tempfile.TemporaryDirectory() as temporary:
            base = {
                "TELEGRAM_TEST_MODE": "0",
                "DB_PATH": str(Path(temporary) / "queue.db"),
                "SOURCE_CHANNEL_ID": "-100111222333",
                "DESTINATION_CHANNEL_ID": "-100444555666",
            }
            test_channel_ids = (
                config.TEST_SOURCE_CHANNEL_ID,
                config.TEST_DESTINATION_CHANNEL_ID,
            )
            for field in ("SOURCE_CHANNEL_ID", "DESTINATION_CHANNEL_ID"):
                for test_channel_id in test_channel_ids:
                    with self.subTest(
                        field=field,
                        test_channel_id=test_channel_id,
                    ):
                        env = dict(base)
                        env[field] = test_channel_id
                        with patch.dict(os.environ, env, clear=False):
                            with self.assertRaisesRegex(
                                config.ConfigurationError,
                                "approved test channel IDs",
                            ):
                                config.validate_config(
                                    require_destination=True,
                                    require_token=False,
                                )
                    if field == "DESTINATION_CHANNEL_ID":
                        with self.subTest(
                            field=field,
                            test_channel_id=test_channel_id,
                            destination_optional=True,
                        ):
                            env = dict(base)
                            env[field] = test_channel_id
                            with patch.dict(os.environ, env, clear=False):
                                with self.assertRaisesRegex(
                                    config.ConfigurationError,
                                    "approved test channel IDs",
                                ):
                                    config.validate_config(
                                        require_destination=False,
                                        require_token=False,
                                    )

    def test_production_config_requires_credentials_and_channel_ids(self):
        with tempfile.TemporaryDirectory() as temporary:
            env = {
                "TELEGRAM_TEST_MODE": "0",
                "DB_PATH": str(Path(temporary) / "queue.db"),
                "TELEGRAM_BOT_TOKEN": "",
                "SOURCE_CHANNEL_ID": "",
                "DESTINATION_CHANNEL_ID": "",
            }
            with patch.dict(os.environ, env, clear=True):
                with self.assertRaises(config.ConfigurationError) as raised:
                    config.validate_config(
                        require_destination=True,
                        require_token=True,
                    )

            error = str(raised.exception)
            self.assertIn("TELEGRAM_BOT_TOKEN", error)
            self.assertIn("SOURCE_CHANNEL_ID", error)
            self.assertIn("DESTINATION_CHANNEL_ID", error)
            self.assertFalse((Path(temporary) / "queue.db").exists())

    def test_production_config_requires_valid_token_without_exposing_it(self):
        with tempfile.TemporaryDirectory() as temporary:
            env = {
                "TELEGRAM_TEST_MODE": "0",
                "DB_PATH": str(Path(temporary) / "queue.db"),
                "SOURCE_CHANNEL_ID": "-100111222333",
                "DESTINATION_CHANNEL_ID": "-100444555666",
                "TELEGRAM_BOT_TOKEN": "invalid-token-value",
            }
            with patch.dict(os.environ, env, clear=True):
                with self.assertRaises(
                    config.ConfigurationError
                ) as raised:
                    config.validate_config(
                        require_destination=True,
                        require_token=True,
                    )

        self.assertIn("format looks invalid", str(raised.exception))
        self.assertNotIn("invalid-token-value", str(raised.exception))

    def test_process_manager_redacts_token_and_token_bearing_url(self):
        token = "12345:unit-test-secret"
        message = (
            f"request failed at https://api.telegram.org/bot{token}/getUpdates "
            f"with token {token}"
        )

        safe_message = process_manager._redact(message, token)

        self.assertNotIn(token, safe_message)
        self.assertNotIn(
            f"https://api.telegram.org/bot{token}/getUpdates",
            safe_message,
        )
        self.assertIn("https://api.telegram.org/bot***REDACTED***", safe_message)

    def test_database_refuses_missing_parent_without_fallback(self):
        with tempfile.TemporaryDirectory() as temporary:
            missing = Path(temporary) / "missing" / "queue.db"
            previous = database.DB_NAME
            try:
                database.DB_NAME = str(missing)
                with self.assertRaisesRegex(
                    database.DatabaseError,
                    "Create it or correct DB_PATH",
                ):
                    with database.get_connection():
                        self.fail("connection should not be opened")
                self.assertFalse(missing.exists())
            finally:
                database.DB_NAME = previous

    def test_isolated_database_initializes_with_wal(self):
        with tempfile.TemporaryDirectory() as temporary:
            database_path = Path(temporary) / "queue.db"
            previous = database.DB_NAME
            try:
                database.DB_NAME = str(database_path)
                database.initialize_database()
                with database.get_connection() as connection:
                    journal_mode = connection.execute(
                        "PRAGMA journal_mode"
                    ).fetchone()[0]
                    integrity = connection.execute(
                        "PRAGMA integrity_check"
                    ).fetchone()[0]
                self.assertEqual(journal_mode.lower(), "wal")
                self.assertEqual(integrity, "ok")
            finally:
                database.DB_NAME = previous

    def test_database_migrates_supported_v3_and_preserves_state(self):
        with tempfile.TemporaryDirectory() as temporary:
            database_path = Path(temporary) / "queue.db"
            connection = sqlite3.connect(database_path)
            try:
                connection.executescript(
                    """
                    CREATE TABLE incoming_posts (
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
                        UNIQUE(channel_id, message_id)
                    );
                    CREATE TABLE bot_state (
                        key TEXT PRIMARY KEY,
                        value TEXT
                    );
                    INSERT INTO bot_state VALUES ('schema_version', '3');
                    INSERT INTO bot_state VALUES ('update_offset', '177');
                    INSERT INTO incoming_posts (
                        channel_id, message_id, message_data, status
                    ) VALUES (-100111222333, 42, '{"text":"preserve"}',
                              'pending');
                    """
                )
                connection.commit()
            finally:
                connection.close()

            previous = database.DB_NAME
            try:
                database.DB_NAME = str(database_path)
                database.initialize_database()
                with database.get_connection() as connection:
                    post = connection.execute(
                        "SELECT id, message_id, message_data "
                        "FROM incoming_posts"
                    ).fetchone()
                    state = database._get_state(connection, "update_offset")
                    version = database._get_state(connection, "schema_version")
                    eligibility = connection.execute(
                        "SELECT live_eligible FROM incoming_posts WHERE id=1"
                    ).fetchone()[0]
                    go_live_cutoff = database._get_state(
                        connection,
                        "go_live_cutoff",
                    )
                    bootstrap_status = database._get_state(
                        connection,
                        "go_live_bootstrap_status",
                    )
                    first_update_id = database._get_state(
                        connection,
                        "go_live_first_update_id",
                    )
                    history_exists = connection.execute(
                        "SELECT 1 FROM sqlite_master "
                        "WHERE type='table' AND name='publish_history'"
                    ).fetchone()
                self.assertEqual(post["id"], 1)
                self.assertEqual(post["message_id"], 42)
                self.assertEqual(post["message_data"], '{"text":"preserve"}')
                self.assertEqual(state, "177")
                self.assertEqual(version, database.SCHEMA_VERSION)
                self.assertEqual(eligibility, 0)
                self.assertIsNone(go_live_cutoff)
                self.assertEqual(bootstrap_status, database.GO_LIVE_PENDING)
                self.assertEqual(first_update_id, "")
                self.assertIsNotNone(history_exists)
            finally:
                database.DB_NAME = previous

    def test_database_rejects_unrecognized_schema_without_modifying_it(self):
        with tempfile.TemporaryDirectory() as temporary:
            for table_sql in (
                (
                    "CREATE TABLE unrelated_records (value TEXT);"
                    "INSERT INTO unrelated_records VALUES ('legacy-data');"
                ),
                (
                    "CREATE TABLE incoming_posts ("
                    "id INTEGER PRIMARY KEY, message_data TEXT);"
                    "CREATE TABLE bot_state (key TEXT, value TEXT);"
                    "INSERT INTO incoming_posts VALUES (1, 'keep-me');"
                ),
                (
                    "CREATE TABLE incoming_posts ("
                    "id INTEGER PRIMARY KEY, message_data TEXT);"
                    "CREATE TABLE bot_state (key TEXT, value TEXT);"
                    "INSERT INTO bot_state VALUES ('schema_version', '2');"
                    "INSERT INTO incoming_posts VALUES (1, 'keep-me');"
                ),
            ):
                with self.subTest(table_sql=table_sql):
                    database_path = Path(temporary) / "unrecognized.db"
                    if database_path.exists():
                        database_path.unlink()
                    connection = sqlite3.connect(database_path)
                    try:
                        connection.executescript(table_sql)
                        connection.commit()
                    finally:
                        connection.close()
                    original_bytes = database_path.read_bytes()

                    previous = database.DB_NAME
                    try:
                        database.DB_NAME = str(database_path)
                        with self.assertRaises(database.DatabaseError):
                            with database.get_connection():
                                self.fail("unsupported schema was accepted")
                    finally:
                        database.DB_NAME = previous

                    self.assertEqual(database_path.read_bytes(), original_bytes)
                    self.assertFalse(Path(f"{database_path}-wal").exists())
                    self.assertFalse(Path(f"{database_path}-shm").exists())

    def test_backup_integrity_exclusive_names_and_retention(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "queue.db"
            backup_dir = root / "backup-staging"
            connection = sqlite3.connect(source)
            try:
                connection.execute("CREATE TABLE sample (value TEXT)")
                connection.execute("INSERT INTO sample VALUES ('isolated')")
            finally:
                connection.close()

            backup_dir.mkdir()
            old_backup = backup_dir / "telegram_queue_backup_old.db"
            old_backup.write_bytes(b"old-backup")
            expired = datetime.now(timezone.utc) - timedelta(days=3)
            os.utime(
                old_backup,
                (expired.timestamp(), expired.timestamp()),
            )
            unrelated = backup_dir / "do-not-remove.db"
            unrelated.write_bytes(b"keep")

            result = backup_database.create_backup(
                source,
                backup_dir,
                retention_days=1,
                timestamp="fixed",
            )
            created = Path(result["backup_path"])
            self.assertEqual(result["integrity"], "ok")
            self.assertTrue(created.is_file())
            self.assertFalse(old_backup.exists())
            self.assertTrue(unrelated.exists())

            original_bytes = created.read_bytes()
            with self.assertRaises(FileExistsError):
                backup_database.create_backup(
                    source,
                    backup_dir,
                    retention_days=1,
                    timestamp="fixed",
                )
            self.assertEqual(created.read_bytes(), original_bytes)

    def test_backup_restore_preserves_database_state_and_history(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "queue.db"
            backup_dir = root / "backups"
            restored = root / "restored.db"
            previous = database.DB_NAME
            try:
                database.DB_NAME = str(source)
                database.initialize_database()
                with database.get_connection() as connection:
                    database._begin_immediate(connection)
                    connection.execute(
                        """
                        INSERT INTO incoming_posts (
                            channel_id, message_id, message_data, status
                        ) VALUES (-100111222333, 73, '{"text":"kept"}',
                                  'published')
                        """
                    )
                    database._set_state(connection, "update_offset", "901")
                    connection.execute(
                        """
                        INSERT INTO publish_history (
                            cycle_id, incoming_post_ids, source_message_ids,
                            destination_message_ids
                        ) VALUES (1, '[1]', '[73]', '[8401]')
                        """
                    )
                    database._commit(connection)

                backup = backup_database.create_backup(source, backup_dir)
                result = database.restore_database_to_path(
                    backup["backup_path"],
                    restored,
                )
                self.assertEqual(result["integrity"], "ok")
                self.assertEqual(result["post_count"], 1)
                self.assertEqual(result["update_offset"], "901")
                database.DB_NAME = str(restored)
                schema = database.verify_schema()
                self.assertTrue(schema["ok"])
                with closing(sqlite3.connect(restored)) as connection:
                    history = connection.execute(
                        "SELECT destination_message_ids FROM publish_history"
                    ).fetchone()[0]
                    message = connection.execute(
                        "SELECT message_data FROM incoming_posts"
                    ).fetchone()[0]
                self.assertEqual(history, "[8401]")
                self.assertEqual(message, '{"text":"kept"}')
            finally:
                database.DB_NAME = previous

    def test_listener_lock_prevents_second_lock(self):
        with tempfile.TemporaryDirectory() as temporary:
            lock_path = Path(temporary) / "listener.lock"
            first = telegram_handler.ListenerSingleInstanceLock(lock_path)
            second = telegram_handler.ListenerSingleInstanceLock(lock_path)
            first.acquire()
            try:
                with self.assertRaises(telegram_handler.ListenerError):
                    second.acquire()
            finally:
                first.release()

    def test_listener_test_mode_lock_is_isolated(self):
        env = {
            "TELEGRAM_TEST_MODE": "1",
            "DB_PATH": str(config.TEST_DB_PATH),
            "SOURCE_CHANNEL_ID": config.TEST_SOURCE_CHANNEL_ID,
            "DESTINATION_CHANNEL_ID": config.TEST_DESTINATION_CHANNEL_ID,
            "SCHEDULER_LOCK_FILE": config.TEST_LOCK_FILE,
            "LISTENER_LOCK_FILE": config.TEST_LISTENER_LOCK_FILE,
        }
        with patch.dict(os.environ, env, clear=False):
            result = config.validate_config(
                require_destination=False,
                require_token=False,
            )
            self.assertEqual(result["TELEGRAM_TEST_MODE"], "1")

    def test_render_paths_require_persistent_disk_and_non_test_channels(self):
        with tempfile.TemporaryDirectory() as temporary:
            mount = Path(temporary)
            settings = {
                "TELEGRAM_TEST_MODE": "0",
                "TELEGRAM_BOT_TOKEN": "12345:placeholder-only",
                "SOURCE_CHANNEL_ID": "-100111222333",
                "DESTINATION_CHANNEL_ID": "-100444555666",
                "DB_PATH": str(mount / "telegram_queue.db"),
                "PUBLISH_INTERVAL_SECONDS": "300",
                "SCHEDULER_LOCK_FILE": str(mount / "scheduler.lock"),
                "LISTENER_LOCK_FILE": str(mount / "listener.lock"),
                "PROCESS_MANAGER_LOCK_FILE": str(
                    mount / "process_manager.lock"
                ),
            }
            original_db = process_manager.DB_NAME
            try:
                process_manager.DB_NAME = settings["DB_PATH"]
                result = process_manager.validate_render_settings(
                    settings,
                    disk_path=mount,
                )
                self.assertEqual(result["DB_PATH"], settings["DB_PATH"])

                invalid_settings = dict(settings)
                invalid_settings["TELEGRAM_TEST_MODE"] = "1"
                with self.assertRaisesRegex(
                    config.ConfigurationError,
                    "TELEGRAM_TEST_MODE=0",
                ):
                    process_manager.validate_render_settings(
                        invalid_settings,
                        disk_path=mount,
                    )

                invalid_settings = dict(settings)
                invalid_settings["DB_PATH"] = str(
                    mount / "telegram_test.db"
                )
                with self.assertRaisesRegex(
                    config.ConfigurationError,
                    "must name telegram_queue.db",
                ):
                    process_manager.validate_render_settings(
                        invalid_settings,
                        disk_path=mount,
                    )

                for field, test_channel_id in (
                    ("SOURCE_CHANNEL_ID", config.TEST_SOURCE_CHANNEL_ID),
                    ("SOURCE_CHANNEL_ID", config.TEST_DESTINATION_CHANNEL_ID),
                    ("DESTINATION_CHANNEL_ID", config.TEST_SOURCE_CHANNEL_ID),
                    (
                        "DESTINATION_CHANNEL_ID",
                        config.TEST_DESTINATION_CHANNEL_ID,
                    ),
                ):
                    with self.subTest(
                        field=field,
                        test_channel_id=test_channel_id,
                    ):
                        invalid_settings = dict(settings)
                        invalid_settings[field] = test_channel_id
                        with self.assertRaisesRegex(
                            config.ConfigurationError,
                            "test channel IDs",
                        ):
                            process_manager.validate_render_settings(
                                invalid_settings,
                                disk_path=mount,
                            )

                invalid_settings = dict(settings)
                invalid_settings["DESTINATION_CHANNEL_ID"] = (
                    settings["SOURCE_CHANNEL_ID"]
                )
                with self.assertRaisesRegex(
                    config.ConfigurationError,
                    "must be different",
                ):
                    process_manager.validate_render_settings(
                        invalid_settings,
                        disk_path=mount,
                    )
            finally:
                process_manager.DB_NAME = original_db

    def test_publishing_history_survives_cycle_restart(self):
        with tempfile.TemporaryDirectory() as temporary:
            database_path = Path(temporary) / "history.db"
            previous = database.DB_NAME
            try:
                database.DB_NAME = str(database_path)
                database.initialize_database()
                with database.get_connection() as connection:
                    database._begin_immediate(connection)
                    cursor = connection.execute(
                        """
                        INSERT INTO incoming_posts (
                            channel_id, message_id, message_type,
                            message_data, status, claim_owner
                        )
                        VALUES (-100111, 44, 'text', '{}',
                                'publishing', 'first-owner')
                        """
                    )
                    post_id = cursor.lastrowid
                    database._commit(connection)

                database.mark_as_published(
                    [post_id],
                    "first-owner",
                    destination_message_ids=[501],
                )
                self.assertTrue(database.restart_posting_cycle())
                self.assertEqual(
                    database.claim_posts([post_id], "second-owner"),
                    [post_id],
                )
                database.mark_as_published(
                    [post_id],
                    "second-owner",
                    destination_message_ids=[502],
                )

                with database.get_connection() as connection:
                    history = connection.execute(
                        """
                        SELECT cycle_id, incoming_post_ids,
                               source_message_ids, destination_message_ids
                        FROM publish_history
                        ORDER BY id
                        """
                    ).fetchall()
                self.assertEqual(len(history), 2)
                self.assertEqual(
                    [row["destination_message_ids"] for row in history],
                    ["[501]", "[502]"],
                )
                self.assertEqual(
                    [row["cycle_id"] for row in history],
                    [1, 2],
                )
            finally:
                database.DB_NAME = previous

    def test_process_manager_stops_sibling_after_unexpected_exit(self):
        environment = os.environ.copy()
        stop = process_manager.run_processes(
            [
                (
                    "listener",
                    [sys.executable, "-c", "raise SystemExit(7)"],
                    environment,
                ),
                (
                    "scheduler",
                    [sys.executable, "-c", "import time; time.sleep(60)"],
                    environment,
                ),
            ],
            poll_interval=0.01,
            terminate_timeout=2,
        )
        self.assertEqual(stop, 1)

    def test_process_manager_terminates_children_on_shutdown(self):
        stop_event = threading.Event()
        timer = threading.Timer(0.2, stop_event.set)
        timer.start()
        environment = os.environ.copy()
        try:
            stop = process_manager.run_processes(
                [
                    (
                        "listener",
                        [sys.executable, "-c", "import time; time.sleep(60)"],
                        environment,
                    ),
                    (
                        "scheduler",
                        [sys.executable, "-c", "import time; time.sleep(60)"],
                        environment,
                    ),
                ],
                stop_event=stop_event,
                poll_interval=0.02,
                terminate_timeout=2,
            )
        finally:
            timer.cancel()
        self.assertEqual(stop, 0)

    def test_worker_commands_share_production_database_and_disable_test_mode(self):
        database_path = "/var/data/telegram_queue.db"
        with tempfile.TemporaryDirectory() as temporary:
            commands = process_manager._worker_commands(
                database_path,
                application_root=temporary,
            )

        self.assertEqual(
            [name for name, _, _ in commands],
            ["listener", "scheduler"],
        )
        for _, command, environment in commands:
            self.assertEqual(environment["DB_PATH"], database_path)
            self.assertEqual(environment["TELEGRAM_TEST_MODE"], "0")
            self.assertEqual(environment["LOG_TO_STDOUT"], "1")
            self.assertTrue(Path(command[1]).is_absolute())

    def test_render_yaml_configures_one_worker_and_one_disk(self):
        repository = Path(__file__).resolve().parent
        blueprint = (repository / "render.yaml").read_text()
        self.assertEqual(blueprint.count("type: worker"), 1)
        self.assertIn("numInstances: 1", blueprint)
        self.assertIn("mountPath: /var/data", blueprint)
        self.assertIn("DB_PATH", blueprint)
        self.assertIn("python process_manager.py", blueprint)
        self.assertIn("sync: false", blueprint)

    def test_render_preflight_rejects_missing_credentials_and_channel_ids(self):
        with tempfile.TemporaryDirectory() as temporary:
            mount = Path(temporary)
            settings = {
                "PERSISTENT_DISK_PATH": str(mount),
                "TELEGRAM_TEST_MODE": "0",
                "DB_PATH": str(mount / "telegram_queue.db"),
                "PUBLISH_INTERVAL_SECONDS": "300",
                "SCHEDULER_LOCK_FILE": str(mount / "scheduler.lock"),
                "LISTENER_LOCK_FILE": str(mount / "listener.lock"),
                "PROCESS_MANAGER_LOCK_FILE": str(
                    mount / "process_manager.lock"
                ),
                "TELEGRAM_BOT_TOKEN": "",
                "SOURCE_CHANNEL_ID": "",
                "DESTINATION_CHANNEL_ID": "",
            }

            with self.assertRaisesRegex(
                config.ConfigurationError,
                "TELEGRAM_BOT_TOKEN",
            ):
                process_manager.validate_render_settings(
                    settings,
                    disk_path=mount,
                )

            settings["TELEGRAM_BOT_TOKEN"] = "12345:placeholder-only"
            with self.assertRaisesRegex(
                config.ConfigurationError,
                "SOURCE_CHANNEL_ID",
            ):
                process_manager.validate_render_settings(
                    settings,
                    disk_path=mount,
                )

            settings["SOURCE_CHANNEL_ID"] = "-100111222333"
            with self.assertRaisesRegex(
                config.ConfigurationError,
                "DESTINATION_CHANNEL_ID",
            ):
                process_manager.validate_render_settings(
                    settings,
                    disk_path=mount,
                )


if __name__ == "__main__":
    unittest.main()
