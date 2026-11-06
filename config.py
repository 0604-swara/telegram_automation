"""
Shared startup configuration validation.

Never logs or returns secret values such as the bot token.
"""

from __future__ import annotations

import os
import re
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

_CHANNEL_ID_RE = re.compile(r"^-?\d{5,20}$")

# Only these channels are permitted during Phase 7 testing.
TEST_SOURCE_CHANNEL_ID = "-1004344475194"
TEST_DESTINATION_CHANNEL_ID = "-1004456077151"

TEST_DB_PATH = (Path.cwd() / "telegram_test.db").resolve()
TEST_LOCK_FILE = "scheduler_test.lock"
TEST_LISTENER_LOCK_FILE = "listener_test.lock"


class ConfigurationError(ValueError):
    """Raised when required configuration is missing or invalid."""


def resolve_db_path(value=None):
    """Resolve DB_PATH consistently for configuration and database access."""
    raw_path = os.getenv("DB_PATH") if value is None else value

    if raw_path is None:
        raw_path = "telegram_queue.db"
    elif not str(raw_path).strip():
        raise ConfigurationError("DB_PATH must not be empty.")

    return str(Path(str(raw_path).strip()).expanduser().resolve())


def _require(name):
    value = os.getenv(name)

    if value is None or str(value).strip() == "":
        raise ConfigurationError(
            f"Missing required environment variable: {name}"
        )

    return str(value).strip()


def _validate_channel_id(name, value):
    if not _CHANNEL_ID_RE.fullmatch(value):
        raise ConfigurationError(
            f"{name} must be a numeric Telegram chat ID."
        )

    return value


def validate_config(require_destination=False, require_token=True):
    """
    Validate environment for listener and/or scheduler startup.

    In test mode, both channel IDs, database path and lock filename
    are strictly restricted to the approved test environment.
    """

    errors = []
    result = {}

    # Validate test mode value.
    test_mode = (os.getenv("TELEGRAM_TEST_MODE") or "").strip()

    if test_mode not in ("", "0", "1"):
        errors.append("TELEGRAM_TEST_MODE must be 0 or 1.")

    result["TELEGRAM_TEST_MODE"] = test_mode

    # Validate bot token without exposing it.
    if require_token:
        token = os.getenv("TELEGRAM_BOT_TOKEN")

        if not token or not token.strip():
            errors.append(
                "Missing required environment variable: TELEGRAM_BOT_TOKEN"
            )
        elif ":" not in token.strip():
            errors.append(
                "TELEGRAM_BOT_TOKEN format looks invalid. "
                "The token value is not shown."
            )

    # Validate source channel.
    try:
        source = _validate_channel_id(
            "SOURCE_CHANNEL_ID",
            _require("SOURCE_CHANNEL_ID"),
        )

        result["SOURCE_CHANNEL_ID"] = source

    except ConfigurationError as exc:
        errors.append(str(exc))

    # In test mode, destination is mandatory even for the listener. When
    # provided in production, also validate it so test channels are rejected.
    configured_destination = os.getenv("DESTINATION_CHANNEL_ID")
    destination_required = (
        require_destination
        or test_mode == "1"
        or bool(configured_destination and configured_destination.strip())
    )

    if destination_required:
        try:
            destination = _validate_channel_id(
                "DESTINATION_CHANNEL_ID",
                _require("DESTINATION_CHANNEL_ID"),
            )

            result["DESTINATION_CHANNEL_ID"] = destination

        except ConfigurationError as exc:
            errors.append(str(exc))

    if test_mode != "1":
        test_channel_ids = {
            TEST_SOURCE_CHANNEL_ID,
            TEST_DESTINATION_CHANNEL_ID,
        }
        configured_channel_ids = {
            result.get("SOURCE_CHANNEL_ID"),
            result.get("DESTINATION_CHANNEL_ID"),
        }
        if test_channel_ids.intersection(configured_channel_ids):
            errors.append(
                "Production configuration cannot use approved test channel IDs."
            )

    # Validate database path without creating the database.
    try:
        db_path = resolve_db_path()
        path = Path(db_path)

        if path.exists() and path.is_dir():
            errors.append("DB_PATH points to a directory, not a database file.")

        if not path.parent.exists():
            errors.append(
                f"DB_PATH parent directory does not exist: {path.parent}"
            )
        elif not path.parent.is_dir():
            errors.append(
                f"DB_PATH parent is not a directory: {path.parent}"
            )

        result["DB_PATH"] = db_path
    except ConfigurationError as exc:
        errors.append(str(exc))
        path = None

    for variable, default in (
        ("SCHEDULER_LOCK_FILE", "scheduler.lock"),
        ("LISTENER_LOCK_FILE", "listener.lock"),
    ):
        lock_value = os.getenv(variable, default).strip()
        if not lock_value:
            errors.append(f"{variable} must not be empty.")
            continue
        lock_path = Path(lock_value).expanduser().resolve()
        if not lock_path.parent.is_dir():
            errors.append(
                f"{variable} parent directory does not exist: "
                f"{lock_path.parent}"
            )

    # Strict Phase 7 isolation.
    if test_mode == "1":

        if path is not None and path != TEST_DB_PATH:
            errors.append(
                "TEST MODE BLOCKED: DB_PATH must resolve exactly to "
                f"{TEST_DB_PATH}"
            )

        if result.get("SOURCE_CHANNEL_ID") != TEST_SOURCE_CHANNEL_ID:
            errors.append(
                "TEST MODE BLOCKED: SOURCE_CHANNEL_ID is not an approved "
                "test channel."
            )

        if (
            result.get("DESTINATION_CHANNEL_ID")
            != TEST_DESTINATION_CHANNEL_ID
        ):
            errors.append(
                "TEST MODE BLOCKED: DESTINATION_CHANNEL_ID is not an "
                "approved test channel."
            )

        lock_file = (
            os.getenv("SCHEDULER_LOCK_FILE", "scheduler.lock").strip()
        )

        if lock_file != TEST_LOCK_FILE:
            errors.append(
                f"TEST MODE requires SCHEDULER_LOCK_FILE={TEST_LOCK_FILE}"
            )

        listener_lock_file = (
            os.getenv(
                "LISTENER_LOCK_FILE",
                TEST_LISTENER_LOCK_FILE,
            ).strip()
            or TEST_LISTENER_LOCK_FILE
        )

        if listener_lock_file != TEST_LISTENER_LOCK_FILE:
            errors.append(
                "TEST MODE requires "
                f"LISTENER_LOCK_FILE={TEST_LISTENER_LOCK_FILE}"
            )

    if errors:
        raise ConfigurationError(" ; ".join(errors))

    return result