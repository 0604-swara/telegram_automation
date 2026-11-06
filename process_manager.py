"""Run the Telegram listener and scheduler under one worker process."""

from __future__ import annotations

import logging
import os
import re
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

from config import (
    ConfigurationError,
    TEST_DESTINATION_CHANNEL_ID,
    TEST_SOURCE_CHANNEL_ID,
    validate_config,
)
from database import DB_NAME, DatabaseError, initialize_database
from scheduler import SchedulerError, SingleInstanceLock

_LOGGER = logging.getLogger("process-manager")
_TERMINATE_TIMEOUT_SECONDS = 75
_POLL_INTERVAL_SECONDS = 0.5


def _configure_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        stream=sys.stdout,
        force=True,
    )


def _redact(text, bot_token):
    if bot_token:
        text = text.replace(bot_token, "***REDACTED***")
    return re.sub(
        r"https?://api\.telegram\.org/bot[^/\s\"']+",
        "https://api.telegram.org/bot***REDACTED***",
        text,
        flags=re.IGNORECASE,
    )


def validate_render_settings(env=None, disk_path=None):
    """Validate production-only Render paths and channel settings."""
    settings = os.environ if env is None else env
    mount = Path(
        disk_path or settings.get("PERSISTENT_DISK_PATH", "/var/data")
    ).expanduser()

    if not mount.is_absolute():
        raise ConfigurationError("PERSISTENT_DISK_PATH must be absolute.")
    if not mount.is_dir():
        raise ConfigurationError(
            f"Persistent disk mount directory is unavailable: {mount}"
        )

    if settings.get("TELEGRAM_TEST_MODE") != "0":
        raise ConfigurationError(
            "Render production requires TELEGRAM_TEST_MODE=0."
        )

    token = settings.get("TELEGRAM_BOT_TOKEN", "").strip()
    if not token or ":" not in token:
        raise ConfigurationError(
            "Missing or invalid TELEGRAM_BOT_TOKEN; value is not shown."
        )

    source = settings.get("SOURCE_CHANNEL_ID", "").strip()
    destination = settings.get("DESTINATION_CHANNEL_ID", "").strip()
    if not re.fullmatch(r"-?\d{5,20}", source):
        raise ConfigurationError("SOURCE_CHANNEL_ID must be a numeric chat ID.")
    if not re.fullmatch(r"-?\d{5,20}", destination):
        raise ConfigurationError(
            "DESTINATION_CHANNEL_ID must be a numeric chat ID."
        )
    test_channel_ids = {
        TEST_SOURCE_CHANNEL_ID,
        TEST_DESTINATION_CHANNEL_ID,
    }
    if test_channel_ids.intersection({source, destination}):
        raise ConfigurationError(
            "Render production cannot use the configured test channel IDs."
        )
    if source == destination:
        raise ConfigurationError(
            "SOURCE_CHANNEL_ID and DESTINATION_CHANNEL_ID must be different."
        )

    raw_db_path = settings.get("DB_PATH", "").strip()
    db_path = Path(raw_db_path).expanduser()
    if not raw_db_path or not db_path.is_absolute():
        raise ConfigurationError("Render DB_PATH must be an absolute path.")
    db_path = db_path.resolve()
    mount = mount.resolve()
    try:
        db_path.relative_to(mount)
    except ValueError as exc:
        raise ConfigurationError(
            "Render DB_PATH must be inside PERSISTENT_DISK_PATH."
        ) from exc
    if db_path.name != "telegram_queue.db":
        raise ConfigurationError(
            "Render DB_PATH must name telegram_queue.db."
        )
    if not db_path.parent.is_dir():
        raise ConfigurationError(
            f"DB_PATH parent directory does not exist: {db_path.parent}"
        )

    interval = settings.get("PUBLISH_INTERVAL_SECONDS", "")
    if interval != "300":
        raise ConfigurationError(
            "Render production requires PUBLISH_INTERVAL_SECONDS=300."
        )

    for name in ("SCHEDULER_LOCK_FILE", "LISTENER_LOCK_FILE",
                 "PROCESS_MANAGER_LOCK_FILE"):
        raw_lock = settings.get(name, "").strip()
        lock_path = Path(raw_lock).expanduser()
        if not raw_lock or not lock_path.is_absolute():
            raise ConfigurationError(
                f"Render {name} must be an absolute path."
            )
        lock_path = lock_path.resolve()
        try:
            lock_path.relative_to(mount)
        except ValueError as exc:
            raise ConfigurationError(
                f"Render {name} must be inside PERSISTENT_DISK_PATH."
            ) from exc
        if not lock_path.parent.is_dir():
            raise ConfigurationError(
                f"{name} parent directory does not exist: "
                f"{lock_path.parent}"
            )

    if not os.access(db_path.parent, os.W_OK):
        raise ConfigurationError(
            f"DB_PATH parent directory is not writable: {db_path.parent}"
        )
    if DB_NAME != str(db_path):
        raise ConfigurationError(
            "Database module and Render DB_PATH resolve differently."
        )
    return {
        "DB_PATH": str(db_path),
        "PERSISTENT_DISK_PATH": str(mount),
    }


def _relay_output(name, stream, bot_token):
    try:
        for line in iter(stream.readline, ""):
            _LOGGER.info("[%s] %s", name, _redact(line.rstrip(), bot_token))
    finally:
        stream.close()


def _stop_processes(processes, timeout_seconds):
    live = [(name, process) for name, process, _ in processes
            if process.poll() is None]
    for name, process in live:
        _LOGGER.info("Sending SIGTERM to %s.", name)
        try:
            process.terminate()
        except OSError:
            _LOGGER.exception("Failed to signal %s.", name)

    deadline = time.monotonic() + timeout_seconds
    for name, process in live:
        remaining = max(0.0, deadline - time.monotonic())
        try:
            process.wait(timeout=remaining)
        except subprocess.TimeoutExpired:
            _LOGGER.error(
                "%s did not stop within %.1f seconds; forcing termination.",
                name,
                timeout_seconds,
            )
            process.kill()
            process.wait()


def _worker_commands(db_path, application_root=None):
    worker_env = os.environ.copy()
    worker_env["DB_PATH"] = db_path
    worker_env["TELEGRAM_TEST_MODE"] = "0"
    worker_env["LOG_TO_STDOUT"] = "1"
    root = Path(application_root or Path(__file__).resolve().parent)
    return [
        (
            "listener",
            [sys.executable, str(root / "telegram_handler.py")],
            worker_env,
        ),
        (
            "scheduler",
            [sys.executable, str(root / "scheduler.py")],
            worker_env,
        ),
    ]


def run_processes(
    commands,
    stop_event=None,
    popen_factory=None,
    poll_interval=_POLL_INTERVAL_SECONDS,
    terminate_timeout=_TERMINATE_TIMEOUT_SECONDS,
):
    """Run named children; shut down all children if any exits."""
    stop_event = stop_event or threading.Event()
    popen_factory = popen_factory or subprocess.Popen
    processes = []
    readers = []
    exit_code = 1

    try:
        for name, command, environment in commands:
            _LOGGER.info("Starting %s.", name)
            process = popen_factory(
                command,
                env=environment,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
            )
            processes.append((name, process, command))
            reader = threading.Thread(
                target=_relay_output,
                args=(
                    name,
                    process.stdout,
                    environment.get("TELEGRAM_BOT_TOKEN", ""),
                ),
                daemon=True,
            )
            reader.start()
            readers.append(reader)

        while not stop_event.wait(poll_interval):
            for name, process, _ in processes:
                status = process.poll()
                if status is not None:
                    _LOGGER.error(
                        "%s exited unexpectedly with status %s.",
                        name,
                        status,
                    )
                    return 1

        _LOGGER.info("Shutdown requested for worker children.")
        exit_code = 0
        return exit_code
    except OSError:
        _LOGGER.exception("Failed to start worker child process.")
        return 1
    finally:
        _stop_processes(processes, terminate_timeout)
        for reader in readers:
            reader.join(timeout=1)
        for name, process, _ in processes:
            _LOGGER.info("%s stopped with status %s.", name, process.poll())


def main():
    _configure_logging()
    manager_lock = None
    try:
        validated = validate_config(
            require_destination=True,
            require_token=True,
        )
        production_paths = validate_render_settings()
        if validated["DB_PATH"] != production_paths["DB_PATH"]:
            raise ConfigurationError(
                "Validated DB_PATH differs from the persistent-disk path."
            )
        if not Path(production_paths["PERSISTENT_DISK_PATH"]).is_mount():
            raise ConfigurationError(
                "PERSISTENT_DISK_PATH is not a mounted Render disk."
            )
        manager_lock = SingleInstanceLock(
            os.environ["PROCESS_MANAGER_LOCK_FILE"]
        )
        manager_lock.acquire()

        initialize_database()
        _LOGGER.info(
            "Worker initialized. listener and scheduler share DB_PATH=%s.",
            production_paths["DB_PATH"],
        )

        commands = _worker_commands(production_paths["DB_PATH"])
        return run_processes(commands, stop_event=_STOP_EVENT)
    except (ConfigurationError, DatabaseError, SchedulerError, OSError):
        _LOGGER.exception("Worker startup failed.")
        return 1
    finally:
        if manager_lock is not None:
            manager_lock.release()
            _LOGGER.info("Worker process lock released.")


def _signal_handler(signum, frame):
    _LOGGER.info("Worker shutdown requested (signal=%s).", signum)
    _STOP_EVENT.set()


_STOP_EVENT = threading.Event()


if __name__ == "__main__":
    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)
    sys.exit(main())
