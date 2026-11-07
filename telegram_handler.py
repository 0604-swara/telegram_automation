import json
import logging
import os
import re
import signal
import sys
import time
import traceback
from contextlib import contextmanager
from logging.handlers import RotatingFileHandler

import requests
from dotenv import load_dotenv

from config import ConfigurationError, validate_config
from database import (
    get_offset,
    initialize_database,
    begin_go_live_bootstrap,
    complete_go_live_bootstrap,
    mark_go_live_webhook_conflict,
    process_telegram_update,
    record_heartbeat,
    require_go_live_ready,
)

load_dotenv()

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
SOURCE_CHANNEL_ID = os.getenv("SOURCE_CHANNEL_ID")

POLL_TIMEOUT_SECONDS = 30
HTTP_TIMEOUT_SECONDS = 35
MIN_RETRY_SECONDS = 5
MAX_RETRY_SECONDS = 60
LOCK_FILE = os.getenv("LISTENER_LOCK_FILE", "listener.lock")

SUPPORTED_MESSAGE_TYPES = {
    "text",
    "photo",
    "video",
    "animation",
    "document",
    "audio",
    "voice",
}

_shutdown_requested = False
_logger = logging.getLogger("listener")


def _configure_logging():
    """Configure listener logging without exposing secrets."""
    if _logger.handlers:
        return _logger

    _logger.setLevel(logging.INFO)
    _logger.propagate = False

    if os.getenv("LOG_TO_STDOUT", "").strip() == "1":
        handler = logging.StreamHandler(sys.stdout)
    else:
        handler = RotatingFileHandler(
            "listener.log",
            maxBytes=5 * 1024 * 1024,
            backupCount=5,
            encoding="utf-8",
        )
    handler.setFormatter(
        logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    )
    _logger.addHandler(handler)
    return _logger


def sanitize_telegram_error(error):
    """
    Remove bot tokens and token-bearing URLs from error text before logging.
    """
    text = str(error)
    if not text:
        return text

    if BOT_TOKEN:
        text = text.replace(BOT_TOKEN, "***REDACTED***")

    # Redact common Telegram bot token URL forms.
    text = re.sub(
        r"https?://api\.telegram\.org/bot[^/\s\"']+",
        "https://api.telegram.org/bot***REDACTED***",
        text,
        flags=re.IGNORECASE,
    )
    text = re.sub(
        r"/bot\d+:[A-Za-z0-9_-]+",
        "/bot***REDACTED***",
        text,
    )
    return text


def _telegram_method_url(method_name):
    if not BOT_TOKEN:
        raise ValueError("TELEGRAM_BOT_TOKEN is not configured")
    return f"https://api.telegram.org/bot{BOT_TOKEN}/{method_name}"


def validate_environment():
    validate_config(require_destination=False, require_token=True)


def request_shutdown(signum=None, frame=None):
    """Signal handler for graceful listener shutdown."""
    global _shutdown_requested
    _shutdown_requested = True
    signal_name = signum if signum is not None else "application"
    _logger.info("Shutdown requested (signal=%s).", signal_name)
    print("\nShutdown requested. Stopping listener cleanly...")


def install_signal_handlers():
    signal.signal(signal.SIGINT, request_shutdown)
    # SIGTERM is available on Unix and modern Windows Python builds.
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, request_shutdown)


class ListenerError(RuntimeError):
    """Raised when the listener cannot acquire its process lock."""


class ListenerSingleInstanceLock:
    """Non-blocking process lock with Windows and POSIX implementations."""

    def __init__(self, path):
        self.path = path
        self.handle = None

    def acquire(self):
        try:
            self.handle = open(self.path, "a+")
            self.handle.seek(0, os.SEEK_END)
            if self.handle.tell() == 0:
                self.handle.write("0")
                self.handle.flush()
            self.handle.seek(0)

            if os.name == "nt":
                import msvcrt

                msvcrt.locking(self.handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(
                    self.handle.fileno(),
                    fcntl.LOCK_EX | fcntl.LOCK_NB,
                )
        except OSError as exc:
            self._close_handle()
            raise ListenerError(
                f"Another listener holds {self.path}, or the lock "
                "cannot be acquired."
            ) from exc

        try:
            self.handle.seek(0)
            self.handle.truncate()
            self.handle.write(f"pid={os.getpid()}\n")
            self.handle.flush()
        except OSError as exc:
            self.release()
            raise ListenerError(
                f"Cannot write listener lock metadata to {self.path}."
            ) from exc

    def _close_handle(self):
        if self.handle is not None:
            try:
                self.handle.close()
            finally:
                self.handle = None

    def release(self):
        if self.handle is None:
            return
        try:
            if os.name == "nt":
                import msvcrt

                try:
                    self.handle.seek(0)
                    msvcrt.locking(
                        self.handle.fileno(),
                        msvcrt.LK_UNLCK,
                        1,
                    )
                except OSError:
                    pass
            else:
                import fcntl

                fcntl.flock(self.handle.fileno(), fcntl.LOCK_UN)
        finally:
            self._close_handle()


@contextmanager
def single_listener_lock(path=LOCK_FILE):
    lock = ListenerSingleInstanceLock(path)
    lock.acquire()
    try:
        yield lock
    finally:
        lock.release()


def _extract_retry_after(response, fallback=MIN_RETRY_SECONDS):
    """Read Telegram retry_after from headers or JSON body."""
    header_value = response.headers.get("Retry-After")
    if header_value:
        try:
            return max(int(header_value), 1)
        except ValueError:
            pass

    try:
        payload = response.json()
    except ValueError:
        return fallback

    parameters = payload.get("parameters") or {}
    retry_after = parameters.get("retry_after")
    if retry_after is None:
        return fallback
    try:
        return max(int(retry_after), 1)
    except (TypeError, ValueError):
        return fallback


def fetch_updates(offset, session, poll_timeout=POLL_TIMEOUT_SECONDS):
    """
    Long-poll Telegram getUpdates.

    Returns the parsed JSON payload.
    Raises requests.RequestException for transport failures.
    Raises RuntimeError for Telegram API / rate-limit errors after logging.
    """
    params = {
        "timeout": poll_timeout,
        "allowed_updates": json.dumps(["channel_post"]),
    }
    if offset is not None:
        params["offset"] = offset

    response = session.get(
        _telegram_method_url("getUpdates"),
        params=params,
        timeout=HTTP_TIMEOUT_SECONDS,
    )

    if response.status_code == 409:
        raise WebhookConflictError(
            "Telegram getUpdates is unavailable while a webhook is configured."
        )
    if response.status_code == 429:
        retry_after = _extract_retry_after(response)
        raise TelegramRateLimitError(retry_after, response.text)

    try:
        payload = response.json()
    except ValueError as exc:
        raise RuntimeError(
            f"Invalid Telegram JSON response (HTTP {response.status_code})"
        ) from exc

    if response.status_code >= 400 or not payload.get("ok"):
        description = payload.get("description") or response.text
        parameters = payload.get("parameters") or {}
        if response.status_code == 429 or "retry_after" in parameters:
            retry_after = _extract_retry_after(response)
            raise TelegramRateLimitError(retry_after, description)
        raise RuntimeError(
            f"Telegram API error (HTTP {response.status_code}): {description}"
        )

    return payload


class WebhookConflictError(RuntimeError):
    """Raised when getUpdates conflicts with an active Telegram webhook."""


class GoLiveBootstrapError(RuntimeError):
    """Raised when the persisted go-live update boundary cannot be established."""


def bootstrap_go_live_boundary(session=None):
    """
    Establish one bounded update-id boundary before starting the listener.

    A negative offset of -1 asks Telegram for only the newest pending update
    and discards older pending updates. This is one request, never a drain
    loop. Updates arriving after Telegram evaluates the request remain beyond
    the returned boundary and are processed normally.
    """
    state = begin_go_live_bootstrap()
    if state["status"] == "ready":
        return state["first_update_id"]

    owns_session = session is None
    session = session or requests.Session()
    try:
        try:
            payload = fetch_updates(-1, session, poll_timeout=0)
        except WebhookConflictError:
            mark_go_live_webhook_conflict()
            raise GoLiveBootstrapError(
                "Go-live bootstrap blocked by a configured Telegram webhook; "
                "remove the webhook and perform explicit operator recovery."
            ) from None
        except Exception:
            raise GoLiveBootstrapError(
                "Go-live boundary request failed after bootstrap began; "
                "database state is ambiguous and requires manual recovery."
            ) from None

        updates = payload.get("result")
        if not isinstance(updates, list):
            raise GoLiveBootstrapError(
                "Go-live boundary response has an invalid updates list; "
                "database state requires manual recovery."
            )
        try:
            boundary_id = max(
                (int(update["update_id"]) for update in updates),
                default=None,
            )
        except (KeyError, TypeError, ValueError):
            raise GoLiveBootstrapError(
                "Go-live boundary response has an invalid update ID; "
                "database state requires manual recovery."
            ) from None

        return complete_go_live_bootstrap(boundary_id)
    finally:
        if owns_session:
            session.close()


class TelegramRateLimitError(RuntimeError):
    def __init__(self, retry_after, detail=""):
        self.retry_after = max(int(retry_after), 1)
        safe_detail = sanitize_telegram_error(detail)
        super().__init__(
            f"Telegram rate limited; retry_after={self.retry_after}s "
            f"detail={safe_detail}"
        )


def _sleep_interruptible(seconds):
    """Sleep in short slices so shutdown can interrupt backoff waits."""
    deadline = time.monotonic() + max(seconds, 0)
    while not _shutdown_requested and time.monotonic() < deadline:
        remaining = deadline - time.monotonic()
        time.sleep(min(1.0, remaining))


def _log_exception_safely(prefix, error):
    """Log an error and traceback with token-bearing text removed."""
    safe_error = sanitize_telegram_error(error)
    safe_trace = sanitize_telegram_error(traceback.format_exc())
    _logger.error("%s: %s", prefix, safe_error)
    _logger.error("Traceback (sanitized):\n%s", safe_trace)
    return safe_error


def _log_processed_update(result):
    if result.get("pre_live_ignored"):
        _logger.info(
            "Ignored pre-go-live channel post; advanced offset to %s",
            result["next_offset"],
        )
        return

    if result["ignored"]:
        _logger.info(
            "Ignored unrelated or pre-go-live update; advanced offset to %s",
            result["next_offset"],
        )
        return

    message_type = result["message_type"] or "unknown"
    media_group_id = result["media_group_id"]

    if message_type not in SUPPORTED_MESSAGE_TYPES:
        _logger.warning(
            "Unsupported or unrecognized message type=%s. "
            "Telegram message_id=%s media_group_id=%s offset=%s",
            message_type,
            result["message_id"],
            media_group_id,
            result["next_offset"],
        )
        if result["inserted"]:
            print(
                f"Unsupported post stored: Telegram ID {result['message_id']} "
                f"({message_type})"
            )
    elif result["inserted"]:
        if media_group_id:
            _logger.info(
                "Album member saved. message_id=%s media_group_id=%s "
                "type=%s offset=%s",
                result["message_id"],
                media_group_id,
                message_type,
                result["next_offset"],
            )
            print(
                f"Album member received: Telegram ID {result['message_id']} "
                f"(group {media_group_id})"
            )
        else:
            _logger.info(
                "Source post saved. message_id=%s type=%s offset=%s",
                result["message_id"],
                message_type,
                result["next_offset"],
            )
            print(
                f"New post received: Telegram ID {result['message_id']} "
                f"({message_type})"
            )
    elif result["duplicate"]:
        _logger.info(
            "Duplicate source update ignored. message_id=%s type=%s "
            "media_group_id=%s offset=%s",
            result["message_id"],
            message_type,
            media_group_id,
            result["next_offset"],
        )


def process_updates(updates):
    """
    Persist each update through the atomic database helper.

    Returns the latest offset after the batch.
    """
    latest_offset = None
    for update in updates:
        result = process_telegram_update(update, SOURCE_CHANNEL_ID)
        latest_offset = result["next_offset"]
        _log_processed_update(result)
    return latest_offset


def listen_to_channel():
    """Continuously poll the source channel and store posts safely."""
    global _shutdown_requested
    _shutdown_requested = False
    install_signal_handlers()

    require_go_live_ready()
    offset = get_offset()
    retry_delay = MIN_RETRY_SECONDS

    _logger.info(
        "Source channel listener started. source_channel_id=%s offset=%s "
        "new_posts_only=1",
        SOURCE_CHANNEL_ID,
        offset,
    )
    print("Source channel listener started!")

    session = requests.Session()
    try:
        while not _shutdown_requested:
            try:
                payload = fetch_updates(offset, session)
                updates = payload.get("result") or []

                if updates:
                    new_offset = process_updates(updates)
                    if new_offset is not None:
                        offset = new_offset

                # Successful poll resets backoff and records liveness.
                record_heartbeat("listener")
                retry_delay = MIN_RETRY_SECONDS

            except TelegramRateLimitError as error:
                wait_for = max(error.retry_after, MIN_RETRY_SECONDS)
                _logger.warning(
                    "Rate limited by Telegram. Waiting %s seconds. detail=%s",
                    wait_for,
                    sanitize_telegram_error(error),
                )
                print(f"Rate limited. Waiting {wait_for} seconds...")
                _sleep_interruptible(wait_for)
                retry_delay = MIN_RETRY_SECONDS

            except requests.exceptions.RequestException as error:
                safe_error = _log_exception_safely(
                    "Telegram connection error",
                    error,
                )
                _logger.error("Retrying in %s seconds.", retry_delay)
                print(
                    f"Connection error. Retrying in {retry_delay} seconds..."
                )
                _sleep_interruptible(retry_delay)
                retry_delay = min(retry_delay * 2, MAX_RETRY_SECONDS)

            except Exception as error:
                safe_error = _log_exception_safely("Listener error", error)
                print(f"Listener error: {safe_error}")
                _sleep_interruptible(retry_delay)
                retry_delay = min(retry_delay * 2, MAX_RETRY_SECONDS)

    finally:
        session.close()
        _logger.info(
            "Source channel listener stopped. last_offset=%s",
            offset,
        )
        print("Listener stopped cleanly.")


def main():
    _configure_logging()
    validate_environment()
    with single_listener_lock(LOCK_FILE):
        initialize_database()
        require_go_live_ready()
        listen_to_channel()
    return 0 if not _shutdown_requested else 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (ValueError, ConfigurationError) as error:
        # Startup configuration errors should not expose secrets.
        print(f"Configuration error: {error}")
        sys.exit(1)
    except ListenerError as error:
        print(f"Listener lock error: {error}")
        sys.exit(2)
