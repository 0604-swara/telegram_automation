"""
Production publishing scheduler.

Business rules:
- Publish every INTERVAL seconds (default 300)
- Newest-first; new posts win at the next slot
- Albums stay together via atomic claims
- Permanently failed posts stay failed until manual reset
- Single-instance lock prevents concurrent schedulers

Telegram Bot API has no copy idempotency key. Stale publishing claims are moved
to failed for manual review (never auto-republished). Ambiguous Telegram
outcomes (timeout, mid-flight connection loss, HTTP 5xx, partial album,
unreadable body) are also forced to failed without automatic retry.
"""

from __future__ import annotations

import logging
import os
import re
import signal
import socket
import sys
import time
import traceback
import uuid
from contextlib import contextmanager
from datetime import datetime, time as datetime_time, timedelta
from logging.handlers import RotatingFileHandler
from zoneinfo import ZoneInfo

import requests
from dotenv import load_dotenv

from config import ConfigurationError, validate_config
from database import (
    ClaimError,
    PublishOwnershipError,
    claim_posts,
    get_connection,
    get_post_number,
    get_queue_stats,
    initialize_database,
    mark_as_failed,
    mark_as_published,
    record_heartbeat,
    release_claim,
    release_stale_claims,
    mark_as_uncertain,
    restart_posting_cycle,
    update_post_number,
)

load_dotenv()

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
SOURCE_CHANNEL_ID = os.getenv("SOURCE_CHANNEL_ID")
DESTINATION_CHANNEL_ID = os.getenv("DESTINATION_CHANNEL_ID")

_CONFIGURATION_ERRORS = []


def _read_seconds_setting(name, default, minimum):
    value = os.getenv(name)
    if value is None:
        return default

    try:
        seconds = int(value)
    except ValueError:
        _CONFIGURATION_ERRORS.append(f"{name} must be an integer.")
        return default

    if seconds < minimum:
        _CONFIGURATION_ERRORS.append(
            f"{name} must be at least {minimum}."
        )
        return default

    return seconds


INTERVAL = _read_seconds_setting("PUBLISH_INTERVAL_SECONDS", 300, 1)
ALBUM_WAIT_SECONDS = _read_seconds_setting("ALBUM_WAIT_SECONDS", 15, 0)
MAX_RETRIES = int(os.getenv("MAX_PUBLISH_RETRIES", "5"))
EMPTY_QUEUE_CHECK_SECONDS = _read_seconds_setting(
    "EMPTY_QUEUE_CHECK_SECONDS",
    30,
    1,
)
STALE_CLAIM_SECONDS = int(os.getenv("STALE_CLAIM_SECONDS", "900"))
TRANSIENT_ATTEMPTS = int(os.getenv("PUBLISH_TRANSIENT_ATTEMPTS", "3"))
HTTP_TIMEOUT_SECONDS = int(os.getenv("PUBLISH_HTTP_TIMEOUT_SECONDS", "30"))
LOCK_FILE = os.getenv("SCHEDULER_LOCK_FILE", "scheduler.lock")
PUBLISH_TIMEZONE = ZoneInfo("Asia/Kolkata")
SLOT_START_GRACE_SECONDS = 5


def _parse_publish_time(value, name):
    try:
        parsed = datetime.strptime(value, "%H:%M").time()
    except (TypeError, ValueError) as exc:
        raise ConfigurationError(
            f"{name} must use 24-hour HH:MM format."
        ) from exc
    if parsed.strftime("%H:%M") != value:
        raise ConfigurationError(
            f"{name} must use zero-padded 24-hour HH:MM format."
        )
    return parsed


def validate_publish_schedule(start_value, end_value, interval):
    """Validate the configured daily local-time publishing schedule."""
    start = _parse_publish_time(start_value, "PUBLISH_START_TIME")
    end = _parse_publish_time(end_value, "PUBLISH_END_TIME")
    if start == end:
        raise ConfigurationError(
            "PUBLISH_START_TIME and PUBLISH_END_TIME must define a "
            "non-empty publishing window; equal times are not supported."
        )
    if interval < 1:
        raise ConfigurationError("PUBLISH_INTERVAL_SECONDS must be positive.")
    return start, end


try:
    PUBLISH_START_TIME, PUBLISH_END_TIME = validate_publish_schedule(
        os.getenv("PUBLISH_START_TIME", "08:00"),
        os.getenv("PUBLISH_END_TIME", "00:00"),
        INTERVAL,
    )
except ConfigurationError as exc:
    _CONFIGURATION_ERRORS.append(str(exc))
    PUBLISH_START_TIME = datetime_time(8, 0)
    PUBLISH_END_TIME = datetime_time(0, 0)

_logger = logging.getLogger("scheduler")
_shutdown_requested = False
_claim_owner = None
_lock_handle = None


class SchedulerError(Exception):
    """Base scheduler error."""


class TelegramRateLimitError(SchedulerError):
    def __init__(self, retry_after, detail=""):
        self.retry_after = max(int(retry_after), 1)
        super().__init__(
            f"Telegram rate limited; retry_after={self.retry_after}s "
            f"detail={sanitize_telegram_error(detail)}"
        )


class TelegramTransientError(SchedulerError):
    """
    Confirmed failure before the request was transmitted.

    Safe to retry while holding the claim (for example ConnectTimeout).
    """


class TelegramPermanentError(SchedulerError):
    """Deterministic Telegram rejection; do not retry the same copy blindly."""


class AmbiguousPublishError(SchedulerError):
    """
    Publish result is uncertain or partial.

    Telegram may already have accepted some or all copies.
    Do not auto-retry - manual review required.
    """


def _configure_logging():
    if _logger.handlers:
        return _logger

    _logger.setLevel(logging.INFO)
    _logger.propagate = False
    if os.getenv("LOG_TO_STDOUT", "").strip() == "1":
        handler = logging.StreamHandler(sys.stdout)
    else:
        handler = RotatingFileHandler(
            "scheduler.log",
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
    text = str(error)
    if not text:
        return text
    if BOT_TOKEN:
        text = text.replace(BOT_TOKEN, "***REDACTED***")
    text = re.sub(
        r"https?://api\.telegram\.org/bot[^/\s\"']+",
        "https://api.telegram.org/bot***REDACTED***",
        text,
        flags=re.IGNORECASE,
    )
    text = re.sub(r"/bot\d+:[A-Za-z0-9_-]+", "/bot***REDACTED***", text)
    return text


def _log_exception_safely(prefix, error):
    safe_error = sanitize_telegram_error(error)
    safe_trace = sanitize_telegram_error(traceback.format_exc())
    _logger.error("%s: %s", prefix, safe_error)
    _logger.error("Traceback (sanitized):\n%s", safe_trace)
    return safe_error


def _telegram_method_url(method_name):
    if not BOT_TOKEN:
        raise ValueError("TELEGRAM_BOT_TOKEN is not configured")
    return f"https://api.telegram.org/bot{BOT_TOKEN}/{method_name}"


def validate_environment():
    if _CONFIGURATION_ERRORS:
        raise ConfigurationError(" ; ".join(_CONFIGURATION_ERRORS))
    validate_config(require_destination=True, require_token=True)


def build_claim_owner():
    return (
        f"scheduler:{socket.gethostname()}:"
        f"{os.getpid()}:{uuid.uuid4().hex[:8]}"
    )


def request_shutdown(signum=None, frame=None):
    global _shutdown_requested
    _shutdown_requested = True
    _logger.info("Shutdown requested (signal=%s).", signum)
    print("\nShutdown requested. Stopping scheduler cleanly...")


def install_signal_handlers():
    signal.signal(signal.SIGINT, request_shutdown)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, request_shutdown)


# Refresh scheduler liveness during long intentional waits (e.g. 5-minute gap).
HEARTBEAT_DURING_WAIT_SECONDS = int(
    os.getenv("SCHEDULER_WAIT_HEARTBEAT_SECONDS", "30")
)


def _sleep_interruptible(seconds):
    """
    Sleep in short slices so shutdown can interrupt waits.

    Also refreshes the scheduler heartbeat periodically so a healthy idle
    wait (up to INTERVAL) is not reported as stale by healthcheck.
    """
    deadline = time.monotonic() + max(float(seconds), 0.0)
    last_heartbeat = 0.0
    while not _shutdown_requested and time.monotonic() < deadline:
        now = time.monotonic()
        if now - last_heartbeat >= HEARTBEAT_DURING_WAIT_SECONDS:
            try:
                record_heartbeat("scheduler")
            except Exception as exc:
                _logger.warning(
                    "Failed to refresh scheduler heartbeat during wait: %s",
                    sanitize_telegram_error(exc),
                )
            last_heartbeat = now
        time.sleep(max(0.0, min(1.0, deadline - time.monotonic())))


def _now_ist():
    return datetime.now(PUBLISH_TIMEZONE)


def _next_publish_slot(now):
    """Return the next interval-aligned slot, inclusive, in Asia/Kolkata."""
    local_now = now.astimezone(PUBLISH_TIMEZONE)
    local_time = local_now.timetz().replace(tzinfo=None)
    crosses_midnight = PUBLISH_START_TIME > PUBLISH_END_TIME

    if crosses_midnight:
        if local_time >= PUBLISH_START_TIME:
            window_start_date = local_now.date()
        elif local_time < PUBLISH_END_TIME:
            window_start_date = local_now.date() - timedelta(days=1)
        else:
            return datetime.combine(
                local_now.date(),
                PUBLISH_START_TIME,
                tzinfo=PUBLISH_TIMEZONE,
            )
        start = datetime.combine(
            window_start_date,
            PUBLISH_START_TIME,
            tzinfo=PUBLISH_TIMEZONE,
        )
        end = datetime.combine(
            window_start_date + timedelta(days=1),
            PUBLISH_END_TIME,
            tzinfo=PUBLISH_TIMEZONE,
        )
    else:
        start = datetime.combine(
            local_now.date(),
            PUBLISH_START_TIME,
            tzinfo=PUBLISH_TIMEZONE,
        )
        end = datetime.combine(
            local_now.date(),
            PUBLISH_END_TIME,
            tzinfo=PUBLISH_TIMEZONE,
        )
        if local_now < start:
            return start
        if local_now >= end:
            tomorrow = local_now.date() + timedelta(days=1)
            return datetime.combine(
                tomorrow,
                PUBLISH_START_TIME,
                tzinfo=PUBLISH_TIMEZONE,
            )

    elapsed = (local_now - start).total_seconds()
    slot_number = int((elapsed + INTERVAL - 1) // INTERVAL)
    candidate = start + timedelta(seconds=slot_number * INTERVAL)
    if candidate >= end:
        next_start_date = start.date() + timedelta(days=1)
        return datetime.combine(
            next_start_date,
            PUBLISH_START_TIME,
            tzinfo=PUBLISH_TIMEZONE,
        )
    return candidate


def _within_publish_window(now):
    local_now = now.astimezone(PUBLISH_TIMEZONE)
    local_time = local_now.timetz().replace(tzinfo=None)
    if PUBLISH_START_TIME > PUBLISH_END_TIME:
        return (
            local_time >= PUBLISH_START_TIME
            or local_time < PUBLISH_END_TIME
        )
    return PUBLISH_START_TIME <= local_time < PUBLISH_END_TIME


# ---------------------------------------------------------------------------
# Single-instance lock (Windows + Linux)
# ---------------------------------------------------------------------------

class SingleInstanceLock:
    """Non-blocking exclusive lock file suitable for Windows and Linux."""

    def __init__(self, path):
        self.path = path
        self.handle = None

    def acquire(self):
        try:
            self.handle = open(self.path, "a+")
            # Ensure the file has at least one byte so msvcrt can lock it.
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
        except (OSError, PermissionError) as exc:
            if self.handle is not None:
                try:
                    self.handle.close()
                except OSError:
                    pass
                self.handle = None
            raise SchedulerError(
                f"Another scheduler instance already holds {self.path}"
            ) from exc

        try:
            self.handle.seek(0)
            self.handle.truncate()
            self.handle.write(
                f"pid={os.getpid()} owner={_claim_owner} "
                f"host={socket.gethostname()}\n"
            )
            self.handle.flush()
        except (OSError, PermissionError) as exc:
            self.release()
            raise SchedulerError(
                f"Failed to write scheduler lock metadata for {self.path}"
            ) from exc

    def release(self):
        if self.handle is None:
            return
        try:
            self.handle.seek(0)
            if os.name == "nt":
                import msvcrt

                try:
                    msvcrt.locking(self.handle.fileno(), msvcrt.LK_UNLCK, 1)
                except OSError:
                    pass
            else:
                import fcntl

                fcntl.flock(self.handle.fileno(), fcntl.LOCK_UN)
        finally:
            try:
                self.handle.close()
            finally:
                self.handle = None


@contextmanager
def single_instance_lock(path=LOCK_FILE):
    lock = SingleInstanceLock(path)
    lock.acquire()
    try:
        yield lock
    finally:
        lock.release()


# ---------------------------------------------------------------------------
# Queue selection (newest-first, albums together)
# ---------------------------------------------------------------------------

def _row_to_post(row):
    return {
        "id": row["id"],
        "message_id": row["message_id"],
        "message_type": row["message_type"],
        "media_group_id": row["media_group_id"],
        "status": row["status"],
        "retry_count": row["retry_count"],
    }


def select_next_publish_unit(max_retries=MAX_RETRIES, album_wait_seconds=ALBUM_WAIT_SECONDS):
    """
    Select the newest eligible publish unit (single post or complete album).

    Returns None when nothing is ready.
    """
    with get_connection() as conn:
        candidates = conn.execute(
            """
            SELECT
                media_group_id,
                MAX(message_id) AS latest_message_id
            FROM incoming_posts
            WHERE status = 'pending'
              AND retry_count < ?
              AND live_eligible = 1
            GROUP BY
                CASE
                    WHEN media_group_id IS NOT NULL
                    THEN media_group_id
                    ELSE 'single_' || message_id
                END
            HAVING
                media_group_id IS NULL
                OR MAX(received_at) <= datetime('now', ?)
            ORDER BY latest_message_id DESC
            """,
            (max_retries, f"-{int(album_wait_seconds)} seconds"),
        ).fetchall()

        for candidate in candidates:
            media_group_id = candidate["media_group_id"]
            latest_message_id = candidate["latest_message_id"]

            if media_group_id is not None:
                members = conn.execute(
                    """
                    SELECT
                        id, message_id, message_type, media_group_id,
                        status, retry_count
                    FROM incoming_posts
                    WHERE media_group_id = ?
                      AND live_eligible = 1
                    ORDER BY message_id ASC
                    """,
                    (media_group_id,),
                ).fetchall()

                if not members:
                    continue

                # Require the full album to be pending and eligible.
                if any(
                    row["status"] != "pending"
                    or row["retry_count"] >= max_retries
                    for row in members
                ):
                    _logger.info(
                        "Skipping incomplete/ineligible album media_group_id=%s",
                        media_group_id,
                    )
                    continue

                posts = [_row_to_post(row) for row in members]
                return {
                    "media_group_id": media_group_id,
                    "posts": posts,
                    "kind": "album",
                }

            rows = conn.execute(
                """
                SELECT
                    id, message_id, message_type, media_group_id,
                    status, retry_count
                FROM incoming_posts
                WHERE message_id = ?
                  AND status = 'pending'
                  AND retry_count < ?
                  AND live_eligible = 1
                LIMIT 1
                """,
                (latest_message_id, max_retries),
            ).fetchall()
            if not rows:
                continue
            posts = [_row_to_post(row) for row in rows]
            return {
                "media_group_id": None,
                "posts": posts,
                "kind": "single",
            }

    return None


# ---------------------------------------------------------------------------
# Telegram publish helpers
# ---------------------------------------------------------------------------

def _extract_retry_after(response, fallback=5):
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
    retry_after = parameters.get("retry_after", fallback)
    try:
        return max(int(retry_after), 1)
    except (TypeError, ValueError):
        return fallback


def _classify_telegram_failure(response, payload):
    status = response.status_code
    description = ""
    if isinstance(payload, dict):
        description = payload.get("description") or response.text
    else:
        description = response.text

    if status == 429:
        raise TelegramRateLimitError(
            _extract_retry_after(response),
            description,
        )

    # 5xx / 408: request may have been accepted or partially processed.
    if status >= 500 or status == 408:
        raise AmbiguousPublishError(
            f"Ambiguous Telegram HTTP {status}; "
            f"response may not reflect final copy state: {description}"
        )

    # Deterministic client / API rejection.
    raise TelegramPermanentError(
        f"Telegram API error (HTTP {status}): {description}"
    )


def _parse_copy_response(response, expected_count):
    try:
        payload = response.json()
    except ValueError as exc:
        # Body unreadable after a completed HTTP exchange — treat as ambiguous.
        raise AmbiguousPublishError(
            f"Unreadable Telegram response (HTTP {response.status_code})"
        ) from exc

    if response.status_code == 429 or (
        isinstance(payload, dict)
        and isinstance(payload.get("parameters"), dict)
        and "retry_after" in payload["parameters"]
    ):
        raise TelegramRateLimitError(
            _extract_retry_after(response),
            payload.get("description", response.text)
            if isinstance(payload, dict)
            else response.text,
        )

    if not response.ok or not (
        isinstance(payload, dict) and payload.get("ok")
    ):
        _classify_telegram_failure(
            response,
            payload if isinstance(payload, dict) else {},
        )

    result = payload.get("result")
    if expected_count == 1:
        if not isinstance(result, dict):
            raise AmbiguousPublishError(
                "Unexpected copyMessage result payload"
            )
        dest_id = result.get("message_id")
        if dest_id is None:
            raise AmbiguousPublishError(
                "copyMessage succeeded without destination message_id"
            )
        return [dest_id]

    if not isinstance(result, list):
        raise AmbiguousPublishError("Unexpected copyMessages result payload")

    if len(result) != expected_count:
        raise AmbiguousPublishError(
            "Partial or mismatched album copy result: "
            f"expected={expected_count} got={len(result)}"
        )

    destination_ids = []
    for item in result:
        if not isinstance(item, dict) or item.get("message_id") is None:
            raise AmbiguousPublishError(
                "Album copy result missing destination message_id"
            )
        destination_ids.append(item["message_id"])
    return destination_ids


def _classify_requests_exception(exc, operation_name):
    """
    Classify transport errors.

    Only failures confirmed to occur before request transmission are retryable.
    Anything that may have reached Telegram is ambiguous.
    """
    # ConnectTimeout is a Timeout subclass but means the TCP connection never
    # completed, so the copy request was not transmitted.
    if isinstance(exc, requests.exceptions.ConnectTimeout):
        return TelegramTransientError(
            f"{operation_name} connect timeout before transmission: "
            f"{sanitize_telegram_error(exc)}"
        )

    if isinstance(exc, requests.exceptions.Timeout):
        return AmbiguousPublishError(
            f"{operation_name} timed out after transmission may have started; "
            "Telegram may have accepted the copy"
        )

    if isinstance(exc, requests.exceptions.ConnectionError):
        return AmbiguousPublishError(
            f"{operation_name} connection lost; transmission may have started "
            f"and Telegram may have accepted the copy: "
            f"{sanitize_telegram_error(exc)}"
        )

    return AmbiguousPublishError(
        f"{operation_name} request failed with uncertain delivery: "
        f"{sanitize_telegram_error(exc)}"
    )


def publish_single(message_id, session=None):
    http = session or requests
    try:
        response = http.post(
            _telegram_method_url("copyMessage"),
            json={
                "chat_id": DESTINATION_CHANNEL_ID,
                "from_chat_id": SOURCE_CHANNEL_ID,
                "message_id": message_id,
            },
            timeout=HTTP_TIMEOUT_SECONDS,
        )
    except requests.exceptions.RequestException as exc:
        raise _classify_requests_exception(exc, "copyMessage") from exc

    return _parse_copy_response(response, expected_count=1)


def publish_album(message_ids, session=None):
    http = session or requests
    try:
        response = http.post(
            _telegram_method_url("copyMessages"),
            json={
                "chat_id": DESTINATION_CHANNEL_ID,
                "from_chat_id": SOURCE_CHANNEL_ID,
                "message_ids": message_ids,
            },
            timeout=HTTP_TIMEOUT_SECONDS,
        )
    except requests.exceptions.RequestException as exc:
        raise _classify_requests_exception(exc, "copyMessages") from exc

    return _parse_copy_response(response, expected_count=len(message_ids))


# ---------------------------------------------------------------------------
# Publish orchestration
# ---------------------------------------------------------------------------

def _force_manual_review(post_ids, claim_owner, error):
    """
    Move ambiguous outcomes out of the automatic retry path.

    mark_as_failed(..., max_retries=0) forces status='failed' immediately
    because retry_count+1 >= 0.
    """
    mark_as_failed(
        post_ids,
        error=f"AMBIGUOUS_PUBLISH_MANUAL_REVIEW: {sanitize_telegram_error(error)}",
        max_retries=0,
        claim_owner=claim_owner,
    )


def process_next_post(
    claim_owner=None,
    session=None,
    enforce_publish_window=False,
):
    """
    Claim and publish the next eligible unit.

    Returns:
      True  — published successfully (caller should wait INTERVAL)
      False — nothing published (empty/retry/failure/shutdown)

    The scheduler loop sets enforce_publish_window so safe retries cannot
    cross the configured daily closing time.
    """
    owner = claim_owner or _claim_owner
    if not owner:
        raise SchedulerError("claim_owner is not set")

    unit = select_next_publish_unit()
    if unit is None:
        restarted = restart_posting_cycle()
        if restarted:
            _logger.info(
                "Publishing cycle restarted. queue=%s",
                get_queue_stats(),
            )
            print("All eligible posts completed. Starting a new publishing cycle.")
            unit = select_next_publish_unit()
        if unit is None:
            return False

    posts = unit["posts"]
    database_ids = [post["id"] for post in posts]
    telegram_ids = [post["message_id"] for post in posts]
    post_number = get_post_number() + 1
    kind = unit["kind"]

    try:
        claimed_ids = claim_posts(database_ids, owner)
    except ClaimError as exc:
        _logger.warning(
            "Could not claim Post #%s ids=%s telegram_ids=%s: %s",
            post_number,
            database_ids,
            telegram_ids,
            sanitize_telegram_error(exc),
        )
        return False

    if claimed_ids != database_ids:
        # claim_posts is all-or-nothing; mismatch is unexpected.
        _logger.error(
            "Claim mismatch for Post #%s requested=%s claimed=%s",
            post_number,
            database_ids,
            claimed_ids,
        )
        return False

    _logger.info(
        "Claimed Post #%s kind=%s db_ids=%s telegram_ids=%s owner=%s",
        post_number,
        kind,
        database_ids,
        telegram_ids,
        owner,
    )

    transient_delay = 5
    try:
        for attempt in range(1, TRANSIENT_ATTEMPTS + 1):
            if enforce_publish_window and not _within_publish_window(
                _now_ist()
            ):
                release_claim(
                    database_ids,
                    claim_owner=owner,
                    error="Publishing window closed before retry",
                    increment_retry=False,
                )
                _logger.info(
                    "Released Post #%s to pending; publishing window closed.",
                    post_number,
                )
                return False
            if _shutdown_requested:
                release_claim(
                    database_ids,
                    claim_owner=owner,
                    error="Shutdown requested before publish completed",
                    increment_retry=False,
                )
                return False

            try:
                if kind == "album":
                    destination_ids = publish_album(telegram_ids, session=session)
                    post_type = "Album"
                else:
                    destination_ids = publish_single(
                        telegram_ids[0],
                        session=session,
                    )
                    post_type = posts[0]["message_type"] or "Message"

                try:
                    mark_as_published(
                        database_ids,
                        claim_owner=owner,
                        destination_message_ids=destination_ids,
                    )
                except Exception as finalize_error:
                    safe_error = sanitize_telegram_error(finalize_error)
                    reconciliation_error = (
                        "TELEGRAM_ACCEPTED_LOCAL_FINALIZATION_FAILED: "
                        f"{safe_error}"
                    )
                    try:
                        mark_as_uncertain(
                            database_ids,
                            claim_owner=owner,
                            destination_message_ids=destination_ids,
                            error=reconciliation_error,
                        )
                    except Exception as quarantine_error:
                        _log_exception_safely(
                            "Telegram accepted the copy but local "
                            "finalization and uncertain-state recording "
                            "failed; claim remains for manual review",
                            quarantine_error,
                        )
                    else:
                        _logger.error(
                            "Telegram accepted Post #%s but local "
                            "finalization failed; quarantined as uncertain. "
                            "Do not retry until destination reconciliation. "
                            "source_ids=%s destination_ids=%s",
                            post_number,
                            telegram_ids,
                            destination_ids,
                        )
                    return False

                update_post_number(post_number)

                _logger.info(
                    "Post #%s published successfully. type=%s "
                    "telegram_ids=%s destination_ids=%s",
                    post_number,
                    post_type,
                    telegram_ids,
                    destination_ids,
                )
                print(f"Post #{post_number} published successfully!")
                return True

            except TelegramRateLimitError as exc:
                _logger.warning(
                    "Post #%s rate-limited on attempt %s/%s; waiting %ss",
                    post_number,
                    attempt,
                    TRANSIENT_ATTEMPTS,
                    exc.retry_after,
                )
                _sleep_interruptible(exc.retry_after)
                continue

            except TelegramTransientError as exc:
                _logger.warning(
                    "Post #%s transient error on attempt %s/%s: %s",
                    post_number,
                    attempt,
                    TRANSIENT_ATTEMPTS,
                    sanitize_telegram_error(exc),
                )
                if attempt >= TRANSIENT_ATTEMPTS:
                    break
                _sleep_interruptible(transient_delay)
                transient_delay = min(transient_delay * 2, 60)
                continue

            except AmbiguousPublishError as exc:
                safe = sanitize_telegram_error(exc)
                _logger.error(
                    "Post #%s ambiguous publish outcome; "
                    "not auto-retrying. telegram_ids=%s detail=%s",
                    post_number,
                    telegram_ids,
                    safe,
                )
                _force_manual_review(database_ids, owner, safe)
                print(
                    f"Post #{post_number} ambiguous publish result - "
                    "marked for manual review."
                )
                return False

            except TelegramPermanentError as exc:
                safe = sanitize_telegram_error(exc)
                _logger.error(
                    "Post #%s permanent Telegram failure: %s",
                    post_number,
                    safe,
                )
                mark_as_failed(
                    database_ids,
                    error=safe,
                    max_retries=0,
                    claim_owner=owner,
                )
                print(f"Publication failed: {safe}")
                return False

            except PublishOwnershipError as exc:
                # Another instance changed ownership — do not force publish.
                _log_exception_safely(
                    f"Post #{post_number} ownership error",
                    exc,
                )
                return False

        # Exhausted transient attempts while still holding the claim.
        mark_as_failed(
            database_ids,
            error="Exhausted transient publish retries",
            max_retries=MAX_RETRIES,
            claim_owner=owner,
        )
        _logger.error(
            "Post #%s failed after transient retries. telegram_ids=%s",
            post_number,
            telegram_ids,
        )
        print(f"Post #{post_number} failed after transient retries.")
        return False

    except Exception as exc:
        safe = _log_exception_safely(
            f"Unexpected publish failure for Post #{post_number}",
            exc,
        )
        try:
            mark_as_failed(
                database_ids,
                error=safe,
                max_retries=MAX_RETRIES,
                claim_owner=owner,
            )
        except Exception as mark_error:
            _log_exception_safely(
                "Failed to mark posts as failed after unexpected error",
                mark_error,
            )
            try:
                release_claim(
                    database_ids,
                    claim_owner=owner,
                    error=safe,
                    increment_retry=True,
                    max_retries=MAX_RETRIES,
                )
            except Exception as release_error:
                _log_exception_safely(
                    "Failed to release claim after unexpected error",
                    release_error,
                )
        print(f"Publication failed: {safe}")
        return False


def run_scheduler_loop(claim_owner=None, session=None):
    """Run at most one publishing attempt per daily IST interval slot."""
    owner = claim_owner or _claim_owner
    http = session or requests.Session()
    owns_session = session is None
    next_publish_at = _next_publish_slot(_now_ist())

    _logger.info(
        "Scheduler loop starting. owner=%s interval=%ss window=%s-%s IST",
        owner,
        INTERVAL,
        PUBLISH_START_TIME.strftime("%H:%M"),
        PUBLISH_END_TIME.strftime("%H:%M"),
    )
    print("Telegram scheduler started!")

    try:
        while not _shutdown_requested:
            now = _now_ist()
            remaining = (next_publish_at - now).total_seconds()
            if remaining > 0:
                _sleep_interruptible(
                    min(EMPTY_QUEUE_CHECK_SECONDS, remaining)
                )
                continue

            if now >= next_publish_at + timedelta(
                seconds=SLOT_START_GRACE_SECONDS
            ):
                _logger.info(
                    "Missed publishing slot at %s IST; skipping catch-up.",
                    next_publish_at.isoformat(),
                )
                next_publish_at = _next_publish_slot(now)
                continue

            try:
                try:
                    published = process_next_post(
                        claim_owner=owner,
                        session=http,
                        enforce_publish_window=True,
                    )
                finally:
                    next_publish_at = _next_publish_slot(
                        _now_ist() + timedelta(seconds=1)
                    )

                if _shutdown_requested:
                    break

                record_heartbeat("scheduler")
                if published:
                    print(
                        f"Next post scheduled in {INTERVAL // 60} minutes "
                        f"({INTERVAL} seconds)."
                    )
                else:
                    stats = get_queue_stats()
                    _logger.info(
                        "No publish this slot. queue=%s; next slot at %s IST",
                        stats,
                        next_publish_at.isoformat(),
                    )
                    print(
                        "No eligible posts. Waiting for the next publish slot."
                    )

            except Exception as exc:
                _log_exception_safely("Scheduler loop error", exc)
                print(f"Scheduler error: {sanitize_telegram_error(exc)}")
                next_publish_at = _next_publish_slot(
                    _now_ist() + timedelta(seconds=1)
                )
    finally:
        if owns_session and hasattr(http, "close"):
            http.close()
        _logger.info("Scheduler loop stopped. queue=%s", get_queue_stats())
        print("Scheduler stopped cleanly.")


def main():
    global _claim_owner, _shutdown_requested

    _configure_logging()
    validate_environment()
    initialize_database()

    _shutdown_requested = False
    _claim_owner = build_claim_owner()
    install_signal_handlers()

    _logger.info(
        "Telegram scheduler starting. claim_owner=%s",
        _claim_owner,
    )

    with single_instance_lock(LOCK_FILE):
        released = release_stale_claims(STALE_CLAIM_SECONDS)
        if released:
            _logger.warning(
                "Moved %s stale claim(s) to failed on startup for manual "
                "review. Automatic republish is disabled because Telegram "
                "may already have accepted the copy.",
                released,
            )
        stats = get_queue_stats()
        _logger.info("Startup queue stats: %s", stats)
        run_scheduler_loop(claim_owner=_claim_owner)

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except SchedulerError as error:
        print(f"Scheduler lock/error: {error}")
        sys.exit(2)
    except (ValueError, ConfigurationError) as error:
        print(f"Configuration error: {error}")
        sys.exit(1)
