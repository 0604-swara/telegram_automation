# Operations Guide

Production-oriented runbook for the Telegram content automation system.

For the prepared Linux VPS and systemd setup, see
[DEPLOYMENT_LINUX.md](./DEPLOYMENT_LINUX.md).

## Install

```powershell
cd C:\Users\asus\OneDrive\Desktop\telegram_automation
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
copy .env.example .env
```

Edit `.env` with the real bot token and channel IDs. Never commit `.env`.

## Required environment variables

| Variable | Used by | Notes |
|----------|---------|-------|
| `TELEGRAM_BOT_TOKEN` | listener, scheduler | Never logged |
| `SOURCE_CHANNEL_ID` | listener, scheduler | Numeric chat id |
| `DESTINATION_CHANNEL_ID` | scheduler | Numeric chat id |
| `DB_PATH` | all | Default `telegram_queue.db`; resolved to an absolute path |

Optional: `PUBLISH_INTERVAL_SECONDS` (default 300), `STALE_CLAIM_SECONDS` (900), `SCHEDULER_LOCK_FILE`, `LISTENER_LOCK_FILE`. `LOG_TO_STDOUT=1` sends application logger messages to stdout (for journald); the default remains rotating local files.

## New posts and publishing schedule

Only posts received after the persisted go-live cutoff are eligible. The
client must upload to the configured private source channel. The bot must be
an administrator allowed to receive channel posts there, and must have
permission to publish/copy messages to the configured destination channel.
Text, single photos with captions, and photo albums are supported; album
members remain grouped.

Historical channel posts are not fetched or imported. Pending Bot API updates
with message dates at or before the go-live cutoff are skipped, and existing
database rows are retained but ineligible for publishing. The cutoff persists
through restarts. Telegram message dates have one-second resolution; posts in
the same timestamp second as the cutoff are conservatively excluded.

The scheduler checks an initial slot at startup and then attempts at most one
publish unit every five minutes. Empty slots are skipped; new arrivals wait
for the next slot and take priority according to newest-first ordering.

## Start

Run in **two separate terminals**:

```powershell
python telegram_handler.py
python scheduler.py
```

Only one scheduler instance can run (`scheduler.lock`). The listener also takes a non-blocking single-instance lock (`listener.lock`).

## Stop

Press `Ctrl+C` in each terminal. Both processes handle SIGINT/SIGTERM, close DB connections, and release the scheduler lock.

## Logs

| File | Process |
|------|---------|
| `listener.log` | Source-channel listener |
| `scheduler.log` | Publisher |

Local logs rotate at ~5 MB (5 backups). With `LOG_TO_STDOUT=1`, application logger output is sent to stdout for collection by a service manager such as journald. Tokens and token-bearing Bot API URLs are redacted.

## Health check

```powershell
python healthcheck.py
```

Read-only: does **not** create or migrate the database. Exit code `0` only when database, listener, and scheduler are all healthy (`ok`). Missing/unknown/stale heartbeats yield a non-zero exit code. JSON includes counts and heartbeat details.

## Queue statistics

```powershell
python -c "from database import get_queue_stats, get_health_report; print(get_queue_stats()); print(get_health_report())"
```

## Backup

```powershell
python -c "from database import backup_database; print(backup_database('manual'))"
```

Uses SQLite `Connection.backup()` (WAL-safe). Creates a new timestamped file and refuses to overwrite an existing backup name.

For automated backup creation into a separate directory with integrity checking and configurable retention, see `backup_database.py` in [DEPLOYMENT_LINUX.md](./DEPLOYMENT_LINUX.md).

## Restore (safe)

Restore **only** into a separate test/recovery file. The API refuses to restore over the live `DB_PATH`.

```powershell
python -c "from database import restore_database_to_path; print(restore_database_to_path('telegram_queue_backup_manual_YYYYMMDD_HHMMSS.db', 'restore_test.db'))"
```

Never point the running app at a restored file until you have verified integrity and offsets offline.

## Failed posts and ambiguous outcomes

Permanent Telegram API rejections (for example HTTP 400) immediately become
`failed` and are not automatically retried. Rate limits and confirmed
pre-transmission connection failures retain their bounded retry behavior.
Timeouts, mid-flight connection loss, server errors, and local finalization
failures after a successful Telegram response are ambiguous and must never be
blindly retried.

The Telegram Bot API has no idempotency key for copy operations. When Telegram
accepts a post but the local database cannot record the success, the row is
quarantined as `uncertain`; claim ownership and returned destination message
IDs are preserved. The scheduler does not republish uncertain rows and pauses
cycle restart until an operator reconciles them.

Inspect:

```powershell
python -c "from database import get_connection;
with get_connection() as c:
  rows=c.execute(\"SELECT id,message_id,status,last_error,destination_message_ids FROM incoming_posts WHERE status='failed'\").fetchall()
  print([dict(r) for r in rows])"
```

Before reset, check the destination channel for possible duplicates (`destination_message_ids` / channel history).

Reset specific ids only after review:

```powershell
python -c "from database import reset_failed_posts; print(reset_failed_posts([123]))"
```

For `uncertain` rows, first inspect the destination channel. If the copy is
present, mark it published and record its destination message ID:

```powershell
python -c "from database import reconcile_uncertain_posts; print(reconcile_uncertain_posts([123], 'published', destination_message_ids=[456]))"
```

If and only if the copy is confirmed absent, explicitly allow a retry:

```powershell
python -c "from database import reconcile_uncertain_posts; print(reconcile_uncertain_posts([123], 'not_published'))"
```

For albums, reconcile all uncertain member IDs together, with destination
message IDs in matching order. Do not retry an uncertain post based solely on
the absence of a local success record.

## Safe restart

1. Stop scheduler (`Ctrl+C`), then listener.
2. Optionally run `python healthcheck.py` and create a backup.
3. Start listener, then scheduler.
4. On scheduler startup, stale `publishing` claims older than the lease window are moved to `failed` for manual review (not auto-republished).

## Regression tests (isolated; no live Telegram)

```powershell
python -m py_compile config.py database.py telegram_handler.py scheduler.py healthcheck.py test_phase5_scheduler.py test_phase6.py
python test_phase5_scheduler.py
python test_phase6.py
```
