# Render Deployment Guide

This project uses **one Render Background Worker** with **one persistent
disk**. `process_manager.py` initializes the persistent SQLite DB, then runs
one listener and one scheduler child process. Both receive the same absolute
`DB_PATH`. Do not create two Render services for these processes.

`render.yaml` describes the service, disk, commands, and non-secret settings.
No Render account was accessed and no deployment was triggered while preparing
this guide.

## 1. Render account and repository

1. Confirm the GitHub repository contains the reviewed production code,
   tests, `requirements.txt`, `render.yaml`, and this guide.
2. Do not include `.env`, `.env.test`, Telegram tokens, local database files,
   backups, logs, or lock files in a deployment commit.
3. Sign in to Render and connect the GitHub account/repository when ready.
   This project has `autoDeploy: false` so later commits should not
   automatically start new deploys; verify the dashboard setting.
4. Choose **New > Blueprint**, select the intended repository and branch,
   and review the Blueprint diff before creating or deploying any resource.
   Do not click the final provisioning/deploy action until separately
   approved.

## 2. Background Worker and disk

The root `render.yaml` defines exactly one worker:

- Build command: `pip install -r requirements.txt`
- Start command: `python process_manager.py`
- One instance, one `starter` plan worker, and a 1 GB persistent disk
- Disk mount: `/var/data`
- No inbound web service or HTTP health endpoint

Persistent disks are only available to paid Render services. Confirm the
current plan, region, minimum disk size, and monthly charge in the dashboard
before provisioning. The blueprint's `starter` plan and 1 GB size are initial
settings, not a quoted price or an instruction to purchase.

All durable DB and lock files are under `/var/data`. The launcher verifies
that `/var/data` is an actual mount point before starting processes; it fails
closed rather than silently writing SQLite into the ephemeral container
filesystem. One persistent disk and one instance are intentional: SQLite WAL
must remain local to the same worker filesystem. Do not scale replicas or
split the listener and scheduler into separate Render services.

Render persistent-disk deployments have downtime during deploys; a
zero-downtime replacement cannot safely mount the same SQLite volume in
parallel. Treat deploys/restarts as brief listener interruptions. The durable
Telegram update offset supports listener recovery.

## 3. Environment variables

Set/check these values in the Render service's Environment page. The
`sync: false` values in `render.yaml` must be entered as protected variables
in the dashboard; do not write real values into YAML or Git.

| Variable | Production value / purpose |
|---|---|
| `TELEGRAM_BOT_TOKEN` | Protected secret; never print it |
| `SOURCE_CHANNEL_ID` | Authorized private source channel |
| `DESTINATION_CHANNEL_ID` | Authorized publishing channel; not a test channel |
| `TELEGRAM_TEST_MODE` | Must be `0`; launcher rejects any other value |
| `PERSISTENT_DISK_PATH` | `/var/data` |
| `DB_PATH` | `/var/data/telegram_queue.db` |
| `PUBLISH_INTERVAL_SECONDS` | `300`; launcher rejects other values |
| `EMPTY_QUEUE_CHECK_SECONDS` | `30` (maximum sleep chunk while waiting for the next fixed slot) |
| `ALBUM_WAIT_SECONDS` | `15` (current default; album grouping wait) |
| `SCHEDULER_LOCK_FILE` | `/var/data/scheduler.lock` |
| `LISTENER_LOCK_FILE` | `/var/data/listener.lock` |
| `PROCESS_MANAGER_LOCK_FILE` | `/var/data/process_manager.lock` |
| `LOG_TO_STDOUT` | `1`; cloud log capture |
| `STALE_CLAIM_SECONDS` | `900` unless deliberately reviewed |
| `BACKUP_DIR` | `/var/data/backups` for local online backup staging |
| `BACKUP_RETENTION_DAYS` | `30` for configured staging retention |
| `PYTHON_VERSION` | `3.11.18`; confirm this patched release is available to Render when creating the service |

`PUBLISH_TRANSIENT_ATTEMPTS`, `MAX_PUBLISH_RETRIES`, and
`PUBLISH_HTTP_TIMEOUT_SECONDS` have code defaults; only override them after
review. The listener and scheduler inherit the same worker environment.
Startup verifies that database and all three lock paths are inside the disk,
that the DB name is `telegram_queue.db`, and that configured test-channel IDs
are not used.

## 4. New posts after go-live

The production behavior is **new-post-only**. The client must upload posts to
the configured private source channel after the system goes live. The bot
must be an administrator of the source channel with permission to receive
channel posts, and must have permission to publish/copy messages in the
destination channel. Eligible content includes text posts, single photos
(including captions), and photo albums; album members are stored together.

Existing channel history is not imported. The Bot API does not provide a
method to enumerate arbitrary historical channel posts. Pending channel
updates created before the persisted go-live cutoff are ignored, and existing
database rows are retained but marked ineligible. Starting or restarting the
worker will not make old rows eligible. No backfill is planned.

The scheduler checks its first slot at startup and then runs at most one
publishing attempt per configured 300-second slot. If a slot has no eligible
post, later arrivals wait for the next slot. At each slot the newest eligible
single post or complete album is selected first; a newly received post
therefore takes priority at the next slot. The next eligible queue cycle
continues after all eligible posts have been published.

Do not upload `telegram_test.db`, local development DBs, or test backups to
the Render disk. Before starting the worker, the operator must confirm the
production disk is intentionally empty or contains the approved production
database. The v5 startup migration retains pre-existing rows and publish
history but marks every existing queue row ineligible. Take and verify an
approved backup before that migration if the database contains records that
must be preserved.

After the persistent disk contains the approved DB (or is intentionally
empty), the launcher:

1. Validates production mode and channel/configuration values.
2. Confirms `/var/data` is mounted and the DB/lock paths are on that disk.
3. Acquires the process-manager lock.
4. Rejects unrelated, unversioned, malformed, or unsupported SQLite schemas
   before changing journal mode or applying migrations. A genuinely empty
   database is initialized; supported version 3 or 4 schemas are migrated
   transactionally to version 5, and version 5 is verified before either
   child starts.
5. Starts exactly one listener and scheduler with the identical DB path.

Schema version 4 introduced persistent `publish_history`; version 5 adds the
new-post eligibility cutoff. Earlier successful publishes cannot be
reconstructed, but new successful publish units are appended transactionally
across cycles. Verify the approved DB state and intended channels before
allowing this worker to start.

There is deliberately no separate web health endpoint. `healthcheck.py` is a
read-only CLI and can be run manually with the same environment when a shell
is available; worker logs and child-process supervision provide the primary
service monitoring.

## 5. Deploy and startup verification

Before any first deployment, finish the local mocked regression tests and
review the Git change set. During eventual Render setup:

1. Review Blueprint variables and disk settings; save the token and channel
   IDs only in Render's protected environment controls.
2. Confirm one worker instance, one `/var/data` persistent disk, and the
   expected branch and `autoDeploy` behavior.
3. Review the final service deployment diff and approve it explicitly.
4. Inspect the Render build/deploy logs and verify the startup lines identify
   the worker, listener, and scheduler without revealing credentials.
5. Verify the listener and scheduler report the same `/var/data/telegram_queue.db`
   path, both locks are acquired, and heartbeats become current.
6. Confirm `PRAGMA journal_mode` is `wal`, schema version is 5, integrity is
   `ok`, `publish_history` exists, and all pre-existing rows have
   `live_eligible=0`.

This is a live Telegram integration. Local tests must use mocked requests.
Never use client channels for a test. A controlled test requires an
explicitly approved test bot and test channels. Run that controlled test in
a separately isolated local/staging environment with `TELEGRAM_TEST_MODE=1`,
the approved test-channel IDs, and a separate test database. The production
Render launcher intentionally refuses test mode and the configured
test-channel IDs, so do not repoint the production worker to test channels.
No live controlled test was performed during preparation.

## 6. Logging and monitoring

The listener, scheduler, and process manager log to stdout/stderr. Child log
lines are relayed with `[listener]` / `[scheduler]` labels and Telegram token
strings and token-bearing Bot API URLs are redacted. Review logs in Render's
service log explorer. No application log files are written to the container,
so application logs do not consume disk indefinitely; verify Render's
current log retention for the selected plan and export logs if longer
retention is required.

The process manager treats any child exit (including an unexpected zero exit)
as a worker failure, sends SIGTERM to its sibling, waits up to 75 seconds,
then force-terminates only if necessary. It exits nonzero so Render can
restart the worker. In the Render Dashboard, set the worker's restart policy
to **On Failure** and review the retry behavior/limit shown by the current UI.
This project intentionally does not put a guessed restart-policy property in
`render.yaml`; verify the dashboard policy after service creation. Alert on
repeated worker restarts and
manual-review failed posts; a process that has stopped cannot notify you
reliably by itself.

The CLI health check requires fresh listener and scheduler heartbeats and a
valid SQLite schema. Run it from Render Shell if available for the selected
service/plan, or use a separately secured monitoring workflow. Do not add a
web service solely to expose a health endpoint for this background worker.

## 7. Backup and restore

`backup_database.py` uses SQLite's online backup API, creates a unique
timestamped file, checks integrity, and applies `BACKUP_RETENTION_DAYS` only
to its own matching backup files. Example command:

```bash
python backup_database.py --directory /var/data/backups --retention-days 30
```

The staging backup directory is on the persistent disk but shares the same
failure domain as the live DB; it is **not** an off-host backup and does not
protect against disk/service/account loss. Before production launch, choose
an operator-controlled off-host destination (for example, an encrypted
object-storage bucket in a separately secured account) and implement a
scheduled transfer of each verified backup. Encrypt in transit and at rest,
keep encryption keys outside Render, grant only the required upload/read
permissions, enable versioning/retention where available, and alert on missed
uploads. Set retention to meet the client's recovery requirements. Render
disk snapshots are supplementary only; confirm their current availability,
retention, and restore semantics in the dashboard.

For each upload, verify the remote object exists and matches the local
backup's cryptographic checksum before applying local retention. On a
scheduled restore drill, download a selected backup to a new isolated path,
verify `PRAGMA integrity_check` is `ok`, run the supported schema verification,
and compare expected post counts, update offset, and `publish_history` rows
with the backup manifest. Keep the live DB and backup untouched during the
drill. The off-host destination, scheduler, encryption/key management,
alerting, and restore drill are not configured here and remain launch
blockers.

Never restore over a running DB or combine a backup with stale WAL/SHM
sidecars. Stop the worker, restore a verified backup to a new path, and
verify schema/integrity, counts, update offset, and publish history before
deliberately switching `DB_PATH`. Keep the prior database and backups until
health checks pass.

## 8. Restart and failure recovery

- **Child exits unexpectedly:** process manager stops its sibling and exits
  nonzero; Render should restart the worker according to the verified restart
  policy.
- **Repeated restarts:** inspect logs and service settings; do not increase
  retries blindly. Fix configuration, DB permissions, or runtime errors.
- **Worker restart/deploy:** persistent disk keeps the DB and lock files;
  OS locks are released when processes exit. Startup migrates safely and
  stale publishing claims are marked failed for manual review.
- **Ambiguous Telegram publish:** do not automatically reset/retry. Check the
  destination channel and stored evidence before manual recovery.
- **Disk snapshot restore:** follow provider warnings because restoring a
  snapshot loses later writes. Prefer a verified logical backup and a
  deliberate recovery review.

## Remaining manual steps

Render account/repository connection, Blueprint review/provisioning, plan
selection, protected variable entry, initial service deploy, Linux runtime
verification, controlled test-channel approval, and production publishing
approval all require an operator. None was performed by preparing these
files.
