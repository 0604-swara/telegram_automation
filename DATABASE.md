# Database Architecture

This document describes the production SQLite queue used by the Telegram
automation system, additive schema migrations, backup verification, and
rollback / recovery procedures.

**Do not delete live database files or backups without an explicit operator
decision.**

## Files

| File | Purpose |
|------|---------|
| `telegram_queue.db` | Live production queue (path override via `DB_PATH`) |
| `telegram_queue.db-wal` / `-shm` | SQLite WAL sidecars (created after WAL mode) |
| `telegram_queue_pre_phase3_*.db` | Verified pre-migration snapshot |
| `telegram_queue_backup_*.db` | Verified backups created by `backup_database()` |
| `telegram_queue_backup.db` | Earlier manual backup (preserved) |
| `telegram.db` | Legacy CSV-era database (archived path; unused by production) |

## Environment

- `DB_PATH` — database file path (default: `telegram_queue.db`). The value is
  resolved to an absolute path for both startup validation and SQLite
  connections. Its parent directory must already exist and be accessible to
  the service account; startup fails clearly instead of falling back to
  another database.
- `STALE_CLAIM_SECONDS` — lease timeout for stuck `publishing` rows (default: `900`)

## Schema overview

### Table: `incoming_posts`

| Column | Type | Notes |
|--------|------|-------|
| `id` | INTEGER PK | Internal row id |
| `channel_id` | INTEGER NOT NULL | Source channel id |
| `message_id` | INTEGER NOT NULL | Telegram message id |
| `media_group_id` | TEXT | Album id when present |
| `message_type` | TEXT | `text`, `photo`, etc. |
| `message_data` | TEXT NOT NULL | Full Telegram message JSON |
| `received_at` | TIMESTAMP | Ingest time |
| `status` | TEXT | `pending`, `publishing`, `published`, `failed`, `uncertain` |
| `retry_count` | INTEGER | Failed publish attempts |
| `last_error` | TEXT | Last error message |
| `published_at` | TIMESTAMP | Last successful publish time |
| `claimed_at` | TIMESTAMP | **Phase 3** claim lease start |
| `claim_owner` | TEXT | **Phase 3** scheduler instance id |
| `destination_message_ids` | TEXT | **Phase 3** JSON list of destination message ids |
| `cycle_id` | INTEGER | **Phase 3** publish cycle that last claimed/published |
| `live_eligible` | INTEGER | `1` only for posts received after the persisted update-ID boundary |

**Unique constraint:** `(channel_id, message_id)` — prevents duplicate ingest.

### Table: `publish_history` (introduced in schema version 4)

Each successful scheduler publish transaction adds one immutable row for the
published unit (one post or a complete photo album). It records the cycle,
source row IDs, source Telegram message IDs, album ID when applicable,
destination Telegram message IDs, and publication timestamp. New cycle
publishes append records rather than overwriting prior history.

The table intentionally does not cascade-delete with queue rows. Backups
include and verify all history rows.

### Status meanings

- `pending` — available to publish only if `live_eligible=1`
- `publishing` — claimed by a scheduler instance (lease)
- `published` — successfully copied to destination in the current cycle
- `failed` — exceeded retry limit; **not** auto-reset on cycle restart (Option A)
- `uncertain` — Telegram accepted a copy but local finalization failed; never
  selected automatically, and the original claim plus returned destination IDs
  are retained for operator reconciliation
- `live_eligible=0` — retained historical row, never selected or cycle-reset

### Table: `bot_state`

Key/value operational state. Important keys:

| Key | Purpose |
|-----|---------|
| `update_offset` | Telegram `getUpdates` offset (must be preserved) |
| `cycle_post_number` | In-cycle Post # sequence |
| `publish_cycle_id` | Monotonic publishing cycle counter |
| `schema_version` | Current schema version (`6`) |
| `go_live_bootstrap_status` | `pending`, `capturing`, `webhook_conflict`, or `ready` |
| `go_live_first_update_id` | Exclusive first-eligible update ID; empty means no pending update was observed |
| `update_offset` | Next Telegram `getUpdates` offset, persisted transactionally |
| `schema_migrated_at` | UTC timestamp of last migration |
| `last_cycle_restart_at` | UTC timestamp of last cycle restart |

## Phase 3 schema modifications (additive only)

1. **`claimed_at`** — when a scheduler claimed the row for publish.
2. **`claim_owner`** — which process owns the claim (duplicate-scheduler protection).
3. **`destination_message_ids`** — destination channel message ids after Telegram accepts the copy (crash recovery evidence).
4. **`cycle_id`** — associates work with a publishing cycle.
5. **Indexes**
   - `idx_incoming_posts_queue` on `(status, retry_count, message_id DESC)`
   - `idx_incoming_posts_media_group` on `(media_group_id)`
   - `idx_incoming_posts_claim` on `(status, claimed_at)`
   - `idx_incoming_posts_cycle` on `(cycle_id)`
6. **WAL mode** — `PRAGMA journal_mode=WAL` for safer concurrent listener + scheduler access.
7. **`busy_timeout=30000`** — reduce `database is locked` failures under contention.
8. **State keys** — `schema_version`, `publish_cycle_id` (initialized if missing).

No columns, rows, or database files were deleted.

## Schema version 4: persistent publish history

The additive migration creates `publish_history` and its cycle index. Existing
queue records and offsets are retained. The migration cannot reconstruct old
publish cycles that were overwritten before version 4; full append-only
history starts with successful publishes after this migration.

## Schema version 5: new-post-only eligibility

The version 5 migration adds `live_eligible`. Existing queue records are
preserved but marked ineligible, so pending or previously published history
cannot enter a new publishing cycle.

## Schema version 6: update-ID go-live boundary

Before the listener starts, the worker persists `capturing`, makes one
bounded `getUpdates` request with offset `-1`, and stores the first eligible
update ID and listener offset. A negative offset returns at most the newest
pending update and discards older pending updates; this handles queues larger
than Telegram's normal 100-update response limit without an unbounded drain.
If the response is empty, the first update arriving after that response is
eligible. Message timestamps do not determine eligibility.

The listener only processes updates after bootstrap becomes `ready`. Each
eligible post insert and its next offset are committed in the same SQLite
transaction. Duplicate delivery is guarded by the unique channel/message key.
Restarts resume the persisted offset and never recapture a ready boundary.

Telegram and SQLite cannot share a transaction. A crash or network failure
while state is `capturing` is ambiguous and blocks startup; HTTP 409 webhook
conflicts are persisted separately and also block startup. After operator
review, recovery requires an explicit call to
`recover_go_live_bootstrap(boundary_update_id, operator_confirmed=True)`.
Passing `None` is only appropriate after an operator confirms an empty
boundary. The Bot API cannot guarantee exactly-once or strict no-loss
processing across this external boundary.

### Startup schema preflight

Startup permits a genuinely empty database, migrates the recognized version 3
application schema through version 6, migrates supported versions 3, 4, or 5,
and accepts an already-current version 6 schema. It rejects unversioned,
malformed, unrelated, or unsupported schemas before enabling WAL or applying
migrations. A rejected database is not silently treated as empty and must be
inspected and backed up before any operator-approved conversion.

`mark_as_published()` inserts the publish-history row in the same SQLite
transaction as the queue status update, so a failed finalization cannot create
a success-shaped history record.

## Publishing claim flow (API for Phase 5)

1. `release_stale_claims()` on scheduler startup
2. Select next eligible pending post(s)
3. `claim_posts(ids, owner)` → all-or-nothing; status becomes `publishing`
4. Call Telegram `copyMessage` / `copyMessages`
5. On success: `mark_as_published(ids, claim_owner, destination_message_ids=...)`
6. On failure: `mark_as_failed(..., claim_owner=...)` with strict ownership,
   or `release_claim(...)` only for clean pre-publish aborts (for example shutdown)

`claim_posts()` never commits a partial album claim. If any requested id is
not claimable, the transaction rolls back and `ClaimError` is raised.

`mark_as_published()` only finalizes rows with `status='publishing'` owned by
the provided `claim_owner`. Pending rows cannot be marked published.

When `mark_as_failed(..., claim_owner=...)` is used by the scheduler:

- every requested row must be `status='publishing'`
- every requested row must have that exact `claim_owner` (NULL is rejected)
- validation runs before any mutation
- album updates are all-or-nothing

**Delivery semantics:** Telegram Bot API does not provide idempotent copy keys.
The scheduler prefers **manual review over silent republish** whenever the
outcome is uncertain.

## Scheduler behavior (Phase 5)

Operational flow:

1. Acquire `scheduler.lock` (single-instance; Windows `msvcrt` / Linux `fcntl`)
2. `release_stale_claims()` on startup
3. Select newest eligible pending unit (single post or complete album)
4. `claim_posts(ids, claim_owner)` — all-or-nothing for albums
5. `copyMessage` / `copyMessages`
6. On success: `mark_as_published(ids, claim_owner, destination_message_ids=...)`
7. Wait five minutes before the next publish slot
8. When no eligible pending/publishing/uncertain work remains,
   `restart_posting_cycle()` resets published → pending (failed rows stay
   failed)

The scheduler checks its initial slot at startup and then makes at most one
publish attempt per 300-second slot. Empty slots are skipped; a post arriving
between slots waits for the next one.

### Stale-claim recovery

`release_stale_claims()` does **not** return rows to `pending`.

For claims older than `STALE_CLAIM_SECONDS` (default 900):

- `status` → `failed`
- `claimed_at` / `claim_owner` cleared
- `destination_message_ids` preserved
- `last_error` records that the publish outcome is uncertain and manual review
  is required

Automatic republish after a crash is intentionally disabled.

### Telegram retry policy

| Scenario | Scheduler behavior |
|----------|-------------------|
| HTTP 429 | Retry after Telegram `retry_after` while holding the claim |
| Confirmed pre-transmission failure (e.g. `ConnectTimeout`) | May retry while holding the claim |
| Read/total timeout | **Ambiguous** → entire unit `failed` for manual review |
| Connection loss after transmission may have started | **Ambiguous** → `failed` |
| HTTP 5xx / HTTP 408 | **Ambiguous** → `failed` |
| Unreadable response body | **Ambiguous** → `failed` |
| Partial album copy result | **Ambiguous** → entire album `failed` |
| Deterministic permanent Telegram rejection | Immediately `failed`; only explicit manual reset retries |
| Telegram success but local publish finalization fails | `uncertain`; claim retained, no automatic retry |
| Stale `publishing` claim after crash/restart | `failed` for manual review; never auto-republished |

Operators must inspect the destination channel and any stored
`destination_message_ids` before calling `reset_failed_posts()`.

## Cycle restart rules

When there are no eligible `pending`, `publishing`, or `uncertain` rows and at
least one eligible `published` row:

- Reset `published` → `pending`
- Clear claim fields (`claimed_at`, `claim_owner`)
- **Preserve** `destination_message_ids` as last-known publish evidence
- Increment `publish_cycle_id`
- Reset `cycle_post_number` to `0`
- Leave `failed` rows unchanged until manual `reset_failed_posts()`
- Do not restart while any eligible `uncertain` row awaits reconciliation

## Backup

```bash
python -c "from database import backup_database; print(backup_database('manual'))"
```

`database.py` also initializes/migrates the database; it is not a backup-only
command. Do not run it against production before the approved go-live.

Or, to back up and migrate:

```bash
python database.py
```

`backup_database()` uses SQLite's `Connection.backup()` API to create a
consistent online snapshot (including WAL content). It does **not** manually
copy `-wal` / `-shm` sidecars. After the snapshot is written it verifies:

- `PRAGMA integrity_check`
- row counts
- `update_offset`
- full `bot_state` key/value set
- full `publish_history` row set (when present)

The live database and all previous backups are preserved.

Backups stored beside the live DB are staging copies, not disaster recovery.
Before production, schedule verified backups to an encrypted off-host
destination in a separately secured failure domain. Verify each remote object
against a checksum, retain backups according to the recovery requirement, and
perform periodic restore drills into a new isolated path. Never restore over a
running database or reuse stale WAL/SHM sidecars.

## Apply / re-apply migration

Schema initialization is safe to run repeatedly and marks existing rows
ineligible during migration. The separate update-ID boundary is captured by
the process manager immediately before starting the listener; running
`database.py` alone does not complete go-live.

```bash
python database.py
```

## Verification checklist

After migration, confirm:

1. `PRAGMA integrity_check` → `ok`
2. `PRAGMA journal_mode` → `wal`
3. Phase 3 columns exist
4. Required queue and publish-history indexes exist
5. Existing offsets and records remain intact through schema migration; the
   explicit go-live bootstrap replaces the listener offset with the boundary
6. `bot_state.go_live_bootstrap_status` is `ready` after successful bootstrap
7. All existing `incoming_posts` rows still present
8. `schema_version` = `6`
9. `publish_history` table exists and is preserved by backup/restore
10. Pre-go-live queue rows have `live_eligible=0`

`verify_schema()` performs these checks programmatically.

## Rollback / recovery

The migration preserves rows, but rollback to an older scheduler is not safe:
older versions do not enforce `live_eligible` and could publish retained
historical rows. Use only a version that enforces the schema 6 new-post-only
policy after migration.

## Uncertain publish reconciliation

The Telegram Bot API does not provide an idempotency key for `copyMessage` or
`copyMessages`. If Telegram accepts a copy but SQLite cannot finalize it, the
queue row is moved to `uncertain`, its claim ownership and returned destination
message IDs are retained, and the scheduler will not retry it or restart the
publishing cycle while uncertain work remains.

Inspect each uncertain row and verify the destination channel before taking
action. For a confirmed copy, record every destination message ID (one per
source row) as published:

```python
from database import reconcile_uncertain_posts
reconcile_uncertain_posts(
    [incoming_post_id],
    "published",
    destination_message_ids=[destination_message_id],
)
```

For a complete album, provide all album row IDs and corresponding destination
message IDs in the same order. Only after confirming that Telegram accepted
no copy may an operator explicitly return the rows to the queue:

```python
from database import reconcile_uncertain_posts
reconcile_uncertain_posts([incoming_post_id], "not_published")
```

This manual resolution clears the retained claim. Uncertain rows are not
eligible for the generic `reset_failed_posts()` operation.

### Option A — Ignore new columns (soft rollback)

Keep the upgraded file. Older code that does not reference the new columns
continues to function. New indexes are harmless.

### Option B — Restore verified pre-migration backup (hard rollback)

1. Stop listener and scheduler processes.
2. Confirm the backup file name (example):
   `telegram_queue_pre_phase3_YYYYMMDD_HHMMSS.db`
3. Optionally keep the upgraded DB as
   `telegram_queue_failed_migration_<timestamp>.db` (do not delete).
4. Replace the live DB:

```bash
# PowerShell example — adjust names to your verified backup
Copy-Item -Path .\telegram_queue.db -Destination .\telegram_queue_upgraded_preserve.db
Copy-Item -Path .\telegram_queue_pre_phase3_XXXXXXXX_XXXXXX.db -Destination .\telegram_queue.db -Force
# Remove WAL sidecars so SQLite does not mix old WAL with restored DB
Remove-Item -ErrorAction SilentlyContinue .\telegram_queue.db-wal, .\telegram_queue.db-shm
```

5. Verify offset and row counts before restarting processes.
6. Restart listener, then scheduler.

### Option C — Manual failed-post recovery

Permanently failed posts are intentionally excluded from automatic cycles:

```python
from database import reset_failed_posts
reset_failed_posts([123, 456])  # specific ids
# or
reset_failed_posts()  # all failed posts after review
```

## Security notes

- Database files are gitignored (`*.db`)
- Do not commit `.env` or database copies containing production content to public repos
- Restrict filesystem permissions on the host so only the service account can read the DB

## Operations (Phase 6)

See `OPERATIONS.md` for startup, shutdown, health check, backup, and restore procedures.

Additional helpers:

- `record_heartbeat('listener'|'scheduler')` — liveness timestamps in `bot_state`
- `get_health_report()` — CLI/monitoring snapshot
- `restore_database_to_path(backup, target)` — restore into a **new** file only; refuses live `DB_PATH`
- `python healthcheck.py` — prints JSON health report
