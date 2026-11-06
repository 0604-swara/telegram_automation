"""Create a verified SQLite online backup without overwriting prior backups."""

from __future__ import annotations

import argparse
import os
import sqlite3
import sys
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path

from database import DB_NAME

BACKUP_PREFIX = "telegram_queue_backup_"


def create_backup(database_path, backup_dir, retention_days=30, timestamp=None):
    if retention_days < 1:
        raise ValueError("retention_days must be at least 1")

    source_path = Path(database_path).expanduser().resolve()
    if not source_path.is_file():
        raise FileNotFoundError(f"Database file not found: {source_path}")

    destination_dir = Path(backup_dir).expanduser().resolve()
    if destination_dir == source_path.parent:
        raise ValueError(
            "Backup directory must be separate from the database directory."
        )
    destination_dir.mkdir(parents=True, exist_ok=True, mode=0o700)

    stamp = timestamp or datetime.now(timezone.utc).strftime(
        "%Y%m%dT%H%M%S_%fZ"
    )
    backup_path = destination_dir / f"{BACKUP_PREFIX}{stamp}.db"

    descriptor = os.open(
        backup_path,
        os.O_CREAT | os.O_EXCL | os.O_WRONLY,
        0o600,
    )
    os.close(descriptor)

    try:
        source_uri = f"{source_path.as_uri()}?mode=ro"
        with closing(
            sqlite3.connect(source_uri, uri=True, timeout=30)
        ) as source:
            with closing(
                sqlite3.connect(backup_path, timeout=30)
            ) as destination:
                source.backup(destination)

        with closing(
            sqlite3.connect(backup_path, timeout=30)
        ) as verification:
            integrity = verification.execute(
                "PRAGMA integrity_check"
            ).fetchone()[0]
            if integrity != "ok":
                raise RuntimeError(
                    f"Backup integrity check failed: {integrity}"
                )

        cutoff = datetime.now(timezone.utc).timestamp() - (
            timedelta(days=retention_days).total_seconds()
        )
        for candidate in destination_dir.iterdir():
            if (
                candidate.is_file()
                and candidate.name.startswith(BACKUP_PREFIX)
                and candidate.name.endswith(".db")
                and candidate != backup_path
                and candidate.stat().st_mtime < cutoff
            ):
                candidate.unlink()

        return {
            "backup_path": str(backup_path),
            "integrity": integrity,
        }
    except Exception:
        for suffix in ("", "-wal", "-shm"):
            partial = Path(f"{backup_path}{suffix}")
            if partial.exists():
                partial.unlink()
        raise


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--database",
        default=DB_NAME,
        help="SQLite database to back up (default: configured DB_PATH)",
    )
    parser.add_argument(
        "--directory",
        default=os.getenv("BACKUP_DIR"),
        help="Separate backup directory (or set BACKUP_DIR)",
    )
    parser.add_argument(
        "--retention-days",
        type=int,
        default=os.getenv("BACKUP_RETENTION_DAYS", "30"),
        help="Keep timestamped backups newer than this many days",
    )
    args = parser.parse_args(argv)

    if not args.directory:
        parser.error("--directory or BACKUP_DIR is required")

    try:
        result = create_backup(
            args.database,
            args.directory,
            retention_days=args.retention_days,
        )
    except (OSError, sqlite3.Error, RuntimeError, ValueError) as exc:
        print(f"Backup failed: {exc}", file=sys.stderr)
        return 1

    print(f"Verified backup created: {result['backup_path']}")
    print(f"Integrity: {result['integrity']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
