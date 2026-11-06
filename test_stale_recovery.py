import os
import sqlite3
import shutil
from pathlib import Path

ROOT = Path(__file__).resolve().parent

BACKUP = ROOT / "telegram_test_before_recovery.db"
TEST_DB = ROOT / "telegram_test_recovery.db"

if not BACKUP.exists():
    raise SystemExit("Backup database not found.")

if TEST_DB.exists():
    raise SystemExit(
        "Recovery test database already exists. "
        "Inspect it before proceeding."
    )

# Create an isolated database copy.
shutil.copy2(BACKUP, TEST_DB)

# Ensure database.py uses the isolated copy.
os.environ["DB_PATH"] = str(TEST_DB)
os.environ["TELEGRAM_TEST_MODE"] = "0"

import database

# Simulate an abandoned claim on one test record.
conn = sqlite3.connect(TEST_DB)

try:
    conn.execute("BEGIN IMMEDIATE")

    row = conn.execute(
        """
        SELECT id, destination_message_ids
        FROM incoming_posts
        WHERE status = 'published'
        ORDER BY id
        LIMIT 1
        """
    ).fetchone()

    if not row:
        raise RuntimeError("No published record available for testing.")

    post_id, original_destination_ids = row

    conn.execute(
        """
        UPDATE incoming_posts
        SET status = 'publishing',
            claim_owner = ?,
            claimed_at = datetime('now', '-1 hour')
        WHERE id = ?
        """,
        ("simulated_crash_owner", post_id),
    )

    conn.commit()

finally:
    conn.close()

print("Simulated interrupted claim:", post_id)

# Run stale-claim recovery.
released = database.release_stale_claims(stale_seconds=900)

print("Stale claims recovered:", released)

# Verify the result.
conn = sqlite3.connect(TEST_DB)

result = conn.execute(
    """
    SELECT status, claim_owner, claimed_at, destination_message_ids
    FROM incoming_posts
    WHERE id = ?
    """,
    (post_id,),
).fetchone()

conn.close()

print("Recovered record:", result)

assert released == 1
assert result[0] == "failed"
assert result[1] is None
assert result[2] is None
assert result[3] == original_destination_ids

print("\nSTALE_CLAIM_RECOVERY_TEST_PASSED")