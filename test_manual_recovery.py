import os
import shutil
import sqlite3
from pathlib import Path

# Use a separate database copy
source_db = Path("telegram_test_recovery.db")
test_db = Path("telegram_test_manual_reset.db")

if not source_db.exists():
    raise FileNotFoundError(source_db)

if test_db.exists():
    test_db.unlink()

shutil.copy2(source_db, test_db)

# Set DB_PATH before importing database.py
os.environ["DB_PATH"] = str(test_db.resolve())

import database

# Find a failed post
conn = sqlite3.connect(test_db)

row = conn.execute("""
    SELECT id, status, destination_message_ids
    FROM incoming_posts
    WHERE status = 'failed'
    LIMIT 1
""").fetchone()

conn.close()

if row is None:
    raise RuntimeError("No failed post found in recovery database.")

post_id = row[0]

print("Before recovery:", row)

# Reset only this post
reset_count = database.reset_failed_posts([post_id])

conn = sqlite3.connect(test_db)

updated_row = conn.execute("""
    SELECT id, status, retry_count, last_error,
           destination_message_ids
    FROM incoming_posts
    WHERE id = ?
""", (post_id,)).fetchone()

conn.close()

print("Reset count:", reset_count)
print("After recovery:", updated_row)

assert reset_count == 1
assert updated_row[1] == "pending"
assert updated_row[2] == 0
assert updated_row[3] is None

print("\nMANUAL_RECOVERY_TEST_PASSED")

# Test empty-list safety
conn = sqlite3.connect(test_db)

conn.execute("""
    UPDATE incoming_posts
    SET status = 'failed',
        last_error = 'Empty-list safety test'
    WHERE id = ?
""", (post_id,))

conn.commit()
conn.close()

empty_reset_count = database.reset_failed_posts([])

conn = sqlite3.connect(test_db)

status = conn.execute("""
    SELECT status
    FROM incoming_posts
    WHERE id = ?
""", (post_id,)).fetchone()[0]

conn.close()

print("Empty-list reset count:", empty_reset_count)
print("Post status after empty-list call:", status)

assert empty_reset_count == 0
assert status == "failed"

print("\nEMPTY_LIST_SAFETY_TEST_PASSED")