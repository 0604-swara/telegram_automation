import sqlite3

DB_NAME = "telegram.db"


def initialize_database():

    conn = sqlite3.connect(DB_NAME)

    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS posts (
            id INTEGER PRIMARY KEY,
            message TEXT NOT NULL,
            status TEXT DEFAULT 'pending',
            published_at TIMESTAMP
        )
    """)

    conn.commit()
    conn.close()


def add_posts(posts):

    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    for _, row in posts.iterrows():

        cursor.execute("""
            INSERT OR IGNORE INTO posts (id, message)
            VALUES (?, ?)
        """, (int(row["id"]), row["message"]))

    conn.commit()
    conn.close()


def get_next_post():

    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT id, message
        FROM posts
        WHERE status = 'pending'
        ORDER BY id
        LIMIT 1
    """)

    post = cursor.fetchone()

    conn.close()

    return post


def mark_as_published(post_id):

    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    cursor.execute("""
        UPDATE posts
        SET status = 'published',
            published_at = CURRENT_TIMESTAMP
        WHERE id = ?
    """, (post_id,))

    conn.commit()
    conn.close()