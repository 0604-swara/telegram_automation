import os
import time
import logging
import requests
import pandas as pd

from dotenv import load_dotenv
from database import (
    initialize_database,
    add_posts,
    get_next_post,
    mark_as_published
)

# Configure logging
logging.basicConfig(
    filename="automation.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Load environment variables
load_dotenv()

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHANNEL_ID = os.getenv("CHANNEL_ID")

# Posting interval: 5 minutes
INTERVAL = 300


def send_message(message):

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"

    payload = {
        "chat_id": CHANNEL_ID,
        "text": message
    }

    response = requests.post(
        url,
        json=payload,
        timeout=30
    )

    result = response.json()

    if not response.ok or not result.get("ok"):
        raise RuntimeError(
            result.get("description", response.text)
        )

    return True


def main():

    if not BOT_TOKEN or not CHANNEL_ID:
        raise ValueError(
            "Missing TELEGRAM_BOT_TOKEN or CHANNEL_ID in .env"
        )

    initialize_database()

    posts = pd.read_csv("posts.csv")

    add_posts(posts)

    print("Telegram automation started!")
    logging.info("Automation started")

    while True:

        post = get_next_post()

        if post is None:
            print("All posts have been published!")
            logging.info("All posts completed")
            break

        post_id, message = post

        try:

            send_message(message)

            mark_as_published(post_id)

            print(f"Post {post_id} published successfully!")
            logging.info(f"Post {post_id} published successfully")

        except Exception as error:

            print(f"Post {post_id} failed: {error}")
            logging.error(f"Post {post_id} failed: {error}")

        print("Next post scheduled in 5 minutes.")

        time.sleep(INTERVAL)


if __name__ == "__main__":
    main()