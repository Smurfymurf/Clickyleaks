import os
import re
import json
import random
import requests
from datetime import datetime
from dotenv import load_dotenv
from supabase import create_client

# === Load environment variables ===
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# === Config ===
SUBREDDIT_FILE = "data/reddit_subreddits.txt"
CHUNK_DIR = "data/youtube8m_chunks"
CHUNK_SIZE = 1000
MAX_POSTS = 100

# ✅ Updated User-Agent with your Reddit username
HEADERS = {
    "User-Agent": "ClickyleaksBot/1.0 by chatbotbuzz"
}

YOUTUBE_PATTERNS = [
    r"https?://(?:www\.)?youtube\.com/watch\?v=([A-Za-z0-9_-]{11})",
    r"https?://(?:www\.)?youtu\.be/([A-Za-z0-9_-]{11})"
]

os.makedirs(CHUNK_DIR, exist_ok=True)

# === Load subreddit list ===
with open(SUBREDDIT_FILE, "r") as f:
    subreddits = [line.strip() for line in f if line.strip()]

subreddit = random.choice(subreddits)

def fetch_reddit_posts(subreddit):
    url = f"https://www.reddit.com/r/{subreddit}/new.json?limit={MAX_POSTS}"
    try:
        res = requests.get(url, headers=HEADERS, timeout=10)
        res.raise_for_status()
        data = res.json()
        return data.get("data", {}).get("children", [])
    except Exception as e:
        print(f"[Error] Fetch failed for /r/{subreddit}: {e}")
        return []

def extract_youtube_ids(text):
    ids = set()
    for pattern in YOUTUBE_PATTERNS:
        ids.update(re.findall(pattern, text))
    return ids

def bulk_check_ids(ids):
    if not ids:
        return set()
    checked = set()
    chunks = [list(ids)[i:i+100] for i in range(0, len(ids), 100)]
    for chunk in chunks:
        res = supabase.table("clickyleaks_checked").select("video_id").in_("video_id", chunk).execute()
        if res.data:
            checked.update(row["video_id"] for row in res.data)
    return checked

def main():
    posts = fetch_reddit_posts(subreddit)
    all_ids = set()

    for post in posts:
        data = post.get("data", {})
        content = f"{data.get('title', '')}\n{data.get('selftext', '')}\n{data.get('url', '')}"
        ids = extract_youtube_ids(content)
        all_ids.update(ids)

    print(f"[{subreddit}] Found {len(all_ids)} video IDs")

    checked = bulk_check_ids(all_ids)
    fresh = list(all_ids - checked)

    if not fresh:
        print("[Info] No new IDs.")
        return

    random.shuffle(fresh)
    chunks = [fresh[i:i+CHUNK_SIZE] for i in range(0, len(fresh), CHUNK_SIZE)]

    for i, chunk in enumerate(chunks):
        filename = f"reddit_chunk_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{i}.json"
        path = os.path.join(CHUNK_DIR, filename)
        with open(path, "w") as f:
            json.dump(chunk, f)
        print(f"[Saved] {len(chunk)} IDs → {path}")

if __name__ == "__main__":
    main()