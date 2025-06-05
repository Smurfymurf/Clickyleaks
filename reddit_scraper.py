import os
import json
import random
import time
import requests
from dotenv import load_dotenv
from supabase import create_client

# === Load environment variables ===
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
REDDIT_CLIENT_ID = os.getenv("REDDIT_CLIENT_ID")
REDDIT_SECRET = os.getenv("REDDIT_SECRET")
REDDIT_USERNAME = os.getenv("REDDIT_USERNAME")
REDDIT_PASSWORD = os.getenv("REDDIT_PASSWORD")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

SUBREDDIT_LIST_PATH = "data/reddit_subreddits.txt"
CHUNK_FILE = "data/reddit_chunks/reddit_chunk.json"
CHECKED_TABLE = "clickyleaks_checked"
TARGET_NEW_IDS = 100

def get_reddit_token():
    print("[Auth] Getting Reddit token...")
    if not all([REDDIT_CLIENT_ID, REDDIT_SECRET, REDDIT_USERNAME, REDDIT_PASSWORD]):
        print("[Error] One or more Reddit credentials are missing.")
        return None
    auth = requests.auth.HTTPBasicAuth(REDDIT_CLIENT_ID, REDDIT_SECRET)
    data = {
        "grant_type": "password",
        "username": REDDIT_USERNAME,
        "password": REDDIT_PASSWORD,
    }
    headers = {"User-Agent": "ClickyleaksBot/0.1 by " + REDDIT_USERNAME}
    try:
        res = requests.post("https://www.reddit.com/api/v1/access_token",
                            auth=auth, data=data, headers=headers)
        res.raise_for_status()
        return res.json()["access_token"]
    except Exception as e:
        print(f"[Error] Reddit auth failed: {e}")
        return None

def get_subreddit_list():
    with open(SUBREDDIT_LIST_PATH, "r") as f:
        return [line.strip() for line in f if line.strip()]

def extract_youtube_ids(posts):
    ids = set()
    for post in posts:
        try:
            url = post["data"]["url"]
            if "youtube.com/watch?v=" in url or "youtu.be/" in url:
                if "youtube.com/watch?v=" in url:
                    video_id = url.split("watch?v=")[-1].split("&")[0]
                else:
                    video_id = url.split("/")[-1].split("?")[0]
                if len(video_id) >= 8:
                    ids.add(video_id)
        except:
            continue
    return list(ids)

def filter_new_ids(video_ids):
    if not video_ids:
        return []
    response = supabase.table(CHECKED_TABLE).select("video_id").in_("video_id", video_ids).execute()
    already = set(row["video_id"] for row in response.data) if response.data else set()
    return [vid for vid in video_ids if vid not in already]

def save_ids_to_chunk(new_ids):
    if not new_ids:
        return

    os.makedirs(os.path.dirname(CHUNK_FILE), exist_ok=True)

    if os.path.exists(CHUNK_FILE):
        with open(CHUNK_FILE, "r") as f:
            current_ids = json.load(f)
    else:
        current_ids = []

    combined = list(set(current_ids + new_ids))
    with open(CHUNK_FILE, "w") as f:
        json.dump(combined, f, indent=2)

    print(f"[Save] Added {len(new_ids)} new IDs to {CHUNK_FILE}")

def main():
    token = get_reddit_token()
    if not token:
        return

    headers = {
        "Authorization": f"bearer {token}",
        "User-Agent": "ClickyleaksBot/0.1 by " + REDDIT_USERNAME
    }

    all_new_ids = []
    subreddits = get_subreddit_list()
    random.shuffle(subreddits)

    for subreddit in subreddits:
        if len(all_new_ids) >= TARGET_NEW_IDS:
            break

        print(f"[Subreddit] Scanning /r/{subreddit}")
        url = f"https://oauth.reddit.com/r/{subreddit}/new.json?limit=100"

        try:
            res = requests.get(url, headers=headers, timeout=10)
            res.raise_for_status()
            posts = res.json()["data"]["children"]
        except Exception as e:
            print(f"[Error] Fetch failed for /r/{subreddit}: {e}")
            continue

        ids = extract_youtube_ids(posts)
        print(f"[{subreddit}] Found {len(ids)} video IDs")

        new_ids = filter_new_ids(ids)
        print(f"[Info] {len(new_ids)} new IDs after deduplication.")
        all_new_ids.extend(new_ids)

        time.sleep(2)  # polite delay

    if all_new_ids:
        final_ids = all_new_ids[:TARGET_NEW_IDS]
        save_ids_to_chunk(final_ids)
    else:
        print("[Done] No new video IDs found in this run.")

if __name__ == "__main__":
    main()