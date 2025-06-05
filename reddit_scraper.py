import os
import json
import random
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

def get_reddit_token():
    print("[Auth] Getting Reddit token...")
    auth = requests.auth.HTTPBasicAuth(REDDIT_CLIENT_ID, REDDIT_SECRET)
    data = {
        "grant_type": "password",
        "username": REDDIT_USERNAME,
        "password": REDDIT_PASSWORD,
    }
    headers = {"User-Agent": f"ClickyleaksBot/0.1 by {REDDIT_USERNAME}"}
    try:
        res = requests.post("https://www.reddit.com/api/v1/access_token",
                            auth=auth, data=data, headers=headers)
        res.raise_for_status()
        token = res.json()["access_token"]
        return token
    except Exception as e:
        print(f"[Error] Reddit auth failed: {e}")
        return None

def get_random_subreddit():
    with open(SUBREDDIT_LIST_PATH, "r") as f:
        subs = [line.strip() for line in f if line.strip()]
    return random.choice(subs)

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

def mark_as_checked(video_ids):
    if not video_ids:
        return
    rows = [{"video_id": vid} for vid in video_ids]
    supabase.table(CHECKED_TABLE).upsert(rows, on_conflict=["video_id"]).execute()

def main():
    token = get_reddit_token()
    if not token:
        return

    subreddit = get_random_subreddit()
    print(f"[Subreddit] Scanning /r/{subreddit}")

    headers = {
        "Authorization": f"bearer {token}",
        "User-Agent": f"ClickyleaksBot/0.1 by {REDDIT_USERNAME}"
    }

    url = f"https://oauth.reddit.com/r/{subreddit}/new.json?limit=100"
    try:
        res = requests.get(url, headers=headers, timeout=10)
        res.raise_for_status()
        posts = res.json()["data"]["children"]
    except Exception as e:
        print(f"[Error] Fetch failed for /r/{subreddit}: {e}")
        return

    ids = extract_youtube_ids(posts)
    print(f"[{subreddit}] Found {len(ids)} video IDs")

    new_ids = filter_new_ids(ids)
    print(f"[Info] {len(new_ids)} new IDs after deduplication.")

    save_ids_to_chunk(new_ids)
    mark_as_checked(new_ids)

if __name__ == "__main__":
    main()