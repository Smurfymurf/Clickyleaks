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
CHUNK_DIR = "data/youtube8m_chunks"
CHECKED_TABLE = "clickyleaks_checked"
MAX_IDS_PER_CHUNK = 10000


def get_reddit_token():
    print("[Auth] Getting Reddit token...")
    if not all([REDDIT_CLIENT_ID, REDDIT_SECRET, REDDIT_USERNAME, REDDIT_PASSWORD]):
        print("[Error] Missing Reddit credentials.")
        return None

    auth = requests.auth.HTTPBasicAuth(REDDIT_CLIENT_ID, REDDIT_SECRET)
    data = {
        "grant_type": "password",
        "username": REDDIT_USERNAME,
        "password": REDDIT_PASSWORD
    }
    headers = {"User-Agent": f"ClickyleaksBot/0.1 by {REDDIT_USERNAME}"}
    try:
        res = requests.post("https://www.reddit.com/api/v1/access_token",
                            auth=auth, data=data, headers=headers)
        res.raise_for_status()
        return res.json()["access_token"]
    except Exception as e:
        print(f"[Error] Reddit auth failed: {e}")
        return None


def load_subreddits():
    with open(SUBREDDIT_LIST_PATH, "r") as f:
        return [line.strip() for line in f if line.strip()]


def extract_youtube_ids(posts):
    ids = set()
    for post in posts:
        try:
            url = post["data"]["url"]
            if "youtube.com/watch?v=" in url or "youtu.be/" in url:
                video_id = (
                    url.split("watch?v=")[-1].split("&")[0]
                    if "youtube.com" in url else url.split("/")[-1].split("?")[0]
                )
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


def get_latest_chunk_number():
    os.makedirs(CHUNK_DIR, exist_ok=True)
    files = [f for f in os.listdir(CHUNK_DIR) if f.startswith("reddit_chunk_") and f.endswith(".json")]
    numbers = [int(f.split("_")[-1].split(".")[0]) for f in files if f.split("_")[-1].split(".")[0].isdigit()]
    return max(numbers) if numbers else 1


def load_chunk(chunk_number):
    path = os.path.join(CHUNK_DIR, f"reddit_chunk_{chunk_number}.json")
    if not os.path.exists(path):
        return []
    with open(path, "r") as f:
        try:
            return json.load(f)
        except json.JSONDecodeError:
            return []


def save_chunk(chunk_number, data):
    path = os.path.join(CHUNK_DIR, f"reddit_chunk_{chunk_number}.json")
    with open(path, "w") as f:
        json.dump(data, f, indent=2)


def save_ids_to_chunks(new_ids):
    if not new_ids:
        return

    chunk_num = get_latest_chunk_number()
    current_data = load_chunk(chunk_num)
    combined = current_data + new_ids

    while len(combined) > MAX_IDS_PER_CHUNK:
        to_save = combined[:MAX_IDS_PER_CHUNK]
        save_chunk(chunk_num, to_save)
        combined = combined[MAX_IDS_PER_CHUNK:]
        chunk_num += 1

    if combined:
        save_chunk(chunk_num, combined)

    print(f"[Save] Added {len(new_ids)} new IDs across chunks.")


def main():
    token = get_reddit_token()
    if not token:
        return

    subreddits = load_subreddits()
    headers = {
        "Authorization": f"bearer {token}",
        "User-Agent": f"ClickyleaksBot/0.1 by {REDDIT_USERNAME}"
    }

    total_new = []
    random.shuffle(subreddits)

    for subreddit in subreddits:
        if len(total_new) >= 100:
            break
        print(f"[Subreddit] Scanning /r/{subreddit}")
        url = f"https://oauth.reddit.com/r/{subreddit}/new.json?limit=100"
        try:
            res = requests.get(url, headers=headers, timeout=10)
            res.raise_for_status()
            posts = res.json()["data"]["children"]
        except Exception as e:
            print(f"[Error] Failed to fetch /r/{subreddit}: {e}")
            continue

        ids = extract_youtube_ids(posts)
        print(f"[{subreddit}] Found {len(ids)} video IDs")
        new_ids = filter_new_ids(ids)
        print(f"[Info] {len(new_ids)} new IDs after deduplication.")
        total_new.extend(new_ids)

    if total_new:
        save_ids_to_chunks(total_new)
    else:
        print("[Info] No new video IDs found.")


if __name__ == "__main__":
    main()