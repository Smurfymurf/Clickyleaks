
import os
import pandas as pd
import random
import requests
import re
from datetime import datetime
from supabase import create_client
from urllib.parse import urlparse

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# Constants
DATASETS = [
    "asaniczka/trending-youtube-videos-113-countries",
    "pyuser11/youtube-trending-videos-updated-daily",
    "canerkonuk/youtube-trending-videos-global",
    "sebastianbesinski/youtube-trending-videos-2025-updated-daily"
]
MAX_VIDEOS = 500
MAX_DOMAINS = 5
WELL_KNOWN_DOMAINS = {"youtube.com", "youtu.be", "facebook.com", "instagram.com", "twitter.com", "linkedin.com", 
                      "tiktok.com", "google.com", "apple.com", "microsoft.com", "amazon.com", "bing.com", "yahoo.com", 
                      "reddit.com", "wikipedia.org", "snapchat.com", "netflix.com", "paypal.com", "adobe.com", "pinterest.com"}

def get_existing_ids():
    result = supabase.table("Clickyleaks_Checked").select("video_id").execute()
    return {row["video_id"] for row in result.data} if result.data else set()

def mark_video_checked(video_id):
    supabase.table("Clickyleaks_Checked").insert({
        "video_id": video_id,
        "scanned_at": datetime.utcnow().isoformat()
    }).execute()

def soft_expired_check(domain):
    try:
        response = requests.head("http://" + domain, timeout=5, allow_redirects=True)
        return response.status_code >= 400
    except Exception:
        return True

def extract_domains(description):
    urls = re.findall(r'(https?://\S+)', description or "")
    domains = []
    for url in urls:
        try:
            hostname = urlparse(url).hostname or ""
            if hostname and hostname.lower() not in WELL_KNOWN_DOMAINS:
                domains.append((url, hostname.lower()))
        except Exception:
            continue
    return domains

def send_discord_alert(domain, video_id):
    message = {
        "content": f"**ClickLeak Found:** `{domain}`\nhttps://www.youtube.com/watch?v={video_id}"
    }
    try:
        requests.post(DISCORD_WEBHOOK_URL, json=message, timeout=5)
    except:
        pass

def main():
    os.makedirs("./data", exist_ok=True)
    dataset_slug = random.choice(DATASETS)
    print(f"ð¦ Downloading dataset: {dataset_slug}")
    os.system(f"kaggle datasets download -d {dataset_slug} -p ./data --unzip")

    csv_files = list(Path("./data").glob("*.csv"))
    if not csv_files:
        raise Exception("â No CSV found in downloaded dataset.")

    df = pd.read_csv(csv_files[0])
    df = df.dropna(subset=["video_id", "description"]).drop_duplicates(subset=["video_id"])
    df["publishedAt"] = pd.to_datetime(df["publishedAt"], errors="coerce")
    df = df.sort_values("publishedAt")

    checked_ids = get_existing_ids()
    promising = 0

    for _, row in df.iterrows():
        vid = row["video_id"]
        if vid in checked_ids:
            continue
        domains = extract_domains(row.get("description", ""))
        for full_url, domain in domains:
            if soft_expired_check(domain):
                supabase.table("Clickyleaks").insert({
                    "domain": domain,
                    "full_url": full_url,
                    "video_id": vid,
                    "scanned_at": datetime.utcnow().isoformat(),
                    "is_available": True,
                    "verified": False
                }).execute()
                send_discord_alert(domain, vid)
                promising += 1
                break
        mark_video_checked(vid)
        if promising >= MAX_DOMAINS:
            break

if __name__ == "__main__":
    main()
