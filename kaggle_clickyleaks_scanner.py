import os
import re
import zipfile
import random
import requests
import pandas as pd
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
from supabase import create_client, Client

# Load environment variables
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Constants
MAX_VIDEOS = 500
MAX_AVAILABLE = 5

# Verified CSV paths based on actual dataset content
DATASETS = [
    {
        "url": "asaniczka/trending-youtube-videos-113-countries",
        "path": "Trending_Youtube_Videos.csv"
    },
    {
        "url": "pyuser11/youtube-trending-videos-updated-daily",
        "path": "TrendingData.csv"  # Corrected path
    },
    {
        "url": "canerkonuk/youtube-trending-videos-global",
        "path": "Global_Youtube_Trending.csv"
    },
    {
        "url": "sebastianbesinski/youtube-trending-videos-2025-updated-daily",
        "path": "trending_yt2025.csv"
    }
]

WELL_KNOWN_DOMAINS = [
    "youtube.com", "youtu.be", "google.com", "facebook.com", "twitter.com", "instagram.com",
    "linkedin.com", "tiktok.com", "amazon.com", "reddit.com", "wikipedia.org", "apple.com",
    "microsoft.com", "paypal.com", "netflix.com", "spotify.com", "discord.com"
]

def send_discord_alert(message: str):
    try:
        requests.post(DISCORD_WEBHOOK_URL, json={"content": message})
    except Exception as e:
        print(f"Failed to send Discord alert: {e}")

def is_potentially_expired(domain: str):
    try:
        response = requests.head(f"http://{domain}", timeout=5)
        return response.status_code in [404, 410]
    except requests.RequestException:
        return True

def extract_domains(description: str):
    if not isinstance(description, str):
        return []
    pattern = r'https?://([A-Za-z0-9.-]+)'
    domains = re.findall(pattern, description)
    return [d for d in domains if all(w not in d for w in WELL_KNOWN_DOMAINS)]

def download_dataset():
    import kaggle
    dataset = random.choice(DATASETS)
    print(f"📦 Downloading dataset: {dataset['url']}")
    kaggle.api.dataset_download_files(dataset['url'], path="data", unzip=True)
    return dataset["path"]

def load_csv(filepath):
    return pd.read_csv(f"data/{filepath}")

def main():
    csv_file = download_dataset()
    df = load_csv(csv_file)
    df = df.sort_values(by="publishedAt" if "publishedAt" in df.columns else "published_at", ascending=True)
    checked_ids = {
        row["video_id"] for row in
        supabase.table("Clickyleaks_Checked").select("video_id").execute().data
    }

    available_domains = 0
    scanned = 0
    for _, row in df.iterrows():
        if scanned >= MAX_VIDEOS or available_domains >= MAX_AVAILABLE:
            break

        video_id = row.get("video_id") or row.get("videoId")
        if not video_id or video_id in checked_ids:
            continue

        description = row.get("description") or ""
        domains = extract_domains(description)
        scanned += 1

        supabase.table("Clickyleaks_Checked").insert({
            "video_id": video_id,
            "scanned_at": datetime.utcnow().isoformat()
        }).execute()

        for domain in domains:
            if is_potentially_expired(domain):
                supabase.table("Clickyleaks").insert({
                    "video_id": video_id,
                    "domain": domain,
                    "available": True,
                    "verified": False,
                    "source": "kaggle",
                    "added_at": datetime.utcnow().isoformat()
                }).execute()
                print(f"✅ Found available domain: {domain} in video {video_id}")
                available_domains += 1
                send_discord_alert(f"🔥 Found potentially expired domain: `{domain}` from video `{video_id}`")
                break

    print(f"✅ Done. Videos scanned: {scanned}, Available domains found: {available_domains}")

if __name__ == "__main__":
    main()