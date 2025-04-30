from datetime import datetime
from pathlib import Path
import os
import random
import re
import zipfile
import pandas as pd
import requests
from supabase import create_client, Client
from urllib.parse import urlparse
from dotenv import load_dotenv

# Load .env if running locally
load_dotenv()

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL")
KAGGLE_USERNAME = os.environ.get("KAGGLE_USERNAME")
KAGGLE_KEY = os.environ.get("KAGGLE_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

KAGGLE_SOURCES = [
    {
        "owner_slug": "asaniczka",
        "dataset_slug": "trending-youtube-videos-113-countries",
        "file_path": "data/yt_trending.csv"
    },
    {
        "owner_slug": "pyuser11",
        "dataset_slug": "youtube-trending-videos-updated-daily",
        "file_path": "yt_trending_videos.csv"
    },
    {
        "owner_slug": "canerkonuk",
        "dataset_slug": "youtube-trending-videos-global",
        "file_path": "US_youtube_trending_data.csv"
    },
    {
        "owner_slug": "sebastianbesinski",
        "dataset_slug": "youtube-trending-videos-2025-updated-daily",
        "file_path": "yt_trending.csv"
    }
]

WELL_KNOWN_DOMAINS = {
    "youtube.com", "youtu.be", "facebook.com", "twitter.com", "instagram.com",
    "linkedin.com", "tiktok.com", "reddit.com", "discord.com", "google.com",
    "apple.com", "microsoft.com", "amazon.com", "netflix.com", "paypal.com",
    "github.com", "bitly.com", "tinyurl.com", "snapchat.com", "spotify.com"
}

def notify_discord(message: str):
    if DISCORD_WEBHOOK_URL:
        requests.post(DISCORD_WEBHOOK_URL, json={"content": message})

def soft_check_expired(domain: str) -> bool:
    try:
        resp = requests.head("http://" + domain, timeout=5)
        return resp.status_code in [404, 410]
    except Exception:
        return True  # Treat as potentially expired if unreachable

def extract_urls(description: str):
    return re.findall(r'(https?://[^\s]+)', str(description))

def extract_domain(url: str) -> str:
    parsed = urlparse(url)
    return parsed.netloc.lower().replace("www.", "")

def main():
    source = random.choice(KAGGLE_SOURCES)
    dataset_ref = f"{source['owner_slug']}/{source['dataset_slug']}"
    file_path = source["file_path"]

    print(f"📦 Downloading dataset: {dataset_ref}")
    notify_discord(f"Clickyleaks scanning `{dataset_ref}`...")

    os.environ["KAGGLE_USERNAME"] = KAGGLE_USERNAME
    os.environ["KAGGLE_KEY"] = KAGGLE_KEY
    os.system(f"kaggle datasets download -d {dataset_ref} -p data --unzip")

    df = pd.read_csv(f"data/{file_path}")
    df = df.dropna(subset=["video_id", "description"]).drop_duplicates("video_id")
    df["video_id"] = df["video_id"].astype(str)

    existing_ids_resp = supabase.table("clickyleaks_checked").select("video_id").execute()
    already_checked = {item['video_id'] for item in existing_ids_resp.data}

    new_domains = []
    scanned = 0

    for _, row in df.sort_values("publishedAt" if "publishedAt" in df.columns else df.columns[0]).iterrows():
        video_id = row["video_id"]
        if video_id in already_checked:
            continue

        urls = extract_urls(row.get("description", ""))
        urls = [url for url in urls if extract_domain(url) not in WELL_KNOWN_DOMAINS]

        for url in urls:
            domain = extract_domain(url)
            if not domain:
                continue
            if soft_check_expired(domain):
                print(f"Found candidate: {domain}")
                new_domains.append({
                    "domain": domain,
                    "source": "YouTube",
                    "video_id": video_id,
                    "available": True,
                    "verified": False,
                    "discovered_at": datetime.utcnow().isoformat()
                })
                if len(new_domains) >= 5:
                    break

        supabase.table("clickyleaks_checked").insert({
            "video_id": video_id,
            "scanned_at": datetime.utcnow().isoformat()
        }).execute()

        scanned += 1
        if len(new_domains) >= 5 or scanned >= 500:
            break

    if new_domains:
        supabase.table("clickyleaks").insert(new_domains).execute()
        notify_discord(f"Clickyleaks discovered `{len(new_domains)}` potential domains!")
    else:
        notify_discord("Clickyleaks scan complete — no new domains found.")

if __name__ == "__main__":
    main()