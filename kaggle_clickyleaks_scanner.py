from pathlib import Path
import os
import zipfile
import random
import pandas as pd
import requests
import re
import time
from supabase import create_client
from urllib.parse import urlparse
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")
KAGGLE_USERNAME = os.getenv("KAGGLE_USERNAME")
KAGGLE_KEY = os.getenv("KAGGLE_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# Well-known domains to skip
WELL_KNOWN_DOMAINS = {
    "youtube.com", "youtu.be", "facebook.com", "twitter.com", "instagram.com",
    "linkedin.com", "google.com", "apple.com", "microsoft.com", "amazon.com",
    "paypal.com", "bit.ly", "discord.com", "reddit.com", "tiktok.com", "openai.com"
}

# Datasets and known CSV filenames
DATASETS = {
    "asaniczka/trending-youtube-videos-113-countries": ["trending_youtube_data.csv"],
    "pyuser11/youtube-trending-videos-updated-daily": ["yt_trending_videos.csv"],
    "canerkonuk/youtube-trending-videos-global": ["youtube_trending_data.csv"],
    "sebastianbesinski/youtube-trending-videos-2025-updated-daily": ["youtube_trending_data_2025.csv"]
}

def soft_domain_check(domain):
    try:
        res = requests.get("http://" + domain, timeout=5)
        return False if res.status_code < 500 else True
    except Exception:
        return True

def extract_links(description):
    return re.findall(r'(https?://[^\s)"]+)', description)

def clean_url(url):
    try:
        parsed = urlparse(url)
        host = parsed.netloc.replace("www.", "").strip()
        return host.lower()
    except:
        return None

def load_csv(filepath):
    return pd.read_csv(filepath)

def log_discord_message(domain, video_id):
    content = f"**Available Domain Found:** `{domain}`\nFrom Video: [Watch](https://youtube.com/watch?v={video_id})"
    requests.post(DISCORD_WEBHOOK_URL, json={"content": content})

def download_dataset(dataset_name):
    os.makedirs("data", exist_ok=True)
    print(f"📦 Downloading dataset: {dataset_name}")
    os.system(f'kaggle datasets download -d {dataset_name} -p data --unzip')
    time.sleep(1)

def main():
    dataset = random.choice(list(DATASETS.keys()))
    download_dataset(dataset)

    csv_files = [Path("data") / name for name in DATASETS[dataset]]
    if not csv_files:
        print("❌ No CSV files found.")
        return

    # Load previously scanned IDs
    checked_resp = supabase.table("Clickyleaks_Checked").select("video_id").execute()
    already_checked = {row["video_id"] for row in checked_resp.data} if checked_resp.data else set()

    found_count = 0
    for csv_path in csv_files:
        df = load_csv(csv_path)

        for _, row in df.iterrows():
            video_id = row.get("video_id") or row.get("videoId")
            description = row.get("description")

            if not video_id or video_id in already_checked or not isinstance(description, str):
                continue

            # Mark video as scanned
            supabase.table("Clickyleaks_Checked").insert({
                "video_id": video_id,
                "scanned_at": datetime.utcnow().isoformat()
            }).execute()
            already_checked.add(video_id)

            for link in extract_links(description):
                domain = clean_url(link)
                if not domain or domain in WELL_KNOWN_DOMAINS:
                    continue

                if soft_domain_check(domain):
                    supabase.table("Clickyleaks").insert({
                        "domain": domain,
                        "source": "kaggle",
                        "video_id": video_id,
                        "available": True,
                        "verified": False,
                        "created_at": datetime.utcnow().isoformat()
                    }).execute()
                    log_discord_message(domain, video_id)
                    found_count += 1
                    if found_count >= 5:
                        return

if __name__ == "__main__":
    main()