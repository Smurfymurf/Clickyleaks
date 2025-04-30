import os
import random
import re
import requests
import pandas as pd
from datetime import datetime
from urllib.parse import urlparse
from pathlib import Path
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

KAGGLE_DATASETS = [
    "asaniczka/trending-youtube-videos-113-countries",
    "pyuser11/youtube-trending-videos-updated-daily",
    "canerkonuk/youtube-trending-videos-global",
    "sebastianbesinski/youtube-trending-videos-2025-updated-daily",
]

WELL_KNOWN_DOMAINS = set([
    "youtube.com", "youtu.be", "facebook.com", "twitter.com", "instagram.com",
    "linkedin.com", "tiktok.com", "reddit.com", "pinterest.com", "discord.gg",
    "google.com", "amazon.com", "apple.com", "microsoft.com", "netflix.com",
    "bing.com", "github.com", "wordpress.com", "shopify.com", "cloudflare.com",
    "godaddy.com", "namecheap.com", "hostgator.com", "bluehost.com"
])

def download_random_dataset():
    dataset = random.choice(KAGGLE_DATASETS)
    print(f"📦 Downloading dataset: {dataset}")
    os.system(f"kaggle datasets download -d {dataset} -p data --unzip --force")

def extract_links(text):
    return re.findall(r'https?://[^\s"\']+', str(text))

def extract_domain(url):
    try:
        return urlparse(url).netloc.lower().replace("www.", "")
    except:
        return None

def is_potentially_expired(domain):
    try:
        resp = requests.get(f"http://{domain}", timeout=5)
        return False  # Domain loads = likely active
    except:
        return True   # Doesn't load = potentially expired

def already_scanned(video_id):
    result = supabase.table("Clickyleaks_Checked").select("video_id").eq("video_id", video_id).execute()
    return len(result.data) > 0

def log_checked_video(video_id):
    supabase.table("Clickyleaks_Checked").insert({"video_id": video_id, "scanned_at": datetime.utcnow().isoformat()}).execute()

def log_domain(domain, video_id):
    supabase.table("Clickyleaks").insert({
        "domain": domain,
        "video_id": video_id,
        "available": True,
        "verified": False,
        "source": "kaggle"
    }).execute()

def soft_check_and_log(domain, video_id):
    if domain in WELL_KNOWN_DOMAINS:
        return False
    if is_potentially_expired(domain):
        log_domain(domain, video_id)
        return True
    else:
        supabase.table("Clickyleaks").insert({
            "domain": domain,
            "video_id": video_id,
            "available": False,
            "verified": True,
            "source": "kaggle"
        }).execute()
        return False

def send_discord_alert(processed, found):
    if DISCORD_WEBHOOK_URL:
        requests.post(DISCORD_WEBHOOK_URL, json={
            "content": f"✅ Kaggle Clickyleaks Scanner finished.\nVideos scanned: **{processed}**\nPotential domains found: **{found}**"
        })

def main():
    download_random_dataset()
    csv_files = list(Path("./data").glob("*.csv"))
    if not csv_files:
        print("❌ No CSV files found.")
        return

    df = pd.read_csv(csv_files[0])
    df = df.drop_duplicates(subset=["video_id"])
    df = df.sort_values(by="publishedAt", ascending=True)

    videos_processed = 0
    domains_found = 0

    for _, row in df.iterrows():
        video_id = str(row["video_id"])
        if already_scanned(video_id):
            continue

        links = extract_links(row.get("description", ""))
        found_in_video = 0

        for link in links:
            domain = extract_domain(link)
            if domain and soft_check_and_log(domain, video_id):
                domains_found += 1
                found_in_video += 1
                if domains_found >= 5:
                    break

        log_checked_video(video_id)
        videos_processed += 1

        if videos_processed >= 500 or domains_found >= 5:
            break

    send_discord_alert(videos_processed, domains_found)

if __name__ == "__main__":
    main()