import os
import re
import csv
import random
import zipfile
import requests
from pathlib import Path
from dotenv import load_dotenv
from supabase import create_client
from urllib.parse import urlparse
from datetime import datetime

# Load environment variables
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")

# Init Supabase client
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# Datasets to pull from
DATASETS = [
    "asaniczka/trending-youtube-videos-113-countries",
    "pyuser11/youtube-trending-videos-updated-daily",
    "canerkonuk/youtube-trending-videos-global",
    "sebastianbesinski/youtube-trending-videos-2025-updated-daily"
]

# Known domains to skip
WELL_KNOWN_DOMAINS = [
    "youtube.com", "youtu.be", "facebook.com", "twitter.com", "instagram.com",
    "linkedin.com", "google.com", "amazon.com", "reddit.com", "discord.com",
    "tiktok.com", "bit.ly", "paypal.com", "apple.com", "microsoft.com",
    "cloudflare.com", "spotify.com", "pinterest.com", "whatsapp.com"
]

# Util: extract domains from text
def extract_domains(text):
    urls = re.findall(r'https?://[^\s)>\]"]+', text or "")
    domains = set()
    for url in urls:
        try:
            domain = urlparse(url).netloc.replace("www.", "")
            if domain and domain not in WELL_KNOWN_DOMAINS:
                domains.add(domain)
        except:
            continue
    return domains

# Util: soft expiry check (simple heuristic)
def is_soft_expired(domain):
    try:
        resp = requests.head(f"http://{domain}", timeout=5)
        return resp.status_code in [404, 410, 503]
    except:
        return True  # If it fails entirely, assume possibly expired

def main():
    # Pick random dataset
    dataset = random.choice(DATASETS)
    print(f"📦 Downloading dataset: {dataset}")
    os.system(f"kaggle datasets download -d {dataset} -p data --force")

    # Unzip all downloaded .zip files
    for zip_path in Path("./data").glob("*.zip"):
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall("./data")

    # Find CSVs
    csv_files = list(Path("./data").glob("*.csv"))
    if not csv_files:
        print("❌ No CSV files found.")
        return

    # Load video IDs already scanned
    existing_ids = set()
    checked_resp = supabase.table("clickyleaks_checked").select("video_id").execute()
    if checked_resp.data:
        existing_ids = {row["video_id"] for row in checked_resp.data}

    # Process up to 500 videos, oldest first
    found_domains = 0
    scanned = 0
    max_found = 5
    max_scan = 500

    for csv_file in sorted(csv_files, key=lambda f: f.stat().st_mtime):
        with open(csv_file, newline='', encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if scanned >= max_scan or found_domains >= max_found:
                    break

                video_id = row.get("video_id") or row.get("Video Id") or row.get("id")
                if not video_id or video_id in existing_ids:
                    continue

                description = row.get("description") or row.get("Description")
                domains = extract_domains(description)

                scanned += 1
                print(f"🔍 {video_id} - {len(domains)} domains found")

                for domain in domains:
                    if is_soft_expired(domain):
                        # Log to Clickyleaks
                        supabase.table("Clickyleaks").insert({
                            "video_id": video_id,
                            "domain": domain,
                            "available": True,
                            "verified": False,
                            "source": "kaggle"
                        }).execute()
                        print(f"✅ Potential expired: {domain}")
                        found_domains += 1
                        if found_domains >= max_found:
                            break

                # Track video as scanned
                supabase.table("clickyleaks_checked").insert({
                    "video_id": video_id,
                    "scanned_at": datetime.utcnow().isoformat()
                }).execute()

        if scanned >= max_scan or found_domains >= max_found:
            break

    # Discord alert
    if DISCORD_WEBHOOK_URL:
        msg = {
            "content": f"**Kaggle Scanner Run Complete**\nVideos scanned: `{scanned}`\nPotential domains found: `{found_domains}`"
        }
        try:
            requests.post(DISCORD_WEBHOOK_URL, json=msg, timeout=10)
        except Exception as e:
            print(f"⚠️ Failed to send Discord alert: {e}")

if __name__ == "__main__":
    main()