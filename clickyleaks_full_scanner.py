import os
import json
import random
import requests
import re
import time
from datetime import datetime
from supabase import create_client
from urllib.parse import urlparse
from pathlib import Path
from playwright.sync_api import sync_playwright
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")

# Supabase client
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# Constants
VIDEO_CHUNK_DIR = Path("data/youtube8m_chunks")
WELL_KNOWN_DOMAINS_CSV = Path("data/well_known_domains.csv")
MAX_VIDEOS = 500
MAX_DOMAINS = 10

# Load well-known domains
def load_well_known_domains():
    if WELL_KNOWN_DOMAINS_CSV.exists():
        with open(WELL_KNOWN_DOMAINS_CSV, "r") as f:
            return set([line.strip() for line in f if line.strip()])
    return set()

WELL_KNOWN_DOMAINS = load_well_known_domains()

# Get already scanned video IDs
def get_scanned_video_ids():
    resp = supabase.table("clickyleaks_checked").select("video_id").execute()
    if resp.data:
        return set(row["video_id"] for row in resp.data)
    return set()

# Parse description for external links
def extract_links_from_description(description):
    return re.findall(r"https?://[\w./?=&%-]+", description or "")

# Get video description with fallback using Playwright
def get_video_description(video_id):
    try:
        # First try YouTube oEmbed (lightweight)
        oembed_url = f"https://www.youtube.com/oembed?url=https://www.youtube.com/watch?v={video_id}&format=json"
        r = requests.get(oembed_url, timeout=5)
        if r.status_code == 200:
            return ""
    except:
        pass

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            page.goto(f"https://www.youtube.com/watch?v={video_id}", timeout=15000)
            time.sleep(2)
            description = page.inner_text("#description")
            browser.close()
            return description
    except:
        return None

# Check if domain might be expired (soft check)
def is_potentially_expired(domain):
    try:
        r = requests.get("http://" + domain, timeout=5)
        if r.status_code in [404, 410, 500]:
            return True
    except:
        return True
    return False

# Discord alert
def send_discord_alert(message):
    if DISCORD_WEBHOOK_URL:
        requests.post(DISCORD_WEBHOOK_URL, json={"content": message})

# Main process

def main():
    print("🔍 Starting Clickyleaks YouTube8M Scan")

    scanned_video_ids = get_scanned_video_ids()
    chunk_files = sorted(VIDEO_CHUNK_DIR.glob("chunk_*.json"))
    chosen_chunk = random.choice(chunk_files)

    print(f"🎯 Chosen chunk: {chosen_chunk.name}")

    with open(chosen_chunk, "r") as f:
        video_ids = json.load(f)

    found_domains = []
    checked = 0

    for video_id in video_ids:
        if video_id in scanned_video_ids:
            continue

        checked += 1
        print(f"▶️ Checking video: {video_id}")

        description = get_video_description(video_id)

        if description is None:
            print(f"⚠️ Skipping dead/unavailable video: {video_id}")
            supabase.table("clickyleaks_checked").insert({"video_id": video_id, "scanned_at": datetime.utcnow().isoformat()}).execute()
            continue

        links = extract_links_from_description(description)

        for link in links:
            parsed = urlparse(link)
            domain = parsed.netloc.replace("www.", "").lower()

            if domain in WELL_KNOWN_DOMAINS:
                continue

            if is_potentially_expired(domain):
                print(f"💡 Potential expired domain: {domain}")
                found_domains.append(domain)
                supabase.table("clickyleaks").insert({
                    "domain": domain,
                    "video_id": video_id,
                    "verified": False,
                    "available": True,
                    "found_at": datetime.utcnow().isoformat()
                }).execute()

                if len(found_domains) >= MAX_DOMAINS:
                    break

        supabase.table("clickyleaks_checked").insert({"video_id": video_id, "scanned_at": datetime.utcnow().isoformat()}).execute()

        if checked >= MAX_VIDEOS or len(found_domains) >= MAX_DOMAINS:
            break

    send_discord_alert(f"✅ Clickyleaks scan complete — {len(found_domains)} potential domains found from {checked} videos.")

if __name__ == "__main__":
    main()
