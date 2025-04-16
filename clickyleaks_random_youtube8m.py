import csv
import requests
import time
from urllib.parse import urlparse
from datetime import datetime
from supabase import create_client
import os
import random

# === CONFIG ===
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

BLOCKED_DOMAINS = [
    "amzn.to", "bit.ly", "t.co", "youtube.com", "instagram.com", "linktr.ee",
    "rumble.com", "facebook.com", "twitter.com", "linkedin.com", "paypal.com",
    "discord.gg", "youtu.be"
]

def load_video_ids():
    with open("youtube8m_video_ids_subset.csv", newline="") as csvfile:
        reader = csv.reader(csvfile)
        return [row[0] for row in reader]

def already_scanned(video_id):
    result = supabase.table("Clickyleaks").select("id").eq("video_id", video_id).execute()
    return len(result.data) > 0

def extract_links(text):
    return re.findall(r'(https?://[^\s)]+)', text)

def is_domain_available(domain):
    root = domain.lower().strip()
    if root.startswith("www."):
        root = root[4:]
    root = root.split("/")[0]
    try:
        requests.get(f"http://{root}", timeout=5)
        return False
    except:
        return True

def send_discord_message(message):
    if DISCORD_WEBHOOK_URL:
        try:
            requests.post(DISCORD_WEBHOOK_URL, json={"content": message})
        except Exception as e:
            print(f"Failed to send Discord message: {e}")

def process_video(video_id):
    if already_scanned(video_id):
        return False

    video_url = f"https://www.youtube.com/watch?v={video_id}"
    print(f"🔍 Checking: {video_url}")
    
    try:
        response = requests.get(video_url, timeout=10)
        if "This video is unavailable" in response.text or response.status_code != 200:
            return False

        links = extract_links(response.text)
        for link in links:
            domain = urlparse(link).netloc.lower()
            if any(domain.endswith(bad) for bad in BLOCKED_DOMAINS):
                continue

            available = is_domain_available(domain)
            print(f"🔍 Logging domain: {domain} | Available: {available}")

            record = {
                "domain": domain,
                "full_url": link,
                "video_id": video_id,
                "video_title": "(unknown from raw)",
                "video_url": video_url,
                "http_status": 200,
                "is_available": available,
                "view_count": 0,
                "discovered_at": datetime.utcnow().isoformat(),
                "scanned_at": datetime.utcnow().isoformat()
            }

            supabase.table("Clickyleaks").insert(record).execute()

            if available:
                send_discord_message(f"🔥 New available domain from YouTube: `{domain}`\n{video_url}")

            return True  # Only one domain per video for now

    except Exception as e:
        print(f"❌ Error processing video {video_id}: {e}")
    return False

def main():
    print("🚀 Clickyleaks Random Scanner Started...")
    ids = load_video_ids()
    random.shuffle(ids)

    found = False
    for i in range(10):  # Try 10 random video IDs per run
        video_id = ids[i]
        if process_video(video_id):
            found = True
        time.sleep(1)

    if not found:
        print("❌ No available domains found after 10 attempts.")

if __name__ == "__main__":
    main()
