import requests, random, csv, re, time
from datetime import datetime
from urllib.parse import urlparse
from supabase import create_client, Client
import os

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

YT8M_MIRROR = "https://storage.googleapis.com/data.yt8m.org/2/j/v/00.json"  # Streamed version fallback
BLOCKED_DOMAINS = [
    "amzn.to", "bit.ly", "t.co", "youtube.com", "instagram.com", "linktr.ee",
    "rumble.com", "facebook.com", "twitter.com", "linkedin.com", "paypal.com",
    "discord.gg", "youtu.be", "i.ytimg.com"
]

def get_random_video_ids(n=50):
    try:
        res = requests.get("https://storage.googleapis.com/data.yt8m.org/2/j/v/00.json", timeout=10)
        all_ids = list(res.json().keys())
        return random.sample(all_ids, min(n, len(all_ids)))
    except Exception as e:
        print(f"❌ Failed to fetch video IDs: {e}")
        return []

def already_scanned(video_id):
    try:
        result = supabase.table("Clickyleaks").select("id").eq("video_id", video_id).execute()
        return len(result.data) > 0
    except:
        return False

def extract_links(text):
    return re.findall(r'(https?://[^\s)]+)', text)

def is_domain_available(domain):
    domain = domain.lower().strip().lstrip("www.").split("/")[0]
    try:
        requests.get(f"http://{domain}", timeout=4)
        return False
    except:
        return True

def send_discord_alert(domain, video_url):
    try:
        message = {
            "content": f"🟢 **Available domain found:** `{domain}`\n📺 {video_url}"
        }
        requests.post(DISCORD_WEBHOOK_URL, json=message, timeout=5)
    except Exception as e:
        print(f"❌ Discord alert failed: {e}")

def process_video(video_id):
    if already_scanned(video_id):
        return

    url = f"https://www.youtube.com/watch?v={video_id}"
    print(f"🔍 Checking: {url}")
    try:
        res = requests.get(f"https://www.youtube.com/watch?v={video_id}", timeout=6)
        html = res.text
        links = extract_links(html)
        for link in links:
            domain = urlparse(link).netloc.lower()
            if not domain or any(blocked in domain for blocked in BLOCKED_DOMAINS):
                continue

            available = is_domain_available(domain)
            print(f"🔍 Logging domain: {domain} | Available: {available}")

            record = {
                "domain": domain,
                "full_url": link,
                "video_id": video_id,
                "video_url": url,
                "is_available": available,
                "discovered_at": datetime.utcnow().isoformat(),
                "scanned_at": datetime.utcnow().isoformat()
            }
            supabase.table("Clickyleaks").insert(record).execute()

            if available:
                send_discord_alert(domain, url)
            break  # Just one link per video
    except Exception as e:
        print(f"❌ Error processing video {video_id}: {e}")

def main():
    print("🚀 Clickyleaks Random Scanner Started...")
    video_ids = get_random_video_ids(n=250)
    if not video_ids:
        print("❌ No video IDs fetched from mirror.")
        return

    for vid in video_ids:
        process_video(vid)
        time.sleep(0.5)

if __name__ == "__main__":
    main()
