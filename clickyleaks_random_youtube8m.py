import csv, random, requests, time, re
from datetime import datetime
from urllib.parse import urlparse
from supabase import create_client, Client
import os

# === ENV CONFIG ===
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# === Load pre-downloaded YouTube video IDs ===
with open("youtube8m_video_ids_subset.csv", newline="") as f:
    reader = csv.reader(f)
    VIDEO_IDS = [row[0] for row in reader if row]

# === Domain checker ===
def is_domain_available(domain):
    try:
        res = requests.get(f"http://{domain}", timeout=4)
        return False
    except:
        return True

# === Discord Alert ===
def notify_discord(message):
    if not DISCORD_WEBHOOK_URL:
        return
    try:
        requests.post(DISCORD_WEBHOOK_URL, json={"content": message}, timeout=5)
    except Exception as e:
        print(f"⚠️ Discord webhook error: {e}")

# === Already scanned? ===
def already_scanned(video_id):
    result = supabase.table("Clickyleaks").select("id").eq("video_id", video_id).execute()
    return len(result.data) > 0

# === Extract links from description ===
def extract_links(text):
    return re.findall(r'(https?://[^\s)]+)', text)

# === Log domain ===
def log_domain(link, domain, video_id, title, view_count, is_available):
    try:
        supabase.table("Clickyleaks").insert({
            "domain": domain,
            "full_url": link,
            "video_id": video_id,
            "video_title": title,
            "video_url": f"https://www.youtube.com/watch?v={video_id}",
            "http_status": 0,
            "is_available": is_available,
            "view_count": view_count,
            "discovered_at": datetime.utcnow().isoformat(),
            "scanned_at": datetime.utcnow().isoformat()
        }).execute()

        if is_available:
            notify_discord(f"🎯 Found available domain: `{domain}`\nFrom video: https://www.youtube.com/watch?v={video_id}")

    except Exception as e:
        print(f"⚠️ Logging error: {e}")

# === Get video metadata from oEmbed ===
def get_video_meta(video_id):
    try:
        res = requests.get(f"https://www.youtube.com/oembed?url=https://www.youtube.com/watch?v={video_id}&format=json", timeout=6)
        if res.status_code == 200:
            data = res.json()
            return {
                "title": data.get("title", "Unknown title"),
                "author": data.get("author_name", "Unknown"),
                "view_count": 0  # Can't get actual views without API
            }
    except:
        pass
    return None

# === Process each video ===
def process_video(video_id):
    if already_scanned(video_id):
        return

    print(f"🔍 Checking: https://www.youtube.com/watch?v={video_id}")
    meta = get_video_meta(video_id)
    if not meta:
        return

    try:
        res = requests.get(f"https://www.youtube.com/watch?v={video_id}", timeout=8)
        matches = extract_links(res.text)
        for link in matches:
            domain = urlparse(link).netloc.lower()
            is_available = is_domain_available(domain)
            print(f"🔍 Logging domain: {domain} | Available: {is_available}")
            log_domain(link, domain, video_id, meta["title"], meta["view_count"], is_available)
            break
    except Exception as e:
        print(f"❌ Error processing video {video_id}: {e}")

# === MAIN ===
def main():
    print("🚀 Clickyleaks Random Scanner Started...")

    sample = random.sample(VIDEO_IDS, 250)
    found = 0

    for vid in sample:
        process_video(vid)
        time.sleep(random.uniform(0.5, 1.2))  # Polite delay
        found += 1

    if found == 0:
        notify_discord("⚠️ Clickyleaks scanner ran but found no domains.")

if __name__ == "__main__":
    main()
