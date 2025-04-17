import requests, re, json, time
from urllib.parse import urlparse
from datetime import datetime
from supabase import create_client, Client
import os

# === CONFIG ===
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")
BASE_CHUNK_URL = "https://smurfymurf.github.io/clickyleaks-chunks"

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
BLOCKED_DOMAINS = ["facebook.com", "youtu.be", "youtube.com", "twitter.com", "instagram.com", "t.co", "bit.ly", "on.fb.me", "itunes.apple.com"]

def get_next_chunk_index():
    checked = supabase.table("Clickyleaks_KaggleCheckedChunks").select("chunk_name").execute()
    scanned = set(row["chunk_name"] for row in checked.data)
    for i in range(1, 1000):
        name = f"chunk_{i:03}.json"
        if name not in scanned:
            return i, name
    return None, None

def mark_chunk_complete(chunk_name):
    supabase.table("Clickyleaks_KaggleCheckedChunks").insert({
        "chunk_name": chunk_name,
        "scanned_at": datetime.utcnow().isoformat()
    }).execute()

def mark_video_checked(video_id):
    try:
        supabase.table("Clickyleaks_KaggleChecked").insert({
            "video_id": video_id,
            "checked_at": datetime.utcnow().isoformat()
        }).execute()
    except Exception as e:
        print(f"⚠️ Failed to log checked video {video_id}: {e}")

def extract_links(text):
    return re.findall(r'(https?://[^\s)]+)', text)

def is_domain_available(domain):
    root = domain.lower().strip()
    if root.startswith("www."):
        root = root[4:]
    root = root.split("/")[0]
    try:
        res = requests.get(f"http://{root}", timeout=5)
        return False
    except:
        return True

def log_available_domain(domain, video, link):
    print(f"🔍 Logging domain: {domain} | Available: True")
    supabase.table("Clickyleaks").insert({
        "domain": domain,
        "full_url": link,
        "video_id": video["_id"],
        "video_title": video.get("title", "N/A"),
        "video_url": f"https://www.youtube.com/watch?v={video['_id']}",
        "http_status": 0,
        "is_available": True,
        "view_count": 0,
        "discovered_at": datetime.utcnow().isoformat(),
        "scanned_at": datetime.utcnow().isoformat()
    }).execute()

def send_discord_alert(domains):
    if not DISCORD_WEBHOOK_URL:
        return
    lines = [f"🔔 **{len(domains)} Available Domain(s) Found!**"]
    for d in domains:
        lines.append(f"🔗 `{d['domain']}` from [Video]({d['video_url']})")
    message = "\n".join(lines)
    try:
        requests.post(DISCORD_WEBHOOK_URL, json={"content": message})
    except Exception as e:
        print(f"⚠️ Discord alert failed: {e}")

def main():
    print("🚀 Clickyleaks Kaggle Chunk Scanner Started...")
    index, chunk_name = get_next_chunk_index()
    if not chunk_name:
        print("✅ No unscanned chunks left.")
        return

    url = f"{BASE_CHUNK_URL}/{chunk_name}"
    print(f"📥 Downloading chunk: {url}")
    try:
        res = requests.get(url, timeout=30)
        data = json.loads(res.text)
    except Exception as e:
        print(f"❌ Failed to download or parse chunk: {e}")
        return

    found_domains = []

    for video in data:
        video_id = video.get("_id")
        if not video_id or not video.get("description"):
            continue
        links = extract_links(video["description"])
        if not links:
            mark_video_checked(video_id)
            continue
        for link in links:
            domain = urlparse(link).netloc.lower()
            if any(bad in domain for bad in BLOCKED_DOMAINS):
                continue
            available = is_domain_available(domain)
            if available:
                log_available_domain(domain, video, link)
                found_domains.append({
                    "domain": domain,
                    "video_url": f"https://www.youtube.com/watch?v={video_id}"
                })
            break  # Check only first link per video
        mark_video_checked(video_id)
        time.sleep(1)

    mark_chunk_complete(chunk_name)

    if found_domains:
        send_discord_alert(found_domains)

    print(f"✅ Finished scanning chunk: {chunk_name}")

if __name__ == "__main__":
    main()
