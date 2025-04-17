import requests, re
from urllib.parse import urlparse
from datetime import datetime
from supabase import create_client, Client
import os
import json

# === CONFIG ===
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")
CHUNK_BASE_URL = "https://smurfymurf.github.io/clickyleaks-chunks/"
TOTAL_CHUNKS = 100  # update if you add more

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def get_next_chunk_index():
    checked = supabase.table("Clickyleaks_KaggleCheckedChunks").select("chunk_name").execute()
    checked_chunks = {row["chunk_name"] for row in checked.data}
    for i in range(1, TOTAL_CHUNKS + 1):
        chunk = f"chunk_{i:03}.json"
        if chunk not in checked_chunks:
            return i, chunk
    return None, None

def mark_chunk_complete(chunk_name):
    supabase.table("Clickyleaks_KaggleCheckedChunks").insert({
        "chunk_name": chunk_name,
        "scanned_at": datetime.utcnow().isoformat()
    }).execute()

def already_scanned(video_id):
    result = supabase.table("Clickyleaks_KaggleChecked").select("id").eq("video_id", video_id).execute()
    return len(result.data) > 0

def mark_video_scanned(video_id):
    supabase.table("Clickyleaks_KaggleChecked").insert({
        "video_id": video_id,
        "scanned_at": datetime.utcnow().isoformat()
    }).execute()

def is_domain_available(domain):
    try:
        res = requests.get(f"http://{domain}", timeout=5)
        return False
    except:
        return True

def send_discord_alert(domain, video_url):
    if not DISCORD_WEBHOOK_URL:
        return
    message = {
        "content": f"🔥 Available domain found: `{domain}`\n🔗 Video: {video_url}"
    }
    try:
        requests.post(DISCORD_WEBHOOK_URL, json=message, timeout=5)
    except:
        pass

def extract_links(text):
    return re.findall(r'(https?://[^\s)]+)', text)

def process_video(video):
    video_id = video.get("_id")
    if not video_id or already_scanned(video_id):
        return

    mark_video_scanned(video_id)
    description = video.get("description", "")
    links = extract_links(description)

    for link in links:
        try:
            domain = urlparse(link).netloc.lower()
            if not domain or len(domain.split(".")) < 2:
                continue
        except Exception as e:
            print(f"⚠️ Skipping invalid URL: {link} ({e})")
            continue

        is_available = is_domain_available(domain)
        print(f"🔍 Logging domain: {domain} | Available: {is_available}")

        if is_available:
            record = {
                "domain": domain,
                "full_url": link,
                "video_id": video_id,
                "video_url": f"https://www.youtube.com/watch?v={video_id}",
                "is_available": True,
                "discovered_at": datetime.utcnow().isoformat(),
                "scanned_at": datetime.utcnow().isoformat(),
                "view_count": 0
            }
            supabase.table("Clickyleaks").insert(record).execute()
            send_discord_alert(domain, record["video_url"])
            break

def main():
    print("🚀 Clickyleaks Kaggle Chunk Scanner Started...")

    index, chunk_name = get_next_chunk_index()
    if not chunk_name:
        print("✅ All chunks scanned.")
        return

    chunk_url = f"{CHUNK_BASE_URL}{chunk_name}"
    print(f"📥 Downloading chunk: {chunk_url}")

    try:
        res = requests.get(chunk_url, timeout=10)
        res.raise_for_status()
        chunk_data = res.json()
    except Exception as e:
        print(f"❌ Failed to download or parse chunk: {e}")
        return

    for video in chunk_data:
        process_video(video)

    mark_chunk_complete(chunk_name)
    print(f"✅ Finished scanning chunk: {chunk_name}")

if __name__ == "__main__":
    main()
