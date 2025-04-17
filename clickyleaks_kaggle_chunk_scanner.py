import requests, json, time, re
from urllib.parse import urlparse
from datetime import datetime
from supabase import create_client, Client
import os

# === CONFIG ===
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")
CHUNK_BASE_URL = "https://smurfymurf.github.io/clickyleaks-chunks/chunk_{:03}.json"

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def get_next_chunk_index():
    checked = supabase.table("Clickyleaks_KaggleCheckedChunks").select("chunk_name").execute()
    used_chunks = {row["chunk_name"] for row in checked.data}
    for i in range(1, 1000):
        name = f"chunk_{i:03}.json"
        if name not in used_chunks:
            return i, name
    return None, None

def download_chunk(index):
    url = CHUNK_BASE_URL.format(index)
    print(f"📥 Downloading chunk: {url}")
    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"❌ Failed to download or parse chunk: {e}")
        return None

def extract_links(text):
    return re.findall(r'(https?://[^\s)]+)', text or "")

def is_domain_available(domain):
    root = domain.lower().strip()
    if root.startswith("www."):
        root = root[4:]
    root = root.split("/")[0]
    try:
        res = requests.get(f"http://{root}", timeout=4)
        return False
    except:
        return True

def send_discord_alert(domain, video_id, video_title, video_url):
    message = {
        "content": f"🚨 **Available domain found!**\n**Domain:** `{domain}`\n**Video:** [{video_title}]({video_url})"
    }
    try:
        requests.post(DISCORD_WEBHOOK_URL, json=message, timeout=5)
    except Exception as e:
        print(f"❌ Discord webhook failed: {e}")

def process_video(video):
    video_id = video.get("_id")
    description = video.get("description", "")
    title = video.get("title", "No Title")
    if not video_id:
        return

    video_url = f"https://www.youtube.com/watch?v={video_id}"
    links = extract_links(description)
    for link in links:
        domain = urlparse(link).netloc.lower()
        if any(bad in domain for bad in ["youtube.com", "youtu.be", "bit.ly", "t.co", "linktr.ee"]):
            continue

        is_available = is_domain_available(domain)
        print(f"🔍 Logging domain: {domain} | Available: {is_available}")
        if is_available:
            record = {
                "domain": domain,
                "full_url": link,
                "video_id": video_id,
                "video_title": title,
                "video_url": video_url,
                "http_status": 0,
                "is_available": True,
                "view_count": 0,
                "discovered_at": datetime.utcnow().isoformat(),
                "scanned_at": datetime.utcnow().isoformat()
            }
            supabase.table("Clickyleaks").insert(record).execute()
            send_discord_alert(domain, video_id, title, video_url)
        break  # Only check first link

def main():
    print("🚀 Clickyleaks Kaggle Chunk Scanner Started...")
    index, chunk_name = get_next_chunk_index()
    if not index:
        print("✅ All chunks processed.")
        return

    data = download_chunk(index)
    if not data:
        print("❌ No new chunk processed.")
        return

    count = 0
    for video in data:
        if count >= 100:
            break
        process_video(video)
        count += 1
        time.sleep(0.5)

    supabase.table("Clickyleaks_KaggleCheckedChunks").insert({
        "chunk_name": chunk_name,
        "checked_at": datetime.utcnow().isoformat()
    }).execute()
    print(f"✅ Finished scanning chunk: {chunk_name}")

if __name__ == "__main__":
    main()
