import requests, json, os, re, time
from urllib.parse import urlparse
from supabase import create_client
from datetime import datetime

# === CONFIG ===
DROPBOX_FOLDER_URL = "https://www.dropbox.com/scl/fo/1ppkjjh5obz5a1intm2l3/AAR46jymkpUBUHh_8ZXGwVU?rlkey=6tpw4o2jt5q2hf42m3l1lsuv8&st=3w42hofh&dl=1"
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

BLOCKED_DOMAINS = [
    "amzn.to", "bit.ly", "t.co", "youtube.com", "instagram.com", "linktr.ee",
    "rumble.com", "facebook.com", "twitter.com", "linkedin.com", "paypal.com",
    "discord.gg", "youtu.be"
]

def get_next_chunk_number():
    result = supabase.table("Clickyleaks_KaggleChunks").select("chunk_filename").order("processed_at", desc=True).limit(1).execute()
    if result.data:
        last_chunk = result.data[0]["chunk_filename"]
        number = int(re.search(r"chunk_(\d+)\.json", last_chunk).group(1)) + 1
    else:
        number = 1
    return f"{number:03d}"

def download_chunk(chunk_num):
    url = f"https://dl.dropboxusercontent.com/scl/fi/1ppkjjh5obz5a1intm2l3/chunk_{chunk_num}.json?rlkey=6tpw4o2jt5q2hf42m3l1lsuv8"
    print(f"📥 Downloading chunk: {url}")
    try:
        res = requests.get(url, timeout=30)
        res.raise_for_status()
        return json.loads(res.content)
    except Exception as e:
        print(f"❌ Failed to download or parse chunk: {e}")
        return None

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

def already_checked(video_id):
    result = supabase.table("Clickyleaks_KaggleChecked").select("id").eq("video_id", video_id).execute()
    return len(result.data) > 0

def send_discord_alert(domain, video_url):
    msg = {
        "content": f"🎯 Available Domain Found: `{domain}`\n🔗 From: {video_url}"
    }
    try:
        requests.post(DISCORD_WEBHOOK_URL, json=msg, timeout=10)
    except Exception as e:
        print(f"❌ Discord alert failed: {e}")

def process_chunk(chunk_num):
    data = download_chunk(chunk_num)
    if not data:
        return False

    for video in data:
        video_id = video.get("_id")
        description = video.get("description", "")
        title = video.get("title", "")

        if already_checked(video_id):
            continue

        links = extract_links(description)
        for link in links:
            domain = urlparse(link).netloc.lower()
            if any(domain.endswith(bad) for bad in BLOCKED_DOMAINS):
                continue

            available = is_domain_available(domain)
            print(f"🔍 Logging: {domain} (Available: {available})")

            if available:
                record = {
                    "domain": domain,
                    "full_url": link,
                    "video_id": video_id,
                    "video_title": title,
                    "video_url": f"https://www.youtube.com/watch?v={video_id}",
                    "http_status": 0,
                    "is_available": True,
                    "view_count": 0,
                    "discovered_at": datetime.utcnow().isoformat(),
                    "scanned_at": datetime.utcnow().isoformat()
                }
                supabase.table("Clickyleaks").insert(record).execute()
                send_discord_alert(domain, record["video_url"])
                break  # Only check one domain per video

        supabase.table("Clickyleaks_KaggleChecked").insert({
            "video_id": video_id,
            "checked_at": datetime.utcnow().isoformat()
        }).execute()

    supabase.table("Clickyleaks_KaggleChunks").insert({
        "chunk_filename": f"chunk_{chunk_num}.json",
        "processed_at": datetime.utcnow().isoformat()
    }).execute()

    print(f"✅ Finished chunk {chunk_num}")
    return True

def main():
    print("🚀 Clickyleaks Kaggle Chunk Scanner Started...")
    chunk_num = get_next_chunk_number()
    success = process_chunk(chunk_num)
    if not success:
        print("❌ No new chunk processed.")

if __name__ == "__main__":
    main()
