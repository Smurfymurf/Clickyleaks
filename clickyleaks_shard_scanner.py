import os, requests, csv, random, time, hashlib
from datetime import datetime
from supabase import create_client
from urllib.parse import urlparse

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

SHARD_BASE_URL = "https://storage.googleapis.com/yt8m-video-id/train"
TOTAL_SHARDS = 3844

def generate_shard_filename(index):
    return f"{index:05d}-of-{TOTAL_SHARDS:05d}.csv"

def is_shard_scanned(shard_name):
    result = supabase.table("Clickyleaks_ShardLog").select("id").eq("shard_name", shard_name).execute()
    return len(result.data) > 0

def mark_shard_scanned(shard_name):
    supabase.table("Clickyleaks_ShardLog").insert({
        "shard_name": shard_name,
        "scanned_at": datetime.utcnow().isoformat()
    }).execute()

def get_random_shard():
    indices = list(range(TOTAL_SHARDS))
    random.shuffle(indices)
    for index in indices:
        shard = generate_shard_filename(index)
        if not is_shard_scanned(shard):
            return shard
    return None

def fetch_shard(shard):
    url = f"{SHARD_BASE_URL}/{shard}"
    print(f"📥 Downloading shard: {url}")
    try:
        res = requests.get(url, timeout=15)
        res.raise_for_status()
        return res.text.splitlines()
    except Exception as e:
        print(f"❌ Failed to download shard: {e}")
        return None

def extract_video_ids(lines):
    reader = csv.reader(lines)
    return [row[0] for row in reader if row]

def already_scanned(video_id):
    result = supabase.table("Clickyleaks").select("id").eq("video_id", video_id).execute()
    return len(result.data) > 0

def extract_links(text):
    return [link for link in re.findall(r'(https?://[^\s)]+)', text)]

def check_domain_availability(domain):
    try:
        res = requests.get(f"http://{domain}", timeout=5)
        return False
    except:
        return True

def scan_video(video_id):
    url = f"https://www.youtube.com/watch?v={video_id}"
    print(f"🔍 Checking: {url}")
    try:
        response = requests.get(f"https://www.youtube.com/watch?v={video_id}", timeout=8)
        html = response.text
        links = extract_links(html)
        for link in links:
            domain = urlparse(link).netloc.lower()
            if not domain or domain.startswith("www."):
                domain = domain.replace("www.", "")
            if already_scanned(video_id):
                return
            available = check_domain_availability(domain)
            print(f"🔍 Logging domain: {domain} | Available: {available}")
            record = {
                "domain": domain,
                "full_url": link,
                "video_id": video_id,
                "video_url": url,
                "is_available": available,
                "view_count": 0,
                "discovered_at": datetime.utcnow().isoformat(),
                "scanned_at": datetime.utcnow().isoformat()
            }
            supabase.table("Clickyleaks").insert(record).execute()
            if available:
                send_discord_alert(domain, url)
            break
    except Exception as e:
        print(f"❌ Error processing video {video_id}: {e}")

def send_discord_alert(domain, video_url):
    if not DISCORD_WEBHOOK_URL:
        return
    message = {
        "content": f"🔥 **Available Domain Found:** `{domain}`\n📹 [Video Link]({video_url})"
    }
    try:
        requests.post(DISCORD_WEBHOOK_URL, json=message, timeout=5)
    except Exception as e:
        print(f"❌ Failed to send Discord alert: {e}")

def main():
    print("🚀 Clickyleaks Random Scanner Started...")
    shard = get_random_shard()
    if not shard:
        print("✅ All shards have been scanned!")
        return

    lines = fetch_shard(shard)
    if not lines:
        print("❌ No video IDs fetched from mirror.")
        return

    video_ids = extract_video_ids(lines)
    random.shuffle(video_ids)

    for vid in video_ids[:250]:
        scan_video(vid)
        time.sleep(1)

    mark_shard_scanned(shard)

if __name__ == "__main__":
    import re
    main()
