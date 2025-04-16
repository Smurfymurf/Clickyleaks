import os, requests, random, csv, time, socket, dns.resolver
from datetime import datetime
from supabase import create_client, Client
from urllib.parse import urlparse
from dotenv import load_dotenv
import re

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

PROCESSED_LOG = "shards_processed.txt"
MIRRORS = ["us", "eu", "asia"]
SHARD_COUNT = 3844

def get_random_shard_url():
    used = set()
    if os.path.exists(PROCESSED_LOG):
        with open(PROCESSED_LOG, "r") as f:
            used = set(line.strip() for line in f.readlines())
    
    all_shards = [f"{i:05d}-of-{SHARD_COUNT:05d}" for i in range(SHARD_COUNT)]
    unused = list(set(all_shards) - used)

    if not unused:
        print("✅ All shards processed. Resetting log.")
        os.remove(PROCESSED_LOG)
        unused = all_shards

    shard_id = random.choice(unused)
    mirror = random.choice(MIRRORS)
    url = f"https://storage.googleapis.com/data.yt8m.org/2/j/v/validate/{shard_id}.tsv"

    with open(PROCESSED_LOG, "a") as f:
        f.write(f"{shard_id}\n")

    return url

def download_shard(url):
    try:
        print(f"📥 Downloading shard: {url}")
        res = requests.get(url, timeout=30)
        res.raise_for_status()
        return res.text.splitlines()
    except Exception as e:
        print(f"❌ Failed to download shard: {e}")
        return []

def extract_video_ids(tsv_lines):
    video_ids = []
    for line in tsv_lines[1:]:
        parts = line.split('\t')
        if parts:
            video_ids.append(parts[0])
    return video_ids

def already_scanned(video_id):
    try:
        result = supabase.table("Clickyleaks").select("id").eq("video_id", video_id).execute()
        return len(result.data) > 0
    except:
        return False

def extract_links(text):
    return re.findall(r'(https?://[^\s)]+)', text)

def is_domain_available(domain):
    try:
        if domain.startswith("www."):
            domain = domain[4:]
        dns.resolver.resolve(domain, "A")
        return False
    except:
        try:
            socket.gethostbyname(domain)
            return False
        except:
            return True

def log_domain(domain, video_id, link, video_title, video_url, view_count=0):
    record = {
        "domain": domain,
        "full_url": link,
        "video_id": video_id,
        "video_title": video_title,
        "video_url": video_url,
        "view_count": view_count,
        "is_available": True,
        "discovered_at": datetime.utcnow().isoformat(),
        "scanned_at": datetime.utcnow().isoformat()
    }
    supabase.table("Clickyleaks").insert(record).execute()
    print(f"✅ Logged available domain: {domain}")

    # Notify Discord
    msg = f"🔥 **Domain Found:** `{domain}`\n🔗 {video_url}\n🎬 {video_title}"
    requests.post(DISCORD_WEBHOOK_URL, json={"content": msg})

def process_video(video_id):
    if already_scanned(video_id):
        return

    url = f"https://www.youtube.com/watch?v={video_id}"
    res = requests.get(url)
    if res.status_code != 200 or "This video is unavailable" in res.text:
        return

    match = re.search(r'<meta name="description" content="(.*?)">', res.text)
    if not match:
        return
    desc = match.group(1)
    links = extract_links(desc)

    for link in links:
        domain = urlparse(link).netloc.lower()
        if not domain or "youtube.com" in domain or "youtu.be" in domain:
            continue

        available = is_domain_available(domain)
        print(f"🔍 Logging domain: {domain} | Available: {available}")
        if available:
            log_domain(domain, video_id, link, f"Video {video_id}", url)
        break  # Only check first link

def main():
    print("🚀 Clickyleaks Random Scanner Started...")
    url = get_random_shard_url()
    lines = download_shard(url)

    if not lines:
        print("❌ No shard data.")
        return

    ids = extract_video_ids(lines)
    random.shuffle(ids)

    for vid in ids[:250]:  # You can increase to 500 or more depending on speed
        print(f"🔍 Checking: https://www.youtube.com/watch?v={vid}")
        try:
            process_video(vid)
            time.sleep(1)
        except Exception as e:
            print(f"❌ Error processing video {vid}: {e}")

if __name__ == "__main__":
    main()
