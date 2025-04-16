import requests, random, os, re, time
from datetime import datetime
from urllib.parse import urlparse
from supabase import create_client, Client

# === ENV ===
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

PROCESSED_LOG = "shards_processed.log"
SHARD_COUNT = 50  # number of validate shards
DOWNLOAD_FOLDER = "yt8m_shards"
os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)

BLOCKED_DOMAINS = [
    "youtube.com", "youtu.be", "bit.ly", "t.co", "facebook.com", "instagram.com",
    "linktr.ee", "patreon.com", "paypal.com", "discord.gg", "reddit.com"
]

def get_random_shard_url():
    used = set()
    if os.path.exists(PROCESSED_LOG):
        with open(PROCESSED_LOG, "r") as f:
            used = set(line.strip() for line in f.readlines())

    all_shards = [f"{i:05d}-of-00050" for i in range(SHARD_COUNT)]
    unused = list(set(all_shards) - used)

    if not unused:
        print("✅ All shards processed. Resetting log.")
        os.remove(PROCESSED_LOG)
        unused = all_shards

    shard_id = random.choice(unused)
    with open(PROCESSED_LOG, "a") as f:
        f.write(f"{shard_id}\n")

    url = f"https://storage.googleapis.com/yt8m-video-id/validate/{shard_id}.csv"
    return url

def download_shard(url):
    try:
        print(f"📥 Downloading shard: {url}")
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        lines = response.text.strip().splitlines()
        return [line.split(",")[0] for line in lines if line.strip()]
    except Exception as e:
        print(f"❌ Failed to download shard: {e}")
        return []

def already_scanned(video_id):
    try:
        result = supabase.table("Clickyleaks").select("id").eq("video_id", video_id).execute()
        return len(result.data) > 0
    except Exception as e:
        print(f"⚠️ Supabase check failed: {e}")
        return True

def extract_links(text):
    return re.findall(r'(https?://[^\s)]+)', text)

def is_domain_available(domain):
    try:
        domain = domain.lower().strip().split("/")[0].replace("www.", "")
        res = requests.get(f"http://{domain}", timeout=5)
        return False
    except:
        return True

def log_to_supabase(record):
    try:
        supabase.table("Clickyleaks").insert(record).execute()
    except Exception as e:
        print(f"⚠️ DB insert error: {e}")

def alert_discord(domain, video_url):
    if not DISCORD_WEBHOOK_URL:
        return
    try:
        payload = {
            "content": f"🔥 New available domain found: **{domain}**\n🔗 {video_url}"
        }
        requests.post(DISCORD_WEBHOOK_URL, json=payload, timeout=10)
    except Exception as e:
        print(f"⚠️ Discord alert error: {e}")

def process_video(video_id):
    if already_scanned(video_id):
        return

    url = f"https://www.youtube.com/watch?v={video_id}"
    print(f"🔍 Checking: {url}")

    try:
        res = requests.get(url, timeout=10)
        if res.status_code != 200:
            return

        html = res.text
        links = extract_links(html)
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
                "video_title": "N/A",
                "video_url": url,
                "http_status": 200,
                "is_available": available,
                "view_count": None,
                "discovered_at": datetime.utcnow().isoformat(),
                "scanned_at": datetime.utcnow().isoformat()
            }

            log_to_supabase(record)
            if available:
                alert_discord(domain, url)
            break

    except Exception as e:
        print(f"❌ Error processing video {video_id}: {e}")

def main():
    print("🚀 Clickyleaks Random Scanner Started...")
    shard_url = get_random_shard_url()
    video_ids = download_shard(shard_url)
    if not video_ids:
        print("❌ No video IDs fetched from mirror.")
        return

    for vid in random.sample(video_ids, min(250, len(video_ids))):
        process_video(vid)
        time.sleep(0.5)

if __name__ == "__main__":
    main()
