import requests, random, csv, time, os, re
from datetime import datetime
from supabase import create_client
from urllib.parse import urlparse

# === CONFIG ===
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# Official Google Cloud mirror setup
PARTITIONS = ["train", "validate", "test"]
MAX_SHARDS = {
    "train": 3844,
    "validate": 474,
    "test": 123
}

BLOCKED_DOMAINS = [
    "amzn.to", "bit.ly", "t.co", "youtube.com", "instagram.com", "linktr.ee",
    "rumble.com", "facebook.com", "twitter.com", "linkedin.com", "paypal.com",
    "discord.gg", "youtu.be", "i.ytimg.com"
]

# === FUNCTIONS ===

def download_random_video_ids():
    partition = random.choice(PARTITIONS)
    max_shard = MAX_SHARDS[partition]
    shard_index = random.randint(0, max_shard - 1)
    shard_id = str(shard_index).zfill(5)

    url = f"https://storage.googleapis.com/yt8m-video-id/{partition}/{shard_id}-of-{str(max_shard).zfill(5)}.csv"
    print(f"📥 Downloading shard: {url}")

    try:
        res = requests.get(url, timeout=15)
        res.raise_for_status()
    except Exception as e:
        print(f"❌ Failed to download shard: {e}")
        return []

    video_ids = []
    lines = res.text.strip().splitlines()
    reader = csv.reader(lines)
    for row in reader:
        if row:
            video_ids.append(row[0])
    return video_ids

def already_scanned(video_id):
    try:
        result = supabase.table("Clickyleaks").select("id").eq("video_id", video_id).execute()
        return len(result.data) > 0
    except:
        return False

def get_video_description(video_id):
    url = f"https://www.youtube.com/watch?v={video_id}"
    try:
        res = requests.get(url, timeout=10)
        matches = re.findall(r'(https?://[^\s"<>()]+)', res.text)
        return matches
    except:
        return []

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

def log_to_supabase(domain, link, video_id, video_url, available):
    try:
        supabase.table("Clickyleaks").insert({
            "domain": domain,
            "full_url": link,
            "video_id": video_id,
            "video_url": video_url,
            "http_status": 0,
            "is_available": available,
            "view_count": None,
            "discovered_at": datetime.utcnow().isoformat(),
            "scanned_at": datetime.utcnow().isoformat()
        }).execute()
    except Exception as e:
        print(f"⚠️ DB insert error: {e}")

def send_discord(msg):
    if DISCORD_WEBHOOK_URL:
        try:
            requests.post(DISCORD_WEBHOOK_URL, json={"content": msg}, timeout=10)
        except:
            print("⚠️ Failed to send Discord notification")

def process_video(video_id):
    if already_scanned(video_id):
        return

    video_url = f"https://www.youtube.com/watch?v={video_id}"
    print(f"🔍 Checking: {video_url}")

    links = get_video_description(video_id)
    for link in links:
        domain = urlparse(link).netloc.lower()
        if any(domain.endswith(bad) for bad in BLOCKED_DOMAINS):
            continue

        available = is_domain_available(domain)
        print(f"🔍 Logging domain: {domain} | Available: {available}")
        log_to_supabase(domain, link, video_id, video_url, available)

        if available:
            send_discord(f"🔥 Found available domain: `{domain}` from {video_url}")
        break

# === MAIN ===

def main():
    print("🚀 Clickyleaks Random Scanner Started...")
    video_ids = download_random_video_ids()
    if not video_ids:
        print("❌ No video IDs fetched from mirror.")
        return

    random.shuffle(video_ids)

    found = 0
    for vid in video_ids[:250]:  # Adjust for how many you want to process per run
        try:
            process_video(vid)
            found += 1
            time.sleep(1)
        except Exception as e:
            print(f"❌ Error processing video {vid}: {e}")
        if found >= 10:
            break

if __name__ == "__main__":
    main()
