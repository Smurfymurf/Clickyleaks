import requests, random, re, socket, os, time
from datetime import datetime
from urllib.parse import urlparse
from supabase import create_client
from pytz import UTC

# === CONFIG ===
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

BLOCKED_DOMAINS = [
    "amzn.to", "bit.ly", "t.co", "youtube.com", "instagram.com", "linktr.ee",
    "rumble.com", "facebook.com", "twitter.com", "linkedin.com", "paypal.com",
    "discord.gg", "youtu.be", "i.ytimg.com"
]

def download_random_video_ids():
    partition = random.choice(["train", "validate", "test"])
    max_shards = {"train": 600, "validate": 50, "test": 50}
    shard_number = random.randint(0, max_shards[partition] - 1)
    shard_id = str(shard_number).zfill(5)
    url = f"https://data.yt8m.org/2/video_id/{partition}/{shard_id}-of-{str(max_shards[partition]).zfill(5)}.csv"

    print(f"📥 Downloading shard: {url}")
    try:
        res = requests.get(url, timeout=20)
        res.raise_for_status()
        return res.text.splitlines()[1:]  # Skip header
    except Exception as e:
        print(f"❌ Failed to download shard: {e}")
        return []

def fetch_video_description(video_id):
    video_url = f"https://www.youtube.com/watch?v={video_id}"
    try:
        res = requests.get(video_url, timeout=10)
        if res.status_code == 200:
            return res.text
    except:
        pass
    return ""

def extract_links(text):
    return re.findall(r'(https?://[^\s)]+)', text)

def is_domain_available(domain):
    domain = domain.lower().strip().split("/")[0]
    if domain.startswith("www."):
        domain = domain[4:]
    try:
        socket.gethostbyname(domain)
        return False
    except:
        return True

def already_scanned(video_id):
    try:
        result = supabase.table("Clickyleaks").select("id").eq("video_id", video_id).execute()
        return len(result.data) > 0
    except:
        return False

def send_discord_alert(domain, url):
    if not DISCORD_WEBHOOK_URL:
        return
    try:
        requests.post(DISCORD_WEBHOOK_URL, json={
            "content": f"🔥 **Available Domain Found:** `{domain}`\n🔗 {url}"
        })
    except:
        print("⚠️ Failed to send Discord alert.")

def process_video(video_id):
    if already_scanned(video_id):
        return

    url = f"https://www.youtube.com/watch?v={video_id}"
    print(f"🔍 Checking: {url}")
    html = fetch_video_description(video_id)
    links = extract_links(html)

    for link in links:
        domain = urlparse(link).netloc.lower()
        if any(bad in domain for bad in BLOCKED_DOMAINS):
            continue
        is_available = is_domain_available(domain)
        print(f"🔍 Logging domain: {domain} | Available: {is_available}")

        record = {
            "domain": domain,
            "full_url": link,
            "video_id": video_id,
            "video_url": url,
            "http_status": 200,
            "is_available": is_available,
            "discovered_at": datetime.utcnow().replace(tzinfo=UTC).isoformat(),
            "scanned_at": datetime.utcnow().replace(tzinfo=UTC).isoformat()
        }

        try:
            supabase.table("Clickyleaks").insert(record).execute()
            if is_available:
                send_discord_alert(domain, url)
        except Exception as e:
            print(f"⚠️ Could not insert into Supabase: {e}")
        break

def main():
    print("🚀 Clickyleaks Random Scanner Started...")
    video_ids = download_random_video_ids()
    if not video_ids:
        print("❌ No video IDs fetched from mirror.")
        return

    count = 0
    for vid in video_ids:
        process_video(vid.strip())
        count += 1
        if count >= 250:
            break
        time.sleep(0.5)

if __name__ == "__main__":
    main()
