import os
import json
import requests
import re
from datetime import datetime
from urllib.parse import urlparse
from supabase import create_client, Client

# === CONFIG ===
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")
YOUTUBE8M_SHARDS_FOLDER = "youtube8m_shards"
VIDEOS_PER_RUN = 10000
MAX_NEW_DOMAINS_PER_RUN = 20
YOUTUBE8M_BASE_URL = "https://data.yt8m.org/2/j/v/video/train"  # Public 8M URL root

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

WELL_KNOWN_DOMAINS = {
    "apple.com", "google.com", "facebook.com", "amazon.com", "youtube.com",
    "microsoft.com", "netflix.com", "instagram.com", "paypal.com", "reddit.com",
    "wikipedia.org", "tumblr.com", "github.com", "linkedin.com", "spotify.com",
    "cnn.com", "bbc.com", "dropbox.com", "airbnb.com", "salesforce.com",
    "tiktok.com", "ebay.com", "zoom.us", "whatsapp.com", "nytimes.com",
    "oracle.com", "bing.com", "slack.com", "notion.so", "wordpress.com",
    "vercel.app", "netlify.app", "figma.com", "medium.com", "shopify.com",
    "yahoo.com", "pinterest.com", "imdb.com", "quora.com", "adobe.com",
    "cloudflare.com", "soundcloud.com", "coursera.org", "kickstarter.com",
    "mozilla.org", "forbes.com", "theguardian.com", "weather.com", "espn.com",
    "msn.com", "okta.com", "bitbucket.org", "vimeo.com", "unsplash.com",
    "canva.com", "zoom.com", "atlassian.com", "ycombinator.com", "stripe.com",
    "zendesk.com", "hotstar.com", "reuters.com", "nationalgeographic.com",
    "weebly.com", "behance.net", "dribbble.com", "skype.com", "opera.com",
    "twitch.tv", "stackoverflow.com", "stackoverflow.blog"
}

def send_discord_alert(message):
    if not DISCORD_WEBHOOK_URL:
        return
    try:
        requests.post(DISCORD_WEBHOOK_URL, json={"content": message}, timeout=5)
    except:
        pass

def extract_links(text):
    return re.findall(r'(https?://[^\s)]+)', text)

def normalize_domain(link: str) -> str:
    try:
        parsed = urlparse(link)
        host = parsed.netloc or parsed.path
        return host.replace("www.", "").lower().strip()
    except:
        return ""

def is_domain_soft_available(domain):
    try:
        res = requests.get(f"http://{domain}", timeout=5)
        return False
    except:
        return True

def fetch_shard_if_missing(shard_filename):
    local_path = os.path.join(YOUTUBE8M_SHARDS_FOLDER, shard_filename)
    if os.path.exists(local_path):
        return local_path
    os.makedirs(YOUTUBE8M_SHARDS_FOLDER, exist_ok=True)
    shard_num = shard_filename.replace("train", "").replace(".json", "")
    download_url = f"{YOUTUBE8M_BASE_URL}{shard_num}.json"
    print(f"📥 Downloading shard: {download_url}")
    r = requests.get(download_url, timeout=60)
    with open(local_path, "wb") as f:
        f.write(r.content)
    return local_path

def find_next_shard_and_index():
    progress = supabase.table("YouTube8M_ScanProgress").select("*").order("scanned_at", desc=True).limit(1).execute()
    if not progress.data:
        return "train00.json", 0
    last = progress.data[0]
    if last["completed"]:
        last_shard_num = int(last["shard_file"].replace("train", "").replace(".json", ""))
        next_shard = f"train{str(last_shard_num + 1).zfill(2)}.json"
        return next_shard, 0
    else:
        return last["shard_file"], last["end_index"]

def process_shard_batch(shard_file, start_index):
    new_domains_logged = 0
    videos_processed = 0

    full_path = fetch_shard_if_missing(shard_file)

    with open(full_path, "r") as f:
        data = json.load(f)

    batch = data[start_index:start_index+VIDEOS_PER_RUN]
    if not batch:
        print(f"✅ Finished all videos in shard {shard_file}. Deleting...")
        os.remove(full_path)
        return True  # Finished shard

    for idx, video in enumerate(batch):
        video_id = video.get("id")
        description = video.get("description", "")
        if not video_id or not description:
            continue

        links = extract_links(description)
        videos_processed += 1

        for link in links:
            domain = normalize_domain(link)
            if not domain or len(domain.split(".")) < 2 or domain in WELL_KNOWN_DOMAINS:
                continue
            if is_domain_soft_available(domain):
                record = {
                    "domain": domain,
                    "full_url": link,
                    "video_id": video_id,
                    "video_url": f"https://www.youtube.com/watch?v={video_id}",
                    "is_available": True,
                    "verified": False,
                    "discovered_at": datetime.utcnow().isoformat(),
                    "scanned_at": datetime.utcnow().isoformat(),
                    "view_count": 0
                }
                supabase.table("Clickyleaks").insert(record).execute()
                new_domains_logged += 1
                print(f"🔥 Logged domain: {domain}")

                if new_domains_logged >= MAX_NEW_DOMAINS_PER_RUN:
                    update_progress(shard_file, start_index + idx + 1, videos_processed, new_domains_logged, completed=False)
                    send_discord_alert(f"✅ Found {new_domains_logged} new domains. Stopping batch early.")
                    return False  # Not done with shard, paused early

    update_progress(shard_file, start_index + VIDEOS_PER_RUN, videos_processed, new_domains_logged, completed=False)
    return False

def update_progress(shard_file, end_index, videos_processed, domains_found, completed):
    supabase.table("YouTube8M_ScanProgress").insert({
        "shard_file": shard_file,
        "start_index": end_index - videos_processed,
        "end_index": end_index,
        "scanned_at": datetime.utcnow().isoformat(),
        "videos_processed": videos_processed,
        "domains_found": domains_found,
        "completed": completed
    }).execute()

def main():
    print("🚀 Starting Clickyleaks YouTube8M Shard Scanner...")
    shard_file, start_index = find_next_shard_and_index()
    print(f"📄 Processing shard: {shard_file} starting at index {start_index}")
    finished_shard = process_shard_batch(shard_file, start_index)
    if finished_shard:
        supabase.table("YouTube8M_ScanProgress").insert({
            "shard_file": shard_file,
            "start_index": start_index,
            "end_index": start_index,
            "scanned_at": datetime.utcnow().isoformat(),
            "videos_processed": 0,
            "domains_found": 0,
            "completed": True
        }).execute()
        send_discord_alert(f"✅ Finished scanning entire shard {shard_file}.")

if __name__ == "__main__":
    main()
