import os
import re
import pandas as pd
import requests
from datetime import datetime
from urllib.parse import urlparse
from kaggle.api.kaggle_api_extended import KaggleApi
from supabase import create_client, Client

# === CONFIG ===
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")

DATASET_SLUG = "canerkonuk/youtube-trending-videos-global"
TARGET_FILE = "youtube_trending_videos_global.csv"
KAGGLE_FILENAME = "./" + TARGET_FILE

CHUNK_SIZE = 1000        # rows per chunk
MAX_RESULTS = 20         # max videos to process per run

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

def download_dataset():
    print("🚀 Loading YouTube Trending dataset from Kaggle...")
    api = KaggleApi()
    api.authenticate()
    api.dataset_download_file(DATASET_SLUG, TARGET_FILE, path=".", force=True)

def extract_links(text):
    return re.findall(r'(https?://[^\s)]+)', text)

def normalize_domain(url: str) -> str:
    try:
        parsed = urlparse(url)
        host = parsed.netloc or parsed.path
        host = host.replace("www.", "").lower().strip()
        return host
    except:
        return url.lower().strip()

def already_scanned(video_id: str) -> bool:
    res = supabase.table("Clickyleaks_KaggleChecked").select("id").eq("video_id", video_id).execute()
    return len(res.data) > 0

def mark_scanned(video_id: str):
    supabase.table("Clickyleaks_KaggleChecked").insert({
        "video_id": video_id,
        "scanned_at": datetime.utcnow().isoformat()
    }).execute()

def is_domain_available(domain: str) -> bool:
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

def process_row(row):
    video_id = row.get("video_id") or row.get("video_id", "")
    description = str(row.get("description", ""))

    links = extract_links(description)
    for link in links:
        domain = normalize_domain(link)
        if not domain or len(domain.split(".")) < 2:
            continue
        if domain in WELL_KNOWN_DOMAINS:
            print(f"🚫 Skipping well-known domain: {domain}")
            continue
        if is_domain_available(domain):
            print(f"✅ Domain available: {domain}")
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

def process_trending_dataset():
    found = 0
    for chunk in pd.read_csv(KAGGLE_FILENAME, chunksize=CHUNK_SIZE):
        for _, row in chunk.iterrows():
            if found >= MAX_RESULTS:
                return
            if not isinstance(row.get("description"), str):
                continue
            if not re.search(r'https?://', row["description"]):
                continue
            video_id = str(row.get("video_id") or row.get("video_id", "")).strip()
            if already_scanned(video_id):
                continue
            mark_scanned(video_id)
            process_row(row)
            found += 1

def main():
    download_dataset()
    process_trending_dataset()

if __name__ == "__main__":
    main()
