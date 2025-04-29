import os
import re
import requests
import pandas as pd
from urllib.parse import urlparse
from datetime import datetime
from supabase import create_client, Client
from kaggle.api.kaggle_api_extended import KaggleApi

# === CONFIG ===
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")
DATASET = "canerkonuk/youtube-trending-videos-global"
FILE_NAME = "US_youtube_trending_data.csv"
DOWNLOAD_DIR = "kaggle_trending"

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

def normalize_domain(domain: str) -> str:
    try:
        parsed = urlparse(domain)
        host = parsed.netloc or parsed.path
        host = host.replace("www.", "").lower().strip()
        return host
    except:
        return domain.lower()

def extract_links(text):
    return re.findall(r'(https?://[^\s)]+)', text)

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
        requests.get(f"http://{domain}", timeout=5)
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

def process_video(video):
    video_id = video["video_id"]
    description = video.get("description", "")

    if already_scanned(video_id):
        return

    if "http" not in description:
        return

    mark_video_scanned(video_id)
    links = extract_links(description)

    for link in links:
        try:
            domain = normalize_domain(link)
            if not domain or len(domain.split(".")) < 2:
                continue
            if domain in WELL_KNOWN_DOMAINS:
                print(f"🚫 Skipping well-known domain: {domain}")
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
    print("🚀 Downloading YouTube trending dataset...")

    api = KaggleApi()
    api.authenticate()

    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    api.dataset_download_file(DATASET, FILE_NAME, path=DOWNLOAD_DIR, force=True)

    csv_path = os.path.join(DOWNLOAD_DIR, FILE_NAME)
    if not csv_path.endswith(".csv"):
        csv_path += ".zip"
        import zipfile
        with zipfile.ZipFile(csv_path, 'r') as zip_ref:
            zip_ref.extractall(DOWNLOAD_DIR)
        csv_path = os.path.join(DOWNLOAD_DIR, FILE_NAME)

    df = pd.read_csv(csv_path)
    print(f"✅ Loaded {len(df)} trending videos")

    for _, row in df.iterrows():
        video = {
            "video_id": row.get("video_id"),
            "description": row.get("description", "")
        }
        process_video(video)

    print("✅ Trending scan complete.")

if __name__ == "__main__":
    main()
