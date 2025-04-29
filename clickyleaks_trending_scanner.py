# Place this in a file called clickyleaks_trending_scanner.py

import os, re, requests
from urllib.parse import urlparse
from datetime import datetime
from supabase import create_client, Client
import pandas as pd
import kagglehub
from kagglehub import KaggleDatasetAdapter

# === CONFIG ===
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

WELL_KNOWN_DOMAINS = {
    "apple.com", "google.com", "facebook.com", "amazon.com", "youtube.com",
    "microsoft.com", "netflix.com", "instagram.com", "paypal.com", "reddit.com",
    "wikipedia.org", "tumblr.com", "github.com", "linkedin.com", "spotify.com",
    "cnn.com", "bbc.com", "dropbox.com", "airbnb.com", "salesforce.com",
    "tiktok.com", "ebay.com", "zoom.us", "whatsapp.com", "nytimes.com",
    "oracle.com", "bing.com", "slack.com", "notion.so", "wordpress.com",
    "vercel.app", "netlify.app", "figma.com", "medium.com", "shopify.com"
}

def extract_links(text):
    return re.findall(r'(https?://[^\s)]+)', text)

def normalize_domain(domain: str) -> str:
    try:
        parsed = urlparse(domain)
        host = parsed.netloc or parsed.path
        host = host.replace("www.", "").lower().strip()
        return host
    except:
        return domain.lower()

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

def process_video(video):
    video_id = str(video.get("video_id"))
    if not video_id or already_scanned(video_id):
        return
    mark_video_scanned(video_id)

    description = str(video.get("description", ""))
    links = extract_links(description)

    for link in links:
        domain = normalize_domain(link)
        if not domain or len(domain.split(".")) < 2 or domain in WELL_KNOWN_DOMAINS:
            print(f"🚫 Skipping domain: {domain}")
            continue

        available = is_domain_available(domain)
        print(f"🔍 Domain: {domain} | Available: {available}")
        if available:
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
    print("🚀 Loading YouTube Trending dataset from Kaggle...")
    df = kagglehub.load_dataset(
        KaggleDatasetAdapter.PANDAS,
        "canerkonuk/youtube-trending-videos-global",
        ""
    )

    for _, row in df.iterrows():
        if "http" in str(row.get("description", "")).lower():
            process_video(row)

    print("✅ Done scanning trending videos.")

if __name__ == "__main__":
    main()
