import requests, whois, time
from urllib.parse import urlparse
from datetime import datetime
from supabase import create_client, Client
import os
from googleapiclient.discovery import build

# === CONFIG ===
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
KEYWORDS = ["crypto wallet", "keto diet", "credit repair", "free stocks", "ai tools"]

# Domains to ignore (known platforms, shorteners, redirectors)
BLOCKED_DOMAINS = [
    "amzn.to", "bit.ly", "t.co", "youtube.com", "instagram.com",
    "linktr.ee", "rumble.com", "facebook.com", "twitter.com",
    "linkedin.com", "paypal.com", "discord.gg", "youtu.be"
]

# === SETUP ===
youtube = build("youtube", "v3", developerKey=YOUTUBE_API_KEY)
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def search_youtube_videos(query, max_results=10):
    request = youtube.search().list(
        q=query,
        part="snippet",
        maxResults=max_results,
        type="video"
    )
    response = request.execute()
    return response.get("items", [])

def get_video_details(video_id):
    request = youtube.videos().list(
        part="snippet,statistics",
        id=video_id
    )
    response = request.execute()
    items = response.get("items", [])
    if items:
        item = items[0]
        return {
            "title": item["snippet"]["title"],
            "description": item["snippet"]["description"],
            "url": f"https://www.youtube.com/watch?v={video_id}",
            "view_count": int(item["statistics"].get("viewCount", 0))
        }
    return None

def extract_links(text):
    import re
    return re.findall(r'(https?://[^\s)]+)', text)

def is_domain_available(domain):
    try:
        info = whois.whois(domain)
        return not bool(info.domain_name)
    except:
        return True

def check_click_leak(link, video_meta):
    domain = urlparse(link).netloc.lower()

    # Skip blocked domains
    if any(domain.endswith(bad) for bad in BLOCKED_DOMAINS):
        return

    try:
        response = requests.head(link, timeout=5, allow_redirects=True)
        status = response.status_code
    except:
        status = 0

    is_available = is_domain_available(domain)

    if status in [404, 410, 0] or is_available:
        print(f"🧨 Leak found: {domain} from {video_meta['title']}")
        record = {
            "domain": domain,
            "full_url": link,
            "video_title": video_meta["title"],
            "video_url": video_meta["url"],
            "http_status": status,
            "is_available": is_available,
            "view_count": video_meta["view_count"],
            "discovered_at": datetime.utcnow().isoformat()
        }
        # Prevent duplicates
        existing = supabase.table("Clickyleaks").select("id").eq("domain", domain).eq("video_url", video_meta["url"]).execute()
        if len(existing.data) == 0:
            supabase.table("Clickyleaks").insert(record).execute()

def main():
    print("🚀 Clickyleaks scan started...")
    for keyword in KEYWORDS:
        print(f"🔎 Searching: {keyword}")
        results = search_youtube_videos(keyword, max_results=10)
        for video in results:
            video_id = video["id"]["videoId"]
            details = get_video_details(video_id)
            if not details:
                continue
            links = extract_links(details["description"])
            for link in links:
                check_click_leak(link, details)
            time.sleep(1)
    print("✅ Scan complete.")

if __name__ == "__main__":
    main()