
import requests, time, random
from urllib.parse import urlparse
from datetime import datetime
from supabase import create_client, Client
import os
from googleapiclient.discovery import build

# === CONFIG ===
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
GODADDY_API_KEY = os.getenv("GODADDY_API_KEY")
GODADDY_API_SECRET = os.getenv("GODADDY_API_SECRET")

# === Setup Supabase + YouTube API
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
youtube = build("youtube", "v3", developerKey=YOUTUBE_API_KEY)

# === Affiliate-style keyword list
KEYWORDS = [
    "crypto wallet", "how to buy bitcoin", "passive income ideas", "affiliate marketing tutorial",
    "make money online", "credit repair tricks", "get out of debt", "keto diet plan", "intermittent fasting",
    "fat burners", "how to lose belly fat", "greens powder review", "natural supplements", "ai tools for work",
    "make money with chatgpt", "ai video generator", "top ai websites", "best copywriting course", "learn coding free",
    "productivity apps", "best chrome extensions", "amazon coupon hacks", "cashback app reviews", "drop shipping tutorial"
]

BLOCKED_DOMAINS = [
    "amzn.to", "bit.ly", "t.co", "youtube.com", "instagram.com", "linktr.ee",
    "rumble.com", "facebook.com", "twitter.com", "linkedin.com", "paypal.com",
    "discord.gg", "youtu.be"
]

def search_youtube(query):
    res = youtube.search().list(q=query, part="snippet", maxResults=10, type="video").execute()
    return res.get("items", [])

def get_video_details(video_id):
    res = youtube.videos().list(part="snippet,statistics", id=video_id).execute()
    items = res.get("items", [])
    if items:
        item = items[0]
        return {
            "title": item["snippet"]["title"],
            "description": item["snippet"]["description"],
            "url": f"https://www.youtube.com/watch?v={video_id}",
            "view_count": int(item["statistics"].get("viewCount", 0))
        }
    return None

def get_related_videos(video_id):
    res = youtube.search().list(relatedToVideoId=video_id, part="snippet", type="video", maxResults=10).execute()
    return res.get("items", [])

def extract_links(text):
    import re
    return re.findall(r'(https?://[^\s)]+)', text)

def is_domain_available(domain):
    headers = {
        "Authorization": f"sso-key {GODADDY_API_KEY}:{GODADDY_API_SECRET}",
        "Accept": "application/json"
    }
    try:
        res = requests.get(f"https://api.godaddy.com/v1/domains/available?domain={domain}", headers=headers, timeout=5)
        if res.status_code == 200:
            return res.json().get("available", False)
    except Exception as e:
        print(f"[GoDaddy Error] {e}")
    return False

def check_click_leak(link, video_meta):
    domain = urlparse(link).netloc.lower()
    if any(domain.endswith(bad) for bad in BLOCKED_DOMAINS):
        return

    try:
        status = requests.head(link, timeout=5, allow_redirects=True).status_code
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
        existing = supabase.table("Clickyleaks").select("id").eq("domain", domain).eq("video_url", video_meta["url"]).execute()
        if len(existing.data) == 0:
            supabase.table("Clickyleaks").insert(record).execute()

def main():
    print("🚀 Clickyleaks scan started...")
    keyword = random.choice(KEYWORDS)
    print(f"🔎 Searching: {keyword}")
    results = search_youtube(keyword)

    if not results:
        print("No results found.")
        return

    picked = random.choice(results)
    video_id = picked["id"]["videoId"]
    details = get_video_details(video_id)
    if not details:
        return

    related = get_related_videos(video_id)

    for vid in related:
        vid_id = vid["id"]["videoId"]
        info = get_video_details(vid_id)
        if not info:
            continue
        links = extract_links(info["description"])
        for link in links:
            check_click_leak(link, info)
        time.sleep(1)

    print("✅ Scan complete.")

if __name__ == "__main__":
    main()
