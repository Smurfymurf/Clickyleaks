import requests, re, random, time
from urllib.parse import urlparse
from datetime import datetime
from supabase import create_client, Client
import os

# === CONFIG ===
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")  # Optional for enrichment

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

BLOCKED_DOMAINS = [
    "amzn.to", "bit.ly", "t.co", "youtube.com", "instagram.com", "linktr.ee",
    "rumble.com", "facebook.com", "twitter.com", "linkedin.com", "paypal.com",
    "discord.gg", "youtu.be"
]

YOUTUBE_TRENDING = "https://www.youtube.com/feed/trending"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}

def get_random_youtube_video():
    try:
        res = requests.get("https://ytroulette.com/", headers=HEADERS, timeout=10)
        matches = re.findall(r"youtube\.com/watch\?v=([\w-]{11})", res.text)
        if matches:
            return random.choice(matches)
    except Exception as e:
        print(f"❌ Failed to get random video: {e}")
    return None

def get_video_description(video_id):
    try:
        url = f"https://www.youtube.com/watch?v={video_id}"
        html = requests.get(url, headers=HEADERS, timeout=10).text
        match = re.search(r'description":{"simpleText":"(.*?)"', html)
        if match:
            return match.group(1).encode('utf-8').decode('unicode_escape')
    except:
        pass
    return ""

def extract_links(text):
    return re.findall(r'(https?://[\w\.-]+)', text)

def is_domain_available(domain):
    try:
        requests.get(f"http://{domain}", timeout=5)
        return False
    except:
        return True

def already_logged(video_id, domain):
    res = supabase.table("Clickyleaks").select("id").match({"video_id": video_id, "domain": domain}).execute()
    return len(res.data) > 0

def send_discord_alert(domain, video_id, video_url):
    message = {
        "content": f"🔥 **Available Domain Found!** `{domain}`\nVideo: {video_url}"
    }
    try:
        requests.post(DISCORD_WEBHOOK_URL, json=message, timeout=10)
    except:
        pass

def enrich_video_data(video_id):
    try:
        url = "https://www.googleapis.com/youtube/v3/videos"
        params = {
            "part": "snippet,statistics",
            "id": video_id,
            "key": YOUTUBE_API_KEY
        }
        res = requests.get(url, params=params, timeout=10)
        data = res.json()
        if data.get("items"):
            item = data["items"][0]
            return {
                "video_title": item["snippet"]["title"],
                "view_count": int(item["statistics"].get("viewCount", 0))
            }
    except:
        pass
    return {"video_title": None, "view_count": None}

def run():
    print("🚀 Clickyleaks Random Scanner Started...")
    attempts = 0
    found = False

    while attempts < 10 and not found:
        video_id = get_random_youtube_video()
        if not video_id:
            attempts += 1
            continue

        video_url = f"https://www.youtube.com/watch?v={video_id}"
        description = get_video_description(video_id)
        links = extract_links(description)

        for link in links:
            domain = urlparse(link).netloc.lower()
            if any(domain.endswith(bad) for bad in BLOCKED_DOMAINS):
                continue

            if already_logged(video_id, domain):
                continue

            available = is_domain_available(domain)
            print(f"🔍 {domain} (Available: {available})")

            enriched = enrich_video_data(video_id) if YOUTUBE_API_KEY else {"video_title": None, "view_count": None}

            record = {
                "domain": domain,
                "full_url": link,
                "video_id": video_id,
                "video_url": video_url,
                "video_title": enriched["video_title"],
                "view_count": enriched["view_count"],
                "is_available": available,
                "discovered_at": datetime.utcnow().isoformat(),
                "scanned_at": datetime.utcnow().isoformat()
            }

            supabase.table("Clickyleaks").insert(record).execute()

            if available:
                send_discord_alert(domain, video_id, video_url)
                found = True
                break

        attempts += 1
        time.sleep(2)

    if not found:
        print("❌ No available domains found after 10 attempts.")

if __name__ == "__main__":
    run()
