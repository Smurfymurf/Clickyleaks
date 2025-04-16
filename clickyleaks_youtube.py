import requests, time, random, re, socket
from urllib.parse import urlparse
from datetime import datetime, timedelta
from supabase import create_client, Client
import os
import traceback

# === CONFIG ===
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

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

def send_discord_message(message):
    try:
        requests.post(DISCORD_WEBHOOK_URL, json={"content": message}, timeout=5)
    except Exception as e:
        print(f"⚠️ Discord webhook failed: {e}")

def get_random_published_before():
    days_ago = random.randint(10, 3650)
    date = datetime.utcnow() - timedelta(days=days_ago)
    return date.isoformat("T") + "Z"

def search_youtube(query, max_pages=5):
    videos = []
    published_before = get_random_published_before()
    page_token = None

    for _ in range(max_pages):
        url = "https://www.googleapis.com/youtube/v3/search"
        params = {
            "part": "snippet",
            "q": query,
            "type": "video",
            "maxResults": 10,
            "order": "relevance",
            "publishedBefore": published_before,
            "key": YOUTUBE_API_KEY
        }
        if page_token:
            params["pageToken"] = page_token

        res = requests.get(url, params=params, timeout=10)
        data = res.json()
        items = data.get("items", [])
        videos.extend(items)

        page_token = data.get("nextPageToken")
        if not page_token:
            break

    return videos

def get_video_details(video_id):
    url = "https://www.googleapis.com/youtube/v3/videos"
    params = {
        "part": "snippet,statistics",
        "id": video_id,
        "key": YOUTUBE_API_KEY
    }
    res = requests.get(url, params=params, timeout=10)
    items = res.json().get("items", [])
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
    return re.findall(r'(https?://[^\s)]+)', text)

def already_scanned_video(video_id):
    result = supabase.table("Clickyleaks").select("id").eq("video_id", video_id).execute()
    return len(result.data) > 0

def already_scanned_domain(domain):
    result = supabase.table("Clickyleaks").select("id").eq("domain", domain).execute()
    return len(result.data) > 0

def is_domain_available(domain):
    try:
        socket.setdefaulttimeout(3)
        socket.gethostbyname(domain)
        return False  # If DNS resolves, domain is taken
    except socket.error:
        return True

def log_click_leak(domain, link, video_meta, video_id, status, available):
    print(f"🔴 Logging domain: {domain} (Available: {available})")
    record = {
        "domain": domain,
        "full_url": link,
        "video_id": video_id,
        "video_title": video_meta["title"],
        "video_url": video_meta["url"],
        "http_status": status,
        "is_available": available,
        "view_count": video_meta["view_count"],
        "discovered_at": datetime.utcnow().isoformat(),
        "scanned_at": datetime.utcnow().isoformat()
    }

    try:
        supabase.table("Clickyleaks").insert(record).execute()
    except Exception as e:
        print(f"⚠️ Insert error: {e}")

def process_link(link, video_meta, video_id, available_domains):
    domain = urlparse(link).netloc.lower().replace("www.", "")
    if any(domain.endswith(bad) for bad in BLOCKED_DOMAINS):
        return

    if already_scanned_domain(domain):
        print(f"⚠️ Skipping already-scanned domain: {domain}")
        return

    try:
        status = requests.head(link, timeout=5, allow_redirects=True).status_code
    except:
        status = 0

    is_available = is_domain_available(domain)
    log_click_leak(domain, link, video_meta, video_id, status, is_available)

    if is_available:
        available_domains.append(f"🟢 `{domain}` - {video_meta['title']}")

def main():
    try:
        print("🚀 Clickyleaks scan started...")
        keyword = random.choice(KEYWORDS)
        print(f"🔎 Searching: {keyword}")
        results = search_youtube(keyword)

        if not results:
            print("No results found.")
            send_discord_message(f"⚠️ Clickyleaks scan failed. No YouTube results for keyword: `{keyword}`.")
            return

        random.shuffle(results)
        available_domains = []

        for result in results:
            video_id = result["id"]["videoId"]
            if already_scanned_video(video_id):
                continue

            video_meta = get_video_details(video_id)
            if not video_meta:
                continue

            links = extract_links(video_meta["description"])
            for link in links:
                process_link(link, video_meta, video_id, available_domains)
                break  # Only check first link per video

            time.sleep(1)

        if available_domains:
            message = "**✅ Clickyleaks Scan Complete — Available Domains Found!**\n" + "\n".join(available_domains)
        else:
            message = "✅ Clickyleaks scan complete. No available domains found."

        send_discord_message(message)
        print("✅ Scan finished.")

    except Exception as e:
        error_details = traceback.format_exc()
        print(f"❌ Script error: {e}")
        send_discord_message(f"❌ Clickyleaks scan failed with error:\n```\n{error_details[:1800]}\n```")

if __name__ == "__main__":
    main()
