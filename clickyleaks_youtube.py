import requests, time, random, re, os
from urllib.parse import urlparse
from datetime import datetime, timedelta
from supabase import create_client, Client

# === CONFIG ===
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
GODADDY_API_KEY = os.getenv("GODADDY_API_KEY")
GODADDY_API_SECRET = os.getenv("GODADDY_API_SECRET")
DISCORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK")

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

def get_random_published_before():
    days_ago = random.randint(10, 3650)
    return (datetime.utcnow() - timedelta(days=days_ago)).isoformat("T") + "Z"

def search_youtube(query, max_pages=5):
    videos = []
    published_before = get_random_published_before()
    page_token = None

    for _ in range(max_pages):
        res = requests.get(
            "https://www.googleapis.com/youtube/v3/search",
            params={
                "part": "snippet", "q": query, "type": "video", "maxResults": 10,
                "order": "relevance", "publishedBefore": published_before, "key": YOUTUBE_API_KEY,
                "pageToken": page_token or ""
            },
            timeout=10
        )
        data = res.json()
        items = data.get("items", [])
        videos.extend(items)
        page_token = data.get("nextPageToken")
        if not page_token:
            break

    return videos

def get_video_details(video_id):
    res = requests.get(
        "https://www.googleapis.com/youtube/v3/videos",
        params={
            "part": "snippet,statistics", "id": video_id, "key": YOUTUBE_API_KEY
        },
        timeout=10
    )
    items = res.json().get("items", [])
    if not items:
        return None
    item = items[0]
    return {
        "title": item["snippet"]["title"],
        "description": item["snippet"]["description"],
        "url": f"https://www.youtube.com/watch?v={video_id}",
        "view_count": int(item["statistics"].get("viewCount", 0))
    }

def extract_links(text):
    return re.findall(r'(https?://[^\s)]+)', text)

def is_domain_available(domain):
    root = domain.lower().strip().replace("www.", "").split("/")[0]
    headers = {
        "Authorization": f"sso-key {GODADDY_API_KEY}:{GODADDY_API_SECRET}",
        "Accept": "application/json"
    }
    try:
        res = requests.get(
            f"https://api.godaddy.com/v1/domains/available?domain={root}",
            headers=headers,
            timeout=6
        )
        if res.status_code == 200:
            return res.json().get("available", False)
        else:
            print(f"⚠️ GoDaddy error {res.status_code} for {root}")
    except Exception as e:
        print(f"❌ GoDaddy exception: {e}")
    return False

def already_scanned(video_id):
    result = supabase.table("Clickyleaks").select("id").eq("video_id", video_id).execute()
    return len(result.data) > 0

def check_click_leak(link, video_meta, video_id):
    domain = urlparse(link).netloc.lower()
    if any(domain.endswith(bad) for bad in BLOCKED_DOMAINS):
        return None

    try:
        status = requests.head(link, timeout=5, allow_redirects=True).status_code
    except:
        status = 0

    is_available = is_domain_available(domain)
    print(f"🔍 Logging: {domain} (Available: {is_available})")

    record = {
        "domain": domain,
        "full_url": link,
        "video_id": video_id,
        "video_title": video_meta["title"],
        "video_url": video_meta["url"],
        "http_status": status,
        "is_available": is_available,
        "view_count": video_meta["view_count"],
        "discovered_at": datetime.utcnow().isoformat(),
        "scanned_at": datetime.utcnow().isoformat()
    }

    try:
        supabase.table("Clickyleaks").insert(record).execute()
    except Exception as e:
        print(f"⚠️ Skipped insert: {e}")

    if is_available:
        return domain
    return None

def send_discord_notification(message):
    if DISCORD_WEBHOOK:
        try:
            requests.post(DISCORD_WEBHOOK, json={"content": message}, timeout=10)
        except Exception as e:
            print(f"⚠️ Discord webhook failed: {e}")

def main():
    start_time = datetime.utcnow()
    found_domains = []
    try:
        print("🚀 Clickyleaks scan started...")
        keyword = random.choice(KEYWORDS)
        print(f"🔎 Searching: {keyword}")
        results = search_youtube(keyword)

        if not results:
            send_discord_notification("❌ Clickyleaks scan failed: No YouTube results.")
            return

        random.shuffle(results)

        for result in results:
            video_id = result["id"]["videoId"]
            if already_scanned(video_id):
                continue

            details = get_video_details(video_id)
            if not details:
                continue

            links = extract_links(details["description"])
            for link in links:
                found = check_click_leak(link, details, video_id)
                if found:
                    found_domains.append(found)
                break  # Only one link per video for now

            time.sleep(1)

        duration = (datetime.utcnow() - start_time).seconds
        if found_domains:
            send_discord_notification(f"✅ Clickyleaks finished in {duration}s. Found {len(found_domains)} available domains:\n" + "\n".join(found_domains))
        else:
            send_discord_notification(f"✅ Clickyleaks finished in {duration}s. No available domains found.")

    except Exception as e:
        send_discord_notification(f"❌ Clickyleaks crashed: {str(e)}")

if __name__ == "__main__":
    main()
