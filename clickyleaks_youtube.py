import requests, time, random, re
from urllib.parse import urlparse
from datetime import datetime, timedelta
from supabase import create_client, Client
import os

# === CONFIG ===
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

KEYWORDS = [
    "best crypto wallets for beginners", "affiliate landing page examples", "how to make money from home 2024",
    "passive income with AI", "weight loss affiliate programs", "best ai tools for content creation",
    "cheapest domain hosting", "2023 clickbank tutorial", "earn bitcoin free", "cheap website builder reviews",
    "fat burner amazon review", "affiliate link cloaking tutorial", "how to create a sales funnel",
    "ai money making methods", "keto diet affiliate", "buy expired domains tutorial",
    "best hosting for affiliate websites", "get paid to write articles", "ai to make money online",
    "how to use chatgpt for side income", "top clickbank products 2024", "high commission affiliate offers",
    "best landing page builders 2024", "youtube automation money", "email marketing for beginners",
    "free crypto airdrops", "make $100/day with AI", "easy affiliate programs to join",
    "viral giveaway tool", "print on demand affiliate tips", "fastest way to grow email list",
    "how to start an affiliate blog", "best ad networks for bloggers", "top weight loss offers 2024",
    "free course hosting platform", "ai tools for dropshipping", "buying backlinks tutorial",
    "affiliate tiktok page", "how to promote affiliate links on Reddit", "high traffic expired domains",
    "best AI content tools", "make money on autopilot", "drop servicing 2024", "no face youtube channel ideas",
    "best niches for affiliate", "seo for affiliate marketers", "chatgpt affiliate prompts",
    "get free leads fast", "money making hacks online", "turn blog into cashflow"
]

BLOCKED_DOMAINS = [
    "amzn.to", "bit.ly", "t.co", "youtube.com", "instagram.com", "linktr.ee",
    "rumble.com", "facebook.com", "twitter.com", "linkedin.com", "paypal.com",
    "discord.gg", "youtu.be"
]

def get_random_published_before():
    # 5 years = 1825 days
    days_ago = random.randint(10, 1825)
    date = datetime.utcnow() - timedelta(days=days_ago)
    return date.isoformat("T") + "Z"

def search_youtube(query, max_pages=10):
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

def is_domain_available(domain):
    root = domain.lower().strip()
    if root.startswith("www."):
        root = root[4:]
    root = root.split("/")[0]

    headers = {
        "Accept": "application/json"
    }

    try:
        res = requests.get(f"http://{root}", timeout=5)
        return False
    except:
        return True

def already_scanned(video_id):
    result = supabase.table("Clickyleaks").select("id").eq("video_id", video_id).execute()
    return len(result.data) > 0

def check_click_leak(link, video_meta, video_id):
    domain = urlparse(link).netloc.lower()
    if any(domain.endswith(bad) for bad in BLOCKED_DOMAINS):
        return

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
        print(f"⚠️ Skipped duplicate video_id insert: {e}")

def main():
    print("🚀 Clickyleaks scan started...")
    keyword = random.choice(KEYWORDS)
    print(f"🔎 Searching: {keyword}")
    results = search_youtube(keyword)

    if not results:
        print("No results found.")
        return

    random.shuffle(results)

    for result in results:
        video_id = result["id"]["videoId"]
        if already_scanned(video_id):
            continue

        details = get_video_details(video_id)
        if not details:
            continue

        # Skip videos under 10k views
        if details["view_count"] < 10000:
            print(f"⚠️ Skipping low-view video ({details['view_count']} views)")
            continue

        links = extract_links(details["description"])
        for link in links:
            check_click_leak(link, details, video_id)
            break

        time.sleep(1)

    print("✅ Scan complete.")

if __name__ == "__main__":
    main()
