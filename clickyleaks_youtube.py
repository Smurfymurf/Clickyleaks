import requests, time, random, re, os
from urllib.parse import urlparse
from datetime import datetime, timedelta
from supabase import create_client, Client

# === CONFIG ===
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Expanded niche keyword list
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
    "make money on autopilot", "drop servicing 2024", "no face youtube channel ideas",
    "seo for affiliate marketers", "chatgpt affiliate prompts", "get free leads fast",
    "turn blog into cashflow", "best chrome extensions for bloggers", "best paid survey sites",
    "2024 insurance hacks", "best dental insurance plans", "cheap car insurance online",
    "how to refinance credit card debt", "best health supplements for men", "natural skincare products 2024",
    "anti-aging affiliate offers", "top rated nootropics", "best protein powders for weight loss",
    "how to get out of debt fast", "budgeting apps review", "credit repair for beginners",
    "life insurance affiliate programs", "top tax saving strategies", "best apps for investing",
    "top 10 gadgets for remote workers", "tools for freelancers", "cashback apps compared",
    "free hosting deals", "best VPNs for streaming", "fitness tech under $50", "most profitable side hustles",
    "AI tools for writing", "website audit tools", "learn to code for free", "best hair growth supplements",
    "acne skincare reviews", "top digital courses to promote", "insurance lead generation guide",
    "financial affiliate blog ideas", "affiliate seo tutorial", "best web hosting for bloggers",
    "web design toolkits", "launch a Shopify store", "AI email writer", "top Fiverr gigs",
    "AI voiceover tools", "side hustle with zero investment", "webinars to make money",
    "best freelancing sites 2024", "legal templates for bloggers", "top 10 webinar tools",
    "how to become a virtual assistant", "finance YouTube automation ideas", "beauty blog affiliate content"
]

BLOCKED_DOMAINS = [
    "amzn.to", "bit.ly", "t.co", "youtube.com", "instagram.com", "linktr.ee",
    "rumble.com", "facebook.com", "twitter.com", "linkedin.com", "paypal.com",
    "discord.gg", "youtu.be"
]

def get_random_published_before():
    # Random video from 100 to 1825 days ago (approx 5 years)
    days_ago = random.randint(100, 1825)
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

    try:
        requests.get(f"http://{root}", timeout=5)
        return False
    except:
        return True

def already_scanned(video_id):
    result = supabase.table("Clickyleaks").select("id").eq("video_id", video_id).execute()
    return len(result.data) > 0

def send_discord_alert(domain, video_meta):
    if not DISCORD_WEBHOOK_URL:
        return

    content = f"🔥 **New Available Domain Found!**\n\n🔗 Domain: `{domain}`\n🎥 Video: [{video_meta['title']}]({video_meta['url']})\n👁️ Views: {video_meta['view_count']}"
    try:
        requests.post(DISCORD_WEBHOOK_URL, json={"content": content}, timeout=10)
    except Exception as e:
        print(f"⚠️ Discord alert failed: {e}")

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
        if is_available:
            send_discord_alert(domain, video_meta)
    except Exception as e:
        print(f"⚠️ Skipped duplicate video_id insert: {e}")

def main():
    print("🚀 Clickyleaks scan started...")

    results = []
    attempts = 0

    while attempts < 3 and not results:
        keyword = random.choice(KEYWORDS)
        print(f"🔎 Searching: {keyword}")
        results = search_youtube(keyword)
        attempts += 1

    if not results:
        print("❌ No results found after 3 attempts.")
        return

    random.shuffle(results)

    for result in results:
        video_id = result["id"]["videoId"]
        if already_scanned(video_id):
            continue

        details = get_video_details(video_id)
        if not details or details["view_count"] < 10000:
            continue

        links = extract_links(details["description"])
        for link in links:
            check_click_leak(link, details, video_id)
            break

        time.sleep(1)

    print("✅ Scan complete.")

if __name__ == "__main__":
    main()
