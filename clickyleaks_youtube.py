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

# === DISCORD ALERT ===
def send_discord_alert(domain, video):
    if not DISCORD_WEBHOOK_URL:
        return
    msg = f"""
🔥 **Available Domain Found!**

🔗 Domain: `{domain}`
🎬 Video: [{video['video_title']}]({video['video_url']})
👁️ Views: {video['view_count']}
📅 Discovered: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC
"""
    try:
        requests.post(DISCORD_WEBHOOK_URL, json={"content": msg})
    except Exception as e:
        print(f"❌ Failed to send Discord alert: {e}")

# === KEYWORDS ===
KEYWORDS = [
    # Affiliate, Health, Finance, Hyper-Niche etc...
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
    "get free leads fast", "money making hacks online", "turn blog into cashflow",
    
    # 🚨 NEW: Health & Beauty
    "top fat burners for women", "natural weight loss hacks", "hair growth supplements 2024",
    "best skincare routines for acne", "anti-aging serums reviews", "best collagen powder on amazon",
    "top supplements for energy", "dermatologist approved face wash", "hair loss shampoo for men",
    "vegan protein powder reviews", "best eye creams for dark circles", "cheap organic skincare routines",
    "skin tightening devices for home", "non-surgical wrinkle treatments", "best teeth whitening kits amazon",
    "essential oils for hair growth", "biotin vs collagen", "top keto snacks 2024",

    # 💰 NEW: Finance & Insurance
    "best credit cards 2024", "top cashback apps", "open a high interest savings account",
    "student loan refinance options", "crypto tax software reviews", "best stock trading apps",
    "life insurance for beginners", "car insurance comparison 2024", "top no fee bank accounts",
    "cheap health insurance hacks", "best cash advance apps", "how to boost your credit score fast",
    "no annual fee credit card offers", "personal loan affiliate programs", "compare home insurance rates",
    "best budgeting apps", "get paid early with direct deposit", "top fintech apps for 2024",

    # 🎯 Hyper-Niche Long Tails
    "tools for creating digital planners", "sell courses using notion", "best email warmup services",
    "make $5 a day from reddit", "gpt prompts for side hustles", "how to monetize low traffic blog",
    "seo audit tools free", "build newsletter empire with beehiiv", "top chrome extensions for bloggers",
    "sell notion templates online", "make money reskinning apps", "how to flip digital assets",
    "best ai voice generators 2024", "affiliate programs that accept beginners", "ai prompt marketplace ideas",
    "free tools for growing substack", "instagram reel monetization 2024", "how to build ai tools without code"
    
]

BLOCKED_DOMAINS = [
    "amzn.to", "bit.ly", "t.co", "youtube.com", "instagram.com", "linktr.ee",
    "rumble.com", "facebook.com", "twitter.com", "linkedin.com", "paypal.com",
    "discord.gg", "youtu.be"
]

def get_random_published_before():
    days_ago = random.randint(10, 1825)  # Up to 5 years ago
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
        views = int(item["statistics"].get("viewCount", 0))
        if views < 10000:
            return None
        return {
            "title": item["snippet"]["title"],
            "description": item["snippet"]["description"],
            "url": f"https://www.youtube.com/watch?v={video_id}",
            "view_count": views
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
        if is_available:
            send_discord_alert(domain, video_meta)
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

        links = extract_links(details["description"])
        for link in links:
            check_click_leak(link, details, video_id)
            break

        time.sleep(1)

    print("✅ Scan complete.")

if __name__ == "__main__":
    main()
