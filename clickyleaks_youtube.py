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

KEYWORDS = [
    # Existing + expanded list
    "best crypto wallets for beginners", "affiliate landing page examples", "how to make money from home 2024",
    "passive income with AI", "weight loss affiliate programs", "best ai tools for content creation",
    "cheapest domain hosting", "2023 clickbank tutorial", "earn bitcoin free", "cheap website builder reviews",
    "fat burner amazon review", "affiliate link cloaking tutorial", "how to create a sales funnel",
    "ai money making methods", "keto diet affiliate", "buy expired domains tutorial",
    "best hosting for affiliate websites", "get paid to write articles", "ai to make money online",
    "top clickbank products 2024", "email marketing for beginners", "make $100/day with AI",
    "viral giveaway tool", "easy affiliate programs to join", "best niches for affiliate",
    "best ai content tools", "chatgpt affiliate prompts", "money making hacks online",
    "top weight loss products", "how to promote affiliate links on Reddit", "high traffic expired domains",
    "insurance affiliate programs", "best health supplements 2024", "anti aging skincare review",
    "best protein powders for weight loss", "credit card comparison site", "life insurance explained",
    "ultimate guide to dropshipping", "how to find cheap car insurance", "top 10 finance books",
    "best personal loan platforms", "top AI stocks to watch", "best passive income websites",
    "top 10 collagen supplements", "nootropics ranking", "top budgeting apps", "best hair regrowth oils",
    "acne skincare routines", "get free travel insurance", "best vitamins for women", "affiliate websites making money",
    "AI tools for productivity", "how to flip domains for profit", "how to get paid surveys",
    "best SaaS affiliate programs", "crypto airdrop strategies", "make money with chatbots",
    "clickfunnels review 2024", "lead generation strategies", "best marketing courses for beginners"
]

BLOCKED_DOMAINS = [
    "amzn.to", "bit.ly", "t.co", "youtube.com", "instagram.com", "linktr.ee",
    "rumble.com", "facebook.com", "twitter.com", "linkedin.com", "paypal.com",
    "discord.gg", "youtu.be"
]

def get_random_published_before():
    days_ago = random.randint(10, 1825)  # Max 5 years
    date = datetime.utcnow() - timedelta(days=days_ago)
    return date.isoformat("T") + "Z"

def search_youtube(query, max_pages=10):
    videos = []
    published_before = get_random_published_before()
    page_token = None

    for page in range(max_pages):
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

        print(f"📡 Requesting page {page+1} for keyword: {query}")
        print(f"🔗 URL: {url}")
        print(f"📦 Params: {params}")

        try:
            res = requests.get(url, params=params, timeout=10)
            if res.status_code != 200:
                print(f"❌ API error: {res.status_code} — {res.text}")
                return []

            data = res.json()
            items = data.get("items", [])
            print(f"📥 {len(items)} videos found in this page.")
            videos.extend(items)
            page_token = data.get("nextPageToken")
            if not page_token:
                break

        except Exception as e:
            print(f"❌ Exception in YouTube search: {e}")
            return []

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
        view_count = int(item["statistics"].get("viewCount", 0))
        if view_count < 10000:
            return None
        return {
            "title": item["snippet"]["title"],
            "description": item["snippet"]["description"],
            "url": f"https://www.youtube.com/watch?v={video_id}",
            "view_count": view_count
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

    supabase.table("Clickyleaks").insert(record).execute()

    if is_available and DISCORD_WEBHOOK_URL:
        requests.post(DISCORD_WEBHOOK_URL, json={"content": f"🔥 **Available domain found!**\n🔗 `{domain}`\n🎥 {video_meta['title']}"})

def main():
    print("🚀 Clickyleaks scan started...")

    MAX_RETRIES = 3
    results = []
    attempt = 0

    while not results and attempt < MAX_RETRIES:
        keyword = random.choice(KEYWORDS)
        print(f"🔎 Attempt {attempt + 1}: Searching for '{keyword}'")
        results = search_youtube(keyword)
        attempt += 1

    if not results:
        print("No results found after retries.")
        if DISCORD_WEBHOOK_URL:
            requests.post(DISCORD_WEBHOOK_URL, json={"content": "⚠️ Clickyleaks YouTube ran but found *no results* after 3 keyword retries."})
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
