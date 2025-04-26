import os
import asyncio
import random
import time
import requests
import re
from urllib.parse import urlparse, parse_qs
from supabase import create_client, Client
from playwright.async_api import async_playwright

# === CONFIG ===
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
DOMAINR_API_KEY = os.getenv("DOMAINR_API_KEY")
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")

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

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; rv:109.0) Gecko/20100101 Firefox/118.0",
]

def normalize_domain(domain: str) -> str:
    try:
        if not domain.startswith("http"):
            domain = "http://" + domain
        parsed = urlparse(domain)
        host = parsed.netloc or parsed.path
        host = host.replace("www.", "")
        host = re.sub(r'[^a-zA-Z0-9.-]', '', host)  # Remove bad characters
        parts = host.split(".")
        return ".".join(parts[-2:]) if len(parts) >= 2 else host
    except:
        return domain

def is_valid_domain(domain: str) -> bool:
    if not domain or len(domain) < 4:
        return False
    if domain.count(".") != 1:
        return False
    if domain.endswith((".", "-", "_")):
        return False
    if any(c in domain for c in " ,;/\\"):
        return False
    return True

def extract_video_id(url_or_id: str) -> str:
    if "youtube.com/watch" in url_or_id:
        return parse_qs(urlparse(url_or_id).query).get("v", [""])[0]
    if "youtu.be/" in url_or_id:
        return url_or_id.split("/")[-1].split("?")[0]
    return url_or_id

def check_domain_domainr(domain: str, retries=2) -> bool:
    root = normalize_domain(domain)
    url = f"https://domainr.p.rapidapi.com/v2/status?domain={root}"
    headers = {
        "X-RapidAPI-Key": DOMAINR_API_KEY,
        "X-RapidAPI-Host": "domainr.p.rapidapi.com"
    }

    for attempt in range(retries + 1):
        try:
            res = requests.get(url, headers=headers, timeout=10)
            data = res.json()
            if "message" in data:
                msg = data["message"].lower()
                if "invalid api key" in msg:
                    print("❌ Invalid Domainr API key.")
                    return None
                if "too many requests" in msg:
                    wait = 10 + attempt * 5
                    print(f"⚠️ Rate limited – sleeping {wait}s...")
                    time.sleep(wait)
                    continue
            if "status" in data and data["status"]:
                status = data["status"][0]["status"]
                return "inactive" in status or "undelegated" in status
            print(f"❌ Unexpected Domainr response: {data}")
            return None
        except Exception as e:
            print(f"❌ Domainr error: {e}")
            time.sleep(5)
    return None

def fetch_video_data_from_api(video_id):
    url = f"https://www.googleapis.com/youtube/v3/videos?part=snippet,statistics&id={video_id}&key={YOUTUBE_API_KEY}"
    try:
        res = requests.get(url)
        data = res.json()
        items = data.get("items", [])
        if not items:
            return None, 0
        snippet = items[0]["snippet"]
        stats = items[0].get("statistics", {})
        title = snippet.get("title", "Unknown")
        views = int(stats.get("viewCount", 0))
        return title, views
    except:
        return None, 0

async def try_goto(page, url, retries=2):
    for attempt in range(retries + 1):
        try:
            await asyncio.sleep(random.uniform(1.0, 3.0))
            await page.goto(url, timeout=20000, wait_until="domcontentloaded")
            await page.evaluate("window.scrollBy(0, 300);")
            await page.wait_for_timeout(2000)
            return True
        except Exception as e:
            print(f"⚠️ Attempt {attempt + 1} failed: {e}")
            await asyncio.sleep(1.5 + attempt)
    return False

async def process_row(row, page):
    domain = row["domain"]
    root_domain = normalize_domain(domain)
    video_url = row["video_url"]
    video_id = extract_video_id(row.get("video_id") or video_url)
    row_id = row["id"]

    print(f"🔍 Checking video: {video_id} | domain: {root_domain}")

    if root_domain in WELL_KNOWN_DOMAINS:
        print(f"🚫 Skipping {root_domain} (well-known)")
        supabase.table("Clickyleaks").update({
            "verified": True,
            "is_available": False
        }).eq("id", row_id).execute()
        return

    if not is_valid_domain(root_domain):
        print(f"🗑️ Deleting junk domain: {root_domain}")
        supabase.table("Clickyleaks").delete().eq("id", row_id).execute()
        return

    success = await try_goto(page, video_url)
    if not success:
        print(f"❌ Could not load video after retries")
        return

    content = await page.content()
    if "video unavailable" in content.lower():
        print(f"❌ Video not found – deleting row")
        supabase.table("Clickyleaks").delete().eq("id", row_id).execute()
        return

    video_title, view_count = fetch_video_data_from_api(video_id)

    if not video_title or view_count == 0:
        try:
            video_title = await page.title()
        except:
            video_title = "Unknown"
        try:
            view_count = await page.evaluate('''() => {
                const el = document.querySelector('#count .view-count');
                if (el) return parseInt(el.textContent.replace(/[^\\d]/g, '')) || 0;
                const alt = [...document.querySelectorAll('span')]
                    .map(el => el.textContent)
                    .find(text => /\\d+ views/.test(text));
                return alt ? parseInt(alt.replace(/[^\\d]/g, '')) : 0;
            }''')
        except:
            view_count = 0

    is_available = check_domain_domainr(root_domain)
    if is_available is None:
        print(f"⚠️ Domain check failed for {root_domain}")
        return

    print(f"✅ Updating row → title: {video_title} | views: {view_count} | available: {is_available}")
    supabase.table("Clickyleaks").update({
        "verified": True,
        "is_available": is_available,
        "video_title": video_title,
        "view_count": view_count
    }).eq("id", row_id).execute()

    await asyncio.sleep(random.uniform(1.0, 2.0))

async def main():
    print("🚀 Clickyleaks Verifier (Single Tab / Low Profile) Starting...")

    response = supabase.table("Clickyleaks") \
        .select("*") \
        .or_("verified.is.false,verified.is.null") \
        .order("discovered_at", desc=False) \
        .limit(100) \
        .execute()

    rows = response.data
    if not rows:
        print("✅ No unverified rows remaining.")
        return

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        context = await browser.new_context(user_agent=random.choice(USER_AGENTS))
        page = await context.new_page()

        for row in rows:
            await process_row(row, page)

        await browser.close()

if __name__ == "__main__":
    print("🚀 Running Video + Domain Verifier (Stealth Mode)")
    asyncio.run(main())