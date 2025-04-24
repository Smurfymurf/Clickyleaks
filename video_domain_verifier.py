import os
import asyncio
import random
import time
import requests
from urllib.parse import urlparse, parse_qs
from supabase import create_client, Client
from playwright.async_api import async_playwright

# === CONFIG ===
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
DOMAINR_API_KEY = os.getenv("DOMAINR_API_KEY")
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
        parts = host.split(".")
        return ".".join(parts[-2:]) if len(parts) >= 2 else host
    except:
        return domain

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
                    print(f"❌ Invalid Domainr API key.")
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

async def process_row(row, page, tab_num):
    domain = row["domain"]
    root_domain = normalize_domain(domain)
    video_url = row["video_url"]
    video_id = extract_video_id(row.get("video_id") or video_url)
    row_id = row["id"]

    print(f"🔍 [Tab {tab_num}] Checking video: {video_id} | domain: {root_domain}")

    if root_domain in WELL_KNOWN_DOMAINS:
        print(f"🚫 [Tab {tab_num}] Skipping {root_domain} (well-known)")
        supabase.table("Clickyleaks").update({
            "verified": True,
            "is_available": False
        }).eq("id", row_id).execute()
        return

    try:
        await page.goto(video_url, timeout=15000)
        await page.wait_for_selector('h1.title, div#title h1', timeout=7000)
    except Exception as e:
        print(f"❌ [Tab {tab_num}] Could not load video: {e}")
        return

    # Scrape video title
    try:
        title = await page.locator("h1.title, div#title h1").inner_text()
    except:
        title = "Unknown"

    # Scrape view count
    try:
        view_str = await page.locator('div#view-count span, #info-container #view-count').inner_text()
        view_count = int(''.join(filter(str.isdigit, view_str)))
    except:
        view_count = 0

    # Domain availability
    is_available = check_domain_domainr(root_domain)
    if is_available is None:
        print(f"⚠️ [Tab {tab_num}] Domain check failed for {root_domain}")
        return

    print(f"✅ [Tab {tab_num}] Updating row → title: {title} | views: {view_count} | available: {is_available}")
    supabase.table("Clickyleaks").update({
        "verified": True,
        "is_available": is_available,
        "video_title": title,
        "view_count": view_count
    }).eq("id", row_id).execute()

    await asyncio.sleep(random.uniform(1.0, 2.0))

async def main():
    print("🚀 Clickyleaks Verifier (Playwright + Domainr) Starting...")

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
        context1 = await browser.new_context(user_agent=random.choice(USER_AGENTS))
        context2 = await browser.new_context(user_agent=random.choice(USER_AGENTS))

        page1 = await context1.new_page()
        page2 = await context2.new_page()

        tasks = []
        for i, row in enumerate(rows):
            page = page1 if i % 2 == 0 else page2
            tab = 1 if i % 2 == 0 else 2
            tasks.append(process_row(row, page, tab))

        await asyncio.gather(*tasks)
        await browser.close()

if __name__ == "__main__":
    print("🚀 Running Video + Domain Verifier (Final Scraper Version)")
    asyncio.run(main())
