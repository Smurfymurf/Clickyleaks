import os
import asyncio
import random
import requests
from urllib.parse import urlparse
from supabase import create_client, Client
from playwright.async_api import async_playwright
import time

# === CONFIG ===
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
DOMAINR_API_KEY = os.getenv("DOMAINR_API_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Well-known domains we skip to save API calls
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
        parts = host.replace("www.", "").split(".")
        return ".".join(parts[-2:]) if len(parts) >= 2 else host
    except:
        return domain

def check_domain_domainr(domain: str) -> bool:
    root = normalize_domain(domain)
    try:
        url = f"https://domainr.p.rapidapi.com/v2/status?domain={root}"
        headers = {
            "X-RapidAPI-Key": DOMAINR_API_KEY,
            "X-RapidAPI-Host": "domainr.p.rapidapi.com"
        }
        res = requests.get(url, headers=headers, timeout=10)
        data = res.json()

        if "status" in data and isinstance(data["status"], list) and data["status"]:
            status = data["status"][0]["status"]
            return "inactive" in status or "undelegated" in status
        else:
            print(f"❌ Invalid Domainr response for {domain}: {data}")
            return None
    except Exception as e:
        print(f"❌ Domainr check failed for {domain}: {e}")
        return None

async def check_video_exists(video_url, page):
    try:
        await page.goto(video_url, timeout=15000)
        content = await page.content()
        return "video unavailable" not in content.lower()
    except Exception:
        return False

async def process_row(row, page, tab_num):
    domain = row["domain"]
    root_domain = normalize_domain(domain)
    video_url = row["video_url"]
    video_id = row.get("video_id", "unknown")
    row_id = row["id"]

    print(f"🔍 [Tab {tab_num}] Checking video: {video_id} | domain: {root_domain}")

    if root_domain in WELL_KNOWN_DOMAINS:
        print(f"🚫 [Tab {tab_num}] Skipping {root_domain} (well-known) – marking unavailable")
        supabase.table("Clickyleaks").update({
            "verified": True,
            "is_available": False
        }).eq("id", row_id).execute()
        return

    # Check if video still exists
    exists = await check_video_exists(video_url, page)
    if not exists:
        print(f"❌ [Tab {tab_num}] Video {video_id} not found – deleting row")
        supabase.table("Clickyleaks").delete().eq("id", row_id).execute()
        return

    # Domain availability check
    is_available = check_domain_domainr(root_domain)

    if is_available is None:
        print(f"⚠️ [Tab {tab_num}] Domainr failed for {domain} – skipping")
        return

    print(f"✅ [Tab {tab_num}] Updating: verified=True, available={is_available}")
    supabase.table("Clickyleaks").update({
        "verified": True,
        "is_available": is_available
    }).eq("id", row_id).execute()
    await asyncio.sleep(3)

async def main():
    print("🚀 Clickyleaks Verifier (Domainr API) Started...")

    response = supabase.table("Clickyleaks") \
        .select("*") \
        .or_("verified.is.false,verified.is.null") \
        .order("discovered_at", desc=False) \
        .limit(100) \
        .execute()

    rows = response.data
    if not rows:
        print("✅ No unverified entries to check.")
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
            tab_num = 1 if i % 2 == 0 else 2
            tasks.append(process_row(row, page, tab_num))

        await asyncio.gather(*tasks)
        await browser.close()

if __name__ == "__main__":
    print("🚀 Running Video + Domain Verifier with Domainr API")
    asyncio.run(main())
