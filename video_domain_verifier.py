import os
import asyncio
import random
from urllib.parse import urlparse
from supabase import create_client, Client
from playwright.async_api import async_playwright
import time

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

WELL_KNOWN_DOMAINS = {
    "apple.com", "microsoft.com", "google.com", "youtube.com", "amazon.com",
    "facebook.com", "instagram.com", "twitter.com", "netflix.com", "tiktok.com",
    "paypal.com", "adobe.com", "dropbox.com", "spotify.com", "whatsapp.com",
    "bing.com", "linkedin.com", "pinterest.com", "zoom.us", "cnn.com",
    "bbc.com", "ebay.com", "reddit.com", "airbnb.com", "nytimes.com",
    "yahoo.com", "icloud.com", "wikipedia.org", "steamcommunity.com", "github.com",
    "wordpress.com", "tumblr.com", "quora.com", "slack.com", "salesforce.com",
    "roblox.com", "netlify.app", "vercel.app", "herokuapp.com", "shopify.com",
    "oracle.com", "atlassian.com", "figma.com", "dribbble.com", "behance.net",
    "medium.com", "coursera.org", "udemy.com", "khanacademy.org", "stackoverflow.com",
    "bitbucket.org", "notion.so", "weebly.com", "wix.com", "canva.com"
}

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; rv:109.0) Gecko/20100101 Firefox/118.0",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:118.0) Gecko/20100101 Firefox/118.0",
]

def get_domain_root(domain):
    parts = domain.lower().strip().replace("www.", "").split(".")
    return ".".join(parts[-2:]) if len(parts) >= 2 else domain

async def check_video_exists(video_url, page):
    try:
        await page.goto(video_url, timeout=15000)
        content = await page.content()
        return "video unavailable" not in content.lower()
    except Exception:
        return False

async def check_domain_via_browser(domain, page, retries=2):
    url = f"https://www.godaddy.com/domainsearch/find?checkAvail=1&domainToCheck={domain}"
    for attempt in range(retries):
        try:
            await page.goto(url, timeout=25000)
            await page.wait_for_timeout(5000)
            content = await page.content()
            if "is taken" in content.lower() or "not available" in content.lower():
                return False
            elif "is available" in content.lower() or "add to cart" in content.lower():
                return True
        except Exception as e:
            print(f"⚠️ Retry {attempt + 1}/{retries} failed for {domain}: {str(e)}")
            await asyncio.sleep(3)
    return None

async def process_row(row, page, index):
    await asyncio.sleep(index * 2)
    domain = row["domain"]
    root_domain = get_domain_root(domain)
    video_url = row["video_url"]
    video_id = row.get("video_id", "unknown")
    row_id = row["id"]

    print(f"🔍 [Tab {index}] Checking video: {video_id} | domain: {root_domain}")

    if root_domain in WELL_KNOWN_DOMAINS:
        print(f"🚫 Skipping {domain} (well-known root: {root_domain}) and auto-marking as unavailable.")
        supabase.table("Clickyleaks").update({
            "verified": True,
            "is_available": False
        }).eq("id", row_id).execute()
        return

    exists = await check_video_exists(video_url, page)
    if not exists:
        print(f"❌ [Tab {index}] Video {video_id} no longer exists. Deleting row...")
        supabase.table("Clickyleaks").delete().eq("id", row_id).execute()
        return

    is_available = await check_domain_via_browser(root_domain, page)

    if is_available is None:
        print(f"⚠️ [Tab {index}] Skipping {domain} due to failed domain availability check.")
        return

    print(f"✅ [Tab {index}] Updating row: verified=True, is_available={is_available}")
    supabase.table("Clickyleaks").update({
        "verified": True,
        "is_available": is_available
    }).eq("id", row_id).execute()

    await asyncio.sleep(5)

async def main():
    print("🚀 Clickyleaks Stealth Verifier Started...")

    response = supabase.table("Clickyleaks") \
        .select("*") \
        .or_("verified.is.false,verified.is.null") \
        .order("discovered_at", desc=False) \
        .limit(100) \
        .execute()

    rows = response.data
    if not rows:
        print("✅ No unverified entries.")
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
            tasks.append(process_row(row, page, i % 2 + 1))

        await asyncio.gather(*tasks)
        await browser.close()

if __name__ == "__main__":
    print("🚀 Running Stealth Video + Domain Verifier...")
    asyncio.run(main())
