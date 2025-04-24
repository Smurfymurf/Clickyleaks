import os
import asyncio
from datetime import datetime
from playwright.async_api import async_playwright
from supabase import create_client
import requests

# Environment variables
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
DOMAINR_API_KEY = os.getenv("DOMAINR_API_KEY")

# Supabase client
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# Full and comprehensive list of domains to ignore
WELL_KNOWN_DOMAINS = {
    "google.com", "youtube.com", "facebook.com", "twitter.com", "instagram.com",
    "linkedin.com", "apple.com", "microsoft.com", "netflix.com", "adobe.com",
    "tumblr.com", "cnn.com", "bbc.com", "yahoo.com", "reddit.com",
    "amazon.com", "ebay.com", "paypal.com", "wikipedia.org", "wordpress.com",
    "pinterest.com", "bing.com", "yandex.ru", "vk.com", "baidu.com",
    "cloudflare.com", "whatsapp.com", "tiktok.com", "snapchat.com", "dropbox.com"
}

async def check_domain(domain):
    url = f"https://api.domainr.com/v2/status?domain={domain}&client_id={DOMAINR_API_KEY}"
    try:
        response = requests.get(url, timeout=10)
        data = response.json()
        status = data["status"][0]["status"]
        return "inactive" in status or "undelegated" in status
    except Exception as e:
        print(f"❌ Error checking domain {domain}: {e}")
        return False

async def fetch_video_info(video_id, browser):
    page = await browser.new_page()
    try:
        await page.goto(f"https://www.youtube.com/watch?v={video_id}", timeout=10000)
        title = await page.locator("h1.title, div#title h1").first.inner_text(timeout=7000)
        views_text = await page.locator("span.view-count").first.inner_text(timeout=5000)
        view_count = int("".join(filter(str.isdigit, views_text)))
        await page.close()
        return title.strip(), view_count
    except Exception as e:
        print(f"❌ Error loading video {video_id}: {e}")
        await page.close()
        return None, None

async def main():
    print("🚀 Running Video + Domain Verifier (Final Scraper Version)")
    print("🚀 Clickyleaks Verifier (Playwright + Domainr) Starting...")

    entries = supabase.table("Clickyleaks").select("id, video_id, domain").eq("verified", False).limit(50).execute()

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        for i, entry in enumerate(entries.data):
            row_id = entry["id"]
            video_id = entry["video_id"]
            domain = entry["domain"].lower()

            print(f"🔍 [Tab {i % 2 + 1}] Checking video: {video_id} | domain: {domain}")

            if domain in WELL_KNOWN_DOMAINS:
                print(f"🚫 [Tab {i % 2 + 1}] Skipping {domain} (well-known)")
                supabase.table("Clickyleaks").update({
                    "verified": True,
                    "scanned_at": datetime.utcnow().isoformat()
                }).eq("id", row_id).execute()
                continue

            is_available = await check_domain(domain)
            title, view_count = await fetch_video_info(video_id, browser)

            update_data = {
                "is_available": is_available,
                "verified": True,
                "scanned_at": datetime.utcnow().isoformat()
            }

            if title:
                update_data["video_title"] = title
                update_data["view_count"] = view_count
                print(f"✅ [Tab {i % 2 + 1}] Updating row → title: {title} | views: {view_count} | available: {is_available}")
            else:
                print(f"❌ [Tab {i % 2 + 1}] Could not load video: https://www.youtube.com/watch?v={video_id}")

            supabase.table("Clickyleaks").update(update_data).eq("id", row_id).execute()

        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())