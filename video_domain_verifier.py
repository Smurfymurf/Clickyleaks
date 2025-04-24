import os
import asyncio
from playwright.async_api import async_playwright
from supabase import create_client, Client
import requests

# Load environment variables
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
DOMAINR_API_KEY = os.getenv("DOMAINR_API_KEY")

# Well-known domains to skip
WELL_KNOWN_DOMAINS = [
    "google.com", "facebook.com", "youtube.com", "twitter.com",
    "instagram.com", "linkedin.com", "wikipedia.org", "tumblr.com",
    "cnn.com", "ebay.com", "amazon.com"
]

# Initialize Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

async def get_video_info(page, video_id):
    url = f"https://www.youtube.com/watch?v={video_id}"
    try:
        await page.goto(url, timeout=20000)

        try:
            await page.wait_for_selector("h1.title, div#title h1", timeout=10000)
            title = await page.locator("h1.title, div#title h1").inner_text()
        except:
            print("⚠️ Retrying with alternate title selector...")
            await page.mouse.wheel(0, 500)
            await page.wait_for_timeout(2000)
            try:
                await page.wait_for_selector("yt-formatted-string.title", timeout=7000)
                title = await page.locator("yt-formatted-string.title").inner_text()
            except:
                title = "Unknown"

        try:
            await page.wait_for_selector("span.view-count", timeout=7000)
            views_text = await page.locator("span.view-count").inner_text()
        except:
            print("⚠️ Retrying with alternate views selector...")
            try:
                await page.wait_for_selector("yt-view-count-renderer span", timeout=5000)
                views_text = await page.locator("yt-view-count-renderer span").inner_text()
            except:
                views_text = "0 views"

        views = int(''.join(filter(str.isdigit, views_text)))
        return title, views
    except Exception as e:
        print(f"❌ Could not load video: {url}\n{e}")
        return None, None

async def check_domain_available(domain):
    try:
        response = requests.get(f"https://api.domainr.com/v2/status?domain={domain}&client_id={DOMAINR_API_KEY}")
        data = response.json()
        return data['status'][0]['summary'] == 'inactive'
    except:
        return False

async def main():
    print("🚀 Running Video + Domain Verifier (Final Scraper Version)")
    print("🚀 Clickyleaks Verifier (Playwright + Domainr) Starting...")

    entries = supabase.table("clickyleaks_youtube").select("id, video_id, domain").eq("verified", False).limit(50).execute()

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        page1 = await context.new_page()
        page2 = await context.new_page()

        tasks = []
        for i, entry in enumerate(entries.data):
            tab = page1 if i % 2 == 0 else page2
            video_id = entry['video_id']
            domain = entry['domain']
            row_id = entry['id']

            if any(d in domain for d in WELL_KNOWN_DOMAINS):
                print(f"🚫 [Tab {1 if tab == page1 else 2}] Skipping {domain} (well-known)")
                supabase.table("clickyleaks_youtube").update({"verified": True, "available": False}).eq("id", row_id).execute()
                continue

            print(f"🔍 [Tab {1 if tab == page1 else 2}] Checking video: {video_id} | domain: {domain}")

            async def process(tab, video_id, domain, row_id):
                title, views = await get_video_info(tab, video_id)
                if title is None:
                    supabase.table("clickyleaks_youtube").update({"verified": True, "available": False}).eq("id", row_id).execute()
                    return
                is_available = await check_domain_available(domain)
                supabase.table("clickyleaks_youtube").update({
                    "verified": True,
                    "available": is_available,
                    "title": title,
                    "views": views
                }).eq("id", row_id).execute()
                print(f"✅ [Tab {1 if tab == page1 else 2}] Updating row → title: {title} | views: {views} | available: {is_available}")

            tasks.append(process(tab, video_id, domain, row_id))

        await asyncio.gather(*tasks)
        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
