import asyncio
import os
import re
import json
from datetime import datetime, timedelta
from urllib.parse import urlparse

import requests
from playwright.async_api import async_playwright
from supabase import create_client

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")
CHUNK_URL = "https://smurfymurf.github.io/clickyleaks-chunks/chunk_102.json"

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

WELL_KNOWN_DOMAINS = {
    "google.com", "facebook.com", "youtube.com", "twitter.com", "instagram.com",
    "linkedin.com", "tiktok.com", "amazon.com", "apple.com", "microsoft.com",
    "reddit.com", "netflix.com", "paypal.com", "yahoo.com", "bing.com",
    "wikipedia.org", "tumblr.com", "spotify.com", "dropbox.com", "medium.com"
}

def already_scanned(video_id):
    res = supabase.table("Clickyleaks_DynamicChecked").select("id").eq("video_id", video_id).execute()
    return bool(res.data)

def mark_video_scanned(video_id):
    supabase.table("Clickyleaks_DynamicChecked").insert({
        "video_id": video_id,
        "scanned_at": datetime.utcnow().isoformat()
    }).execute()

def extract_links(text):
    return re.findall(r'(https?://[^\s)"]+)', text)

def normalize_domain(link):
    try:
        parsed = urlparse(link)
        host = parsed.netloc or parsed.path
        return host.replace("www.", "").lower().strip()
    except:
        return None

def is_domain_available(domain):
    try:
        requests.get(f"http://{domain}", timeout=5)
        return False
    except:
        return True

def send_discord_alert(domain, video_url):
    if not DISCORD_WEBHOOK_URL:
        return
    try:
        requests.post(DISCORD_WEBHOOK_URL, json={
            "content": f"🔥 **Available domain found:** `{domain}`\n🔗 {video_url}"
        }, timeout=5)
    except:
        pass

async def fetch_video_description(page, video_id):
    await page.goto(f"https://www.youtube.com/watch?v={video_id}", timeout=30000)
    await page.wait_for_selector('meta[name="description"]', timeout=10000)
    element = await page.query_selector('meta[name="description"]')
    return await element.get_attribute("content") if element else ""

async def fetch_video_publish_date(page):
    try:
        await page.wait_for_selector('meta[itemprop="datePublished"]', timeout=10000)
        element = await page.query_selector('meta[itemprop="datePublished"]')
        content = await element.get_attribute("content")
        return datetime.strptime(content, "%Y-%m-%d")
    except:
        return None

async def process_video(page, video_id):
    if already_scanned(video_id):
        return False

    mark_video_scanned(video_id)

    await page.goto(f"https://www.youtube.com/watch?v={video_id}", timeout=30000)
    published_date = await fetch_video_publish_date(page)

    if not published_date:
        print(f"⚠️ Skipping {video_id} — couldn't get publish date")
        return False

    now = datetime.utcnow()
    if not (now - timedelta(days=365*7) <= published_date <= now - timedelta(days=365*3)):
        print(f"⏩ Skipping {video_id} — published {published_date.date()} (outside 3–7 year range)")
        return False

    description = await fetch_video_description(page, video_id)
    links = extract_links(description)

    for link in links:
        domain = normalize_domain(link)
        if not domain or len(domain.split(".")) < 2 or domain in WELL_KNOWN_DOMAINS:
            continue

        if is_domain_available(domain):
            print(f"✅ Available: {domain}")
            supabase.table("Clickyleaks_DynamicFound").insert({
                "domain": domain,
                "full_url": link,
                "video_id": video_id,
                "video_url": f"https://www.youtube.com/watch?v={video_id}",
                "found_at": datetime.utcnow().isoformat()
            }).execute()
            send_discord_alert(domain, f"https://www.youtube.com/watch?v={video_id}")
            return True
        else:
            print(f"❌ Taken: {domain}")
    return False

async def main():
    print("🚀 Clickyleaks Dynamic Explorer Starting...")

    resp = requests.get(CHUNK_URL)
    video_ids = resp.json()
    print(f"📥 Loaded {len(video_ids)} video IDs")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()

        for video_id in video_ids:
            print(f"🌱 Scanning video: {video_id}")
            try:
                await process_video(page, video_id)
            except Exception as e:
                print(f"❗ Error processing {video_id}: {e}")
        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())