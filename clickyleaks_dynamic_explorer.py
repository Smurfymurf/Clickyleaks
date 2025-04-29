# clickyleaks_dynamic_explorer.py

import os, re, asyncio, requests
from datetime import datetime
from urllib.parse import urlparse
from supabase import create_client, Client
from playwright.async_api import async_playwright

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")
CHUNK_BASE_URL = "https://smurfymurf.github.io/clickyleaks-chunks/"
SEED_CHUNK = 102

WELL_KNOWN_DOMAINS = {"google.com", "facebook.com", "youtube.com", "amazon.com", "twitter.com", "instagram.com"}

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def already_scanned(video_id):
    res = supabase.table("clickyleaks_dynamicchecked").select("id").eq("video_id", video_id).execute()
    return len(res.data) > 0

def mark_video_scanned(video_id):
    supabase.table("clickyleaks_dynamicchecked").insert({
        "video_id": video_id,
        "scanned_at": datetime.utcnow().isoformat()
    }).execute()

def extract_links(text):
    return re.findall(r'https?://[^\s)"]+', text)

def normalize_domain(url):
    try:
        parsed = urlparse(url)
        domain = parsed.netloc or parsed.path
        return domain.replace("www.", "").lower()
    except:
        return ""

def is_domain_available(domain):
    try:
        requests.get(f"http://{domain}", timeout=5)
        return False
    except:
        return True

def send_discord(domain, video_url):
    if not DISCORD_WEBHOOK_URL: return
    try:
        requests.post(DISCORD_WEBHOOK_URL, json={
            "content": f"🔥 Available domain found: `{domain}`\n🔗 {video_url}"
        }, timeout=5)
    except: pass

async def fetch_description(page, video_id):
    url = f"https://www.youtube.com/watch?v={video_id}"
    try:
        await page.goto(url, timeout=15000)
        await page.wait_for_selector('meta[name="description"]', timeout=10000)
        content = await page.locator('meta[name="description"]').get_attribute("content")
        return content
    except:
        return None

async def process_video(page, video_id):
    if already_scanned(video_id): return False
    mark_video_scanned(video_id)

    desc = await fetch_description(page, video_id)
    if not desc: return False

    links = extract_links(desc)
    for link in links:
        domain = normalize_domain(link)
        if not domain or domain in WELL_KNOWN_DOMAINS: continue

        if is_domain_available(domain):
            supabase.table("Clickyleaks").insert({
                "domain": domain,
                "full_url": link,
                "video_id": video_id,
                "video_url": f"https://www.youtube.com/watch?v={video_id}",
                "is_available": True,
                "discovered_at": datetime.utcnow().isoformat(),
                "scanned_at": datetime.utcnow().isoformat(),
                "view_count": 0
            }).execute()
            send_discord(domain, f"https://www.youtube.com/watch?v={video_id}")
            return True
    return False

async def main():
    print("🚀 Clickyleaks Dynamic Explorer Starting...")
    chunk_url = f"{CHUNK_BASE_URL}chunk_{SEED_CHUNK}.json"
    print(f"📥 Loading seed chunk: {chunk_url}")

    res = requests.get(chunk_url)
    res.raise_for_status()
    seed_videos = res.json()

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        for video in seed_videos:
            video_id = video.get("_id")
            if not video_id: continue
            print(f"🌱 Seed: {video_id}")
            found = await process_video(page, video_id)
            if not found: continue

            # Follow 5 related videos
            await page.goto(f"https://www.youtube.com/watch?v={video_id}")
            await page.wait_for_selector("ytd-compact-video-renderer", timeout=10000)
            related = await page.query_selector_all("ytd-compact-video-renderer a#thumbnail")
            for r in related[:5]:
                href = await r.get_attribute("href")
                if href and "watch?v=" in href:
                    next_id = href.split("v=")[-1].split("&")[0]
                    await process_video(page, next_id)

        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
