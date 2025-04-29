# clickyleaks_dynamic_explorer.py (FULL)

import os
import random
import re
import asyncio
import requests
import json
from urllib.parse import urlparse
from datetime import datetime
from supabase import create_client, Client
from playwright.async_api import async_playwright

# === CONFIG ===
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")
CHUNK_BASE_URL = "https://raw.githubusercontent.com/Smurfymurf/clickyleaks-chunks/main/"
START_CHUNK = 101
TOTAL_CHUNKS = 1000
MAX_NEW_DOMAINS_PER_RUN = 20
MAX_VIDEOS_SCANNED_PER_RUN = 300
HEADLESS_MODE = True

# === WELL-KNOWN DOMAINS FILTER ===
WELL_KNOWN_DOMAINS = {...}  # (use your big list here — same as before)

# === INIT SUPABASE ===
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# === UTILS ===

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64)...",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)...",
    "Mozilla/5.0 (X11; Linux x86_64)...",
]

async def launch_browser():
    playwright = await async_playwright().start()
    browser = await playwright.chromium.launch(headless=HEADLESS_MODE)
    context = await browser.new_context(
        user_agent=random.choice(USER_AGENTS),
        viewport={"width": 1280, "height": 720},
        locale="en-US"
    )
    page = await context.new_page()
    return playwright, browser, context, page

async def polite_delay(video_count=0):
    if video_count > 0 and video_count % 10 == 0:
        await asyncio.sleep(random.randint(10, 15))
    else:
        await asyncio.sleep(random.randint(3, 7))

def extract_links(text):
    return re.findall(r'(https?://[^\s)]+)', text)

def normalize_domain(link: str) -> str:
    try:
        parsed = urlparse(link)
        host = parsed.netloc or parsed.path
        return host.replace("www.", "").lower().strip()
    except:
        return ""

def is_domain_soft_available(domain):
    try:
        requests.get(f"http://{domain}", timeout=5)
        return False
    except:
        return True

def send_discord_alert(message):
    if not DISCORD_WEBHOOK_URL:
        return
    try:
        requests.post(DISCORD_WEBHOOK_URL, json={"content": message}, timeout=5)
    except:
        pass

async def get_current_seed():
    progress = supabase.table("Clickyleaks_SeedProgress").select("*").order("updated_at", desc=True).limit(1).execute()
    if not progress.data:
        current_chunk = START_CHUNK
        current_index = 0
    else:
        last = progress.data[0]
        current_chunk = last["chunk_number"]
        current_index = last["video_index"] + 1

    chunk_url = f"{CHUNK_BASE_URL}chunk_{current_chunk}.json"
    try:
        res = requests.get(chunk_url, timeout=20)
        res.raise_for_status()
        chunk_data = res.json()
    except:
        return None, None

    if current_index >= len(chunk_data):
        current_chunk += 1
        current_index = 0
        if current_chunk > START_CHUNK + TOTAL_CHUNKS:
            return None, None
        return await get_current_seed()

    video_entry = chunk_data[current_index]
    video_id = video_entry.get("_id") or video_entry.get("video_id")
    if not video_id:
        supabase.table("Clickyleaks_SeedProgress").insert({
            "chunk_number": current_chunk,
            "video_index": current_index,
            "updated_at": datetime.utcnow().isoformat()
        }).execute()
        return await get_current_seed()

    supabase.table("Clickyleaks_SeedProgress").insert({
        "chunk_number": current_chunk,
        "video_index": current_index,
        "updated_at": datetime.utcnow().isoformat()
    }).execute()

    return video_id, current_chunk

async def scan_video(page, video_id):
    await page.goto(f"https://www.youtube.com/watch?v={video_id}", timeout=60000)
    await polite_delay()

    description = ""
    related_video_ids = []

    try:
        description_element = await page.query_selector('meta[name="description"]')
        if description_element:
            description = await description_element.get_attribute('content') or ""

        related_elements = await page.query_selector_all('a#thumbnail[href*="/watch?v="]')
        for elem in related_elements:
            href = await elem.get_attribute('href')
            if href and "/watch?v=" in href:
                vid = href.split("v=")[-1].split("&")[0]
                if len(vid) == 11:
                    related_video_ids.append(vid)
    except Exception as e:
        print(f"⚠️ Scraping error: {e}")

    return description, list(set(related_video_ids))

async def process_description(description, video_id):
    links = extract_links(description)
    new_domains = 0

    for link in links:
        domain = normalize_domain(link)
        if not domain or len(domain.split(".")) < 2:
            continue
        if domain in WELL_KNOWN_DOMAINS:
            continue

        if is_domain_soft_available(domain):
            record = {
                "domain": domain,
                "full_url": link,
                "video_id": video_id,
                "video_url": f"https://www.youtube.com/watch?v={video_id}",
                "is_available": True,
                "discovered_at": datetime.utcnow().isoformat(),
                "scanned_at": datetime.utcnow().isoformat(),
                "view_count": 0
            }
            supabase.table("Clickyleaks").insert(record).execute()
            send_discord_alert(f"🔥 New available domain found: `{domain}` from video {record['video_url']}")
            new_domains += 1

    return new_domains

async def main():
    playwright, browser, context, page = await launch_browser()

    seed_video_id, _ = await get_current_seed()
    if not seed_video_id:
        print("✅ All seeds exhausted!")
        return

    to_scan = [seed_video_id]
    scanned = 0
    new_domains_logged = 0

    while to_scan and scanned < MAX_VIDEOS_SCANNED_PER_RUN and new_domains_logged < MAX_NEW_DOMAINS_PER_RUN:
        current_id = to_scan.pop(0)
        description, related_videos = await scan_video(page, current_id)
        new_domains_logged += await process_description(description, current_id)
        to_scan += random.sample(related_videos, min(len(related_videos), 3))  # Pick a few random related
        scanned += 1
        await polite_delay(scanned)

    await browser.close()
    print(f"✅ Run complete. {new_domains_logged} new domains found across {scanned} videos.")

if __name__ == "__main__":
    asyncio.run(main())
