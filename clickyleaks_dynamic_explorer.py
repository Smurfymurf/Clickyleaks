
import asyncio
import requests
from datetime import datetime, timedelta
from playwright.async_api import async_playwright
from supabase import create_client
import os
import re

# Supabase setup
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# Constants
CHUNK_BASE_URL = "https://smurfymurf.github.io/clickyleaks-chunks/"
CHUNK_TOTAL = 110
VIDEOS_PER_RUN = 200
AGE_MIN_YEARS = 3
AGE_MAX_YEARS = 7

WELL_KNOWN_DOMAINS = ["youtube.com", "facebook.com", "twitter.com", "instagram.com", "tiktok.com", "linkedin.com", "google.com"]

# --- Supabase helpers ---

def already_scanned(video_id):
    res = supabase.table("Clickyleaks_DynamicChecked").select("id").eq("video_id", video_id).execute()
    return bool(res.data)

def mark_video_scanned(video_id):
    supabase.table("Clickyleaks_DynamicChecked").insert({"video_id": video_id}).execute()

def get_current_chunk_progress():
    result = supabase.table("Clickyleaks_SeedProgress").select("*").limit(1).execute()
    if result.data:
        return result.data[0]["chunk_index"], result.data[0]["video_index"]
    return 0, 0

def update_chunk_progress(chunk_index, video_index):
    supabase.table("Clickyleaks_SeedProgress").delete().neq("chunk_index", -1).execute()
    supabase.table("Clickyleaks_SeedProgress").insert({
        "chunk_index": chunk_index,
        "video_index": video_index
    }).execute()

def store_soft_checked_domains(domains, video_id):
    for domain in domains:
        supabase.table("Clickyleaks").insert({
            "video_id": video_id,
            "domain": domain,
            "is_verified": False,
            "is_available": None,
            "source": "YouTube Dynamic Explorer"
        }).execute()

# --- Logic helpers ---

def extract_domains(text):
    domain_pattern = r'https?://(?:www\.)?([a-zA-Z0-9.-]+\.[a-zA-Z]{2,})'
    matches = re.findall(domain_pattern, text)
    return [d.lower() for d in matches if d.lower() not in WELL_KNOWN_DOMAINS]

def is_within_age_range(published_date):
    now = datetime.utcnow()
    published = datetime.strptime(published_date, "%Y-%m-%dT%H:%M:%S.%fZ")
    return now - timedelta(days=AGE_MAX_YEARS * 365) < published < now - timedelta(days=AGE_MIN_YEARS * 365)

async def fetch_description(page, video_id):
    await page.goto(f"https://www.youtube.com/watch?v={video_id}")
    await page.wait_for_selector('meta[name="description"]', timeout=10000)
    description = await page.locator('meta[name="description"]').get_attribute('content')
    return description or ""

async def get_related_videos(page, video_id, cap=100):
    await page.goto(f"https://www.youtube.com/watch?v={video_id}")
    await page.wait_for_selector("ytd-watch-next-secondary-results-renderer", timeout=10000)
    hrefs = await page.eval_on_selector_all("ytd-compact-video-renderer a#thumbnail", "elements => elements.map(e => e.href)")
    video_ids = [url.split("v=")[-1].split("&")[0] for url in hrefs if "watch?v=" in url]
    return list(dict.fromkeys(video_ids))[:cap]

async def process_video(page, video_id):
    if already_scanned(video_id):
        return False
    mark_video_scanned(video_id)

    try:
        description = await fetch_description(page, video_id)
        domains = extract_domains(description)
        if domains:
            store_soft_checked_domains(domains, video_id)
            return True
    except:
        pass
    return False

# --- Main run logic ---

async def main():
    chunk_index, last_video_index = get_current_chunk_progress()
    found_count = 0

    async with async_playwright() as p:
        browser = await p.chromium.launch()
        page = await browser.new_page()

        while found_count < VIDEOS_PER_RUN:
            chunk_url = f"{CHUNK_BASE_URL}chunk_{chunk_index}.json"
            chunk = requests.get(chunk_url).json()
            videos = chunk if isinstance(chunk, list) else chunk.get("videos", [])

            if last_video_index >= len(videos):
                chunk_index += 1
                last_video_index = 0
                if chunk_index >= CHUNK_TOTAL:
                    break
                continue

            seed = videos[last_video_index]
            start_video_id = seed["_id"]
            print(f"ð± Seed: {seed['title']} ({start_video_id})")

            # Skip if seed already used
            if already_scanned(start_video_id):
                last_video_index += 1
                update_chunk_progress(chunk_index, last_video_index)
                continue

            # Store seed as scanned
            mark_video_scanned(start_video_id)

            # Always fetch related, even if seed is out of range
            try:
                related = await get_related_videos(page, start_video_id, VIDEOS_PER_RUN * 2)
                for rel_id in related:
                    if found_count >= VIDEOS_PER_RUN:
                        break
                    if already_scanned(rel_id):
                        continue

                    await page.wait_for_timeout(1000)
                    await page.goto(f"https://www.youtube.com/watch?v={rel_id}")
                    date_meta = await page.locator("meta[itemprop='datePublished']").get_attribute("content")
                    if date_meta and is_within_age_range(date_meta):
                        success = await process_video(page, rel_id)
                        if success:
                            found_count += 1
            except:
                pass

            last_video_index += 1
            update_chunk_progress(chunk_index, last_video_index)

        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
