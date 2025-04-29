import asyncio
import json
import os
import re
from datetime import datetime, timedelta

import requests
from playwright.async_api import async_playwright
from supabase import create_client

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

WELL_KNOWN_DOMAINS = ["youtube.com", "facebook.com", "instagram.com", "twitter.com", "linkedin.com", "tiktok.com", "google.com", "bit.ly"]

CHUNK_BASE_URL = "https://smurfymurf.github.io/clickyleaks-chunks/"
CHUNK_TABLE = "Clickyleaks_SeedProgress"
SCANNED_VIDEOS_TABLE = "Clickyleaks_DynamicChecked"
DOMAINS_TABLE = "Clickyleaks"

VIDEO_AGE_MIN = 3  # in years
VIDEO_AGE_MAX = 7

VIDEOS_PER_RUN = 200  # Cap to conserve GitHub minutes


def get_current_chunk_progress():
    result = supabase.table(CHUNK_TABLE).select("*").order("id", desc=True).limit(1).execute()
    if result.data:
        return result.data[0]["chunk_index"], result.data[0]["video_index"]
    return 0, 0


def update_chunk_progress(chunk_index, video_index):
    supabase.table(CHUNK_TABLE).insert({
        "chunk_index": chunk_index,
        "video_index": video_index,
        "timestamp": datetime.utcnow().isoformat()
    }).execute()


def already_scanned(video_id):
    res = supabase.table(SCANNED_VIDEOS_TABLE).select("id").eq("video_id", video_id).execute()
    return len(res.data) > 0


def mark_video_scanned(video_id):
    supabase.table(SCANNED_VIDEOS_TABLE).insert({
        "video_id": video_id,
        "scanned_at": datetime.utcnow().isoformat()
    }).execute()


def extract_domains(description):
    urls = re.findall(r'(https?://[^\s]+)', description)
    clean_urls = [url.split("?")[0].split("#")[0] for url in urls]
    domains = [re.sub(r'^https?://(www\.)?', '', url).strip("/") for url in clean_urls]
    return list(set(domains))


def is_well_known(domain):
    return any(domain.startswith(wkd) or domain in wkd for wkd in WELL_KNOWN_DOMAINS)


def soft_check(domain):
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        r = requests.get(f"http://{domain}", headers=headers, timeout=5)
        return r.status_code not in [404, 502, 503]
    except:
        return False


async def fetch_video_description(page, video_id):
    await page.goto(f"https://www.youtube.com/watch?v={video_id}", timeout=30000)
    await page.wait_for_selector("meta[name='description']", timeout=10000)
    desc = await page.locator("meta[name='description']").get_attribute("content")
    return desc or ""


async def get_video_metadata(page, video_id):
    await page.goto(f"https://www.youtube.com/watch?v={video_id}", timeout=30000)
    await page.wait_for_selector("meta[itemprop='uploadDate']", timeout=10000)
    date_str = await page.locator("meta[itemprop='uploadDate']").get_attribute("content")
    return datetime.strptime(date_str, "%Y-%m-%d") if date_str else None


async def process_video(page, video_id):
    if already_scanned(video_id):
        return False

    mark_video_scanned(video_id)

    upload_date = await get_video_metadata(page, video_id)
    if not upload_date:
        return False

    age = (datetime.utcnow() - upload_date).days / 365
    if not (VIDEO_AGE_MIN <= age <= VIDEO_AGE_MAX):
        return False

    description = await fetch_video_description(page, video_id)
    domains = extract_domains(description)

    found_domains = 0
    for domain in domains:
        if is_well_known(domain):
            continue
        if not soft_check(domain):
            supabase.table(DOMAINS_TABLE).insert({
                "domain": domain,
                "video_id": video_id,
                "found_at": datetime.utcnow().isoformat(),
                "is_available": None,
                "verified": False,
                "source": "YouTube Dynamic"
            }).execute()
            found_domains += 1

    return found_domains > 0


async def get_related_videos(page, start_video_id, max_results):
    await page.goto(f"https://www.youtube.com/watch?v={start_video_id}", timeout=30000)
    await page.wait_for_selector("ytd-watch-next-secondary-results-renderer", timeout=10000)
    hrefs = await page.locator("a#thumbnail").evaluate_all("nodes => nodes.map(n => n.href)")
    video_ids = [re.search(r"v=([a-zA-Z0-9_-]{11})", h).group(1) for h in hrefs if "watch?v=" in h]
    unique_ids = list(dict.fromkeys(video_ids))  # Deduplicate
    return unique_ids[:max_results]


async def main():
    print("🚀 Clickyleaks Dynamic Explorer Starting...")

    chunk_index, last_video_index = get_current_chunk_progress()
    chunk_url = f"{CHUNK_BASE_URL}chunk_{chunk_index}.json"
    print(f"📥 Loading seed chunk: {chunk_url}")

    chunk = requests.get(chunk_url).json()
    seed_videos = chunk["videos"]

    if last_video_index >= len(seed_videos):
        chunk_index += 1
        last_video_index = 0
        chunk_url = f"{CHUNK_BASE_URL}chunk_{chunk_index}.json"
        chunk = requests.get(chunk_url).json()
        seed_videos = chunk["videos"]
        print(f"➡️ Moving to next chunk: {chunk_index}")

    start_video_id = seed_videos[last_video_index]
    print(f"🌱 Seed: {start_video_id}")
    update_chunk_progress(chunk_index, last_video_index + 1)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        related_ids = await get_related_videos(page, start_video_id, VIDEOS_PER_RUN)

        for video_id in related_ids:
            try:
                found = await process_video(page, video_id)
                if found:
                    print(f"✅ Found expired domain(s) in video {video_id}")
            except Exception as e:
                print(f"⚠️ Error processing video {video_id}: {e}")

        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())