import asyncio
import json
import os
import re
import time
from datetime import datetime, timedelta

import requests
from playwright.async_api import async_playwright
from supabase import create_client

# --- Config ---
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

DISCORD_WEBHOOK_URL = os.environ["DISCORD_WEBHOOK_URL"]

CHUNK_URL_TEMPLATE = "https://smurfymurf.github.io/clickyleaks-chunks/chunk_{}.json"
VIDEOS_PER_RUN = 200

MIN_AGE_DAYS = 365 * 3   # 3 years
MAX_AGE_DAYS = 365 * 7   # 7 years

WELL_KNOWN_DOMAINS = ["youtube.com", "facebook.com", "instagram.com", "twitter.com", "linkedin.com", "tiktok.com"]

# --- Supabase helpers ---
def get_current_chunk_progress():
    result = supabase.table("Clickyleaks_SeedProgress").select("*").execute()
    if not result.data:
        return 0, 0
    return result.data[0]["chunk_index"], result.data[0]["video_index"]

def update_chunk_progress(chunk_index, video_index):
    supabase.table("Clickyleaks_SeedProgress").upsert({
        "id": 1,
        "chunk_index": chunk_index,
        "video_index": video_index
    }).execute()

def mark_video_scanned(video_id):
    supabase.table("Clickyleaks_DynamicChecked").upsert({
        "video_id": video_id
    }).execute()

def already_scanned(video_id):
    res = supabase.table("Clickyleaks_DynamicChecked").select("id").eq("video_id", video_id).execute()
    return bool(res.data)

def log_potential_domain(domain, video_id, views):
    supabase.table("Clickyleaks").insert({
        "domain": domain,
        "source_video_id": video_id,
        "views": views,
        "available": True,
        "verified": False
    }).execute()

def send_discord_notification(domain, video_id, views):
    message = {
        "content": f"**New Potential Domain Found!**\n\nDomain: `{domain}`\nSource Video: https://youtube.com/watch?v={video_id}\nViews: {views:,}"
    }
    requests.post(DISCORD_WEBHOOK_URL, json=message)

# --- Playwright helpers ---
async def fetch_video_description(page, video_id):
    await page.goto(f"https://www.youtube.com/watch?v={video_id}", timeout=60000)
    try:
        await page.wait_for_selector('meta[name="description"]', timeout=10000)
        content = await page.locator('meta[name="description"]').get_attribute('content')
        return content or ""
    except:
        return ""

async def get_related_videos(page, video_id, limit):
    await page.goto(f"https://www.youtube.com/watch?v={video_id}", timeout=60000)
    try:
        await page.wait_for_selector("ytd-watch-next-secondary-results-renderer", timeout=10000)
    except:
        return []

    video_elements = await page.locator('ytd-compact-video-renderer').all()
    related_ids = []
    for elem in video_elements:
        try:
            url = await elem.locator('a#thumbnail').get_attribute('href')
            if url and "/watch?v=" in url:
                related_id = url.split('=')[1]
                related_ids.append(related_id)
                if len(related_ids) >= limit:
                    break
        except:
            continue
    return related_ids

def extract_domains_from_text(text):
    regex = r"(https?://)?(www\.)?([a-zA-Z0-9\-\.]+\.[a-zA-Z]{2,})"
    matches = re.findall(regex, text)
    domains = {match[2].lower() for match in matches}
    return [d for d in domains if d not in WELL_KNOWN_DOMAINS]

def is_expired_soft(domain):
    try:
        response = requests.get(f"http://{domain}", timeout=5)
        if response.status_code in [403, 404, 502, 503]:
            return True
        return False
    except:
        return True

def within_age_range(published_date):
    now = datetime.utcnow()
    published = datetime.strptime(published_date, "%Y-%m-%dT%H:%M:%S.%fZ")
    age_days = (now - published).days
    return MIN_AGE_DAYS <= age_days <= MAX_AGE_DAYS

# --- Main process ---
async def process_video(page, video_id):
    if already_scanned(video_id):
        return False
    description = await fetch_video_description(page, video_id)
    mark_video_scanned(video_id)

    found = False
    for domain in extract_domains_from_text(description):
        if is_expired_soft(domain):
            log_potential_domain(domain, video_id, 0)
            send_discord_notification(domain, video_id, 0)
            found = True
    return found

async def main():
    chunk_index, last_video_index = get_current_chunk_progress()
    chunk_url = CHUNK_URL_TEMPLATE.format(chunk_index)
    chunk_data = requests.get(chunk_url)
    if not chunk_data.ok:
        print(f"Failed to fetch chunk {chunk_index}")
        return
    chunk = chunk_data.json()

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        for i in range(last_video_index, len(chunk)):
            start_video = chunk[i]
            start_video_id = start_video["_id"]
            start_video_published = start_video["publishedDate"]

            print(f"🌱 Seed: {start_video}")

            related_ids = await get_related_videos(page, start_video_id, VIDEOS_PER_RUN)
            if not related_ids:
                continue

            checked = 0
            for video_id in related_ids:
                if checked >= VIDEOS_PER_RUN:
                    break

                try:
                    metadata = requests.get(f"https://noembed.com/embed?url=https://youtube.com/watch?v={video_id}").json()
                    published_date = metadata.get("upload_date")
                    if published_date:
                        published = datetime.strptime(published_date, "%Y%m%d")
                        days_old = (datetime.utcnow() - published).days
                        if MIN_AGE_DAYS <= days_old <= MAX_AGE_DAYS:
                            await process_video(page, video_id)
                            checked += 1
                except Exception as e:
                    continue

            # Update progress
            update_chunk_progress(chunk_index, i + 1)

            # Stop after first usable seed
            break

        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())