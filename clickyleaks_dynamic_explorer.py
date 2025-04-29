import asyncio
import os
import re
import requests
from datetime import datetime, timedelta
from urllib.parse import urlparse
from playwright.async_api import async_playwright
from supabase import create_client

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")

WELL_KNOWN_DOMAINS = ["youtube.com", "facebook.com", "twitter.com", "instagram.com", "google.com", "bit.ly", "youtu.be"]
CHUNK_BASE_URL = "https://smurfymurf.github.io/clickyleaks-chunks/chunk_"
VIDEOS_PER_RUN = 200
MIN_AGE_DAYS = 3 * 365
MAX_AGE_DAYS = 7 * 365

def already_scanned(video_id):
    res = supabase.table("Clickyleaks_DynamicChecked").select("id").eq("video_id", video_id).execute()
    return len(res.data) > 0

def mark_video_scanned(video_id):
    supabase.table("Clickyleaks_DynamicChecked").insert({"video_id": video_id}).execute()

def soft_check_domain(domain):
    try:
        response = requests.head(f"http://{domain}", timeout=5, allow_redirects=True)
        if response.status_code >= 400:
            return True
    except Exception:
        return True
    return False

def add_to_clickyleaks(domain, video_id):
    supabase.table("Clickyleaks").insert({
        "domain": domain,
        "source_video_id": video_id,
        "is_verified": False
    }).execute()

def get_current_chunk_progress():
    result = supabase.table("Clickyleaks_SeedProgress").select("*").limit(1).execute()
    if result.data:
        return result.data[0]["chunk_index"], result.data[0]["video_index"]
    return 0, 0

def update_chunk_progress(chunk_index, video_index):
    supabase.table("Clickyleaks_SeedProgress").upsert({
        "id": 1,
        "chunk_index": chunk_index,
        "video_index": video_index,
        "timestamp": datetime.utcnow().isoformat()
    }).execute()

async def fetch_description(page, video_id):
    await page.goto(f"https://www.youtube.com/watch?v={video_id}", timeout=60000)
    await page.wait_for_selector('meta[name="description"]', timeout=10000)
    content = await page.locator('meta[name="description"]').get_attribute("content")
    return content or ""

def extract_domains(text):
    urls = re.findall(r'(https?://[^\s]+)', text)
    domains = set()
    for url in urls:
        parsed = urlparse(url)
        domain = parsed.netloc.lower()
        if any(wkd in domain for wkd in WELL_KNOWN_DOMAINS):
            continue
        domains.add(domain)
    return list(domains)

def is_within_age_range(published_date):
    try:
        published = datetime.fromisoformat(published_date.replace("Z", "+00:00"))
        age = (datetime.utcnow() - published).days
        return MIN_AGE_DAYS <= age <= MAX_AGE_DAYS
    except Exception:
        return False

async def get_related_videos(page, video_id, max_related):
    await page.goto(f"https://www.youtube.com/watch?v={video_id}", timeout=60000)
    try:
        await page.wait_for_selector("ytd-watch-next-secondary-results-renderer", timeout=15000)
    except Exception:
        print(f"⚠️ Timeout waiting for related for {video_id}, skipping...")
        return []

    related_elements = await page.query_selector_all("ytd-compact-video-renderer")
    related_ids = []

    for element in related_elements:
        href = await element.get_attribute("href")
        if href and "/watch?v=" in href:
            related_id = href.split("v=")[-1].split("&")[0]
            if related_id != video_id:
                related_ids.append(related_id)
        if len(related_ids) >= max_related:
            break

    return related_ids

async def process_video(page, video_id):
    if already_scanned(video_id):
        return False

    description = await fetch_description(page, video_id)
    domains = extract_domains(description)
    found = False
    for domain in domains:
        if soft_check_domain(domain):
            add_to_clickyleaks(domain, video_id)
            print(f"✅ Found expired-looking domain: {domain}")
            found = True
    mark_video_scanned(video_id)
    return found

async def main():
    print("🚀 Clickyleaks Dynamic Explorer Starting...")
    chunk_index, last_video_index = get_current_chunk_progress()
    chunk_url = f"{CHUNK_BASE_URL}{chunk_index}.json"
    chunk = requests.get(chunk_url).json()
    seed_videos = chunk if isinstance(chunk, list) else chunk["videos"]
    found_count = 0

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        for i in range(last_video_index, len(seed_videos)):
            if found_count >= VIDEOS_PER_RUN:
                break
            seed = seed_videos[i]
            start_video_id = seed.get("_id") or seed.get("video_id")
            published_date = seed.get("publishedDate") or ""

            print(f"🌱 Seed: {seed}")
            if not is_within_age_range(published_date):
                print("⏭️ Video outside age range, skipping seed...")
                continue

            related_ids = await get_related_videos(page, start_video_id, VIDEOS_PER_RUN)
            for vid in related_ids:
                if found_count >= VIDEOS_PER_RUN:
                    break
                if await process_video(page, vid):
                    found_count += 1

            update_chunk_progress(chunk_index, i + 1)

        await browser.close()

    print(f"✅ Done. Domains found: {found_count}")
    if found_count > 0:
        requests.post(DISCORD_WEBHOOK_URL, json={"content": f"Clickyleaks run complete. Found {found_count} potential expired domains."})

if __name__ == "__main__":
    asyncio.run(main())