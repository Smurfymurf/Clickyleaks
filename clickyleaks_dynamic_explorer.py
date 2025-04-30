import asyncio
import json
import os
import re
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

MIN_AGE_DAYS = 365 * 3
MAX_AGE_DAYS = 365 * 7
MIN_VIEW_COUNT = 20000

# Top 100 well-known domains (common social, corporate, media, services)
WELL_KNOWN_DOMAINS = [
    "google.com", "youtube.com", "facebook.com", "twitter.com", "instagram.com", "linkedin.com",
    "wikipedia.org", "amazon.com", "apple.com", "microsoft.com", "netflix.com", "yahoo.com",
    "reddit.com", "bing.com", "whatsapp.com", "office.com", "live.com", "zoom.us", "pinterest.com",
    "ebay.com", "tiktok.com", "dropbox.com", "adobe.com", "spotify.com", "wordpress.com", "paypal.com",
    "imdb.com", "cnn.com", "bbc.com", "quora.com", "tumblr.com", "roblox.com", "stackexchange.com",
    "stack overflow.com", "etsy.com", "weebly.com", "blogger.com", "telegram.org", "kickstarter.com",
    "fiverr.com", "canva.com", "coursera.org", "udemy.com", "codepen.io", "producthunt.com",
    "github.com", "gitlab.com", "bitbucket.org", "unsplash.com", "airbnb.com", "booking.com",
    "tripadvisor.com", "indeed.com", "glassdoor.com", "zillow.com", "craigslist.org", "vimeo.com",
    "archive.org", "forbes.com", "bloomberg.com", "nytimes.com", "npr.org", "huffpost.com", "msn.com",
    "foxnews.com", "nbcnews.com", "cnbc.com", "marketwatch.com", "wsj.com", "weather.com", "accuweather.com",
    "ycombinator.com", "medium.com", "notion.so", "slack.com", "trello.com", "asana.com", "salesforce.com",
    "oracle.com", "ibm.com", "intuit.com", "quickbooks.com", "mailchimp.com", "getresponse.com",
    "aweber.com", "convertkit.com", "discord.com", "x.com", "protonmail.com", "icloud.com", "icloud.net",
    "duckduckgo.com", "opera.com", "mozilla.org", "brave.com", "coinbase.com", "binance.com", "opensea.io",
    "etherscan.io", "polygonscan.com", "chainlink.com", "coindesk.com", "cointelegraph.com"
]

# --- Supabase helpers ---
def get_current_chunk_progress():
    result = supabase.table("Clickyleaks_SeedProgress").select("*").execute()
    if not result.data:
        return 0, 0
    return result.data[0]["chunk_index"], result.data[0]["video_index"]

def update_chunk_progress(chunk_index, video_index):
    supabase.table("Clickyleaks_SeedProgress").upsert({
        "chunk_index": chunk_index,
        "video_index": video_index
    }, on_conflict=["chunk_index"]).execute()

def mark_video_scanned(video_id):
    supabase.table("Clickyleaks_DynamicChecked").upsert({
        "video_id": video_id
    }, on_conflict=["video_id"]).execute()

def already_scanned(video_id):
    res = supabase.table("Clickyleaks_DynamicChecked").select("id").eq("video_id", video_id).execute()
    return bool(res.data)

def log_potential_domain(domain, video_id, views, available):
    supabase.table("Clickyleaks").insert({
        "domain": domain,
        "source_video_id": video_id,
        "views": views,
        "available": available,
        "verified": False
    }).execute()

def send_discord_notification(domain, video_id, views):
    message = {
        "content": f"**New Potential Domain Found!**\n\nDomain: `{domain}`\nSource Video: https://youtube.com/watch?v={video_id}\nViews: {views:,}"
    }
    requests.post(DISCORD_WEBHOOK_URL, json=message)

def send_discord_summary(processed, found):
    message = {
        "content": f"**Clickyleaks Run Complete**\n\nRelated videos processed: `{processed}`\nPotential domains found: `{found}`"
    }
    requests.post(DISCORD_WEBHOOK_URL, json=message)

# --- Helpers ---
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
        return response.status_code in [403, 404, 502, 503]
    except:
        return True

# --- Main logic ---
async def process_video(page, video_id, views):
    if already_scanned(video_id):
        return False
    description = await fetch_video_description(page, video_id)
    mark_video_scanned(video_id)
    found = False
    for domain in extract_domains_from_text(description):
        if is_expired_soft(domain):
            log_potential_domain(domain, video_id, views, available=True)
            send_discord_notification(domain, video_id, views)
        else:
            log_potential_domain(domain, video_id, views, available=False)
        found = True
    return found

async def main():
    chunk_index, last_video_index = get_current_chunk_progress()
    chunk_url = CHUNK_URL_TEMPLATE.format(chunk_index)
    res = requests.get(chunk_url)
    if not res.ok:
        print(f"❌ Failed to load chunk {chunk_index}")
        return
    chunk = res.json()

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        for i in range(last_video_index, len(chunk)):
            start_video = chunk[i]
            start_video_id = start_video["_id"]
            print(f"🌱 Seed: {start_video_id}")
            mark_video_scanned(start_video_id)

            related_ids = await get_related_videos(page, start_video_id, VIDEOS_PER_RUN)
            if not related_ids:
                continue

            checked = 0
            found_domains = 0
            for video_id in related_ids:
                if checked >= VIDEOS_PER_RUN:
                    break
                try:
                    metadata = requests.get(f"https://noembed.com/embed?url=https://youtube.com/watch?v={video_id}").json()
                    published_date = metadata.get("upload_date")
                    views = int(metadata.get("view_count", 0))
                    if published_date and views >= MIN_VIEW_COUNT:
                        published = datetime.strptime(published_date, "%Y%m%d")
                        age = (datetime.utcnow() - published).days
                        if MIN_AGE_DAYS <= age <= MAX_AGE_DAYS:
                            success = await process_video(page, video_id, views)
                            if success:
                                found_domains += 1
                            checked += 1
                except:
                    continue

            update_chunk_progress(chunk_index, i + 1)
            send_discord_summary(checked, found_domains)
            break

        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())