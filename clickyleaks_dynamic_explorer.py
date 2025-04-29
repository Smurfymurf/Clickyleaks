import os
import re
import random
import asyncio
import requests
from datetime import datetime
from urllib.parse import urlparse
from supabase import create_client, Client
from playwright.async_api import async_playwright

# === CONFIG ===
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")
STARTING_CHUNK = 102
MAX_VIDEOS_PER_RUN = 50  # Stop after scanning this many videos each run

CHUNK_BASE_URL = "https://smurfymurf.github.io/clickyleaks-chunks/"
WELL_KNOWN_DOMAINS = {
    "apple.com", "google.com", "facebook.com", "amazon.com", "youtube.com",
    "microsoft.com", "netflix.com", "instagram.com", "paypal.com", "reddit.com",
    "wikipedia.org", "tumblr.com", "github.com", "linkedin.com", "spotify.com",
    "cnn.com", "bbc.com", "dropbox.com", "airbnb.com", "salesforce.com",
    "tiktok.com", "ebay.com", "zoom.us", "whatsapp.com", "nytimes.com",
    "oracle.com", "bing.com", "slack.com", "notion.so", "wordpress.com",
    "vercel.app", "netlify.app", "figma.com", "medium.com", "shopify.com",
    "yahoo.com", "pinterest.com", "imdb.com", "quora.com", "adobe.com",
    "cloudflare.com", "soundcloud.com", "coursera.org", "kickstarter.com",
    "mozilla.org", "forbes.com", "theguardian.com", "weather.com", "espn.com",
    "msn.com", "okta.com", "bitbucket.org", "vimeo.com", "unsplash.com",
    "canva.com", "zoom.com", "atlassian.com", "ycombinator.com", "stripe.com",
    "zendesk.com", "hotstar.com", "reuters.com", "nationalgeographic.com",
    "weebly.com", "behance.net", "dribbble.com", "skype.com", "opera.com",
    "twitch.tv", "stackoverflow.com", "stackoverflow.blog"
}

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def extract_links(text):
    return re.findall(r'(https?://[^\s)]+)', text)

def normalize_domain(link: str) -> str:
    parsed = urlparse(link)
    host = parsed.netloc or parsed.path
    return host.replace("www.", "").lower()

async def fetch_video_description(page, video_id):
    video_url = f"https://www.youtube.com/watch?v={video_id}"
    await page.goto(video_url)
    await page.wait_for_selector('meta[name="description"]', timeout=10000)
    description_element = await page.query_selector('meta[name="description"]')
    if description_element:
        return await description_element.get_attribute('content')
    return ""

def is_domain_available(domain):
    try:
        res = requests.get(f"http://{domain}", timeout=5)
        return False
    except:
        return True

def send_discord_alert(domain, video_url):
    if not DISCORD_WEBHOOK_URL:
        return
    message = {"content": f"🔥 Available domain found: `{domain}`\n🔗 {video_url}"}
    try:
        requests.post(DISCORD_WEBHOOK_URL, json=message, timeout=5)
    except:
        pass

def already_scanned(video_id):
    result = supabase.table("Clickyleaks_DynamicChecked").select("id").eq("video_id", video_id).execute()
    return len(result.data) > 0

def mark_video_scanned(video_id):
    supabase.table("Clickyleaks_DynamicChecked").insert({
        "video_id": video_id,
        "scanned_at": datetime.utcnow().isoformat()
    }).execute()

async def process_video(page, video_id):
    if already_scanned(video_id):
        print(f"⚠️ Already scanned: {video_id}, skipping.")
        return False

    description = await fetch_video_description(page, video_id)
    if not description:
        print(f"⚠️ No description found for {video_id}, skipping.")
        mark_video_scanned(video_id)
        return False

    links = extract_links(description)
    for link in links:
        try:
            domain = normalize_domain(link)
            if not domain or len(domain.split(".")) < 2:
                continue
            if domain in WELL_KNOWN_DOMAINS:
                print(f"🚫 Skipping well-known domain: {domain}")
                continue
        except Exception as e:
            print(f"⚠️ Invalid link: {link} ({e})")
            continue

        available = is_domain_available(domain)
        print(f"🔍 Checking domain: {domain} | Available: {available}")

        if available:
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
            send_discord_alert(domain, f"https://www.youtube.com/watch?v={video_id}")
            break

    mark_video_scanned(video_id)
    return True

async def main():
    print("🚀 Clickyleaks Dynamic Explorer Starting...")
    chunk_number = STARTING_CHUNK
    chunk_url = f"{CHUNK_BASE_URL}chunk_{chunk_number}.json"

    print(f"📥 Loading seed chunk: {chunk_url}")
    res = requests.get(chunk_url)
    res.raise_for_status()
    videos = res.json()

    seed_video = random.choice(videos)
    seed_video_id = seed_video["_id"]

    scanned = 0

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        current_video_id = seed_video_id

        while scanned < MAX_VIDEOS_PER_RUN:
            found = await process_video(page, current_video_id)
            if found:
                scanned += 1

            # Now try to get related videos
            video_url = f"https://www.youtube.com/watch?v={current_video_id}"
            await page.goto(video_url)
            await page.wait_for_selector('ytd-compact-video-renderer', timeout=10000)
            related = await page.query_selector_all('ytd-compact-video-renderer')

            if not related:
                print("⚠️ No related videos found, picking new seed...")
                current_video_id = random.choice(videos)["_id"]
                continue

            next_video = random.choice(related)
            href = await next_video.query_selector('a#thumbnail')
            if href:
                link = await href.get_attribute('href')
                if link and "/watch?v=" in link:
                    current_video_id = link.split("v=")[-1].split("&")[0]
                else:
                    current_video_id = random.choice(videos)["_id"]
            else:
                current_video_id = random.choice(videos)["_id"]

        await browser.close()

    print(f"✅ Scan complete. {scanned} videos processed.")

if __name__ == "__main__":
    asyncio.run(main())
