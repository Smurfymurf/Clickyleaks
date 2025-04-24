import os
import re
import tldextract
import asyncio
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout
from supabase import create_client, Client
import httpx

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
DOMAINR_API_KEY = os.getenv("DOMAINR_API_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

WELL_KNOWN_DOMAINS = set([
    "google.com", "youtube.com", "facebook.com", "twitter.com", "instagram.com",
    "linkedin.com", "wikipedia.org", "apple.com", "microsoft.com", "reddit.com",
    "yahoo.com", "amazon.com", "netflix.com", "whatsapp.com", "tiktok.com",
    "office.com", "zoom.us", "dropbox.com", "paypal.com", "bing.com", "tumblr.com",
    "imgur.com", "pinterest.com", "adobe.com", "mozilla.org", "quora.com",
    "nytimes.com", "bbc.com", "cnn.com", "forbes.com", "msn.com", "weather.com",
    "espn.com", "nasa.gov", "gov.uk", "usa.gov", "cdc.gov", "who.int",
    "archive.org", "cloudflare.com", "wordpress.com", "blogspot.com", "bit.ly",
    "tinyurl.com", "github.com", "gitlab.com", "stackoverflow.com", "medium.com",
    "soundcloud.com", "vimeo.com", "kickstarter.com", "indiegogo.com", "unsplash.com",
    "canva.com", "notion.so", "slack.com", "trello.com", "figma.com", "sketch.com",
    "typeform.com", "surveymonkey.com", "mailchimp.com", "getresponse.com",
    "godaddy.com", "namecheap.com", "bluehost.com", "shopify.com", "wix.com",
    "squarespace.com", "weebly.com", "bigcartel.com", "bitbucket.org", "heroku.com",
    "replit.com", "glitch.me", "vercel.app", "netlify.app", "firebaseapp.com"
])

def is_well_known(domain: str) -> bool:
    extracted = tldextract.extract(domain)
    root_domain = f"{extracted.domain}.{extracted.suffix}"
    return root_domain in WELL_KNOWN_DOMAINS

async def check_domain_availability(domain: str) -> bool:
    try:
        url = f"https://api.domainr.com/v2/status?domain={domain}&client_id={DOMAINR_API_KEY}"
        async with httpx.AsyncClient() as client:
            response = await client.get(url)
            data = response.json()
            return "inactive" in data["status"][0]["status"]
    except Exception as e:
        print(f"â Error checking domain {domain}: {e}")
        return False

async def fetch_video_info(page, video_id: str):
    url = f"https://www.youtube.com/watch?v={video_id}"
    try:
        await page.goto(url, timeout=10000)
        await page.wait_for_selector("h1.title, div#title h1", timeout=7000)
        await page.wait_for_selector("span.view-count", timeout=5000)
        title = await page.title()
        views_text = await page.locator("span.view-count").first.inner_text()
        return title, views_text
    except PlaywrightTimeout as e:
        print(f"â Error loading video {video_id}: {e}")
        return None, None

async def process_entry(entry, page, tab_id):
    id, video_id, domain = entry["id"], entry["video_id"], entry["domain"]
    print(f"ð [Tab {tab_id}] Checking video: {video_id} | domain: {domain}")

    if is_well_known(domain):
        print(f"ð« [Tab {tab_id}] Skipping {domain} (well-known)")
        supabase.table("clickyleaks_youtube").update({"verified": True, "available": False}).eq("id", id).execute()
        return

    available = await check_domain_availability(domain)
    title, views = await fetch_video_info(page, video_id)
    supabase.table("clickyleaks_youtube").update({
        "verified": True,
        "available": available,
        "video_title": title,
        "video_views": views
    }).eq("id", id).execute()

async def main():
    entries = supabase.table("clickyleaks_youtube").select("id, video_id, domain").eq("verified", False).limit(50).execute().data

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        page1 = await context.new_page()
        page2 = await context.new_page()
        tasks = []
        for i, entry in enumerate(entries):
            page = page1 if i % 2 == 0 else page2
            tasks.append(process_entry(entry, page, 1 if i % 2 == 0 else 2))
        await asyncio.gather(*tasks)
        await browser.close()

if __name__ == "__main__":
    print("ð Running Video + Domain Verifier (Final Scraper Version)")
    print("ð Clickyleaks Verifier (Playwright + Domainr) Starting...")
    asyncio.run(main())