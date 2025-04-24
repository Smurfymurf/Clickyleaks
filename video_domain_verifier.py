import os
import re
import asyncio
import tldextract
import httpx
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
from supabase import create_client, Client

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
DOMAINR_API_KEY = os.getenv("DOMAINR_API_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

WELL_KNOWN_DOMAINS = {
    "google.com", "www.google.com", "facebook.com", "www.facebook.com",
    "youtube.com", "www.youtube.com", "twitter.com", "www.twitter.com",
    "instagram.com", "www.instagram.com", "linkedin.com", "www.linkedin.com",
    "wikipedia.org", "www.wikipedia.org", "apple.com", "www.apple.com",
    "microsoft.com", "www.microsoft.com", "amazon.com", "www.amazon.com",
    "yahoo.com", "www.yahoo.com", "reddit.com", "www.reddit.com",
    "netflix.com", "www.netflix.com", "whatsapp.com", "www.whatsapp.com",
    "tiktok.com", "www.tiktok.com", "pinterest.com", "www.pinterest.com",
    "paypal.com", "www.paypal.com", "imdb.com", "www.imdb.com",
    "fandom.com", "www.fandom.com", "bbc.com", "www.bbc.com",
    "cnn.com", "www.cnn.com", "tumblr.com", "www.tumblr.com",
    "office.com", "www.office.com", "live.com", "www.live.com",
    "dropbox.com", "www.dropbox.com", "zoom.us", "www.zoom.us",
    "walmart.com", "www.walmart.com", "bestbuy.com", "www.bestbuy.com",
    "craigslist.org", "www.craigslist.org", "bing.com", "www.bing.com",
    "ebay.com", "www.ebay.com", "msn.com", "www.msn.com",
    "etsy.com", "www.etsy.com", "quora.com", "www.quora.com",
    "nih.gov", "www.nih.gov", "cdc.gov", "www.cdc.gov",
    "nasa.gov", "www.nasa.gov", "whitehouse.gov", "www.whitehouse.gov",
    "gov.uk", "www.gov.uk", "gov.au", "www.gov.au",
    "gov.ca", "www.gov.ca", "gov.br", "www.gov.br",
    "who.int", "www.who.int", "baidu.com", "www.baidu.com",
    "vk.com", "www.vk.com", "naver.com", "www.naver.com",
    "twitch.tv", "www.twitch.tv", "roblox.com", "www.roblox.com",
    "spotify.com", "www.spotify.com", "adobe.com", "www.adobe.com",
    "cloudflare.com", "www.cloudflare.com", "stackoverflow.com", "www.stackoverflow.com",
    "github.com", "www.github.com", "gitlab.com", "www.gitlab.com",
    "medium.com", "www.medium.com", "vimeo.com", "www.vimeo.com",
    "wordpress.com", "www.wordpress.com", "blogspot.com", "www.blogspot.com",
    "weebly.com", "www.weebly.com", "wix.com", "www.wix.com",
    "duckduckgo.com", "www.duckduckgo.com", "icloud.com", "www.icloud.com",
    "archive.org", "www.archive.org", "slideshare.net", "www.slideshare.net",
    "slack.com", "www.slack.com", "bit.ly", "t.co", "lnkd.in",
    "goo.gl", "tinyurl.com", "ow.ly", "is.gd", "buff.ly", "y2u.be",
    "youtu.be", "forms.gle", "forms.office.com", "meet.google.com",
    "teams.microsoft.com", "calendly.com", "notion.so", "airtable.com",
    "figma.com", "canva.com", "mailchimp.com", "sendgrid.com",
    "zendesk.com", "intercom.com", "typeform.com", "surveymonkey.com",
    "hbr.org", "forbes.com", "bloomberg.com", "reuters.com",
    "nytimes.com", "wsj.com", "guardian.co.uk"
}

def extract_base_domain(domain):
    extracted = tldextract.extract(domain)
    return ".".join(part for part in [extracted.domain, extracted.suffix] if part)

async def check_domain_availability(domain):
    base = extract_base_domain(domain)
    if base in WELL_KNOWN_DOMAINS:
        return False
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(f"https://api.domainr.com/v2/status?domain={base}&client_id={DOMAINR_API_KEY}")
            status = response.json().get("status", [])[0].get("status", "")
            return "inactive" in status or "undelegated" in status or "marketed" in status
    except Exception as e:
        print(f"â Error checking domain {domain}: {e}")
        return False

async def fetch_video_details(video_id, page):
    url = f"https://www.youtube.com/watch?v={video_id}"
    try:
        await page.goto(url, timeout=10000)
        await page.wait_for_selector("h1.title, div#title h1", timeout=7000)
        await page.wait_for_selector("span.view-count", timeout=5000)
        title = await page.title()
        views = await page.locator("span.view-count").first.inner_text()
        return title.strip(), views.strip()
    except PlaywrightTimeoutError as e:
        print(f"â Timeout loading video {video_id}: {e}")
    except Exception as e:
        print(f"â Error loading video {video_id}: {e}")
    return None, None

async def main():
    print("ð Running Video + Domain Verifier (Final Scraper Version)")
    print("ð Clickyleaks Verifier (Playwright + Domainr) Starting...")

    entries = supabase.table("clickyleaks_youtube").select("id, video_id, domain").eq("verified", False).limit(50).execute()
    rows = entries.data

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page1 = await browser.new_page()
        page2 = await browser.new_page()

        for i, row in enumerate(rows):
            page = page1 if i % 2 == 0 else page2
            tab = 1 if i % 2 == 0 else 2
            video_id = row["video_id"]
            domain = row["domain"]
            print(f"ð [Tab {tab}] Checking video: {video_id} | domain: {domain}")

            is_available = await check_domain_availability(domain)
            if not is_available:
                await supabase.table("clickyleaks_youtube").update({
                    "verified": True,
                    "is_available": False
                }).eq("id", row["id"]).execute()
                continue

            title, views = await fetch_video_details(video_id, page)
            if title and views:
                await supabase.table("clickyleaks_youtube").update({
                    "verified": True,
                    "is_available": True,
                    "video_title": title,
                    "video_views": views
                }).eq("id", row["id"]).execute()
            else:
                await supabase.table("clickyleaks_youtube").update({
                    "verified": True,
                    "is_available": True,
                    "video_title": None,
                    "video_views": None
                }).eq("id", row["id"]).execute()

        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
