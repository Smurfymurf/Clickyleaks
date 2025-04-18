
import asyncio
from playwright.async_api import async_playwright
import requests
from urllib.parse import quote
from supabase import create_client
import os

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

async def is_video_live(video_id):
    url = f"https://www.youtube.com/oembed?url=https://www.youtube.com/watch?v={video_id}&format=json"
    try:
        res = requests.get(url, timeout=10)
        return res.status_code == 200
    except:
        return False

async def is_domain_available(domain, page):
    try:
        encoded_domain = quote(domain)
        search_url = f"https://www.godaddy.com/domainsearch/find?checkAvail=1&domainToCheck={encoded_domain}"
        await page.goto(search_url, timeout=20000)
        await page.wait_for_selector('text="Exact Match"', timeout=8000)
        element_text = await page.inner_text('[data-cy="exact-match"]')
        return domain.lower() in element_text.lower()
    except:
        return False

async def main():
    print("ð Running Playwright Verifier for unverified domains...")
    results = supabase.table("Clickyleaks").select("*").eq("verified", False).execute()
    rows = results.data
    if not rows:
        print("â No unverified entries found.")
        return

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        for row in rows:
            video_id = row["video_id"]
            domain = row["domain"]
            entry_id = row["id"]

            if not await is_video_live(video_id):
                print(f"â Video does not exist: {video_id}")
                supabase.table("Clickyleaks").delete().eq("id", entry_id).execute()
                continue

            if await is_domain_available(domain, page):
                print(f"â Domain is available: {domain}")
                supabase.table("Clickyleaks").update({
                    "verified": True
                }).eq("id", entry_id).execute()
            else:
                print(f"â Domain is not available: {domain}")
                supabase.table("Clickyleaks").delete().eq("id", entry_id).execute()

        await browser.close()

asyncio.run(main())
