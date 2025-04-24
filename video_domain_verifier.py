import os
import asyncio
import re
from urllib.parse import urlparse
from supabase import create_client, Client
from playwright.async_api import async_playwright
import requests

# === CONFIG ===
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
DOMAINR_API_KEY = os.getenv("DOMAINR_API_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Well-known domains to skip
WELL_KNOWN_DOMAINS = {
    "google.com", "youtube.com", "facebook.com", "cnn.com", "tumblr.com",
    "apple.com", "itunes.com", "amazon.com", "microsoft.com", "ebay.com"
}


def get_domain_root(domain):
    parts = domain.lower().replace("www.", "").split(".")
    if len(parts) >= 2:
        return ".".join(parts[-2:])
    return domain


def should_skip(domain):
    root = get_domain_root(domain)
    return root in WELL_KNOWN_DOMAINS


def check_domain_domainr(domain):
    try:
        headers = {
            "X-RapidAPI-Key": DOMAINR_API_KEY,
            "X-RapidAPI-Host": "domainr.p.rapidapi.com"
        }
        params = {"domain": domain}
        res = requests.get("https://domainr.p.rapidapi.com/v2/status", headers=headers, params=params, timeout=10)
        data = res.json()

        if "status" not in data or not data["status"]:
            raise ValueError("Missing status")

        status_code = data["status"][0]["status"]
        return "inactive" in status_code or "undelegated" in status_code or "marketed" in status_code

    except Exception as e:
        print(f"❌ Domainr check failed for {domain}: {e}")
        return None


async def get_video_details(page, video_url):
    try:
        await page.goto(video_url, timeout=20000)
        await page.wait_for_selector("#title h1", timeout=7000)
        await page.wait_for_selector("#view-count", timeout=7000)

        title_element = await page.query_selector("#title h1")
        view_element = await page.query_selector("#view-count")

        title = (await title_element.inner_text()).strip() if title_element else "Unknown"
        view_text = (await view_element.inner_text()).strip() if view_element else "0 views"

        match = re.search(r"([\d,]+)", view_text)
        views = int(match.group(1).replace(",", "")) if match else 0

        return title, views

    except Exception as e:
        print(f"❌ Could not load video: {e}")
        return "Unknown", 0


async def update_row(row, page, tab):
    domain = row["domain"]
    video_url = row["video_url"]
    video_id = row["video_id"]
    row_id = row["id"]
    domain_root = get_domain_root(domain)

    print(f"🔍 [Tab {tab}] Checking video: {video_id} | domain: {domain_root}")

    if should_skip(domain_root):
        print(f"🚫 [Tab {tab}] Skipping {domain} (well-known)")
        supabase.table("Clickyleaks").update({
            "verified": True,
            "is_available": False
        }).eq("id", row_id).execute()
        return

    title, views = await get_video_details(page, video_url)
    is_available = check_domain_domainr(domain_root)

    if is_available is None:
        print(f"⚠️ [Tab {tab}] Skipping {domain} due to failed Domainr check.")
        return

    print(f"✅ [Tab {tab}] Updating row → title: {title[:40]} | views: {views} | available: {is_available}")
    supabase.table("Clickyleaks").update({
        "verified": True,
        "is_available": is_available,
        "video_title": title,
        "view_count": views
    }).eq("id", row_id).execute()


async def main():
    print("🚀 Running Video + Domain Verifier (Final Scraper Version)")
    print("🚀 Clickyleaks Verifier (Playwright + Domainr) Starting...")

    response = supabase.table("Clickyleaks") \
        .select("*") \
        .or_("verified.is.false,verified.is.null") \
        .order("discovered_at", desc=False) \
        .limit(100) \
        .execute()

    rows = response.data
    if not rows:
        print("✅ No rows to verify.")
        return

    async with async_playwright() as pw:
        browser = await pw.firefox.launch(headless=True)
        context = await browser.new_context()

        # Split into two tabs to double up processing
        tab1 = await context.new_page()
        tab2 = await context.new_page()

        for i in range(0, len(rows), 2):
            tasks = []
            if i < len(rows):
                tasks.append(update_row(rows[i], tab1, 1))
            if i + 1 < len(rows):
                tasks.append(update_row(rows[i + 1], tab2, 2))
            await asyncio.gather(*tasks)

        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
