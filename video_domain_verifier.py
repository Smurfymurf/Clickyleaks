import os
import asyncio
from datetime import datetime
from urllib.parse import urlparse

from supabase import create_client, Client
from playwright.async_api import async_playwright

# === CONFIG ===
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
GODADDY_API_KEY = os.getenv("GODADDY_API_KEY")
GODADDY_API_SECRET = os.getenv("GODADDY_API_SECRET")

YOUTUBE_TEMPLATE = "https://www.youtube.com/watch?v="

HEADERS_GODADDY = {
    "Authorization": f"sso-key {GODADDY_API_KEY}:{GODADDY_API_SECRET}",
    "Accept": "application/json"
}

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


def get_domain_root(domain):
    parts = domain.lower().strip().split(".")
    if len(parts) >= 2:
        return ".".join(parts[-2:])
    return domain


async def check_video_exists(video_url, page):
    try:
        await page.goto(video_url, timeout=15000)
        content = await page.content()
        return "video unavailable" not in content.lower()
    except Exception:
        return False


def check_domain_godaddy(domain):
    import requests
    try:
        response = requests.get(
            f"https://api.godaddy.com/v1/domains/available?domain={domain}&checkType=FAST&forTransfer=false",
            headers=HEADERS_GODADDY,
            timeout=10
        )
        data = response.json()
        return data.get("available", False)
    except Exception:
        return False


async def update_row(row, page):
    domain = get_domain_root(row["domain"])
    video_url = row["video_url"]
    video_id = row["video_id"]
    row_id = row["id"]

    print(f"🔍 Checking video: {video_id} | domain: {domain}")

    # 1. Check video existence via Playwright
    exists = await check_video_exists(video_url, page)
    if not exists:
        print(f"❌ Video {video_id} no longer exists. Removing row...")
        supabase.table("Clickyleaks").delete().eq("id", row_id).execute()
        return

    # 2. Check domain availability
    is_available = check_domain_godaddy(domain)

    # 3. Update row with status
    print(f"✅ Updating row: verified={True}, available={is_available}")
    supabase.table("Clickyleaks").update({
        "verified": True,
        "is_available": is_available
    }).eq("id", row_id).execute()


async def main():
    print("🚀 Clickyleaks Playwright Verifier Started...")

    response = supabase.table("Clickyleaks") \
        .select("*") \
        .or_("verified.is.false,verified.is.null") \
        .limit(20) \
        .execute()

    rows = response.data
    if not rows:
        print("✅ No unverified entries.")
        return

    async with async_playwright() as pw:
        browser = await pw.firefox.launch(headless=True)
        page = await browser.new_page()

        for row in rows:
            await update_row(row, page)

        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
