import os
import requests
from datetime import datetime
from urllib.parse import urlparse
from supabase import create_client, Client

# === CONFIG ===
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
GODADDY_API_KEY = os.getenv("GODADDY_API_KEY")
GODADDY_API_SECRET = os.getenv("GODADDY_API_SECRET")
YOUTUBE_VIDEO_URL_TEMPLATE = "https://www.youtube.com/watch?v="

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

HEADERS_GODADDY = {
    "Authorization": f"sso-key {GODADDY_API_KEY}:{GODADDY_API_SECRET}",
    "Accept": "application/json"
}

def check_video_exists(video_url):
    try:
        res = requests.get(video_url, timeout=10)
        return "video unavailable" not in res.text.lower() and res.status_code == 200
    except Exception:
        return False

def get_domain_root(domain):
    parts = domain.lower().strip().split(".")
    if len(parts) >= 2:
        return ".".join(parts[-2:])
    return domain

def check_domain_godaddy(domain):
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

def update_row(row):
    domain = get_domain_root(row["domain"])
    video_id = row["video_id"]
    video_url = row["video_url"]
    row_id = row["id"]

    print(f"🔍 Checking video: {video_id} | domain: {domain}")

    if not check_video_exists(video_url):
        print(f"❌ Video {video_id} no longer exists. Deleting row...")
        supabase.table("Clickyleaks").delete().eq("id", row_id).execute()
        return

    is_available = check_domain_godaddy(domain)

    view_count = None
    try:
        view_count = int(row.get("view_count", 0)) if row.get("view_count") else 0
    except:
        view_count = 0

    print(f"✅ Updating record: domain={domain}, available={is_available}, view_count={view_count}")
    supabase.table("Clickyleaks").update({
        "is_available": is_available,
        "verified": True,
        "view_count": view_count
    }).eq("id", row_id).execute()

def main():
    print("🚀 Clickyleaks Enrichment Script Started...")

    response = supabase.table("Clickyleaks") \
        .select("*") \
        .or_("verified.is.false,verified.is.null") \
        .limit(20) \
        .execute()

    rows = response.data
    if not rows:
        print("✅ No unverified rows found.")
        return

    for row in rows:
        update_row(row)

if __name__ == "__main__":
    main()
