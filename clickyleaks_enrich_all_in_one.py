import requests
import os
from datetime import datetime
from supabase import create_client, Client
from urllib.parse import urlparse

# === ENV CONFIG ===
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
GODADDY_KEY = os.getenv("GODADDY_API_KEY")
GODADDY_SECRET = os.getenv("GODADDY_API_SECRET")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

HEADERS_GODADDY = {
    "Authorization": f"sso-key {GODADDY_KEY}:{GODADDY_SECRET}",
    "Accept": "application/json"
}

def is_video_live(video_id):
    url = "https://www.googleapis.com/youtube/v3/videos"
    params = {
        "part": "status",
        "id": video_id,
        "key": YOUTUBE_API_KEY
    }
    res = requests.get(url, params=params, timeout=10)
    items = res.json().get("items", [])
    return bool(items)

def is_domain_available_godaddy(domain):
    url = f"https://api.godaddy.com/v1/domains/available?domain={domain}"
    try:
        res = requests.get(url, headers=HEADERS_GODADDY, timeout=10)
        data = res.json()
        return data.get("available", False)
    except Exception as e:
        print(f"⚠️ GoDaddy check failed for {domain}: {e}")
        return False

def clean_domain(domain):
    domain = domain.strip().lower()
    if domain.startswith("www."):
        domain = domain[4:]
    domain = domain.split("/")[0]
    return domain

def main():
    print("🚀 Clickyleaks Enrichment Script (GoDaddy + YouTube) Started...")

    response = supabase.table("Clickyleaks").select("*").eq("verified", False).limit(100).execute()
    entries = response.data

    if not entries:
        print("✅ No unverified entries to process.")
        return

    for entry in entries:
        domain = clean_domain(entry["domain"])
        video_id = entry["video_id"]
        row_id = entry["id"]

        print(f"🔍 Checking video ID: {video_id} and domain: {domain}")

        if not is_video_live(video_id):
            print(f"❌ Video not live: {video_id} — Deleting row.")
            supabase.table("Clickyleaks").delete().eq("id", row_id).execute()
            continue

        is_available = is_domain_available_godaddy(domain)
        print(f"✅ Video exists | Domain '{domain}' Available: {is_available}")

        supabase.table("Clickyleaks").update({
            "is_available": is_available,
            "verified": True,
            "scanned_at": datetime.utcnow().isoformat()
        }).eq("id", row_id).execute()

    print("✅ Enrichment pass complete.")

if __name__ == "__main__":
    main()
