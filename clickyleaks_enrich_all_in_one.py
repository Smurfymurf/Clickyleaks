import os
import requests
from supabase import create_client, Client
from urllib.parse import urlparse
from datetime import datetime

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def is_domain_available(domain):
    root = domain.lower().strip()
    if root.startswith("www."):
        root = root[4:]
    root = root.split("/")[0]
    try:
        requests.get(f"http://{root}", timeout=5)
        return False
    except:
        return True

def get_youtube_view_count(video_id):
    try:
        res = requests.get(f"https://www.youtube.com/watch?v={video_id}", timeout=5)
        if "Video unavailable" in res.text:
            return None  # Video doesn't exist
        match = next((line for line in res.text.splitlines() if 'viewCount' in line), None)
        if match:
            import re
            view_count = re.findall(r'"viewCount":"(\d+)"', match)
            return int(view_count[0]) if view_count else 0
        return 0
    except:
        return None

def send_discord_alert(domain, video_url):
    requests.post(DISCORD_WEBHOOK_URL, json={
        "content": f"🔥 Available domain found: `{domain}`\n🔗 {video_url}"
    })

def main():
    print("🚀 Clickyleaks Enrichment Script Started...")

    result = supabase.table("Clickyleaks") \
        .select("*") \
        .or_("verified.is.false,verified.is.null") \
        .limit(50) \
        .execute()

    rows = result.data
    print(f"🔎 Found {len(rows)} unverified rows")

    for row in rows:
        id = row["id"]
        domain = row["domain"]
        video_url = row["video_url"]
        video_id = row["video_id"]

        print(f"🔍 Checking: {domain} | Video: {video_id}")

        view_count = get_youtube_view_count(video_id)
        if view_count is None:
            print(f"❌ Video {video_id} no longer exists. Deleting row.")
            supabase.table("Clickyleaks").delete().eq("id", id).execute()
            continue

        available = is_domain_available(domain)
        if not available:
            print(f"❌ Domain {domain} no longer available. Deleting row.")
            supabase.table("Clickyleaks").delete().eq("id", id).execute()
            continue

        print(f"✅ {domain} is still available with {view_count} views.")
        supabase.table("Clickyleaks").update({
            "view_count": view_count,
            "verified": True,
            "scanned_at": datetime.utcnow().isoformat()
        }).eq("id", id).execute()

        send_discord_alert(domain, video_url)

    print("✅ Enrichment complete.")

if __name__ == "__main__":
    main()
