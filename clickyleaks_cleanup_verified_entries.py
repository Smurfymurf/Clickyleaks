import requests
import os
from supabase import create_client, Client

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
GODADDY_API_KEY = os.getenv("GODADDY_API_KEY")
GODADDY_API_SECRET = os.getenv("GODADDY_API_SECRET")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def video_exists(video_id):
    url = f"https://www.youtube.com/oembed?url=https://www.youtube.com/watch?v={video_id}&format=json"
    try:
        res = requests.get(url, timeout=5)
        return res.status_code == 200
    except:
        return False

def domain_available_godaddy(domain):
    url = f"https://api.godaddy.com/v1/domains/available?domain={domain}&checkType=FULL"
    headers = {
        "Authorization": f"sso-key {GODADDY_API_KEY}:{GODADDY_API_SECRET}",
        "Accept": "application/json"
    }
    try:
        res = requests.get(url, headers=headers, timeout=10)
        data = res.json()
        return data.get("available") is True and data.get("exactMatch") is True
    except Exception as e:
        print(f"Error checking {domain}: {e}")
        return False

def run_cleanup():
    print("🚀 Starting Clickyleaks Verified Cleanup...")
    res = supabase.table("Clickyleaks").select("*").eq("verified", True).limit(1000).execute()
    rows = res.data
    if not rows:
        print("✅ No verified entries to clean.")
        return

    for row in rows:
        domain = row["domain"]
        video_id = row["video_id"]
        id_ = row["id"]

        print(f"🔍 Rechecking {domain} (Video ID: {video_id})")

        # Step 1: Check video
        if not video_exists(video_id):
            print(f"❌ Video not found: {video_id} — Deleting entry")
            supabase.table("Clickyleaks").delete().eq("id", id_).execute()
            continue

        # Step 2: Check domain
        if not domain_available_godaddy(domain):
            print(f"❌ Domain no longer available: {domain} — Deleting entry")
            supabase.table("Clickyleaks").delete().eq("id", id_).execute()
        else:
            print(f"✅ Still valid: {domain}")

    print("🏁 Cleanup finished.")

if __name__ == "__main__":
    run_cleanup()