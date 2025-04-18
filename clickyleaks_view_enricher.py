import requests
from supabase import create_client, Client
import time

# Supabase credentials
SUPABASE_URL = "https://qbjcrvrsrivohcecjbij.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..."
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# GoDaddy credentials
GODADDY_API_KEY = "dL3WQsXW4CjU_Ai76RpB3utPSoxM3fT1gpQ"
GODADDY_API_SECRET = "63NHtkfJLbnVnjCbMH21Us"

def video_exists(video_id):
    url = f"https://www.youtube.com/oembed?url=https://www.youtube.com/watch?v={video_id}&format=json"
    try:
        r = requests.get(url, timeout=8)
        return r.status_code == 200
    except:
        return False

def get_youtube_view_count(video_id):
    info_url = f"https://www.youtube.com/watch?v={video_id}"
    headers = {
        "User-Agent": "Mozilla/5.0"
    }
    try:
        r = requests.get(info_url, headers=headers, timeout=10)
        if "viewCount" in r.text:
            import re
            match = re.search(r'"viewCount":"(\d+)"', r.text)
            return int(match.group(1)) if match else None
    except:
        pass
    return None

def is_domain_available(domain):
    url = f"https://api.godaddy.com/v1/domains/available?domain={domain}"
    headers = {
        "Authorization": f"sso-key {GODADDY_API_KEY}:{GODADDY_API_SECRET}",
        "Accept": "application/json"
    }
    try:
        r = requests.get(url, headers=headers, timeout=10)
        r.raise_for_status()
        return r.json().get("available", False)
    except Exception as e:
        print(f"⚠️ Domain check failed: {domain} -> {e}")
        return False

def main():
    print("🚀 Running Clickyleaks Combined Verifier...")

    # Step 1: Check if any unverified rows exist
    unverified_response = supabase.table("Clickyleaks")\
        .select("id")\
        .or_("verified.eq.false,verified.is.null")\
        .limit(1).execute()

    if unverified_response.data:
        print("🔍 Checking only unverified entries...")
        query = supabase.table("Clickyleaks")\
            .select("id, domain, video_id")\
            .or_("verified.eq.false,verified.is.null")\
            .limit(1000)
    else:
        print("🧹 No unverified found — scanning ALL entries just to be sure...")
        query = supabase.table("Clickyleaks")\
            .select("id, domain, video_id")\
            .limit(1000)

    response = query.execute()

    for row in response.data:
        row_id = row["id"]
        domain = row["domain"]
        video_id = row["video_id"]

        print(f"\n🔍 Checking YouTube ID: {video_id}")
        if not video_exists(video_id):
            print(f"❌ Video does not exist. Deleting row for domain: {domain}")
            supabase.table("Clickyleaks").delete().eq("id", row_id).execute()
            continue

        print(f"✅ Video exists. Checking domain: {domain}")
        available = is_domain_available(domain)
        if available:
            view_count = get_youtube_view_count(video_id)
            supabase.table("Clickyleaks").update({
                "verified": True,
                "view_count": view_count
            }).eq("id", row_id).execute()
            print(f"✅ Domain available + updated: {domain} | Views: {view_count or '–'}")
        else:
            print(f"❌ Domain not available. Removing: {domain}")
            supabase.table("Clickyleaks").delete().eq("id", row_id).execute()

        time.sleep(1.2)

if __name__ == "__main__":
    main()