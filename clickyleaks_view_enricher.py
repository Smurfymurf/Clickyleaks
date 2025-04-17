import requests
from supabase import create_client
from datetime import datetime
import os

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def get_unscanned_rows():
    return supabase.table("Clickyleaks")\
        .select("*")\
        .or_("view_count.is.null,view_count.eq.0")\
        .limit(100)\
        .execute().data

def check_video(video_id):
    url = f"https://www.googleapis.com/youtube/v3/videos?part=statistics,snippet&id={video_id}&key={YOUTUBE_API_KEY}"
    try:
        res = requests.get(url, timeout=10)
        res.raise_for_status()
        data = res.json()
        if not data["items"]:
            return None  # video doesn't exist
        item = data["items"][0]
        stats = item["statistics"]
        snippet = item["snippet"]
        return {
            "view_count": int(stats.get("viewCount", 0)),
            "title": snippet.get("title", ""),
            "url": f"https://www.youtube.com/watch?v={video_id}"
        }
    except Exception as e:
        print(f"⚠️ Error checking video {video_id}: {e}")
        return "error"

def send_discord_alert(domain, views, title, video_url):
    try:
        payload = {
            "content": f"🔍 **New Verified Clickyleak!**\n"
                       f"**Domain:** `{domain}`\n"
                       f"**Views:** `{views}`\n"
                       f"**Video:** [{title}]({video_url})"
        }
        res = requests.post(DISCORD_WEBHOOK_URL, json=payload, timeout=10)
        if res.status_code == 204:
            print("📣 Discord alert sent.")
        else:
            print(f"⚠️ Discord alert failed: {res.status_code}")
    except Exception as e:
        print(f"❌ Discord error: {e}")

def enrich():
    print("🚀 Running View Count Enricher...")
    rows = get_unscanned_rows()

    if not rows:
        print("✅ No videos to enrich.")
        return

    for row in rows:
        video_id = row["video_id"]
        domain = row["domain"]
        result = check_video(video_id)

        if result == "error":
            continue
        elif result is None:
            print(f"❌ Video {video_id} not found. Deleting entry.")
            supabase.table("Clickyleaks").delete().eq("id", row["id"]).execute()
        else:
            views = result["view_count"]
            title = result["title"]
            url = result["url"]
            print(f"🔢 Video {video_id} has {views} views. Updating...")
            supabase.table("Clickyleaks").update({
                "view_count": views,
                "scanned_at": datetime.utcnow().isoformat()
            }).eq("id", row["id"]).execute()

            if views > 0:
                send_discord_alert(domain, views, title, url)

if __name__ == "__main__":
    enrich()
