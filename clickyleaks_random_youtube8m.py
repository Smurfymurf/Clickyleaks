import requests, os, time, random, re
from urllib.parse import urlparse
from datetime import datetime
from supabase import create_client, Client

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
BLOCKED_DOMAINS = ["youtube.com", "youtu.be", "i.ytimg.com", "instagram.com", "twitter.com"]

def extract_links(text):
    return re.findall(r'(https?://[^\s)]+)', text)

def is_domain_available(domain):
    try:
        res = requests.get(f"http://{domain}", timeout=5)
        return False
    except:
        return True

def already_logged(video_id):
    res = supabase.table("Clickyleaks").select("id").eq("video_id", video_id).execute()
    return len(res.data) > 0

def send_discord_alert(domain, video_url, views):
    msg = {
        "content": f"🔥 **Available Domain Found:** `{domain}`\n🔗 Video: {video_url}\n👀 Views (est): {views}"
    }
    try:
        requests.post(DISCORD_WEBHOOK_URL, json=msg)
    except Exception as e:
        print(f"❌ Failed to send Discord alert: {e}")

def process_video(video_id):
    if already_logged(video_id):
        return

    url = f"https://www.youtube.com/watch?v={video_id}"
    print(f"🔍 Checking: {url}")

    try:
        res = requests.get(url)
        html = res.text
    except Exception as e:
        print(f"❌ Error loading page: {e}")
        return

    links = extract_links(html)
    for link in links:
        domain = urlparse(link).netloc.lower()
        if any(bad in domain for bad in BLOCKED_DOMAINS):
            continue

        available = is_domain_available(domain)
        print(f"🔍 Logging domain: {domain} | Available: {available}")

        supabase.table("Clickyleaks").insert({
            "video_id": video_id,
            "domain": domain,
            "full_url": link,
            "video_url": url,
            "is_available": available,
            "discovered_at": datetime.utcnow().isoformat()
        }).execute()

        if available:
            send_discord_alert(domain, url, "N/A")

        break

def main():
    print("🚀 Clickyleaks Random Scanner Started...")
    result = supabase.table("Clickyleaks_VideoIDs").select("video_id").limit(250).execute()
    video_ids = [r["video_id"] for r in result.data]
    random.shuffle(video_ids)

    if not video_ids:
        print("❌ No video IDs to scan.")
        return

    for vid in video_ids:
        process_video(vid)
        time.sleep(0.5)

if __name__ == "__main__":
    main()
