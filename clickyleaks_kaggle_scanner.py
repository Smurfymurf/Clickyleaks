import requests, re, time
from datetime import datetime
from urllib.parse import urlparse
from supabase import create_client
import os
import json

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")
KAGGLE_JSON_URL = os.getenv("KAGGLE_JSON_URL")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

BLOCKED_DOMAINS = [
    "youtube.com", "youtu.be", "facebook.com", "instagram.com", "twitter.com",
    "paypal.com", "linkedin.com", "linktr.ee", "amzn.to", "bit.ly", "t.co"
]

def is_domain_available(domain):
    try:
        res = requests.get(f"http://{domain}", timeout=5)
        return False
    except:
        return True

def extract_links(text):
    return re.findall(r'(https?://[^\s)]+)', text)

def already_scanned(video_id):
    res = supabase.table("Clickyleaks_Scanned").select("video_id").eq("video_id", video_id).execute()
    return len(res.data) > 0

def log_scan(video_id):
    supabase.table("Clickyleaks_Scanned").insert({
        "video_id": video_id,
        "scanned_at": datetime.utcnow().isoformat()
    }).execute()

def log_domain(domain, link, video_id, title, url, is_available):
    record = {
        "domain": domain,
        "full_url": link,
        "video_id": video_id,
        "video_title": title,
        "video_url": url,
        "http_status": 0,
        "is_available": is_available,
        "view_count": None,
        "discovered_at": datetime.utcnow().isoformat(),
        "scanned_at": datetime.utcnow().isoformat()
    }
    supabase.table("Clickyleaks").insert(record).execute()

    if is_available:
        msg = f"🔥 New available domain: `{domain}`\nFrom video: [{title}]({url})"
        requests.post(DISCORD_WEBHOOK_URL, json={"content": msg})

def main():
    print("🚀 Clickyleaks Kaggle Scanner Started...")

    try:
        res = requests.get(KAGGLE_JSON_URL)
        data = res.json()
    except Exception as e:
        print(f"❌ Failed to load JSON: {e}")
        return

    random.shuffle(data)
    scanned = 0

    for item in data:
        video_id = item.get("_id")
        title = item.get("title", "")
        desc = item.get("description", "")
        video_url = f"https://www.youtube.com/watch?v={video_id}"

        if not video_id or already_scanned(video_id):
            continue

        links = extract_links(desc)
        scanned += 1

        for link in links:
            domain = urlparse(link).netloc.lower()
            if any(domain.endswith(bad) for bad in BLOCKED_DOMAINS):
                continue

            available = is_domain_available(domain)
            print(f"🔍 Logging: {domain} | Available: {available}")
            log_domain(domain, link, video_id, title, video_url, available)
            break  # only check first link

        log_scan(video_id)
        time.sleep(0.5)

        if scanned >= 100:
            break

    print(f"✅ Scan complete. Scanned {scanned} videos.")

if __name__ == "__main__":
    main()
