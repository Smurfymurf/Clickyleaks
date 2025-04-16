import requests, time, random, re, os
from urllib.parse import urlparse
from datetime import datetime
from supabase import create_client, Client
import pandas as pd

# === CONFIG ===
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
BLOCKED_DOMAINS = ["amzn.to", "bit.ly", "t.co", "youtube.com", "instagram.com", "linktr.ee", "rumble.com",
                   "facebook.com", "twitter.com", "linkedin.com", "paypal.com", "discord.gg", "youtu.be"]

# Load a subset of video IDs (assuming CSV is pre-downloaded)
video_df = pd.read_csv("youtube8m_video_ids_subset.csv")
video_ids = video_df["video_id"].dropna().tolist()
random.shuffle(video_ids)


def already_scanned(video_id):
    try:
        result = supabase.table("Clickyleaks").select("id").eq("video_id", video_id).execute()
        return len(result.data) > 0
    except:
        return False


def extract_links(text):
    return re.findall(r'(https?://[^\s)]+)', text)


def is_domain_available(domain):
    domain = domain.lower().strip().replace("www.", "").split("/")[0]
    try:
        res = requests.get(f"http://{domain}", timeout=5)
        return False
    except:
        return True


def notify_discord(message):
    try:
        requests.post(DISCORD_WEBHOOK_URL, json={"content": message}, timeout=5)
    except:
        pass


def process_video(video_id):
    if already_scanned(video_id):
        return False

    url = f"https://www.youtube.com/watch?v={video_id}"
    try:
        res = requests.get(url, timeout=8)
        html = res.text
        links = extract_links(html)
    except:
        return False

    for link in links:
        domain = urlparse(link).netloc.lower()
        if any(domain.endswith(bad) for bad in BLOCKED_DOMAINS):
            continue

        available = is_domain_available(domain)
        record = {
            "domain": domain,
            "full_url": link,
            "video_id": video_id,
            "video_url": url,
            "is_available": available,
            "discovered_at": datetime.utcnow().isoformat(),
            "scanned_at": datetime.utcnow().isoformat()
        }
        supabase.table("Clickyleaks").insert(record).execute()

        if available:
            notify_discord(f"🔥 Available domain found: {domain}\n🔗 {link}\n🎥 {url}")
        return True

    return False


def main():
    print("🚀 Clickyleaks Random Scanner Started...")
    found = 0
    for vid in video_ids[:250]:
        print(f"🔍 Checking: https://www.youtube.com/watch?v={vid}")
        try:
            if process_video(vid):
                found += 1
        except Exception as e:
            print(f"❌ Error processing video {vid}: {e}")
        time.sleep(0.5)

    if found == 0:
        notify_discord("❌ No available domains found in this run.")
    else:
        notify_discord(f"✅ Run complete: {found} domain(s) found.")


if __name__ == "__main__":
    main()
