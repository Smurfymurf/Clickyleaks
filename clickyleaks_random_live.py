import requests, random, re, socket, dns.resolver
from datetime import datetime
from urllib.parse import urlparse
from bs4 import BeautifulSoup
from supabase import create_client, Client
import os

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

BLOCKED_DOMAINS = [
    "amzn.to", "bit.ly", "t.co", "youtube.com", "instagram.com", "linktr.ee",
    "rumble.com", "facebook.com", "twitter.com", "linkedin.com", "paypal.com",
    "discord.gg", "youtu.be"
]

YOUTUBE_BASE = "https://www.youtube.com/watch?v="


def generate_random_id():
    chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
    return ''.join(random.choice(chars) for _ in range(11))


def already_scanned(video_id):
    try:
        result = supabase.table("Clickyleaks").select("id").eq("video_id", video_id).execute()
        return len(result.data) > 0
    except:
        return False


def extract_links(description_html):
    soup = BeautifulSoup(description_html, "html.parser")
    text = soup.get_text()
    return re.findall(r'(https?://[^\s)]+)', text)


def check_domain_availability(domain):
    domain = domain.replace("www.", "").split("/")[0]
    try:
        socket.gethostbyname(domain)
        dns.resolver.resolve(domain, 'A')
        return False
    except:
        return True


def send_discord_alert(domain, video_url):
    message = f"**Available Domain Found:** `{domain}`\nSource: {video_url}"
    requests.post(DISCORD_WEBHOOK_URL, json={"content": message})


def process_video(video_id):
    if already_scanned(video_id):
        return False

    url = YOUTUBE_BASE + video_id
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        r = requests.get(url, headers=headers, timeout=8)
        if "Video unavailable" in r.text or r.status_code != 200:
            return False

        links = extract_links(r.text)
        if not links:
            return False

        first_link = links[0]
        domain = urlparse(first_link).netloc.lower()

        if any(domain.endswith(bad) for bad in BLOCKED_DOMAINS):
            return False

        available = check_domain_availability(domain)

        print(f"🔍 Logging: {domain} | Available: {available}")

        if available:
            supabase.table("Clickyleaks").insert({
                "domain": domain,
                "full_url": first_link,
                "video_id": video_id,
                "video_url": url,
                "is_available": True,
                "discovered_at": datetime.utcnow().isoformat(),
                "scanned_at": datetime.utcnow().isoformat()
            }).execute()
            send_discord_alert(domain, url)
        else:
            supabase.table("Clickyleaks").insert({
                "domain": domain,
                "full_url": first_link,
                "video_id": video_id,
                "video_url": url,
                "is_available": False,
                "discovered_at": datetime.utcnow().isoformat(),
                "scanned_at": datetime.utcnow().isoformat()
            }).execute()
        return True

    except Exception as e:
        print(f"❌ Error processing video {video_id}: {e}")
        return False


if __name__ == "__main__":
    print("🚀 Clickyleaks Random Scanner Started...")
    valid_count = 0
    attempts = 0
    
    while valid_count < 100 and attempts < 500:
        video_id = generate_random_id()
        print(f"🔍 Checking: {YOUTUBE_BASE + video_id}")
        if process_video(video_id):
            valid_count += 1
        attempts += 1

    print(f"✅ Run complete. {valid_count} valid videos processed.")
