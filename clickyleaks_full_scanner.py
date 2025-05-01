import os
import json
import random
import re
import requests
from pathlib import Path
from urllib.parse import urlparse
from supabase import create_client, Client
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

CHUNK_DIR = Path("data/youtube8m_chunks")
MAX_DOMAINS_PER_RUN = 10
VIDEO_SCAN_CAP = 500

with open("data/well_known_domains.csv") as f:
    WELL_KNOWN_DOMAINS = set([line.strip().lower() for line in f if line.strip()])

def get_random_chunk():
    return random.choice(list(CHUNK_DIR.glob("chunk_*.json")))

def load_chunk(chunk_file):
    with open(chunk_file) as f:
        data = json.load(f)
    return data

def extract_links(text):
    urls = re.findall(r'https?://[^\s)"\'>]+', text)
    return [url for url in urls if urlparse(url).netloc not in WELL_KNOWN_DOMAINS]

def is_domain_expired(domain):
    try:
        resp = requests.head(f"http://{domain}", timeout=5)
        return resp.status_code in [403, 404, 410, 500, 502, 503]
    except:
        return True

def get_video_description_api(video_id):
    api_key = os.getenv("YOUTUBE_API_KEY")
    try:
        resp = requests.get(
            f"https://www.googleapis.com/youtube/v3/videos?part=snippet&id={video_id}&key={api_key}"
        )
        data = resp.json()
        if 'items' in data and data['items']:
            return data['items'][0]['snippet']['description']
    except:
        pass
    return None

def get_video_description_playwright(video_id):
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            page.goto(f"https://www.youtube.com/watch?v={video_id}", timeout=15000)
            desc = page.locator("#description").inner_text(timeout=5000)
            browser.close()
            return desc
    except:
        return None

def send_discord_alert(domains):
    if not domains:
        return
    content = "**🚨 New Potential Expired Domains Found!**\n" + "\n".join([f"- {d}" for d in domains])
    try:
        requests.post(DISCORD_WEBHOOK_URL, json={"content": content})
    except Exception as e:
        print(f"Failed to send Discord alert: {e}")

def main():
    chunk_file = get_random_chunk()
    print(f"Scanning from: {chunk_file.name}")

    all_ids = load_chunk(chunk_file)
    existing_ids_resp = supabase.table("clickyleaks_checked").select("video_id").execute()
    checked_ids = set(row["video_id"] for row in existing_ids_resp.data or [])

    new_ids = [vid for vid in all_ids if vid not in checked_ids][:VIDEO_SCAN_CAP]
    promising_domains = []

    for video_id in new_ids:
        desc = get_video_description_api(video_id)
        if desc is None:
            desc = get_video_description_playwright(video_id)

        if not desc:
            print(f"Skipping dead/unavailable video: {video_id}")
            continue

        links = extract_links(desc)
        for link in links:
            domain = urlparse(link).netloc.lower().replace("www.", "")
            if domain in WELL_KNOWN_DOMAINS:
                continue
            if is_domain_expired(domain):
                print(f"✅ Found: {domain}")
                promising_domains.append(domain)
                supabase.table("clickyleaks").insert({
                    "domain": domain,
                    "source": f"https://www.youtube.com/watch?v={video_id}",
                    "available": True,
                    "verified": False
                }).execute()
                if len(promising_domains) >= MAX_DOMAINS_PER_RUN:
                    break

        supabase.table("clickyleaks_checked").insert({
            "video_id": video_id
        }).execute()

        if len(promising_domains) >= MAX_DOMAINS_PER_RUN:
            break

    send_discord_alert(promising_domains)
    print(f"Scan complete. {len(promising_domains)} domains added.")

if __name__ == "__main__":
    main()
