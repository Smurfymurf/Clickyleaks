import os
import json
import re
import time
from datetime import datetime, timedelta

from supabase import create_client
from playwright.sync_api import sync_playwright

# Supabase
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# Config
CHUNK_DIR = "data/youtube8m_chunks"
WELL_KNOWN_PATH = "data/well_known_domains.csv"
PROGRESS_TABLE = "clickyleaks_chunk_progress"
CHECKED_TABLE = "clickyleaks_checked"
MAIN_TABLE = "Clickyleaks"
MAX_DOMAINS = 5
MAX_RUNTIME_MINUTES = 45

# Load known domains
with open(WELL_KNOWN_PATH, "r") as f:
    WELL_KNOWN_DOMAINS = set(line.strip().split(",")[0].lower() for line in f if line.strip())

print(f"✅ Loaded {len(WELL_KNOWN_DOMAINS)} well-known domains.")

def get_current_chunk_and_index():
    resp = supabase.table(PROGRESS_TABLE).select("*").order("updated_at", desc=True).limit(1).execute()
    if resp.data:
        return resp.data[0]["chunk_name"], resp.data[0]["last_scanned_index"]
    return "chunk_1.json", 0

def save_progress(chunk_name, index, done=False):
    supabase.table(PROGRESS_TABLE).upsert({
        "chunk_name": chunk_name,
        "last_scanned_index": index,
        "fully_scanned": done
    }, on_conflict=["chunk_name"]).execute()
    print(f"📝 Progress saved — {chunk_name}, Index: {index}, Done: {done}")

def already_checked(video_id):
    result = supabase.table(CHECKED_TABLE).select("video_id").eq("video_id", video_id).execute()
    return len(result.data) > 0

def extract_links(body):
    return re.findall(r"https?://[^\s)>\"]+", body)

def extract_domain(url):
    match = re.search(r"(https?://)?([A-Za-z0-9.-]+\.[A-Za-z]{2,})", url)
    return match.group(2).lower() if match else None

def is_valid_domain(domain):
    return domain and domain not in WELL_KNOWN_DOMAINS

def check_video_live(page, video_id):
    try:
        url = f"https://www.youtube.com/watch?v={video_id}"
        page.goto(url, timeout=10000)
        page.wait_for_timeout(3000)
        if "Video unavailable" in page.content():
            return None
        return page.inner_text("body")
    except Exception:
        return None

def main():
    start_time = datetime.utcnow()
    chunk_name, start_index = get_current_chunk_and_index()
    chunk_path = os.path.join(CHUNK_DIR, chunk_name)

    if not os.path.exists(chunk_path):
        print(f"🚫 Chunk file not found: {chunk_path}")
        return

    print(f"📦 Scanning from: {chunk_name}, starting at index {start_index}")

    with open(chunk_path, "r") as f:
        videos = json.load(f)

    domains_found = 0

    with sync_playwright() as p:
        browser = p.firefox.launch(headless=True)
        page = browser.new_page()

        for i in range(start_index, len(videos)):
            video_id = videos[i]
            print(f"🔍 Checking video: {video_id}")

            if already_checked(video_id):
                print(f"⏩ Already checked: {video_id}")
                continue

            if (datetime.utcnow() - start_time) > timedelta(minutes=MAX_RUNTIME_MINUTES):
                print("⏱️ Runtime cap hit — exiting.")
                save_progress(chunk_name, i)
                return

            if domains_found >= MAX_DOMAINS:
                print(f"✅ Domain cap hit ({MAX_DOMAINS}) — exiting.")
                save_progress(chunk_name, i)
                return

            body = check_video_live(page, video_id)
            if not body:
                print(f"⚠️ Skipping dead/unavailable video: {video_id}")
                supabase.table(CHECKED_TABLE).insert({"video_id": video_id}).execute()
                save_progress(chunk_name, i + 1)
                continue

            links = extract_links(body)
            new_domains = set()

            for link in links:
                domain = extract_domain(link)
                if is_valid_domain(domain):
                    new_domains.add((domain, link))

            for domain, full_url in new_domains:
                try:
                    supabase.table(MAIN_TABLE).upsert({
                        "domain": domain,
                        "full_url": full_url,
                        "video_id": video_id,
                        "video_url": f"https://www.youtube.com/watch?v={video_id}",
                        "is_available": True,
                        "verified": False,
                        "discovered_at": datetime.utcnow().isoformat()
                    }, on_conflict=["video_id", "domain"]).execute()
                    print(f"🌐 Domain added: {domain} from {video_id}")
                    domains_found += 1
                except Exception as e:
                    print(f"❌ Error inserting {domain}: {str(e)}")

            supabase.table(CHECKED_TABLE).insert({"video_id": video_id}).execute()
            save_progress(chunk_name, i + 1)

        # Chunk completed
        save_progress(chunk_name, len(videos), done=True)
        print(f"✅ Finished chunk {chunk_name}")

if __name__ == "__main__":
    main()
