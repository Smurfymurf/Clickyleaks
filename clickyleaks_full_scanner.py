import os
import json
import re
import time
from datetime import datetime, timedelta
from uuid import uuid4

from supabase import create_client
from playwright.sync_api import sync_playwright

# Supabase env vars
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# Config
CHUNK_DIR = "data/youtube8m_chunks"
WELL_KNOWN_PATH = "data/well_known_domains.csv"
MAX_DOMAINS = 5
MAX_RUNTIME_MINUTES = 45
PROGRESS_TABLE = "clickyleaks_chunk_progress"
CHECKED_TABLE = "clickyleaks_checked"
MAIN_TABLE = "Clickyleaks"

# Load well-known domains
with open(WELL_KNOWN_PATH, "r") as f:
    WELL_KNOWN_DOMAINS = set(line.strip().split(",")[0].lower() for line in f if line.strip())
print(f"✅ Loaded {len(WELL_KNOWN_DOMAINS)} well-known domains.")

def get_next_chunk():
    result = supabase.table(PROGRESS_TABLE).select("*").order("updated_at", desc=False).execute()
    scanned_chunks = {row["chunk_name"]: row for row in result.data}
    
    for filename in sorted(os.listdir(CHUNK_DIR)):
        if not filename.endswith(".json"):
            continue
        full_path = os.path.join(CHUNK_DIR, filename)
        if filename not in scanned_chunks or not scanned_chunks[filename]["fully_scanned"]:
            last_index = scanned_chunks[filename]["last_scanned_index"] if filename in scanned_chunks else 0
            return filename, full_path, last_index
    return None, None, None

def save_progress(chunk_name, index, fully_scanned=False):
    supabase.table(PROGRESS_TABLE).upsert({
        "chunk_name": chunk_name,
        "last_scanned_index": index,
        "fully_scanned": fully_scanned,
        "updated_at": datetime.utcnow().isoformat()
    }, on_conflict=["chunk_name"]).execute()
    print(f"📝 Progress saved — {chunk_name}, Index: {index}, Done: {fully_scanned}")

def already_checked(video_id):
    res = supabase.table(CHECKED_TABLE).select("video_id").eq("video_id", video_id).execute()
    return bool(res.data)

def is_valid_domain(link):
    parsed = re.search(r"(https?://)?([A-Za-z0-9.-]+\.[A-Za-z]{2,})", link)
    if parsed:
        domain = parsed.group(2).lower()
        return domain not in WELL_KNOWN_DOMAINS
    return False

def extract_links_from_description(description):
    return re.findall(r"https?://[^\s)>\"]+", description)

def check_video_live(page, video_id):
    try:
        page.goto(f"https://www.youtube.com/watch?v={video_id}", timeout=10000)
        page.wait_for_timeout(3000)
        if "Video unavailable" in page.content():
            return None
        return page.inner_text("body")
    except Exception:
        return None

def main():
    start_time = datetime.utcnow()
    chunk_name, chunk_path, start_index = get_next_chunk()

    if not chunk_name:
        print("✅ All chunks scanned.")
        return
    if not os.path.exists(chunk_path):
        print(f"🚫 Missing chunk: {chunk_path}")
        return

    print(f"📦 Scanning from: {chunk_name}, starting at index {start_index}")

    with open(chunk_path, "r") as f:
        videos = json.load(f)

    total_videos = len(videos)
    domains_found = 0

    with sync_playwright() as p:
        browser = p.firefox.launch(headless=True)
        page = browser.new_page()

        for i in range(start_index, total_videos):
            video_id = videos[i].get("id")
            print(f"🔍 Checking video: {video_id}")
            if already_checked(video_id):
                print(f"⏩ Already checked: {video_id}")
                continue

            # Runtime check
            if datetime.utcnow() - start_time > timedelta(minutes=MAX_RUNTIME_MINUTES):
                print("⏱️ Runtime limit hit — pausing.")
                save_progress(chunk_name, i, fully_scanned=False)
                return

            # Domain cap check
            if domains_found >= MAX_DOMAINS:
                print("✅ Domain cap hit.")
                save_progress(chunk_name, i, fully_scanned=False)
                return

            body = check_video_live(page, video_id)
            if not body:
                print(f"⚠️ Dead video: {video_id}")
                supabase.table(CHECKED_TABLE).insert({"video_id": video_id}).execute()
                save_progress(chunk_name, i + 1)
                continue

            links = extract_links_from_description(body)
            new_domains = set()

            for link in links:
                if is_valid_domain(link):
                    domain = re.search(r"(https?://)?([A-Za-z0-9.-]+\.[A-Za-z]{2,})", link).group(2).lower()
                    new_domains.add(domain)

            for domain in new_domains:
                supabase.table(MAIN_TABLE).insert({
                    "domain": domain,
                    "source_video": video_id,
                    "verified": False,
                    "is_available": True
                }).execute()
                domains_found += 1
                print(f"🌐 Domain added: {domain} from {video_id}")

            supabase.table(CHECKED_TABLE).insert({"video_id": video_id}).execute()
            save_progress(chunk_name, i + 1)

    save_progress(chunk_name, total_videos, fully_scanned=True)
    print(f"✅ Finished scanning: {chunk_name}")

if __name__ == "__main__":
    main()
