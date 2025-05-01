from datetime import datetime, timedelta
import json
import os
import re
import time

from playwright.sync_api import sync_playwright
from supabase import create_client

# ENV variables
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# Config
CHUNK_DIR = "Clickyleaks/data/youtube8m_chunks"
WELL_KNOWN_PATH = "data/well_known_domains.csv"
MAX_DOMAINS = 5
MAX_RUNTIME_MINUTES = 45
PROGRESS_TABLE = "clickyleaks_chunk_progress"
CHECKED_TABLE = "clickyleaks_checked"
MAIN_TABLE = "Clickyleaks"

# Load well-known domains
with open(WELL_KNOWN_PATH, "r") as f:
    WELL_KNOWN_DOMAINS = set(domain.strip().lower() for domain in f if domain.strip())
print(f"✅ Loaded {len(WELL_KNOWN_DOMAINS)} well-known domains.")

def get_current_chunk_and_index():
    resp = supabase.table(PROGRESS_TABLE).select("*").execute()
    if resp.data:
        return resp.data[0]["chunk_number"], resp.data[0]["video_index"]
    return 1, 0

def save_progress(chunk_number, video_index):
    supabase.table(PROGRESS_TABLE).upsert({"id": 1, "chunk_number": chunk_number, "video_index": video_index}).execute()
    print(f"📝 Progress saved — Chunk: {chunk_number}, Index: {video_index}")

def already_checked(video_id):
    result = supabase.table(CHECKED_TABLE).select("video_id").eq("video_id", video_id).execute()
    return len(result.data) > 0

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
    chunk_number, video_index = get_current_chunk_and_index()
    chunk_path = f"{CHUNK_DIR}/chunk_{chunk_number}.json"

    if not os.path.exists(chunk_path):
        print(f"🚫 Chunk file not found: {chunk_path}")
        return

    print(f"📦 Scanning from: chunk_{chunk_number}.json, starting at index {video_index}")

    with open(chunk_path, "r") as f:
        data = json.load(f)

    video_ids = list(data["videos"].keys())
    total_videos = len(video_ids)
    domains_found = 0

    with sync_playwright() as p:
        browser = p.firefox.launch(headless=True)
        page = browser.new_page()

        for i in range(video_index, total_videos):
            video_id = video_ids[i]
            if already_checked(video_id):
                print(f"⏩ Already checked: {video_id}")
                continue

            # Stop if runtime cap hit
            if datetime.utcnow() - start_time > timedelta(minutes=MAX_RUNTIME_MINUTES):
                print("⏱️ Runtime cap hit — saving progress and stopping.")
                save_progress(chunk_number, i)
                return

            # Stop if domain cap hit
            if domains_found >= MAX_DOMAINS:
                print(f"✅ Found {MAX_DOMAINS} domains — saving progress and stopping.")
                save_progress(chunk_number, i)
                return

            body = check_video_live(page, video_id)
            if not body:
                print(f"⚠️ Skipping dead/unavailable video: {video_id}")
                supabase.table(CHECKED_TABLE).insert({"video_id": video_id}).execute()
                save_progress(chunk_number, i + 1)
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
            save_progress(chunk_number, i + 1)

        # Finished chunk
        save_progress(chunk_number + 1, 0)
        print(f"✅ Finished chunk {chunk_number}. Moving to next on next run.")

if __name__ == "__main__":
    main()
