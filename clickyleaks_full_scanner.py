import os
import json
import re
import time
from datetime import datetime, timedelta
from supabase import create_client
from playwright.sync_api import sync_playwright

# === ENV CONFIG ===
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# === CONSTANTS ===
CHUNK_DIR = "data/youtube8m_chunks"
WELL_KNOWN_PATH = "data/well_known_domains.csv"
MAX_DOMAINS = 5
MAX_RUNTIME_MINUTES = 45
PROGRESS_TABLE = "clickyleaks_chunk_progress"
CHECKED_TABLE = "clickyleaks_checked"
MAIN_TABLE = "Clickyleaks"

# === Load Well-Known Domains ===
with open(WELL_KNOWN_PATH, "r") as f:
    WELL_KNOWN_DOMAINS = set(line.strip().split(',')[0].lower() for line in f if line.strip())
print(f"✅ Loaded {len(WELL_KNOWN_DOMAINS)} well-known domains.")

# === Supabase Helpers ===
def get_next_chunk():
    # Get next chunk to scan
    chunks = sorted(os.listdir(CHUNK_DIR))
    for chunk_name in chunks:
        if not chunk_name.endswith(".json"):
            continue
        res = supabase.table(PROGRESS_TABLE).select("*").eq("chunk_name", chunk_name).execute()
        if res.data:
            if not res.data[0]["fully_scanned"]:
                return chunk_name, res.data[0]["last_scanned_index"]
        else:
            # New chunk, not yet scanned
            return chunk_name, 0
    return None, None

def update_progress(chunk_name, index, fully=False):
    print(f"📝 Saving progress — Chunk: {chunk_name}, Index: {index}, Fully Scanned: {fully}")
    supabase.table(PROGRESS_TABLE).upsert({
        "chunk_name": chunk_name,
        "last_scanned_index": index,
        "fully_scanned": fully
    }, on_conflict=["chunk_name"]).execute()

def already_checked(video_id):
    result = supabase.table(CHECKED_TABLE).select("video_id").eq("video_id", video_id).execute()
    return len(result.data) > 0

# === Scraping Logic ===
def is_valid_domain(link):
    match = re.search(r"(https?://)?([A-Za-z0-9.-]+\.[A-Za-z]{2,})", link)
    if match:
        domain = match.group(2).lower()
        return domain not in WELL_KNOWN_DOMAINS
    return False

def extract_links(text):
    return re.findall(r"https?://[^\s)>\"]+", text)

def check_video_live(page, video_id):
    try:
        page.goto(f"https://www.youtube.com/watch?v={video_id}", timeout=10000)
        page.wait_for_timeout(3000)
        content = page.content()
        if "Video unavailable" in content:
            return None
        return page.inner_text("body")
    except Exception as e:
        print(f"⚠️ Error loading video {video_id}: {e}")
        return None

# === Main ===
def main():
    start_time = datetime.utcnow()

    chunk_name, video_index = get_next_chunk()
    if not chunk_name:
        print("🎉 All chunks scanned!")
        return

    chunk_path = f"{CHUNK_DIR}/{chunk_name}"
    if not os.path.exists(chunk_path):
        print(f"🚫 Chunk file not found: {chunk_path}")
        return

    print(f"📦 Scanning from: {chunk_name}, starting at index {video_index}")

    with open(chunk_path, "r") as f:
        chunk_data = json.load(f)

    videos = chunk_data.get("videos", [])
    total_videos = len(videos)
    domains_found = 0

    with sync_playwright() as p:
        browser = p.firefox.launch(headless=True)
        page = browser.new_page()

        for i in range(video_index, total_videos):
            video_id = videos[i]["id"]
            print(f"🔍 Checking video: {video_id}")

            if already_checked(video_id):
                print(f"⏩ Already checked: {video_id}")
                continue

            # Runtime limit
            if datetime.utcnow() - start_time > timedelta(minutes=MAX_RUNTIME_MINUTES):
                print("⏱️ Runtime cap hit — saving progress and stopping.")
                update_progress(chunk_name, i)
                return

            # Domain cap
            if domains_found >= MAX_DOMAINS:
                print(f"✅ Found {MAX_DOMAINS} domains — saving progress and stopping.")
                update_progress(chunk_name, i)
                return

            body = check_video_live(page, video_id)
            if not body:
                print(f"⚠️ Skipping dead/unavailable video: {video_id}")
                supabase.table(CHECKED_TABLE).insert({"video_id": video_id}).execute()
                update_progress(chunk_name, i + 1)
                continue

            links = extract_links(body)
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
                print(f"🌐 Domain added: {domain} from {video_id}")
                domains_found += 1

            supabase.table(CHECKED_TABLE).insert({"video_id": video_id}).execute()
            update_progress(chunk_name, i + 1)

        # Finished full chunk
        update_progress(chunk_name, total_videos, fully=True)
        print(f"✅ Finished chunk: {chunk_name}. Moving to next chunk on next run.")

if __name__ == "__main__":
    main()
