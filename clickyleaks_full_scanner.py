import os
import re
import json
import time
import requests
from datetime import datetime, timedelta
from playwright.sync_api import sync_playwright
from supabase import create_client
import tldextract

# === ENV and config ===
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

CHUNK_DIR = "data/youtube8m_chunks"
WELL_KNOWN_PATH = "data/well_known_domains.csv"
MAX_DOMAINS = 5
MAX_RUNTIME_MINUTES = 45

PROGRESS_TABLE = "clickyleaks_chunk_progress"
CHECKED_TABLE = "clickyleaks_checked"
MAIN_TABLE = "Clickyleaks"

# === Load well-known domains ===
with open(WELL_KNOWN_PATH, "r") as f:
    WELL_KNOWN_DOMAINS = set(
        tldextract.extract(line.strip().split(",")[0].lower()).registered_domain
        for line in f if line.strip()
    )
print(f"✅ Loaded {len(WELL_KNOWN_DOMAINS)} well-known domains.")

# === Helpers ===

def get_current_chunk_and_index():
    resp = supabase.table(PROGRESS_TABLE).select("*").order("updated_at", desc=True).limit(1).execute()
    if resp.data:
        return resp.data[0]["chunk_name"], resp.data[0]["last_scanned_index"]
    return "chunk_1.json", 0

def save_progress(chunk_name, index, done=False):
    supabase.table(PROGRESS_TABLE).upsert({
        "chunk_name": chunk_name,
        "last_scanned_index": index,
        "fully_scanned": done,
        "updated_at": datetime.utcnow().isoformat()
    }, on_conflict=["chunk_name"]).execute()
    print(f"📝 Progress saved — {chunk_name}, Index: {index}, Done: {done}")

def already_checked(video_id):
    result = supabase.table(CHECKED_TABLE).select("video_id").eq("video_id", video_id).execute()
    return len(result.data) > 0

def extract_links_from_description(description):
    return re.findall(r"https?://[^\s)>\"]+", description)

def extract_root_domain(url):
    ext = tldextract.extract(url)
    return ext.registered_domain

def is_valid_domain(link):
    root = extract_root_domain(link)
    return root and root not in WELL_KNOWN_DOMAINS

def soft_check_domain_availability(domain):
    try:
        requests.get(f"http://{domain}", timeout=5)
        return False  # Domain resolves
    except:
        return True   # Domain unreachable → maybe expired

def check_video_live(page, video_id):
    try:
        page.goto(f"https://www.youtube.com/watch?v={video_id}", timeout=10000)
        page.wait_for_timeout(3000)
        content = page.content()
        if "Video unavailable" in content:
            return None, None, None
        title = page.title()
        view_text = page.inner_text("body")
        match = re.search(r"([\d,]+)\sviews", view_text)
        view_count = int(match.group(1).replace(",", "")) if match else None
        return page.inner_text("body"), title, view_count
    except Exception:
        return None, None, None

# === Main ===

def main():
    start_time = datetime.utcnow()
    chunk_name, video_index = get_current_chunk_and_index()
    chunk_path = f"{CHUNK_DIR}/{chunk_name}"

    if not os.path.exists(chunk_path):
        print(f"🚫 Chunk file not found: {chunk_path}")
        return

    print(f"📦 Scanning from: {chunk_name}, starting at index {video_index}")
    with open(chunk_path, "r") as f:
        videos = json.load(f)

    total_videos = len(videos)
    domains_found = 0

    with sync_playwright() as p:
        browser = p.firefox.launch(headless=True)
        page = browser.new_page()

        for i in range(video_index, total_videos):
            video_id = videos[i]  # ✅ FIXED: video_id is a string
            print(f"🔍 Checking video: {video_id}")

            if already_checked(video_id):
                print(f"⏩ Already checked: {video_id}")
                continue

            if datetime.utcnow() - start_time > timedelta(minutes=MAX_RUNTIME_MINUTES):
                save_progress(chunk_name, i, done=False)
                print("⏱️ Runtime cap hit — stopping.")
                return

            if domains_found >= MAX_DOMAINS:
                save_progress(chunk_name, i, done=False)
                print(f"✅ Hit domain cap ({MAX_DOMAINS}) — stopping.")
                return

            body, title, views = check_video_live(page, video_id)
            if not body:
                supabase.table(CHECKED_TABLE).insert({"video_id": video_id}).execute()
                save_progress(chunk_name, i + 1)
                continue

            links = extract_links_from_description(body)
            for link in links:
                root_domain = extract_root_domain(link)
                if not root_domain or root_domain in WELL_KNOWN_DOMAINS:
                    continue
                if not soft_check_domain_availability(root_domain):
                    continue

                try:
                    supabase.table(MAIN_TABLE).upsert({
                        "domain": root_domain,
                        "full_url": link,
                        "video_title": title,
                        "video_url": f"https://www.youtube.com/watch?v={video_id}",
                        "view_count": views,
                        "video_id": video_id,
                        "verified": False,
                        "is_available": True,
                        "discovered_at": datetime.utcnow().isoformat()
                    }, on_conflict=["video_id", "domain"]).execute()
                    domains_found += 1
                    print(f"🌐 Domain added: {root_domain} from {video_id}")
                except Exception as e:
                    print(f"⚠️ Error inserting {root_domain}: {e}")

            supabase.table(CHECKED_TABLE).insert({"video_id": video_id}).execute()
            save_progress(chunk_name, i + 1)

        save_progress(chunk_name, total_videos, done=True)
        print(f"✅ Finished {chunk_name}.")

if __name__ == "__main__":
    main()
