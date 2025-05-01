import os
import json
import re
import time
from datetime import datetime, timedelta
import tldextract

from playwright.sync_api import sync_playwright
from supabase import create_client

# === Config ===
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
        tldextract.extract(d.strip().lower()).registered_domain
        for d in f if d.strip()
    )
print(f"✅ Loaded {len(WELL_KNOWN_DOMAINS)} well-known domains.")


def get_current_chunk_and_index():
    resp = supabase.table(PROGRESS_TABLE).select("*").execute()
    if resp.data:
        row = resp.data[0]
        return row["chunk_name"], row.get("last_scanned_index", 0)
    return "chunk_1.json", 0


def save_progress(chunk_name, video_index, done=False):
    supabase.table(PROGRESS_TABLE).upsert({
        "chunk_name": chunk_name,
        "last_scanned_index": video_index,
        "fully_scanned": done
    }, on_conflict=["chunk_name"]).execute()
    print(f"📝 Progress saved — {chunk_name}, Index: {video_index}, Done: {done}")


def already_checked(video_id):
    result = supabase.table(CHECKED_TABLE).select("video_id").eq("video_id", video_id).execute()
    return len(result.data) > 0


def extract_links_from_description(text):
    return re.findall(r"https?://[^\s)>\"]+", text)


def extract_root_domain(link):
    ext = tldextract.extract(link)
    return ext.registered_domain


def is_well_known_domain(domain):
    return domain in WELL_KNOWN_DOMAINS


def check_video_live(page, video_id):
    try:
        url = f"https://www.youtube.com/watch?v={video_id}"
        page.goto(url, timeout=10000)
        page.wait_for_timeout(3000)
        if "Video unavailable" in page.content():
            return None
        return page.content()
    except Exception:
        return None


def get_video_meta(page):
    try:
        title = page.title()
        view_count = 0
        match = re.search(r"([\d,]+) views", page.content())
        if match:
            view_count = int(match.group(1).replace(",", ""))
        return title, view_count
    except:
        return "", 0


def main():
    start_time = datetime.utcnow()
    chunk_name, video_index = get_current_chunk_and_index()
    chunk_path = os.path.join(CHUNK_DIR, chunk_name)

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
            video_id = videos[i].get("id")
            print(f"🔍 Checking video: {video_id}")

            if already_checked(video_id):
                print(f"⏩ Already checked: {video_id}")
                continue

            if datetime.utcnow() - start_time > timedelta(minutes=MAX_RUNTIME_MINUTES):
                save_progress(chunk_name, i, done=False)
                print("⏱️ Runtime cap hit.")
                return

            if domains_found >= MAX_DOMAINS:
                save_progress(chunk_name, i, done=False)
                print(f"✅ Found {MAX_DOMAINS} domains.")
                return

            body = check_video_live(page, video_id)
            if not body:
                print(f"⚠️ Dead video: {video_id}")
                supabase.table(CHECKED_TABLE).insert({"video_id": video_id}).execute()
                save_progress(chunk_name, i + 1)
                continue

            title, view_count = get_video_meta(page)
            links = extract_links_from_description(body)
            new_domains = set()

            for full_url in links:
                root_domain = extract_root_domain(full_url)
                if not root_domain or is_well_known_domain(root_domain):
                    continue

                try:
                    supabase.table(MAIN_TABLE).upsert({
                        "domain": root_domain,
                        "video_id": video_id,
                        "video_url": f"https://www.youtube.com/watch?v={video_id}",
                        "video_title": title,
                        "view_count": view_count,
                        "full_url": full_url,
                        "is_available": True,
                        "verified": False
                    }, on_conflict=["video_id", "domain"]).execute()
                    print(f"🌐 Domain added: {root_domain} from {video_id}")
                    domains_found += 1
                except Exception as e:
                    print(f"⚠️ Error inserting {root_domain}: {e}")

            supabase.table(CHECKED_TABLE).insert({"video_id": video_id}).execute()
            save_progress(chunk_name, i + 1)

        save_progress(chunk_name, 0, done=True)
        print(f"✅ Finished {chunk_name}.")


if __name__ == "__main__":
    main()
