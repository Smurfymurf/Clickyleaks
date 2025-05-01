import os
import re
import json
import time
import random
import requests
import tldextract
from datetime import datetime, timedelta
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
from dotenv import load_dotenv
import undetected_chromedriver as uc
from supabase import create_client

# === Load .env ===
load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
DISCORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK_URL")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

CHUNK_DIR = "data/youtube8m_chunks"
WELL_KNOWN_PATH = "data/well_known_domains.csv"
PROGRESS_TABLE = "clickyleaks_chunk_progress"
CHECKED_TABLE = "clickyleaks_checked"
MAIN_TABLE = "Clickyleaks"

MAX_DOMAINS = 5
MAX_RUNTIME_MINUTES = 20

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_3)",
    "Mozilla/5.0 (X11; Linux x86_64)",
]

with open(WELL_KNOWN_PATH, "r") as f:
    WELL_KNOWN_DOMAINS = set(
        tldextract.extract(line.strip().split(",")[0].lower()).top_domain_under_public_suffix
        for line in f if line.strip()
    )
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
        "fully_scanned": done,
        "updated_at": datetime.utcnow().isoformat()
    }, on_conflict=["chunk_name"]).execute()
    print(f"📌 Progress saved — {chunk_name}, Index: {index}, Done: {done}")

def already_checked(video_id):
    result = supabase.table(CHECKED_TABLE).select("video_id").eq("video_id", video_id).execute()
    return len(result.data) > 0

def extract_links_from_description(text):
    return re.findall(r"https?://[^\s)>\"]+", text)

def extract_root_domain(url):
    ext = tldextract.extract(url)
    return ext.top_domain_under_public_suffix

def soft_check_domain_availability(domain):
    try:
        requests.get(f"http://{domain}", timeout=5)
        return False
    except:
        return True

def get_video_data(driver, video_id):
    try:
        driver.get(f"https://www.youtube.com/watch?v={video_id}")
        time.sleep(4)

        title = driver.title.replace(" - YouTube", "").strip()
        if title.lower() == "youtube" or not title:
            print(f"⚠️ Title invalid or blank for video {video_id}")
            return None, None, None

        try:
            description_el = driver.find_element(By.CSS_SELECTOR, "#description")
            description = description_el.text
        except NoSuchElementException:
            description = ""

        try:
            view_text = driver.find_element(By.CSS_SELECTOR, "span.view-count").text
            views = int(re.sub(r"[^\d]", "", view_text))
        except:
            views = None

        return description, title, views
    except Exception as e:
        print(f"❌ Error loading video {video_id}: {e}")
        return None, None, None

def send_discord_alert(stats):
    domain_list = "\n".join(f"- {d}" for d in stats["new_domains"]) if stats["new_domains"] else "_None_"
    message = {
        "content": (
            f"🔔 **Clickyleaks Scan Complete**\n"
            f"📦 Chunk: `{stats['chunk']}`\n"
            f"🎥 Videos scanned: **{stats['videos_scanned']}**\n"
            f"🔸 Well-known domains skipped: **{stats['well_known_skipped']}**\n"
            f"⛔️ Active domains skipped: **{stats['resolves_skipped']}**\n"
            f"ℹ️ Videos with no links: **{stats['no_links']}**\n"
            f"❌ Unavailable videos: **{stats['unavailable']}**\n"
            f"✅ Domains logged: **{len(stats['new_domains'])}**\n{domain_list}"
        )
    }
    try:
        requests.post(DISCORD_WEBHOOK, json=message, timeout=10)
        print("📣 Discord alert sent.")
    except Exception as e:
        print(f"⚠️ Failed to send Discord alert: {e}")

def main():
    chunk_name, start_index = get_current_chunk_and_index()
    path = f"{CHUNK_DIR}/{chunk_name}"

    if not os.path.exists(path):
        print(f"❌ Chunk file not found: {path}")
        return

    with open(path, "r") as f:
        videos = json.load(f)

    stats = {
        "chunk": chunk_name,
        "videos_scanned": 0,
        "well_known_skipped": 0,
        "resolves_skipped": 0,
        "no_links": 0,
        "unavailable": 0,
        "new_domains": []
    }

    options = uc.ChromeOptions()
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument(f"user-agent={random.choice(USER_AGENTS)}")

    driver = uc.Chrome(options=options, headless=True)
    start_time = datetime.utcnow()
    domains_found = 0

    for i in range(start_index, len(videos)):
        video_id = videos[i]
        print(f"\n🔍 Checking video: {video_id}")
        stats["videos_scanned"] += 1

        if already_checked(video_id):
            print(f"⏭️ Already checked: {video_id}")
            continue

        if datetime.utcnow() - start_time > timedelta(minutes=MAX_RUNTIME_MINUTES):
            save_progress(chunk_name, i)
            print("⏱️ Max runtime reached — exiting.")
            break

        if domains_found >= MAX_DOMAINS:
            save_progress(chunk_name, i)
            print("✅ Domain cap reached — exiting.")
            break

        desc, title, views = get_video_data(driver, video_id)
        if not desc:
            print(f"❌ Video unavailable or no description: {video_id}")
            stats["unavailable"] += 1
            supabase.table(CHECKED_TABLE).insert({"video_id": video_id}).execute()
            save_progress(chunk_name, i + 1)
            continue

        links = extract_links_from_description(desc)
        if not links:
            print(f"ℹ️ No links found in: {video_id}")
            stats["no_links"] += 1
        else:
            for link in links:
                root = extract_root_domain(link)
                if root in WELL_KNOWN_DOMAINS:
                    print(f"🔸 Skipped well-known domain: {root}")
                    stats["well_known_skipped"] += 1
                    continue
                if not soft_check_domain_availability(root):
                    print(f"⛔️ Domain resolves (active): {root}")
                    stats["resolves_skipped"] += 1
                    continue

                try:
                    supabase.table(MAIN_TABLE).upsert({
                        "domain": root,
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
                    stats["new_domains"].append(root)
                    print(f"✅ Domain saved: {root} from {video_id}")
                except Exception as e:
                    print(f"⚠️ Failed to save {root}: {e}")

        supabase.table(CHECKED_TABLE).insert({"video_id": video_id}).execute()
        save_progress(chunk_name, i + 1)
        time.sleep(random.uniform(3, 7))

    save_progress(chunk_name, len(videos), done=True)
    print("\n✅ Scan finished.")
    send_discord_alert(stats)
    driver.quit()

if __name__ == "__main__":
    main()