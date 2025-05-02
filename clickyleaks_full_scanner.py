import os
import re
import json
import time
import random
import requests
import tldextract
from datetime import datetime, timedelta
from dotenv import load_dotenv
from supabase import create_client

# === Load .env ===
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
DISCORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK_URL")
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

CHUNK_DIR = "data/youtube8m_chunks"
WELL_KNOWN_PATH = "data/well_known_domains.csv"
PROGRESS_TABLE = "clickyleaks_chunk_progress"
CHECKED_TABLE = "clickyleaks_checked"
MAIN_TABLE = "Clickyleaks"

MAX_DOMAINS = 10
MAX_RUNTIME_MINUTES = 5

SUPPORTED_TLDS = {
    "com", "me", "net", "org", "sh", "io", "co", "club", "biz", "mobi", "info", "us",
    "domains", "cloud", "fr", "au", "ru", "uk", "nl", "fi", "br", "hr", "ee", "ca",
    "sk", "se", "no", "cz", "it", "in", "icu", "top", "xyz", "cn", "cf", "hk", "sg",
    "pt", "site", "kz", "si", "ae", "do", "yoga", "xxx", "ws", "work", "wiki",
    "watch", "wtf", "world", "website", "vip", "ly", "dev", "network", "company",
    "page", "rs", "run", "science", "sex", "shop", "solutions", "so", "studio",
    "style", "tech", "travel", "vc", "pub", "pro", "app", "press", "ooo", "de"
}

# === Load well-known domains ===
with open(WELL_KNOWN_PATH, "r") as f:
    WELL_KNOWN_DOMAINS = set(
        line.strip().split(",")[0].lower()
        for line in f if line.strip()
    )
print(f"[Init] Loaded {len(WELL_KNOWN_DOMAINS)} well-known domains.")

def get_current_chunk_and_index():
    all_chunks = [f for f in os.listdir(CHUNK_DIR) if f.endswith(".json")]
    if not all_chunks:
        print("[Error] No chunk files found.")
        return None, 0

    chunk_name = random.choice(all_chunks)

    resp = supabase.table(PROGRESS_TABLE).select("*").eq("chunk_name", chunk_name).limit(1).execute()
    if resp.data:
        print(f"[Resume] Found saved progress for {chunk_name}")
        return chunk_name, resp.data[0]["last_scanned_index"]

    print(f"[New Chunk] Starting fresh on {chunk_name}")
    return chunk_name, 0

def save_progress(chunk_name, index, done=False):
    print(f"[Progress] Saving progress: {chunk_name} at index {index} (done={done})")
    supabase.table(PROGRESS_TABLE).upsert({
        "chunk_name": chunk_name,
        "last_scanned_index": index,
        "fully_scanned": done,
        "updated_at": datetime.utcnow().isoformat()
    }, on_conflict=["chunk_name"]).execute()

def already_checked(video_id):
    result = supabase.table(CHECKED_TABLE).select("video_id").eq("video_id", video_id).execute()
    return len(result.data) > 0

def extract_links_from_description(text):
    return re.findall(r"https?://[^\s)>\"]+", text)

def extract_root_domain(url):
    ext = tldextract.extract(url)
    return ".".join(part for part in [ext.domain, ext.suffix] if part)

def soft_check_domain_availability(domain):
    try:
        resp = requests.get(f"http://{domain}", timeout=5, allow_redirects=True)
        return resp.status_code >= 400
    except:
        return True

def get_video_data_youtube_api(video_id):
    url = "https://www.googleapis.com/youtube/v3/videos"
    params = {
        "part": "snippet,statistics",
        "id": video_id,
        "key": YOUTUBE_API_KEY
    }
    try:
        res = requests.get(url, params=params, timeout=10)
        data = res.json()

        if "items" not in data or not data["items"]:
            return None, None, None

        item = data["items"][0]
        snippet = item.get("snippet", {})
        statistics = item.get("statistics", {})

        title = snippet.get("title", "").strip()
        description = snippet.get("description", "")
        views = int(statistics.get("viewCount", 0)) if "viewCount" in statistics else 0

        return description, title, views
    except Exception as e:
        print(f"[Error] YouTube API fetch failed for {video_id}: {e}")
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
            f"✅ Potential available domains found: **{len(stats['new_domains'])}**\n{domain_list}"
        )
    }
    print("[Alert] Sending Discord summary.")
    try:
        requests.post(DISCORD_WEBHOOK, json=message, timeout=10)
    except Exception as e:
        print(f"[Error] Discord webhook error: {e}")

def main():
    chunk_name, start_index = get_current_chunk_and_index()
    if not chunk_name:
        return

    path = f"{CHUNK_DIR}/{chunk_name}"
    if not os.path.exists(path):
        print(f"[Error] Chunk file not found: {path}")
        return

    with open(path, "r") as f:
        videos = json.load(f)

    print(f"[Start] Scanning {len(videos)} videos from {chunk_name} starting at index {start_index}")

    stats = {
        "chunk": chunk_name,
        "videos_scanned": 0,
        "well_known_skipped": 0,
        "resolves_skipped": 0,
        "no_links": 0,
        "unavailable": 0,
        "new_domains": []
    }

    start_time = datetime.utcnow()
    domains_found = 0

    for i in range(start_index, len(videos)):
        video_id = videos[i]
        stats["videos_scanned"] += 1

        if already_checked(video_id):
            print(f"[Skip] Already checked {video_id}")
            continue

        if datetime.utcnow() - start_time > timedelta(minutes=MAX_RUNTIME_MINUTES):
            print("[Stop] Max runtime reached.")
            save_progress(chunk_name, i)
            send_discord_alert(stats)
            return

        if domains_found >= MAX_DOMAINS:
            print("[Stop] Max domains found.")
            save_progress(chunk_name, i)
            send_discord_alert(stats)
            return

        desc, title, views = get_video_data_youtube_api(video_id)
        if not desc or views < 20000:
            print(f"[Skip] {video_id} - No description or under 20K views ({views})")
            stats["unavailable"] += 1
            supabase.table(CHECKED_TABLE).insert({"video_id": video_id}).execute()
            save_progress(chunk_name, i + 1)
            continue

        links = extract_links_from_description(desc)
        if not links:
            stats["no_links"] += 1
            print(f"[No Links] {video_id}")
        else:
            for link in links:
                root = extract_root_domain(link)
                tld = tldextract.extract(link).suffix.lower()
                if tld not in SUPPORTED_TLDS:
                    print(f"[Skip] Unsupported TLD: {tld} ({root})")
                    continue
                if root in WELL_KNOWN_DOMAINS:
                    stats["well_known_skipped"] += 1
                    print(f"[Skip] Well-known: {root}")
                    continue
                if not soft_check_domain_availability(root):
                    stats["resolves_skipped"] += 1
                    print(f"[Skip] Still resolves: {root}")
                    continue

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
                print(f"[Log] Domain logged: {root}")

        supabase.table(CHECKED_TABLE).insert({"video_id": video_id}).execute()
        save_progress(chunk_name, i + 1)
        time.sleep(random.uniform(1, 2))

    save_progress(chunk_name, len(videos), done=True)
    send_discord_alert(stats)
    print("[Done] Scan complete.")

if __name__ == "__main__":
    main()
