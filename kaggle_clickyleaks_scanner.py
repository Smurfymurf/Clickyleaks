import os
import random
import pandas as pd
import requests
from supabase import create_client
from datetime import datetime

# ENV vars
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
DISCORD_WEBHOOK_URL = os.environ["DISCORD_WEBHOOK_URL"]

# Supabase client
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# Datasets (rotate randomly)
DATASETS = [
    "asaniczka/trending-youtube-videos-113-countries",
    "pyuser11/youtube-trending-videos-updated-daily",
    "canerkonuk/youtube-trending-videos-global",
    "sebastianbesinski/youtube-trending-videos-2025-updated-daily"
]

# Soft expiration check (heuristic)
def might_be_expired(domain):
    return not any(domain.endswith(known) for known in WELL_KNOWN_DOMAINS)

WELL_KNOWN_DOMAINS = [
    "youtube.com", "youtu.be", "facebook.com", "instagram.com", "twitter.com",
    "t.co", "tiktok.com", "reddit.com", "linkedin.com", "patreon.com",
    "spotify.com", "discord.gg", "discord.com", "linktr.ee", "apple.com",
    "paypal.com", "amazon.com", "soundcloud.com", "github.com", "bit.ly",
    "google.com", "forms.gle", "cash.app", "store.steampowered.com"
]

def already_scanned(video_id):
    result = supabase.table("Clickyleaks_Checked").select("video_id").eq("video_id", video_id).execute()
    return len(result.data) > 0

def mark_video_scanned(video_id):
    supabase.table("Clickyleaks_Checked").insert({
        "video_id": video_id,
        "scanned_at": datetime.utcnow().isoformat()
    }).execute()

def extract_domains(description):
    import re
    urls = re.findall(r'(https?://[^\s]+)', description or "")
    return [url for url in urls if might_be_expired(url)]

def scan_video(row):
    video_id = row["video_id"]
    desc = row.get("description") or row.get("desc") or ""
    domains = extract_domains(desc)
    promising = []
    for url in domains:
        try:
            domain = url.split("//")[-1].split("/")[0].lower()
            if domain and might_be_expired(domain):
                promising.append((domain, url))
        except Exception:
            continue
    return promising

def main():
    dataset = random.choice(DATASETS)
    print(f"📦 Selected dataset: {dataset}")
    df = pd.read_csv(f"https://huggingface.co/datasets/{dataset}/resolve/main/latest.csv")
    df = df.sort_values(by="publishedAt" if "publishedAt" in df.columns else df.columns[0])

    scanned = 0
    added = 0

    for _, row in df.iterrows():
        video_id = row["video_id"]
        if already_scanned(video_id):
            continue
        scanned += 1
        mark_video_scanned(video_id)

        results = scan_video(row)
        for domain, full_url in results:
            supabase.table("Clickyleaks").insert({
                "domain": domain,
                "full_url": full_url,
                "video_id": video_id,
                "scanned_at": datetime.utcnow().isoformat(),
                "is_available": True,
                "verified": False
            }).execute()
            added += 1

        if added >= 5 or scanned >= 500:
            break

    # Discord alert
    summary = f"🔍 Scan complete — {scanned} videos scanned, {added} potential domains found."
    requests.post(DISCORD_WEBHOOK_URL, json={"content": summary})
    print(summary)

if __name__ == "__main__":
    main()
