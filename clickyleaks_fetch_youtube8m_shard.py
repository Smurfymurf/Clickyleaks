import requests, random, csv, io, os
from supabase import create_client, Client
from datetime import datetime

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

MIRRORS = [
    "https://storage.googleapis.com/yt8m-video-id",
    "https://asia.storage.googleapis.com/yt8m-video-id",
    "https://eu.storage.googleapis.com/yt8m-video-id"
]

PARTITIONS = ["train", "validate", "test"]
SHARDS_PER_PARTITION = {"train": 3844, "validate": 50, "test": 50}

def get_next_shard():
    used = supabase.table("Clickyleaks_ShardLog").select("shard_id").execute().data
    used_ids = set([row["shard_id"] for row in used]) if used else []
    attempts = 0

    while attempts < 20:
        part = random.choice(PARTITIONS)
        max_idx = SHARDS_PER_PARTITION[part]
        idx = random.randint(0, max_idx - 1)
        shard_id = f"{part}_{idx:05d}"

        if shard_id not in used_ids:
            return part, idx, shard_id
        attempts += 1

    return None, None, None

def fetch_and_store_shard():
    partition, index, shard_id = get_next_shard()
    if not shard_id:
        print("❌ No unused shard available.")
        return

    filename = f"{index:05d}-of-{SHARDS_PER_PARTITION[partition]:05d}.csv"
    for mirror in MIRRORS:
        url = f"{mirror}/{partition}/{filename}"
        print(f"📥 Downloading shard: {url}")
        try:
            res = requests.get(url, timeout=15)
            res.raise_for_status()
            break
        except Exception as e:
            print(f"⚠️ Failed on mirror {mirror}: {e}")
    else:
        print("❌ All mirrors failed.")
        return

    csv_text = res.content.decode("utf-8")
    reader = csv.DictReader(io.StringIO(csv_text))
    rows = [{"video_id": r["YTID"], "partition": partition} for r in reader]

    if not rows:
        print("❌ No video IDs in shard.")
        return

    print(f"✅ Inserting {len(rows)} video IDs...")
    supabase.table("Clickyleaks_VideoIDs").insert(rows).execute()
    supabase.table("Clickyleaks_ShardLog").insert({"shard_id": shard_id, "fetched_at": datetime.utcnow().isoformat()}).execute()
    print("✅ Shard stored and logged.")

if __name__ == "__main__":
    print("🚀 Clickyleaks Random Shard Fetcher Started...")
    fetch_and_store_shard()
