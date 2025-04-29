import requests, re, os, pandas as pd
from urllib.parse import urlparse
from datetime import datetime
from supabase import create_client, Client
from kaggle.api.kaggle_api_extended import KaggleApi

# === CONFIG ===
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")
KAGGLE_USERNAME = os.getenv("KAGGLE_USERNAME")
KAGGLE_KEY = os.getenv("KAGGLE_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

WELL_KNOWN_DOMAINS = {
    "apple.com", "google.com", "facebook.com", "amazon.com", "youtube.com",
    "microsoft.com", "netflix.com", "instagram.com", "paypal.com", "reddit.com",
    "wikipedia.org", "tumblr.com", "github.com", "linkedin.com", "spotify.com",
    "cnn.com", "bbc.com", "dropbox.com", "airbnb.com", "salesforce.com",
    "tiktok.com", "ebay.com", "zoom.us", "whatsapp.com", "nytimes.com",
    "oracle.com", "bing.com", "slack.com", "notion.so", "wordpress.com",
    "vercel.app", "netlify.app", "figma.com", "medium.com", "shopify.com",
    "yahoo.com", "pinterest.com", "imdb.com", "quora.com", "adobe.com",
    "cloudflare.com", "soundcloud.com", "coursera.org", "kickstarter.com",
    "mozilla.org", "forbes.com", "theguardian.com", "weather.com", "espn.com",
    "msn.com", "okta.com", "bitbucket.org", "vimeo.com", "unsplash.com",
    "canva.com", "zoom.com", "atlassian.com", "ycombinator.com", "stripe.com",
    "zendesk.com", "hotstar.com", "reuters.com", "nationalgeographic.com",
    "weebly.com", "behance.net", "dribbble.com", "skype.com", "opera.com",
    "twitch.tv", "stackoverflow.com", "stackoverflow.blog"
}

def normalize_domain(link: str) -> str:
    parsed = urlparse(link)
    domain = parsed.netloc or parsed.path
    return domain.lower().replace("www.", "").strip()

def extract_links(text):
    return re.findall(r'(https?://[^\s)]+)', str(text))

def mark_video_scanned(video_id):
    supabase.table("Clickyleaks_KaggleChecked").insert({
        "video_id": video_id,
        "scanned_at": datetime.utcnow().isoformat()
    }).execute()

def send_discord_alert(domain, video_url):
    if not DISCORD_WEBHOOK_URL:
        return
    message = {
        "content": f"🚨 Domain logged: `{domain}`\n🔗 Video: {video_url}"
    }
    try:
        requests.post(DISCORD_WEBHOOK_URL, json=message, timeout=5)
    except:
        pass

def load_scanned_ids():
    print("🔁 Loading scanned video IDs from Supabase...")
    ids = set()
    offset = 0
    while True:
        response = supabase.table("Clickyleaks_KaggleChecked").select("video_id").range(offset, offset + 999).execute()
        if not response.data:
            break
        ids.update(row["video_id"] for row in response.data)
        offset += 1000
    print(f"✅ Loaded {len(ids)} previously scanned video IDs.")
    return ids

def main():
    print("🚀 Loading YouTube Trending dataset from Kaggle...")
    dataset_slug = "canerkonuk/youtube-trending-videos-global"
    target_file = "youtube_trending_videos_global.csv"

    api = KaggleApi()
    api.authenticate()
    print(f"Dataset URL: https://www.kaggle.com/datasets/{dataset_slug}")

    api.dataset_download_file(dataset_slug, target_file, path=".", force=True)

    df = pd.read_csv(target_file, low_memory=False)
    scanned_ids = load_scanned_ids()
    new_logged = 0
    MAX_NEW_ENTRIES = 100

    for _, row in df.iterrows():
        if new_logged >= MAX_NEW_ENTRIES:
            print("✅ Reached max new entries for this run.")
            break

        video_id = str(row.get("video_id"))
        if not video_id or video_id in scanned_ids:
            continue

        description = str(row.get("description", ""))
        links = extract_links(description)

        for link in links:
            domain = normalize_domain(link)
            if not domain or len(domain.split(".")) < 2 or domain in WELL_KNOWN_DOMAINS:
                continue

            print(f"🆕 Logging domain: {domain} from video {video_id}")
            supabase.table("Clickyleaks").insert({
                "domain": domain,
                "full_url": link,
                "video_id": video_id,
                "video_url": f"https://www.youtube.com/watch?v={video_id}",
                "is_available": None,
                "discovered_at": datetime.utcnow().isoformat(),
                "scanned_at": datetime.utcnow().isoformat(),
                "view_count": 0
            }).execute()

            mark_video_scanned(video_id)
            send_discord_alert(domain, f"https://www.youtube.com/watch?v={video_id}")
            new_logged += 1
            break

    print(f"🎉 Script completed. {new_logged} new domains logged.")

if __name__ == "__main__":
    main()
