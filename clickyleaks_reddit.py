import requests, time, random, re, socket, dns.resolver
from urllib.parse import urlparse
from datetime import datetime
from supabase import create_client, Client
import os

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

SUBREDDITS = [
    "sidehustle", "workfromhome", "Entrepreneur", "digitalnomad", "passive_income",
    "Affiliatemarketing", "Freelance", "SEO", "JustStart", "InternetIsBeautiful"
]

KEYWORDS = [
    "make money", "side hustle", "affiliate link", "my blog", "check out my site",
    "visit my website", "passive income", "use my referral", "join now", "how I earn"
]

BLOCKED_DOMAINS = [
    "youtube.com", "youtu.be", "facebook.com", "twitter.com", "reddit.com",
    "instagram.com", "tiktok.com", "linkedin.com", "bit.ly", "linktr.ee"
]

def send_discord_notification(message: str):
    if not DISCORD_WEBHOOK_URL:
        print("⚠️ No Discord webhook set.")
        return
    try:
        requests.post(DISCORD_WEBHOOK_URL, json={"content": message})
    except Exception as e:
        print(f"❌ Discord notification error: {e}")

def get_pushshift_results(subreddit, keyword, retries=3):
    for _ in range(retries):
        try:
            url = f"https://api.pushshift.io/reddit/search/comment/?q={keyword}&subreddit={subreddit}&size=50"
            res = requests.get(url, timeout=10)
            return res.json().get("data", [])
        except Exception as e:
            print(f"⏳ Retry Pushshift for {subreddit}/{keyword}: {e}")
            time.sleep(2)
    raise Exception(f"Pushshift failed after {retries} retries.")

def extract_links(text):
    return re.findall(r'(https?://[^\s)]+)', text)

def domain_already_checked(domain):
    result = supabase.table("Clickyleaks").select("id").eq("domain", domain).execute()
    return len(result.data) > 0

def is_domain_available(domain):
    root = domain.lower().split("/")[0].replace("www.", "")
    try:
        socket.gethostbyname(root)
        dns.resolver.resolve(root, 'A')
        return False
    except:
        return True

def log_domain(domain, full_url, source, is_available):
    try:
        supabase.table("Clickyleaks").insert({
            "domain": domain,
            "full_url": full_url,
            "video_id": None,
            "video_title": f"Reddit ({source})",
            "video_url": f"https://reddit.com/r/{source}",
            "http_status": None,
            "is_available": is_available,
            "view_count": None,
            "discovered_at": datetime.utcnow().isoformat(),
            "scanned_at": datetime.utcnow().isoformat()
        }).execute()
    except Exception as e:
        print(f"⚠️ Insert error: {e}")

def run_scan():
    print("🚀 Clickyleaks Reddit Scan Started...")
    start = time.time()
    found, available = 0, 0

    subreddit = random.choice(SUBREDDITS)
    keyword = random.choice(KEYWORDS)

    print(f"🔍 {subreddit} — {keyword}")
    try:
        results = get_pushshift_results(subreddit, keyword)
    except Exception as e:
        send_discord_notification(f"❌ Reddit scan failed: {e}")
        raise

    for post in results:
        body = post.get("body", "")
        links = extract_links(body)
        for link in links:
            domain = urlparse(link).netloc.lower()
            if any(domain.endswith(bad) for bad in BLOCKED_DOMAINS):
                continue
            if domain_already_checked(domain):
                continue

            found += 1
            is_avail = is_domain_available(domain)
            if is_avail:
                available += 1
                print(f"🟢 AVAILABLE: {domain}")
            else:
                print(f"🔴 Taken: {domain}")

            log_domain(domain, link, subreddit, is_avail)

    duration = round(time.time() - start)
    msg = f"✅ Reddit scan done.\n**Keyword:** `{keyword}`\n**Subreddit:** `{subreddit}`\n**Found:** {found}\n**Available:** {available}\n⏱️ {duration}s"
    send_discord_notification(msg)

if __name__ == "__main__":
    try:
        run_scan()
    except Exception as ex:
        send_discord_notification(f"🔥 Clickyleaks Reddit failed: `{str(ex)}`")
        raise
