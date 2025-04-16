import requests, time, random, re, socket, dns.resolver
from datetime import datetime
from urllib.parse import urlparse
from supabase import create_client, Client
import os

# === CONFIG ===
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
DISCORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

KEYWORDS = [
    "affiliate", "make money", "passive income", "crypto", "bitcoin", "drop shipping",
    "cashback", "get rich", "side hustle", "free traffic", "seo", "google ads", "email list"
]

SUBREDDITS = [
    "workfromhome", "sidehustle", "Entrepreneur", "affiliatemarketing", "freelance", "smallbusiness",
    "passive_income", "digital_marketing", "marketing", "seo", "emailmarketing", "CryptoCurrency",
    "Bitcoin", "Ethereum", "cryptomarkets", "deals", "frugal", "hustle", "WorkOnline", "financialindependence"
]

BLOCKED_DOMAINS = [
    "reddit.com", "youtube.com", "bit.ly", "t.co", "facebook.com", "instagram.com", "twitter.com"
]

# === DOMAIN CHECK ===
def is_domain_available(domain):
    domain = domain.lower().strip()
    if domain.startswith("www."):
        domain = domain[4:]
    domain = domain.split("/")[0]
    try:
        dns.resolver.resolve(domain, 'A')
        return False
    except:
        try:
            socket.gethostbyname(domain)
            return False
        except:
            return True

# === SUPABASE ===
def domain_already_logged(domain):
    domain = domain.lower()
    res = supabase.table("Clickyleaks_Reddit").select("id").eq("domain", domain).execute()
    return len(res.data) > 0

def log_domain(data):
    try:
        supabase.table("Clickyleaks_Reddit").insert(data).execute()
    except Exception as e:
        print(f"⚠️ Failed to log to Supabase: {e}")

# === DISCORD ===
def send_discord(message):
    try:
        requests.post(DISCORD_WEBHOOK, json={"content": message})
    except:
        pass

# === MAIN ===
def run_scan():
    start_time = time.time()
    found = 0
    scanned = 0

    for _ in range(100):
        keyword = random.choice(KEYWORDS)
        subreddit = random.choice(SUBREDDITS)

        url = f"https://api.pushshift.io/reddit/search/submission/?q={keyword}&subreddit={subreddit}&size=100"
        print(f"🔍 {subreddit} — {keyword}")
        res = requests.get(url, timeout=10)
        posts = res.json().get("data", [])

        for post in posts:
            scanned += 1
            text = post.get("selftext", "") + " " + post.get("url", "")
            links = re.findall(r'(https?://[^\s)]+)', text)

            for link in links:
                domain = urlparse(link).netloc.lower()
                if any(domain.endswith(bad) for bad in BLOCKED_DOMAINS):
                    continue

                if domain_already_logged(domain):
                    continue

                available = is_domain_available(domain)

                log_domain({
                    "domain": domain,
                    "full_url": link,
                    "post_title": post.get("title"),
                    "post_url": f"https://reddit.com{post.get('permalink', '')}",
                    "subreddit": subreddit,
                    "is_available": available,
                    "created_utc": datetime.utcnow().isoformat()
                })

                if available:
                    found += 1
                    print(f"✅ Found available domain: {domain}")
                    send_discord(f"🔥 Available domain from Reddit: {domain} ({subreddit})")

                break

    duration = int(time.time() - start_time)
    send_discord(f"✅ Reddit scan complete. Scanned: {scanned}, Found available: {found}, Duration: {duration}s")

if __name__ == "__main__":
    try:
        run_scan()
    except Exception as e:
        send_discord(f"❌ Reddit scan failed: {e}")
        raise
