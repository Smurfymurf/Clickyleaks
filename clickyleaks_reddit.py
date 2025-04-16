import requests, time, random, re, socket, dns.resolver, os
from urllib.parse import urlparse
from datetime import datetime, timedelta
from supabase import create_client, Client

# === CONFIG ===
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

SUBREDDITS = [
    "Entrepreneur", "SaaS", "Frugal", "Ecommerce", "CryptoMoonShots", "WebDev",
    "StockMarket", "SideHustle", "IndieHackers", "Money", "AmazonFBA",
    "SEO", "Beermoney", "Crypto_General", "Marketing", "Instagram",
    "WorkOnline", "LearnCrypto", "Copywriting", "TechNewsToday",
    "AffiliateMarketing", "Fitness", "SmallBusiness", "RealEstate",
    "LearnProgramming", "CryptoCurrency", "FatFire", "FinancialIndependence",
    "GrowMyBusiness"
]

KEYWORDS = [
    "make money", "affiliate link", "coupon code", "discount link", "expired site",
    "fat burner", "buy now", "check this domain", "side hustle"
]

BLOCKED_DOMAINS = [
    "reddit.com", "bit.ly", "t.co", "imgur.com", "youtube.com", "youtu.be",
    "instagram.com", "facebook.com", "twitter.com", "linkedin.com"
]

POST_LOOKBACK_DAYS = 365 * 5
PUSHSHIFT_RETRIES = 2
TIMEOUT = 12

def send_discord_message(message):
    try:
        requests.post(DISCORD_WEBHOOK_URL, json={"content": message}, timeout=8)
    except Exception as e:
        print(f"⚠️ Discord error: {e}")

def domain_available(domain):
    try:
        dns.resolver.resolve(domain)
        return False
    except:
        try:
            socket.gethostbyname(domain)
            return False
        except:
            return True

def extract_links(text):
    return re.findall(r'(https?://[^\s)]+)', text)

def is_checked(domain):
    root = domain.replace("www.", "")
    result = supabase.table("Clickyleaks_Reddit").select("id").eq("domain", root).execute()
    return len(result.data) > 0

def insert_domain(domain, full_url, post_url, subreddit, keyword, available):
    try:
        supabase.table("Clickyleaks_Reddit").insert({
            "domain": domain.replace("www.", ""),
            "full_url": full_url,
            "reddit_post_url": post_url,
            "subreddit": subreddit,
            "keyword": keyword,
            "is_available": available,
            "discovered_at": datetime.utcnow().isoformat()
        }).execute()
    except Exception as e:
        print(f"⚠️ DB insert error: {e}")

def scan_subreddit(subreddit, keyword):
    after = int((datetime.utcnow() - timedelta(days=POST_LOOKBACK_DAYS)).timestamp())
    url = f"https://api.pushshift.io/reddit/search/submission/?subreddit={subreddit}&q={keyword}&size=10&after={after}"
    for attempt in range(PUSHSHIFT_RETRIES):
        try:
            res = requests.get(url, timeout=TIMEOUT)
            posts = res.json().get("data", [])
            for post in posts:
                post_url = f"https://reddit.com{post.get('permalink')}"
                text = post.get("selftext", "") + " " + post.get("title", "")
                links = extract_links(text)
                for link in links:
                    domain = urlparse(link).netloc.lower()
                    if any(domain.endswith(bad) for bad in BLOCKED_DOMAINS):
                        continue
                    if is_checked(domain):
                        continue
                    is_avail = domain_available(domain)
                    print(f"🔍 {subreddit} — {keyword} — {domain} (Available: {is_avail})")
                    insert_domain(domain, link, post_url, subreddit, keyword, is_avail)
            return True
        except Exception as e:
            print(f"❌ Request failed: {e}")
            time.sleep(2)
    return False

def run_scan():
    print("🚀 Clickyleaks Reddit Scan Started...")
    found = 0
    scanned = 0
    random.shuffle(SUBREDDITS)
    for subreddit in SUBREDDITS:
        keyword = random.choice(KEYWORDS)
        scanned += 1
        success = scan_subreddit(subreddit, keyword)
        if not success:
            continue
    send_discord_message(f"✅ Reddit scan complete. Subreddits checked: {scanned}. Domains found: {found}.")

if __name__ == "__main__":
    try:
        run_scan()
    except Exception as e:
        send_discord_message(f"❌ Reddit scan failed: {str(e)}")
        raise
