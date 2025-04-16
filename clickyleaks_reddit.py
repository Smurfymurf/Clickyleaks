import requests, time, random, re, socket, dns.resolver
from urllib.parse import urlparse
from datetime import datetime
from supabase import create_client, Client
import os

# === CONFIG ===
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

SUBREDDITS = [
    "Entrepreneur", "SideHustle", "AffiliateMarketing", "DigitalMarketing",
    "WorkOnline", "Beermoney", "Passive_Income", "Dropship", "SmallBusiness",
    "Marketing", "CryptoCurrency", "DeFi", "SEO", "SocialMedia", "FIREy",
    "Frugal", "budgetfood", "Productivity", "Copywriting", "OnlineIncome",
    "FinancialIndependence", "Money", "investing", "StockMarket", "Keto",
    "Supplements", "CryptoMarkets", "LearnCrypto", "Crypto_General", "Fitness",
    "FatFire", "CryptoMoonShots", "TechNewsToday", "WebDev", "LearnProgramming",
    "CodingHelp", "IndieHackers", "SaaS", "Startup", "GrowMyBusiness",
    "YouTube", "Instagram", "TikTokGrowth", "Ecommerce", "AmazonFBA",
    "PrintOnDemand", "Finance", "RealEstate", "HomeImprovement"
]

KEYWORDS = [
    "make money", "side hustle", "affiliate link", "discount link",
    "check this out", "my site", "use my code", "link below",
    "crypto tool", "fat burner", "keto tips", "weight loss",
    "passive income", "my blog", "get it here", "use my link",
    "check out this product", "found this site", "coupon code",
    "buy now", "check this domain", "expired site"
]

BLOCKED_DOMAINS = ["youtube.com", "t.co", "facebook.com", "instagram.com", "twitter.com", "linkedin.com", "youtu.be"]

MAX_SUBREDDITS = 200
RANDOM_DELAY_RANGE = (0.5, 1.2)
RETRY_ON_FAIL = True

# === DOMAIN CHECK ===
def is_domain_available(domain):
    try:
        dns.resolver.resolve(domain)
        return False
    except:
        try:
            socket.gethostbyname(domain)
            return False
        except:
            return True

# === LOGGING ===
def send_discord_message(content):
    if not DISCORD_WEBHOOK_URL: return
    try:
        requests.post(DISCORD_WEBHOOK_URL, json={"content": content})
    except Exception as e:
        print(f"❌ Discord webhook failed: {e}")

# === HELPERS ===
def extract_links(text):
    return re.findall(r'(https?://[^\s)]+)', text)

def extract_domain(url):
    parsed = urlparse(url)
    return parsed.netloc.lower().replace("www.", "")

def already_scanned(domain):
    res = supabase.table("Clickyleaks_Reddit").select("id").eq("domain", domain).execute()
    return len(res.data) > 0

def log_to_supabase(domain, full_url, subreddit, title, permalink, available):
    try:
        supabase.table("Clickyleaks_Reddit").insert({
            "domain": domain,
            "full_url": full_url,
            "subreddit": subreddit,
            "title": title,
            "permalink": f"https://reddit.com{permalink}",
            "discovered_at": datetime.utcnow().isoformat(),
            "available": available
        }).execute()
        return True
    except Exception as e:
        print(f"❌ Supabase insert error: {e}")
        return False

# === MAIN ===
def run_scan():
    print("🚀 Clickyleaks Reddit Scan Started...")
    found_available = []

    for subreddit in random.sample(SUBREDDITS, min(len(SUBREDDITS), MAX_SUBREDDITS)):
        keyword = random.choice(KEYWORDS)
        print(f"🔍 {subreddit} — {keyword}")

        url = f"https://api.pushshift.io/reddit/search/submission/?subreddit={subreddit}&q={keyword}&size=25"
        try:
            res = requests.get(url, timeout=12)
            posts = res.json().get("data", [])
        except Exception as e:
            print(f"❌ Request failed: {e}")
            continue

        for post in posts:
            text = f"{post.get('title', '')}\n{post.get('selftext', '')}"
            links = extract_links(text)
            for link in links:
                domain = extract_domain(link)
                if domain in BLOCKED_DOMAINS:
                    continue
                if already_scanned(domain):
                    continue

                available = is_domain_available(domain)
                print(f"🔴 Logging domain: {domain} (Available: {available})")
                success = log_to_supabase(domain, link, subreddit, post.get("title", ""), post.get("permalink", ""), available)
                if success and available:
                    found_available.append(domain)

                time.sleep(random.uniform(*RANDOM_DELAY_RANGE))
                break

    if found_available:
        send_discord_message(f"✅ Found {len(found_available)} available domains on Reddit:\n" + "\n".join(found_available))
    else:
        send_discord_message("ℹ️ Reddit scan complete. No available domains found.")

if __name__ == "__main__":
    try:
        run_scan()
    except Exception as e:
        print(f"💥 Script failed: {e}")
        send_discord_message(f"❌ Reddit scan failed: {e}")
