import requests
import random
import re
import os
from datetime import datetime
from urllib.parse import urlparse
from supabase import create_client, Client

# === CONFIG ===
REDDIT_USER_AGENT = "ClickyleaksRedditBot/0.1"
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
GODADDY_API_KEY = os.getenv("GODADDY_API_KEY")
GODADDY_API_SECRET = os.getenv("GODADDY_API_SECRET")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

KEYWORDS = [
    "freelance platforms", "gaming accessories", "health gadgets", "freelance tools",
    "investing apps", "affiliate marketing", "passive income", "keto diet",
    "credit repair", "drop shipping", "seo software", "ai tools",
    "web hosting", "vpn services", "web scraping tools"
]

SUBREDDITS = [
    "CryptoCurrency", "smallbusiness", "Entrepreneur", "YouTube", "seo",
    "digitalmarketing", "marketing", "technology", "Finance", "affiliatemarketing",
    "deals", "ecommerce", "shopify", "sidehustle", "workonline"
]

BLOCKED_DOMAINS = [
    "reddit.com", "twitter.com", "facebook.com", "youtube.com", "instagram.com",
    "bit.ly", "t.co", "youtu.be", "linkedin.com", "paypal.com", "discord.gg"
]

def get_random_subreddit_post():
    subreddit = random.choice(SUBREDDITS)
    keyword = random.choice(KEYWORDS)
    url = f"https://www.reddit.com/r/{subreddit}/search.json?q={keyword}&restrict_sr=1&sort=new&limit=25"
    headers = {"User-Agent": REDDIT_USER_AGENT}
    try:
        res = requests.get(url, headers=headers, timeout=10)
        posts = res.json().get("data", {}).get("children", [])
        return posts
    except Exception as e:
        print(f"❌ Reddit error: {e}")
        return []

def extract_links(text):
    return re.findall(r'(https?://[^\s)]+)', text)

def is_domain_available(domain):
    root = domain.lower().strip()
    if root.startswith("www."):
        root = root[4:]
    root = root.split("/")[0]

    headers = {
        "Authorization": f"sso-key {GODADDY_API_KEY}:{GODADDY_API_SECRET}",
        "Accept": "application/json"
    }

    print(f"🔍 Checking domain availability: {root}")
    try:
        res = requests.get(
            f"https://api.godaddy.com/v1/domains/available?domain={root}",
            headers=headers,
            timeout=6
        )
        if res.status_code == 200:
            data = res.json()
            return root, data.get("available", False)
        else:
            print(f"⚠️ GoDaddy error {res.status_code} for {root}")
    except Exception as e:
        print(f"❌ GoDaddy exception: {e}")
    return root, False

def already_checked(domain):
    res = supabase.table("ClickyleaksReddit").select("id").eq("domain", domain).execute()
    return len(res.data) > 0

def process_post(post):
    text = post["data"].get("selftext", "") + "\n" + post["data"].get("title", "")
    links = extract_links(text)
    permalink = "https://reddit.com" + post["data"].get("permalink", "")
    subreddit = post["data"].get("subreddit", "")

    for link in links:
        domain = urlparse(link).netloc.lower()
        if any(domain.endswith(bad) for bad in BLOCKED_DOMAINS):
            continue

        root_domain, available = is_domain_available(domain)
        if already_checked(root_domain):
            print(f"🔁 Already checked: {root_domain}")
            continue

        record = {
            "domain": root_domain,
            "full_url": link,
            "subreddit": subreddit,
            "post_url": permalink,
            "is_available": available,
            "discovered_at": datetime.utcnow().isoformat()
        }

        try:
            supabase.table("ClickyleaksReddit").insert(record).execute()
            print(f"✅ Added to DB: {root_domain}")
        except Exception as e:
            print(f"⚠️ Insert error: {e}")

        break  # only first link

def main():
    print("🚀 Clickyleaks Reddit scan started...")
    posts = get_random_subreddit_post()
    for post in posts:
        process_post(post)
    print("✅ Reddit scan complete.")

if __name__ == "__main__":
    main()
