import os
import requests
from datetime import datetime, timedelta
from supabase import create_client
from dotenv import load_dotenv

# === Load env ===
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
APILAYER_KEY = os.getenv("APILAYER_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

SUPPORTED_TLDS = {
    ".com", ".net", ".org", ".info", ".biz", ".us", ".co", ".io", ".me", ".app", ".dev", ".page", ".xyz", ".top", ".site", ".club",
    ".online", ".store", ".tech", ".website", ".space", ".shop", ".cloud", ".press", ".wiki", ".ly", ".ca", ".uk", ".de", ".fr", ".in",
    ".ru", ".br", ".au", ".cn", ".cz", ".sk", ".fi", ".no", ".se", ".pt", ".hr", ".ee", ".kz", ".si", ".rs", ".vc", ".do", ".ae", ".sg",
    ".mobi", ".studio", ".style", ".pro", ".solutions", ".wtf", ".run", ".watch", ".work", ".yoga", ".ooo", ".network", ".company",
    ".world", ".sex", ".xxx", ".cf", ".icu", ".nl"
}

def check_domain(domain):
    tld = "." + domain.split(".")[-1].lower()
    if tld not in SUPPORTED_TLDS:
        print(f"[SKIP] Unsupported TLD: {domain}")
        return "unsupported"

    headers = {"apikey": APILAYER_KEY}
    url = f"https://api.apilayer.com/whois/check?domain={domain}"
    try:
        res = requests.get(url, headers=headers, timeout=15)
        if res.status_code == 200:
            return res.json().get("result")
        elif res.status_code == 429:
            print(f"[RATE LIMIT] API rate limit hit for {domain}")
            return None
        else:
            print(f"[ERROR] API response {res.status_code} for {domain}")
            return None
    except requests.exceptions.Timeout:
        print(f"[TIMEOUT] Skipped (timeout): {domain}")
        return None
    except Exception as e:
        print(f"[ERROR] Request failed for {domain}: {e}")
        return None

def main():
    # 1. Fetch domains with no last_verified_at
    no_verified = supabase.table("Clickyleaks")\
        .select("*")\
        .eq("is_available", True)\
        .eq("verified", True)\
        .is_("last_verified_at", None)\
        .is_("unsupported_tld", None)\
        .limit(25)\
        .execute().data

    # 2. Fetch domains verified more than 48 hours ago
    cutoff = (datetime.utcnow() - timedelta(hours=48)).isoformat()
    stale_verified = supabase.table("Clickyleaks")\
        .select("*")\
        .eq("is_available", True)\
        .eq("verified", True)\
        .lt("last_verified_at", cutoff)\
        .is_("unsupported_tld", None)\
        .limit(25)\
        .execute().data

    to_check = no_verified + stale_verified
    print(f"[INFO] Checking {len(to_check)} domains...")

    for row in to_check:
        domain = row["domain"]
        domain_id = row["id"]
        now = datetime.utcnow().isoformat()

        result = check_domain(domain)
        if result == "available":
            supabase.table("Clickyleaks").update({
                "last_verified_at": now
            }).eq("id", domain_id).execute()
            print(f"[✓] {domain} → Still AVAILABLE")
        elif result == "registered":
            supabase.table("Clickyleaks").update({
                "is_available": False,
                "taken": True,
                "last_verified_at": now
            }).eq("id", domain_id).execute()
            print(f"[×] {domain} → Now REGISTERED")
        elif result == "unsupported":
            supabase.table("Clickyleaks").update({
                "unsupported_tld": True
            }).eq("id", domain_id).execute()
            print(f"[SKIP] Marked unsupported: {domain}")
        else:
            print(f"[!] Skipped or failed: {domain}")

if __name__ == "__main__":
    main()
