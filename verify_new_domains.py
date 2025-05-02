import os
import requests
from datetime import datetime
from supabase import create_client
from dotenv import load_dotenv
import tldextract

# === Load env ===
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
APILAYER_KEY = os.getenv("APILAYER_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
WHOIS_ENDPOINT = "https://api.apilayer.com/whois/check"

SUPPORTED_TLDS = {
    "com", "me", "net", "org", "sh", "io", "co", "club", "biz", "mobi", "info",
    "us", "domains", "cloud", "fr", "au", "ru", "uk", "nl", "fi", "br", "hr",
    "ee", "ca", "sk", "se", "no", "cz", "it", "in", "icu", "top", "xyz", "cn",
    "cf", "hk", "sg", "pt", "site", "kz", "si", "ae", "do", "yoga", "xxx", "ws",
    "work", "wiki", "watch", "wtf", "world", "website", "vip", "ly", "dev",
    "network", "company", "page", "rs", "run", "science", "sex", "shop",
    "solutions", "so", "studio", "style", "tech", "travel", "vc", "pub", "pro",
    "app", "press", "ooo", "de"
}

def check_domain(domain):
    headers = {"apikey": APILAYER_KEY}
    res = requests.get(f"{WHOIS_ENDPOINT}?domain={domain}", headers=headers, timeout=10)
    if res.status_code == 200:
        data = res.json()
        return data.get("available", False)
    return False

def is_supported_tld(domain):
    ext = tldextract.extract(domain)
    return ext.suffix.lower() in SUPPORTED_TLDS

def main():
    results = supabase.table("Clickyleaks").select("*").eq("is_available", True).eq("verified", False).limit(50).execute()
    domains = results.data

    for row in domains:
        domain = row["domain"]
        domain_id = row["id"]

        if not is_supported_tld(domain):
            print(f"[SKIP] Unsupported TLD: {domain}")
            continue

        is_available = check_domain(domain)
        now = datetime.utcnow().isoformat()

        supabase.table("Clickyleaks").update({
            "is_available": is_available,
            "verified": True,
            "taken": not is_available,
            "last_verified_at": now
        }).eq("id", domain_id).execute()

        print(f"[✓] {domain} → {'Available' if is_available else 'Taken'}")

if __name__ == "__main__":
    main()
