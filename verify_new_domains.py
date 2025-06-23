import os
import requests
from datetime import datetime
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
    domain = domain.replace(",", ".").strip().lower()
    tld = "." + domain.split(".")[-1]
    if tld not in SUPPORTED_TLDS:
        print(f"[SKIP] Unsupported TLD: {domain}")
        return "unsupported"

    headers = {"apikey": APILAYER_KEY}
    url = f"https://api.apilayer.com/whois/check?domain={domain}"

    for attempt in range(3):
        try:
            res = requests.get(url, headers=headers, timeout=15)
            if res.status_code == 200:
                return res.json().get("result")
            elif res.status_code == 400:
                print(f"[ERROR] Malformed request for {domain}")
                return None
            else:
                print(f"[ERROR] API response {res.status_code} for {domain}")
                return None
        except requests.exceptions.Timeout:
            print(f"[TIMEOUT] {domain} (attempt {attempt + 1}/3)")
        except Exception as e:
            print(f"[ERROR] Request failed for {domain}: {e}")
        time.sleep(2)  # Wait before retry

    return None

def main():
    rows = supabase.table("Clickyleaks") \
        .select("*") \
        .eq("is_available", True) \
        .eq("verified", False) \
        .is_("unsupported_tld", None) \
        .limit(50) \
        .execute().data

    for row in rows:
        domain = row["domain"]
        domain_id = row["id"]
        now = datetime.utcnow().isoformat()

        result = check_domain(domain)
        if result == "available":
            supabase.table("Clickyleaks").update({
                "is_available": True,
                "verified": True,
                "taken": False,
                "last_verified_at": now
            }).eq("id", domain_id).execute()
            print(f"[✓] {domain} → Verified as AVAILABLE")
        elif result == "registered":
            supabase.table("Clickyleaks").update({
                "is_available": False,
                "verified": True,
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
