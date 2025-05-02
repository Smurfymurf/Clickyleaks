import os
import requests
from datetime import datetime
from supabase import create_client
from dotenv import load_dotenv

# === Load env ===
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
APILAYER_KEY = os.getenv("APILAYER_KEY")  # Your key: sSmsd461UVYVpBUajOxh94ZiVJPCELAs

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
WHOIS_ENDPOINT = "https://api.apilayer.com/whois/check"

def check_domain(domain):
    headers = {"apikey": APILAYER_KEY}
    res = requests.get(f"{WHOIS_ENDPOINT}?domain={domain}", headers=headers, timeout=10)
    if res.status_code == 200:
        data = res.json()
        return data.get("available", False)
    return False

def main():
    results = supabase.table("Clickyleaks").select("*").eq("available", True).eq("verified", False).limit(50).execute()
    domains = results.data

    for row in domains:
        domain = row["domain"]
        domain_id = row["id"]

        is_available = check_domain(domain)
        now = datetime.utcnow().isoformat()

        supabase.table("Clickyleaks").update({
            "verified": True,
            "taken": not is_available,
            "last_verified_at": now
        }).eq("id", domain_id).execute()

        print(f"[✓] {domain} → {'Available' if is_available else 'Taken'}")

if __name__ == "__main__":
    main()
