import os
import requests
from supabase import create_client
from urllib.parse import quote

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
GODADDY_API_KEY = os.getenv("GODADDY_API_KEY")
GODADDY_API_SECRET = os.getenv("GODADDY_API_SECRET")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def check_exact_match_availability(domain):
    url = f"https://api.godaddy.com/v1/domains/available?domain={quote(domain)}&checkType=FAST&forTransfer=false"
    headers = {
        "Authorization": f"sso-key {GODADDY_API_KEY}:{GODADDY_API_SECRET}",
        "Accept": "application/json"
    }
    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code == 200:
            data = response.json()
            return data.get("exactMatch", False)
        else:
            print(f"⚠️ GoDaddy check failed for {domain}: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Exception checking domain {domain}: {e}")
        return False

def main():
    print("🚀 Running One-Time Clickyleaks Domain Verifier...")

    result = supabase.table("Clickyleaks").select("*").execute()
    rows = result.data or []

    print(f"🔍 Checking {len(rows)} domains...")

    for row in rows:
        domain = row["domain"]
        id_ = row["id"]

        print(f"🔎 Verifying: {domain}")

        if check_exact_match_availability(domain):
            print(f"✅ Exact match available: {domain}")
            supabase.table("Clickyleaks").update({"verified": True}).eq("id", id_).execute()
        else:
            print(f"❌ Not available. Removing: {domain}")
            supabase.table("Clickyleaks").delete().eq("id", id_).execute()

    print("✅ Done.")

if __name__ == "__main__":
    main()