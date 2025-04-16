import os
import whois
import time
from datetime import datetime
from supabase import create_client, Client

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def is_domain_registered(domain):
    try:
        w = whois.whois(domain)
        return bool(w.domain_name)
    except:
        return False

def recheck_domains():
    print("🔎 Starting premium recheck...")

    # Fetch all domains previously marked as available
    response = supabase.table("Clickyleaks").select("*").eq("is_available", True).execute()
    domains = response.data

    if not domains:
        print("✅ No available domains found to recheck.")
        return

    print(f"🔁 Rechecking {len(domains)} domains...")

    for entry in domains:
        domain = entry["domain"]
        domain_id = entry["id"]

        print(f"🔍 Rechecking domain: {domain}")

        if is_domain_registered(domain):
            print(f"❌ Domain is actually taken: {domain}")
            supabase.table("Clickyleaks").update({
                "is_available": False,
                "scanned_at": datetime.utcnow().isoformat()
            }).eq("id", domain_id).execute()
        else:
            print(f"✅ Still available: {domain}")
            supabase.table("Clickyleaks").update({
                "scanned_at": datetime.utcnow().isoformat()
            }).eq("id", domain_id).execute()

        time.sleep(1.5)  # avoid hammering WHOIS

    print("🎉 Premium recheck complete.")

if __name__ == "__main__":
    recheck_domains()
