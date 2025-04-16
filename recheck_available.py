import os
import requests
from datetime import datetime
from supabase import create_client, Client

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# === Availability Check ===
def is_domain_available(domain):
    try:
        import socket
        socket.setdefaulttimeout(5)
        socket.gethostbyname(domain)
        return False  # If it resolves, it's taken
    except socket.gaierror:
        return True

# === Re-check and Update ===
def reverify_domains():
    print("🔁 Re-verifying available domains...")
    
    rows = (
        supabase.table("Clickyleaks")
        .select("id, domain")
        .eq("is_available", True)
        .execute()
    )

    for row in rows.data:
        domain = row['domain']
        domain_id = row['id']
        print(f"🔍 Re-checking: {domain}")

        available = is_domain_available(domain)

        supabase.table("Clickyleaks").update({
            "is_available": available,
            "scanned_at": datetime.utcnow().isoformat()
        }).eq("id", domain_id).execute()

        if not available:
            print(f"❌ No longer available: {domain}")
        else:
            print(f"✅ Still available: {domain}")

if __name__ == "__main__":
    reverify_domains()
