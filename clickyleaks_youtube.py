import os
import re
import uuid
import socket
import dns.resolver
import requests
import random
import time
from datetime import datetime
from pytz import UTC
from supabase import create_client, Client
from googleapiclient.discovery import build

# Supabase config
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# YouTube API config
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
youtube = build("youtube", "v3", developerKey=YOUTUBE_API_KEY)

# DNS resolver setup
resolver = dns.resolver.Resolver()
resolver.nameservers = ['8.8.8.8']  # Google DNS

# Keywords to search
KEYWORDS = [
    "affiliate marketing tutorial", "make money online", "start a blog", "best vpn deals",
    "free hosting", "cashback sites", "product review", "how to save money shopping",
    "passive income", "crypto affiliate", "web hosting affiliate", "cheap domain names",
    "email marketing tools", "best ai tools", "learn coding free"
]

def is_domain_available(domain):
    domain = domain.replace("http://", "").replace("https://", "").replace("www.", "").strip("/")
    try:
        answers = resolver.resolve(domain, 'A')
        if answers:
            return False
    except (dns.resolver.NXDOMAIN, dns.resolver.NoAnswer, dns.resolver.NoNameservers, dns.exception.Timeout):
        pass

    try:
        socket.gethostbyname(domain)
        return False
    except socket.gaierror:
        return True

def extract_domains(text):
    urls = re.findall(r'https?://[\w.-]+(?:\.[a-z]{2,})+', text)
    return [url.split("//")[-1].split("/")[0] for url in urls]

def already_checked(video_id):
    response = supabase.table("Clickyleaks_YouTube").select("video_id").eq("video_id", video_id).execute()
    return len(response.data) > 0

def save_to_supabase(video_data, domain):
    is_available = is_domain_available(domain)
    entry = {
        "id": str(uuid.uuid4()),
        "video_id": video_data['id'],
        "video_url": f"https://www.youtube.com/watch?v={video_data['id']}",
        "title": video_data['title'],
        "views": video_data['views'],
        "domain": domain,
        "available": is_available,
        "created_at": datetime.now(UTC).isoformat()
    }
    supabase.table("Clickyleaks_YouTube").insert(entry).execute()
    print(f"{'✅' if is_available else '🔴'} Logged domain: {domain}")

def search_and_log():
    print("🚀 Clickyleaks scan started...")
    keyword = random.choice(KEYWORDS)
    print(f"🔎 Searching: {keyword}")
    
    search_response = youtube.search().list(
        q=keyword,
        part="snippet",
        type="video",
        maxResults=10
    ).execute()

    for item in search_response.get("items", []):
        video_id = item["id"]["videoId"]
        if already_checked(video_id):
            continue

        video_details = youtube.videos().list(
            part="snippet,statistics",
            id=video_id
        ).execute()

        if not video_details["items"]:
            continue

        snippet = video_details["items"][0]["snippet"]
        stats = video_details["items"][0].get("statistics", {})
        description = snippet.get("description", "")
        domains = extract_domains(description)

        for domain in set(domains):
            save_to_supabase({
                "id": video_id,
                "title": snippet.get("title", ""),
                "views": int(stats.get("viewCount", 0))
            }, domain)
            time.sleep(1.5)

if __name__ == "__main__":
    search_and_log()
