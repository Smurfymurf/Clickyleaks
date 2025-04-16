import os
import re
import uuid
import socket
import dns.resolver
import random
from datetime import datetime
from pytz import UTC
from supabase import create_client, Client
from googleapiclient.discovery import build

# Env vars
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")

# Initialize clients
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
youtube = build("youtube", "v3", developerKey=YOUTUBE_API_KEY)

# Keywords
KEYWORDS = [
    "make money online", "affiliate marketing", "drop shipping", "crypto wallet",
    "work from home", "online business", "earn passive income", "ai tools",
    "best ai tools", "learn coding free", "seo software", "stock market tools",
    "budgeting app", "weight loss hacks", "digital marketing", "email marketing",
    "how to buy bitcoin", "investing for beginners", "crypto trading tutorial",
    "top web hosting", "best vpn", "productivity hacks", "ai video tools",
    "graphic design tools", "freelancing guide"
]

def extract_domains(text):
    if not text:
        return []
    return re.findall(r'https?://(?:www\.)?([^\s/]+)', text)

def is_domain_available(domain):
    try:
        socket.gethostbyname(domain)
        return False
    except socket.gaierror:
        pass

    try:
        dns.resolver.resolve(domain, "A")
        return False
    except:
        return True

def already_checked(video_id):
    response = supabase.table("Clickyleaks").select("video_id").eq("video_id", video_id).execute()
    return len(response.data) > 0

def insert_domain(domain, video, available):
    video_id = video["id"]
    video_title = video["snippet"]["title"]
    view_count = int(video.get("statistics", {}).get("viewCount", 0))

    supabase.table("Clickyleaks").insert({
        "id": str(uuid.uuid4()),
        "domain": domain,
        "video_title": video_title,
        "video_url": f"https://www.youtube.com/watch?v={video_id}",
        "view_count": view_count,
        "available": available,
        "is_checked": True,
        "video_id": video_id,
        "created_at": datetime.now(UTC).isoformat()
    }).execute()

def search_and_log():
    print("🚀 Clickyleaks scan started...")
    query = random.choice(KEYWORDS)
    print(f"🔎 Searching: {query}")

    search_response = youtube.search().list(
        q=query,
        part="snippet",
        type="video",
        maxResults=15
    ).execute()

    for item in search_response.get("items", []):
        video_id = item["id"]["videoId"]

        if already_checked(video_id):
            continue

        video_details = youtube.videos().list(
            part="snippet,statistics",
            id=video_id
        ).execute()

        video = video_details["items"][0]
        description = video["snippet"].get("description", "")
        domains = extract_domains(description)

        for domain in domains:
            available = is_domain_available(domain)
            print(f"{'🟢' if available else '🔴'} Logging domain: {domain}")
            insert_domain(domain, video, available)

if __name__ == "__main__":
    search_and_log()
