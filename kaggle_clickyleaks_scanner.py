from datetime import datetime
from pathlib import Path
import os
import random
import re
import pandas as pd
import requests
from supabase import create_client, Client
from urllib.parse import urlparse
from dotenv import load_dotenv
import logging
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

# Load .env if running locally
load_dotenv()

# Configuration
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL")
KAGGLE_USERNAME = os.environ.get("KAGGLE_USERNAME")
KAGGLE_KEY = os.environ.get("KAGGLE_KEY")

# Initialize Supabase
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

KAGGLE_SOURCES = [
    {
        "owner_slug": "asaniczka",
        "dataset_slug": "trending-youtube-videos-113-countries",
        "file_pattern": "*.csv"
    },
    {
        "owner_slug": "pyuser11",
        "dataset_slug": "youtube-trending-videos-updated-daily",
        "file_pattern": "*.csv"
    },
    {
        "owner_slug": "canerkonuk",
        "dataset_slug": "youtube-trending-videos-global",
        "file_pattern": "US*.csv"
    },
    {
        "owner_slug": "sebastianbesinski",
        "dataset_slug": "youtube-trending-videos-2025-updated-daily",
        "file_pattern": "*.csv"
    }
]

WELL_KNOWN_DOMAINS = {
    "youtube.com", "youtu.be", "facebook.com", "twitter.com", "instagram.com",
    "linkedin.com", "tiktok.com", "reddit.com", "discord.com", "google.com",
    "apple.com", "microsoft.com", "amazon.com", "netflix.com", "paypal.com",
    "github.com", "bitly.com", "tinyurl.com", "snapchat.com", "spotify.com"
}

def notify_discord(message: str):
    if DISCORD_WEBHOOK_URL:
        try:
            requests.post(DISCORD_WEBHOOK_URL, json={"content": message}, timeout=10)
        except Exception as e:
            logging.error(f"Discord notification failed: {str(e)}")

def soft_check_expired(domain: str) -> bool:
    try:
        resp = requests.head(
            "http://" + domain,
            timeout=5,
            headers={"User-Agent": "Mozilla/5.0"},
            allow_redirects=True
        )
        return resp.status_code in [404, 410, 403]
    except requests.exceptions.SSLError:
        try:
            resp = requests.head(
                "https://" + domain,
                timeout=5,
                headers={"User-Agent": "Mozilla/5.0"},
                allow_redirects=True,
                verify=False
            )
            return resp.status_code in [404, 410, 403]
        except Exception:
            return True
    except Exception:
        return True  # Treat as potentially expired if unreachable

def extract_urls(text: str) -> list:
    if not isinstance(text, str):
        return []
    return re.findall(r'(https?://[^\s>"\'\)]+)', text)

def extract_domain(url: str) -> str:
    try:
        parsed = urlparse(url)
        if not parsed.netloc:
            return ""
        domain = parsed.netloc.lower()
        if domain.startswith("www."):
            domain = domain[4:]
        return domain
    except Exception:
        return ""

def download_kaggle_dataset(owner_slug: str, dataset_slug: str, file_pattern: str) -> Path:
    try:
        # Ensure data directory exists
        Path("data").mkdir(exist_ok=True)
        
        # Clean previous downloads
        for f in Path("data").glob("*"):
            f.unlink()
        
        # Download dataset
        cmd = f"kaggle datasets download -d {owner_slug}/{dataset_slug} -p data --unzip --force"
        logging.info(f"Executing: {cmd}")
        result = os.system(cmd)
        
        if result != 0:
            raise Exception(f"Kaggle download failed with exit code {result}")
        
        # Find the matching file
        matching_files = list(Path("data").rglob(file_pattern))
        if not matching_files:
            raise Exception(f"No files matching {file_pattern} found in downloaded dataset")
        
        return matching_files[0]
    except Exception as e:
        notify_discord(f"⚠️ Dataset download failed: {str(e)}")
        raise

def main():
    try:
        # Select and download dataset
        source = random.choice(KAGGLE_SOURCES)
        dataset_ref = f"{source['owner_slug']}/{source['dataset_slug']}"
        logging.info(f"📦 Processing dataset: {dataset_ref}")
        notify_discord(f"Clickyleaks scanning `{dataset_ref}`...")
        
        # Set Kaggle credentials
        os.environ["KAGGLE_USERNAME"] = KAGGLE_USERNAME
        os.environ["KAGGLE_KEY"] = KAGGLE_KEY
        
        # Download dataset
        data_file = download_kaggle_dataset(
            source['owner_slug'],
            source['dataset_slug'],
            source['file_pattern']
        )
        logging.info(f"📄 Processing file: {data_file}")
        
        # Read CSV with error handling
        try:
            df = pd.read_csv(data_file, on_bad_lines='skip', low_memory=False)
        except Exception as e:
            logging.error(f"CSV read error: {str(e)}")
            df = pd.read_csv(data_file, encoding='latin1', on_bad_lines='skip', low_memory=False)
        
        # Ensure required columns exist
        if 'video_id' not in df.columns:
            df = df.rename(columns={df.columns[0]: 'video_id'})
        if 'description' not in df.columns:
            df['description'] = ''
        
        df = df.dropna(subset=["video_id"]).drop_duplicates("video_id")
        df["video_id"] = df["video_id"].astype(str)
        
        # Get already checked videos
        existing_ids_resp = supabase.table("clickyleaks_checked").select("video_id").execute()
        already_checked = {item['video_id'] for item in existing_ids_resp.data}
        
        new_domains = []
        scanned = 0
        
        # Process videos
        for _, row in df.iterrows():
            video_id = row["video_id"]
            if video_id in already_checked:
                continue
            
            urls = extract_urls(row.get("description", ""))
            for url in urls:
                domain = extract_domain(url)
                if not domain or domain in WELL_KNOWN_DOMAINS:
                    continue
                
                if soft_check_expired(domain):
                    logging.info(f"Found candidate: {domain}")
                    new_domains.append({
                        "domain": domain,
                        "source": "YouTube",
                        "video_id": video_id,
                        "available": True,
                        "verified": False,
                        "discovered_at": datetime.utcnow().isoformat()
                    })
                    if len(new_domains) >= 5:
                        break
            
            # Mark as checked
            supabase.table("clickyleaks_checked").insert({
                "video_id": video_id,
                "scanned_at": datetime.utcnow().isoformat()
            }).execute()
            
            scanned += 1
            if len(new_domains) >= 5 or scanned >= 500:
                break
        
        # Save results
        if new_domains:
            # Check for duplicates
            existing_domains_resp = supabase.table("clickyleaks").select("domain").execute()
            existing_domains = {item['domain'] for item in existing_domains_resp.data}
            new_domains = [d for d in new_domains if d['domain'] not in existing_domains]
            
            if new_domains:
                supabase.table("clickyleaks").insert(new_domains).execute()
                msg = f"Clickyleaks discovered `{len(new_domains)}` potential domains!"
                logging.info(msg)
                notify_discord(msg)
            else:
                msg = "Clickyleaks scan complete - all found domains already exist."
                logging.info(msg)
                notify_discord(msg)
        else:
            msg = "Clickyleaks scan complete — no new domains found."
            logging.info(msg)
            notify_discord(msg)
            
    except Exception as e:
        logging.error(f"Script failed: {str(e)}", exc_info=True)
        notify_discord(f"🚨 Clickyleaks scan failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()