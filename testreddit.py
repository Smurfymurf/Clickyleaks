import os
import requests
from dotenv import load_dotenv

load_dotenv()

client_id = os.getenv("REDDIT_CLIENT_ID")
secret = os.getenv("REDDIT_SECRET")
username = os.getenv("REDDIT_USERNAME")
password = os.getenv("REDDIT_PASSWORD")

auth = requests.auth.HTTPBasicAuth(client_id, secret)
data = {
    "grant_type": "password",
    "username": username,
    "password": password
}
headers = {"User-Agent": "ClickyleaksTestBot/0.1 by " + username}

res = requests.post("https://www.reddit.com/api/v1/access_token",
                    auth=auth, data=data, headers=headers)

print(res.status_code)
print(res.text)