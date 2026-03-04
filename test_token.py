import os
import requests
from dotenv import load_dotenv

load_dotenv()

CLIENT_ID = os.getenv("FT_CLIENT_ID")
CLIENT_SECRET = os.getenv("FT_CLIENT_SECRET")
SCOPE = os.getenv("FT_SCOPE")

TOKEN_URL = "https://entreprise.francetravail.fr/connexion/oauth2/access_token?realm=/partenaire"

payload = {
    "grant_type": "client_credentials",
    "client_id": CLIENT_ID,
    "client_secret": CLIENT_SECRET,
    "scope": SCOPE
}

response = requests.post(
    TOKEN_URL,
    data=payload,
    headers={"Content-Type": "application/x-www-form-urlencoded"}
)

print("Status:", response.status_code)
print(response.json())
