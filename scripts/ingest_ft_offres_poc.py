import requests
import datetime
import json
import boto3
import os

def get_token():
    url = "https://entreprise.francetravail.fr/connexion/oauth2/access_token?realm=/partenaire"

    data = {
        "grant_type": "client_credentials",
        "client_id": os.getenv("FT_CLIENT_ID"),
        "client_secret": os.getenv("FT_CLIENT_SECRET"),
        # test 1: scope simple
        "scope": "api_offresdemploiv2 o2dsoffre"
        # test 2 (si 403): "scope": "api_offresdemploiv2 o2dsoffre"
    }

    r = requests.post(url, data=data, timeout=30)
    print("Token status:", r.status_code)
    print("Token body preview:", r.text[:300])
    r.raise_for_status()

    payload = r.json()
    print("Token scope returned:", payload.get("scope"))
    print("Token expires_in:", payload.get("expires_in"))

    return payload["access_token"]


TOKEN = get_token()

url = "https://api.francetravail.io/partenaire/offresdemploi/v2/offres/search"

headers = {
    "Authorization": f"Bearer {TOKEN}",
    "Accept": "application/json",
    "User-Agent": "job-market-intel/0.1"
}

params = {
    "motsCles": "data engineer",
    "departement": "75",
    "range": "0-49",
    "sort": "1"
}

response = requests.get(url, headers=headers, params=params, timeout=30)

print("GET status:", response.status_code)
print("GET Content-Type:", response.headers.get("Content-Type"))
print("GET body preview:", repr(response.text[:300]))

response.raise_for_status()
data = response.json()

today = datetime.date.today().isoformat()
filename = f"offres_paris_{today}.json"

with open(filename, "w") as f:
    json.dump(data, f)

print("Saved locally:", filename)

s3 = boto3.client("s3")
s3.upload_file(
    filename,
    "job-market-intel-kevan",
    f"raw/francetravail/offres/v2/dt={today}/{filename}"
)

print("Uploaded to S3")