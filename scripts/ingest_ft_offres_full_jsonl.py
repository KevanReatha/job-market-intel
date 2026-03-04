import requests
import datetime
import json
import boto3
import os

BUCKET = "job-market-intel-kevan"
PREFIX = "raw/francetravail/offres/v2"

def get_token():
    url = "https://entreprise.francetravail.fr/connexion/oauth2/access_token?realm=/partenaire"
    data = {
        "grant_type": "client_credentials",
        "client_id": os.getenv("FT_CLIENT_ID"),
        "client_secret": os.getenv("FT_CLIENT_SECRET"),
        # IMPORTANT: scope requis pour /offres/search
        "scope": "api_offresdemploiv2 o2dsoffre",
    }
    r = requests.post(url, data=data, timeout=30)
    r.raise_for_status()
    return r.json()["access_token"]

def parse_total_from_content_range(content_range: str) -> int:
    # Exemple: "offres 0-49/64"
    # On veut 64
    if not content_range or "/" not in content_range:
        raise ValueError(f"Missing or invalid Content-Range header: {content_range!r}")
    return int(content_range.split("/")[-1])

def fetch_all_offers(token: str, mots_cles: str, departement: str, sort: str = "1"):
    url = "https://api.francetravail.io/partenaire/offresdemploi/v2/offres/search"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "User-Agent": "job-market-intel/0.1",
    }

    page_size = 150  # limite API
    start = 0
    total = None

    while True:
        end = start + page_size - 1
        params = {
            "motsCles": mots_cles,
            "departement": departement,
            "range": f"{start}-{end}",
            "sort": sort,
        }

        r = requests.get(url, headers=headers, params=params, timeout=30)

        # Debug utile en cas d’erreur
        if r.status_code >= 400:
            print("ERROR status:", r.status_code)
            print("WWW-Authenticate:", r.headers.get("Www-Authenticate"))
            print("Body preview:", repr(r.text[:300]))
            r.raise_for_status()

        data = r.json()
        results = data.get("resultats", [])

        content_range = r.headers.get("Content-Range")
        if total is None:
            total = parse_total_from_content_range(content_range)
            print("Total offers:", total)

        print(f"Fetched range {start}-{end} -> {len(results)} results")

        for item in results:
            yield item

        # stop condition
        if (start + len(results)) >= total:
            break

        # next page
        start += page_size

def main():
    today = datetime.date.today().isoformat()

    token = get_token()

    # v1: Paris département 75, query "data engineer"
    mots_cles = os.getenv("FT_QUERY", "data engineer")
    departement = os.getenv("FT_DEPARTEMENT", "75")

    filename = f"offres_paris_{today}.jsonl"
    s3_key = f"{PREFIX}/dt={today}/{filename}"

    count = 0
    with open(filename, "w", encoding="utf-8") as f:
        for offer in fetch_all_offers(token, mots_cles=mots_cles, departement=departement):
            f.write(json.dumps(offer, ensure_ascii=False) + "\n")
            count += 1

    print("Saved locally:", filename, "rows:", count)

    s3 = boto3.client("s3")
    s3.upload_file(filename, BUCKET, s3_key)
    print("Uploaded to S3:", f"s3://{BUCKET}/{s3_key}")

if __name__ == "__main__":
    main()