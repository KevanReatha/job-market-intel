import requests
import datetime
import json
import boto3
import os
from requests.exceptions import JSONDecodeError

BUCKET = "job-market-intel-kevan"
PREFIX = "raw/francetravail/offres/v2"

SEARCH_KEYWORDS = [
    # DATA ENGINEERING
    "data engineer",
    "ingénieur data",

    # DATA SCIENCE / AI
    "data scientist",
    "machine learning engineer",
    "ingénieur machine learning",
    "ml engineer",
    "ai engineer",
    "ingénieur ia",

    # ANALYTICS / BI
    "data analyst",
    "analyste data",
    "bi analyst",
    "analyste bi",
    "bi developer",
    "développeur bi",
    "business intelligence",
    "data visualization",
    "data visualisation",

    # SOFTWARE
    "backend engineer",
    "backend developer",
    "ingénieur backend",
    "développeur backend",
    "software engineer",
    "software developer",
    "ingénieur logiciel",
    "développeur logiciel",

    # ANALYTICS ENGINEERING
    "analytics engineer",
]

def get_token():
    url = "https://entreprise.francetravail.fr/connexion/oauth2/access_token?realm=/partenaire"
    data = {
        "grant_type": "client_credentials",
        "client_id": os.getenv("FT_CLIENT_ID"),
        "client_secret": os.getenv("FT_CLIENT_SECRET"),
        "scope": "api_offresdemploiv2 o2dsoffre",
    }
    r = requests.post(url, data=data, timeout=30)
    r.raise_for_status()
    return r.json()["access_token"]


def parse_total_from_content_range(content_range: str) -> int:
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

    page_size = 150
    start = 0
    total = None

    while True:
        end = start + page_size - 1
        params = {
            "motsCles": mots_cles,
            "range": f"{start}-{end}",
            "sort": sort,
        }

        if departement:
            params["departement"] = departement
            
        r = requests.get(url, headers=headers, params=params, timeout=30)

        if r.status_code >= 400:
            print("ERROR status:", r.status_code)
            print("Keyword:", mots_cles)
            print("WWW-Authenticate:", r.headers.get("Www-Authenticate"))
            print("Body preview:", repr(r.text[:300]))
            r.raise_for_status()

        if not r.text.strip():
            print(f"WARNING empty response body for keyword='{mots_cles}' range {start}-{end}")
            break

        try:
            data = r.json()
        except JSONDecodeError:
            print(f"WARNING non-JSON response for keyword='{mots_cles}' range {start}-{end}")
            print("Status:", r.status_code)
            print("Body preview:", repr(r.text[:300]))
            break

        results = data.get("resultats", [])

        content_range = r.headers.get("Content-Range")
        if total is None:
            total = parse_total_from_content_range(content_range)
            print(f"Total offers for keyword '{mots_cles}':", total)

        print(f"Fetched keyword='{mots_cles}' range {start}-{end} -> {len(results)} results")

        for item in results:
            yield item

        if (start + len(results)) >= total:
            break

        start += page_size


def main():
    today = datetime.date.today().isoformat()
    token = get_token()

    departement = os.getenv("FT_DEPARTEMENT")

    local_filename = f"/tmp/offres_tech_france_{today}.jsonl"
    s3_filename = f"offres_tech_france_{today}.jsonl"
    s3_key = f"{PREFIX}/dt={today}/{s3_filename}"

    total_count = 0
    keyword_counts = {}

    with open(local_filename, "w", encoding="utf-8") as f:
        for keyword in SEARCH_KEYWORDS:
            print(f"\n=== Fetching keyword: {keyword} ===")
            keyword_count = 0

            for offer in fetch_all_offers(token, mots_cles=keyword, departement=departement):
                offer["search_keyword"] = keyword
                f.write(json.dumps(offer, ensure_ascii=False) + "\n")
                keyword_count += 1
                total_count += 1

            keyword_counts[keyword] = keyword_count
            print(f"Finished keyword='{keyword}' -> rows={keyword_count}")

    print("\n=== INGESTION SUMMARY ===")
    print("Saved locally:", local_filename)
    print("Total rows written:", total_count)
    print("Rows by keyword:")
    for keyword, count in keyword_counts.items():
        print(f"  {keyword}: {count}")

    s3 = boto3.client("s3")
    s3.upload_file(local_filename, BUCKET, s3_key)
    print("Uploaded to S3:", f"s3://{BUCKET}/{s3_key}")

    os.remove(local_filename)
    print("Removed local temp file:", local_filename)


if __name__ == "__main__":
    main()