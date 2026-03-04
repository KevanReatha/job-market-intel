import os
import requests
from dotenv import load_dotenv

load_dotenv()

TOKEN_URL = "https://entreprise.francetravail.fr/connexion/oauth2/access_token?realm=/partenaire"
SCOPE = "api_offresdemploiv2"

CLIENT_ID = os.getenv("FT_CLIENT_ID")
CLIENT_SECRET = os.getenv("FT_CLIENT_SECRET")

def get_token() -> str:
    r = requests.post(
        TOKEN_URL,
        data={
            "grant_type": "client_credentials",
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "scope": SCOPE,
        },
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=30,
    )
    r.raise_for_status()
    return r.json()["access_token"]

def search_offers(token: str):
    # Endpoint widely referenced for Offres d'emploi v2
    url = "https://api.francetravail.io/partenaire/offresdemploi/v2/offres/search"

    params = {
        "motsCles": "data engineer",
        "departement": "75",   # Paris
        "range": "0-9",        # first 10 results (common pattern in FT APIs)
    }

    r = requests.get(url, headers={"Authorization": f"Bearer {token}"}, params=params, timeout=30)
    
    print("Status:", r.status_code)
    print("Content-Type:", r.headers.get("Content-Type"))
    print("Body preview:\n", r.text[:800])   # <-- IMPORTANT

    data = r.json()
    print("Top-level keys:", list(data.keys()))
    # Usually results are in a list field; let's print a small sample safely
    results = None
    for k in ["resultats", "results", "offres"]:
        if k in data:
            results = data[k]
            print("Results field:", k, "count:", len(results) if isinstance(results, list) else type(results))
            break
    if isinstance(results, list) and results:
        sample = results[0]
        print("Sample keys:", list(sample.keys()))
        # try common id fields
        offer_id = sample.get("id") or sample.get("idOffre") or sample.get("identifiant")
        print("Sample offer id:", offer_id)
        # description field names vary; print lengths if present
        for dk in ["description", "descriptionEntreprise", "descriptif", "texte"]:
            if dk in sample and isinstance(sample[dk], str):
                print(dk, "len:", len(sample[dk]))
    return data

if __name__ == "__main__":
    token = get_token()
    search_offers(token)
