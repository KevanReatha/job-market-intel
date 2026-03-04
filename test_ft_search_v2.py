import os
import requests
from dotenv import load_dotenv

load_dotenv()

CLIENT_ID = os.getenv("FT_CLIENT_ID")
CLIENT_SECRET = os.getenv("FT_CLIENT_SECRET")
SCOPE = os.getenv("FT_SCOPE", "api_offresdemploiv2")

TOKEN_URL = "https://entreprise.francetravail.fr/connexion/oauth2/access_token?realm=/partenaire"
SEARCH_URL = "https://api.francetravail.io/partenaire/offresdemploi/v2/offres/search"

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
    params = {
        "motsCles": "data engineer",
        "departement": "75",
        "range": "0-9",
        "sort": "1",           # date décroissante (optionnel)
        "origineOffre": "1,2", # FT + partenaires (optionnel)
    }

    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    }

    r = requests.get(SEARCH_URL, headers=headers, params=params, timeout=30)

    print("Status:", r.status_code)
    print("URL:", r.url)
    print("Content-Range:", r.headers.get("Content-Range"))
    print("Content-Type:", r.headers.get("Content-Type"))
    print("Body length:", len(r.text))

    if r.status_code != 200:
        # Important: afficher le message d’erreur même si ce n’est pas du JSON
        print("Body preview:", repr(r.text[:500]))
        return None

    data = r.json()
    print("Top-level keys:", list(data.keys()))
    results = data.get("resultats", [])
    print("Nb resultats:", len(results))

    if results:
        sample = results[0]
        print("\nSample keys:", list(sample.keys()))
        print("Sample id:", sample.get("id"))
        desc = sample.get("description") or ""
        print("Description length:", len(desc))
        print("Description preview:", desc[:400])

    return data

if __name__ == "__main__":
    token = get_token()
    search_offers(token)
