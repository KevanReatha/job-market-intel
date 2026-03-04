import os
from offres_emploi import Api

CLIENT_ID = os.getenv("FT_CLIENT_ID")
CLIENT_SECRET = os.getenv("FT_CLIENT_SECRET")

client = Api(client_id=CLIENT_ID, client_secret=CLIENT_SECRET)

# Test minimal (récupère les dernières offres)
data = client.search()

print("Top keys:", data.keys())
print("Content-Range:", data.get("Content-Range"))  # utile pour total results
resultats = data.get("resultats", [])
print("Nb resultats:", len(resultats))

if resultats:
    sample = resultats[0]
    print("Sample keys:", sample.keys())
    # teste la “description” / “descriptif” selon le schéma
    for k in ["description", "descriptif", "texte"]:
        if k in sample and isinstance(sample[k], str):
            print(k, "len:", len(sample[k]))
            print("preview:", sample[k][:300])
            break
