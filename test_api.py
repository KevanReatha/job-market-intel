import os
import requests
from dotenv import load_dotenv

load_dotenv()

APP_ID = os.getenv("ADZUNA_APP_ID")
APP_KEY = os.getenv("ADZUNA_APP_KEY")

url = "https://api.adzuna.com/v1/api/jobs/au/search/1"

params = {
    "app_id": APP_ID,
    "app_key": APP_KEY,
    "results_per_page": 5,
    "what": "data engineer",
    "content-type": "application/json"
}

response = requests.get(url, params=params)

print("Status:", response.status_code)

data = response.json()

print("Keys in response:", data.keys())

if "results" in data:
    print("Number of results:", len(data["results"]))
    print("Sample job keys:", data["results"][0].keys())

print("Total count:", data["count"])

sample = data["results"][0]

print("\nJob ID:", sample["id"])
print("Created:", sample["created"])
print("Title:", sample["title"])

desc = sample["description"]
print("Description length:", len(desc))
print("Description preview:\n", desc[:300])

print("\nSalary predicted?:", sample.get("salary_is_predicted"))
print("Salary min:", sample.get("salary_min"))
print("Salary max:", sample.get("salary_max"))
