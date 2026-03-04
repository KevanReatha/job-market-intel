import os
import json
import datetime
import boto3
import pandas as pd

def s3_download(bucket: str, key: str, local_path: str) -> None:
    s3 = boto3.client("s3")
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    s3.download_file(bucket, key, local_path)

def s3_upload(bucket: str, key: str, local_path: str) -> None:
    s3 = boto3.client("s3")
    s3.upload_file(local_path, bucket, key)

def flatten_offer(o: dict, dt: str) -> dict:
    lieu = o.get("lieuTravail") or {}
    ent = o.get("entreprise") or {}
    salaire = o.get("salaire") or {}
    return {
        "offer_id": o.get("id"),
        "title": o.get("intitule"),
        "rome_code": o.get("romeCode"),
        "rome_label": o.get("romeLibelle"),
        "created_at": o.get("dateCreation"),
        "updated_at": o.get("dateActualisation"),
        "department_or_location": lieu.get("libelle"),
        "company_name": ent.get("nom"),
        "contract_type": o.get("typeContrat"),
        "contract_label": o.get("typeContratLibelle"),
        "salary_label": salaire.get("libelle") or salaire.get("commentaire"),
        "description": o.get("description"),
        "dt": dt,
        "source": "francetravail",
    }

def main():
    bucket = os.getenv("S3_BUCKET", "job-market-intel-kevan")
    dt = os.getenv("DT", datetime.date.today().isoformat())

    raw_key = f"raw/francetravail/offres/v2/dt={dt}/offres_paris_{dt}.jsonl"
    local_raw = f"/tmp/offres_{dt}.jsonl"

    stage_key = f"stage/francetravail/offres/v2/dt={dt}/offres_paris_{dt}.parquet"
    local_parquet = f"/tmp/offres_{dt}.parquet"

    print("Downloading:", f"s3://{bucket}/{raw_key}")
    s3_download(bucket, raw_key, local_raw)

    rows = []
    with open(local_raw, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            o = json.loads(line)
            rows.append(flatten_offer(o, dt))

    df = pd.DataFrame(rows)
    print("Rows:", len(df), "Cols:", len(df.columns))

    df.to_parquet(local_parquet, index=False)
    print("Saved parquet locally:", local_parquet)

    print("Uploading:", f"s3://{bucket}/{stage_key}")
    s3_upload(bucket, stage_key, local_parquet)
    print("Done.")

if __name__ == "__main__":
    main()