import os
import json
import datetime
import boto3
import pandas as pd
import html
import re


def s3_download(bucket: str, key: str, local_path: str) -> None:
    s3 = boto3.client("s3")
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    s3.download_file(bucket, key, local_path)


def s3_upload(bucket: str, key: str, local_path: str) -> None:
    s3 = boto3.client("s3")
    s3.upload_file(local_path, bucket, key)


def decode_html_recursive(text: str) -> str:
    previous = None
    while previous != text:
        previous = text
        text = html.unescape(text)
    return text


def clean_text_basic(text: str | None) -> str | None:
    if text is None:
        return None

    text = decode_html_recursive(text)
    text = text.replace("\xa0", " ")

    # Convert common HTML breaks / paragraphs
    text = re.sub(r"(?i)<br\s*/?>", "\n", text)
    text = re.sub(r"(?i)</p\s*>", "\n", text)
    text = re.sub(r"(?i)<p\s*>", "", text)

    # Remove remaining HTML tags
    text = re.sub(r"<[^>]+>", " ", text)

    # Replace common bad separators / artefacts
    text = text.replace("***", "\n")
    text = text.replace("•", "\n- ")
    text = text.replace("¿", " ")
    text = text.replace("…", "...")
    text = text.replace(" ", " ")

    # Add spacing when text is glued after punctuation / parentheses
    text = re.sub(r"([.:;!?])([A-ZÀ-ÖØ-Þ])", r"\1 \2", text)
    text = re.sub(r"(\))([A-ZÀ-ÖØ-Þ])", r"\1 \2", text)

    # Add line breaks before common section headers
    text = re.sub(r"(Description du poste\s*:?)", r"\n\1", text)
    text = re.sub(r"(Description du profil\s*:?)", r"\n\1", text)
    text = re.sub(r"(Profil recherché\s*:?)", r"\n\1", text)
    text = re.sub(r"(Vos missions\s*:?)", r"\n\1", text)
    text = re.sub(r"(Compétences\s*:?)", r"\n\1", text)
    text = re.sub(r"(Missions\s*:?)", r"\n\1", text)
    text = re.sub(r"(Contexte\s*:?)", r"\n\1", text)
    text = re.sub(r"(Fonctions et responsabilités\s*:?)", r"\n\1", text)
    text = re.sub(r"(Qualités requises\s*:?)", r"\n\1", text)

    # Normalize spaces
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)

    return text.strip()


def clean_title(text: str | None) -> str | None:
    if text is None:
        return None

    text = decode_html_recursive(text)
    text = text.replace("\xa0", " ")
    text = text.lower()

    # Remove common gender markers
    text = re.sub(r"\b(h\/f|f\/h)\b", "", text)
    text = re.sub(r"\((h\/f|f\/h)\)", "", text, flags=re.IGNORECASE)

    # Normalize f/m/d style markers
    text = re.sub(r"\(\s*f\s*/\s*m\s*/\s*d\s*\)", "(f/m/d)", text, flags=re.IGNORECASE)

    # Remove empty parentheses left behind
    text = re.sub(r"\(\s*\)", "", text)

    # Normalize spacing around separators
    text = re.sub(r"\s*-\s*", " - ", text)
    text = re.sub(r"\s*/\s*", " / ", text)

    # Collapse repeated separators
    text = re.sub(r"( - ){2,}", " - ", text)

    # Normalize spaces
    text = re.sub(r"\s+", " ", text)

    # Remove trailing light punctuation
    text = re.sub(r"[.,;:]+$", "", text).strip()

    return text if text != "" else None


def clean_company(text: str | None) -> str | None:
    if text is None:
        return None

    text = decode_html_recursive(text)
    text = text.replace("\xa0", " ")
    text = re.sub(r"\s+", " ", text).strip()

    if text == "":
        return None

    # Remove only light trailing punctuation
    text = re.sub(r"[.,;:]+$", "", text).strip()

    if text == "":
        return None

    return text


def flatten_offer(o: dict) -> dict:
    lieu = o.get("lieuTravail") or {}
    ent = o.get("entreprise") or {}
    salaire = o.get("salaire") or {}

    title = o.get("intitule")
    company = ent.get("nom")
    description = o.get("description")

    return {
        "offer_id": o.get("id"),
        "title": title,
        "title_clean": clean_title(title),
        "rome_code": o.get("romeCode"),
        "rome_label": o.get("romeLibelle"),
        "created_at": o.get("dateCreation"),
        "updated_at": o.get("dateActualisation"),
        "department_or_location": lieu.get("libelle"),
        "company_name": company,
        "company_name_clean": clean_company(company),
        "contract_type": o.get("typeContrat"),
        "contract_label": o.get("typeContratLibelle"),
        "salary_label": salaire.get("libelle") or salaire.get("commentaire"),
        "description": description,
        "description_clean": clean_text_basic(description),
        "source": "francetravail",
    }


def main():
    bucket = os.getenv("S3_BUCKET", "job-market-intel-kevan")
    dt = os.getenv("DT", datetime.date.today().isoformat())

    raw_key = f"raw/francetravail/offres/v2/dt={dt}/offres_paris_{dt}.jsonl"
    local_raw = f"/tmp/offres_{dt}.jsonl"

    # dt stays in the S3 path only (partition), not in parquet columns
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
            rows.append(flatten_offer(o))

    df = pd.DataFrame(rows)
    print("Rows:", len(df), "Cols:", len(df.columns))

    df.to_parquet(local_parquet, index=False)
    print("Saved parquet locally:", local_parquet)

    print("Uploading:", f"s3://{bucket}/{stage_key}")
    s3_upload(bucket, stage_key, local_parquet)
    print("Done.")


if __name__ == "__main__":
    main()