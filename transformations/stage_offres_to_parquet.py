import os
import json
import datetime
import boto3
import pandas as pd
import html
import re
from pathlib import Path

def s3_download(bucket: str, key: str, local_path: str) -> None:
    s3 = boto3.client("s3")
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    s3.download_file(bucket, key, local_path)


def s3_upload(bucket: str, key: str, local_path: str) -> None:
    s3 = boto3.client("s3")
    s3.upload_file(local_path, bucket, key)

def load_skills_dictionary():
    config_path = Path(__file__).resolve().parents[1] / "config" / "skills_dictionary.json"

    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)

    
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

def get_role_family(title_clean: str | None) -> str | None:
    if title_clean is None:
        return None

    t = title_clean.lower().strip()

    # False positives first
    if "data center" in t:
        return "false_positive"

    # Most specific roles first
    if "analytics engineer" in t:
        return "analytics_engineer"

    if "machine learning engineer" in t or "ml engineer" in t:
        return "ml_engineer"

    if "ai engineer" in t or "ia engineer" in t:
        return "ai_engineer"

    if "llm" in t or "genai" in t or "agentique" in t:
        return "ai_engineer"

    if "data scientist" in t:
        return "data_scientist"

    if "data engineer" in t or "data ingénieur" in t or "ingenieur data" in t or "ingénieur data" in t:
        return "data_engineer"

    if "architecte data" in t or "data architect" in t:
        return "data_architect"

    if "data platform" in t or "platform" in t:
        return "data_platform"

    if "chef de projet data" in t:
        return "data_project"

    if "data gouvernance" in t or "data governance" in t:
        return "data_governance"

    if "data quality" in t or "master data" in t:
        return "data_governance"

    if "data analyst" in t or "analyste data" in t:
        return "data_analyst"

    if "business intelligence" in t or "power bi" in t or "bi " in t or "& bi" in t or "/ bi" in t:
        return "bi_analytics"

    if "head of data" in t or "data manager" in t:
        return "data_management"

    return "other"


def extract_skills(text: str | None, skills_dict: dict) -> list[str]:
    if text is None:
        return []

    text = text.lower()

    found_skills = []

    for skill, patterns in skills_dict.items():
        for p in patterns:
            if re.search(p, text):
                found_skills.append(skill)
                break

    return sorted(set(found_skills))


def flatten_offer(o: dict, skills_dict: dict) -> dict:
    lieu = o.get("lieuTravail") or {}
    ent = o.get("entreprise") or {}
    salaire = o.get("salaire") or {}

    title = o.get("intitule")
    title_clean = clean_title(title)
    
    company = ent.get("nom")
    description = o.get("description")
    description_clean = clean_text_basic(description)
    skills = extract_skills(description_clean, skills_dict)
    
    return {
        "offer_id": o.get("id"),
        "title": title,
        "title_clean": title_clean,
        "role_family": get_role_family(title_clean),
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
        "description_clean": description_clean,
        "skills": skills,
        "source": "francetravail",
    }

def run_quality_checks(df: pd.DataFrame, dt: str) -> None:
    total_rows = len(df)

    if total_rows == 0:
        raise ValueError(f"[QUALITY] No rows produced for dt={dt}")

    title_clean_not_null = df["title_clean"].notna() & (df["title_clean"].str.strip() != "")
    title_clean_pct = round(100 * title_clean_not_null.sum() / total_rows, 2)

    relevant_role = df["role_family"].notna() & (~df["role_family"].isin(["other", "false_positive"]))
    relevant_role_pct = round(100 * relevant_role.sum() / total_rows, 2)

    company_not_null = df["company_name_clean"].notna() & (df["company_name_clean"].astype(str).str.strip() != "")
    company_pct = round(100 * company_not_null.sum() / total_rows, 2)

    print(f"[QUALITY] dt={dt}")
    print(f"[QUALITY] total_rows={total_rows}")
    print(f"[QUALITY] title_clean_populated_pct={title_clean_pct}")
    print(f"[QUALITY] relevant_role_family_pct={relevant_role_pct}")
    print(f"[QUALITY] company_name_clean_populated_pct={company_pct}")
    
def print_dataset_summary(df: pd.DataFrame) -> None:
    print("\n[SUMMARY]")

    total_offers = len(df)
    print(f"offers: {total_offers}")

    unique_roles = df["role_family"].nunique(dropna=True)
    print(f"unique_roles: {unique_roles}")

    # Flatten skills list
    all_skills = []
    for skills in df["skills"]:
        if skills:
            all_skills.extend(skills)

    unique_skills = len(set(all_skills))
    print(f"unique_skills: {unique_skills}")

    # Top roles
    print("\ntop_roles:")
    top_roles = df["role_family"].value_counts().head(5)
    for role, count in top_roles.items():
        print(f"  {role}: {count}")

    # Top skills
    print("\ntop_skills:")
    if all_skills:
        skill_counts = pd.Series(all_skills).value_counts().head(5)
        for skill, count in skill_counts.items():
            print(f"  {skill}: {count}")
            
            
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

    skills_dict = load_skills_dictionary()
    
    rows = []
    with open(local_raw, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            o = json.loads(line)
            rows.append(flatten_offer(o, skills_dict))

    df = pd.DataFrame(rows)
    print("Rows:", len(df), "Cols:", len(df.columns))

    run_quality_checks(df, dt)
    
    print_dataset_summary(df)
    
    df.to_parquet(local_parquet, index=False)
    print("Saved parquet locally:", local_parquet)

    print("Uploading:", f"s3://{bucket}/{stage_key}")
    s3_upload(bucket, stage_key, local_parquet)
    print("Done.")


if __name__ == "__main__":
    main()