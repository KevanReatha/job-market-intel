import os
import json
import datetime
import boto3
import pandas as pd
import html
import re
from pathlib import Path
import unicodedata

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

def load_enrichment_rules(path="config/enrichment_rules.json"):
    with open(path, "r") as f:
        return json.load(f)

RULES = load_enrichment_rules()

def normalize_text(text: str | None) -> str:
    if not text:
        return ""

    text = text.lower()
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("utf-8")
    text = re.sub(r"\s+", " ", text).strip()

    return text


def keyword_match(text: str, keyword: str) -> bool:
    text_norm = normalize_text(text)
    kw_norm = normalize_text(keyword)

    pattern = r"\b" + re.escape(kw_norm) + r"\b"
    return re.search(pattern, text_norm) is not None


def score_categories(text: str | None, rules_dict: dict) -> dict:
    text_norm = normalize_text(text)

    if not text_norm:
        return {}

    scores = {}

    for category, keywords in rules_dict.items():
        score = 0

        for kw in keywords:
            if keyword_match(text_norm, kw):
                score += 1

        if score > 0:
            scores[category] = score

    return scores


def infer_category(text: str | None, rules_dict: dict, multi_sector_enabled: bool = False) -> str:
    scores = score_categories(text, rules_dict)

    if not scores:
        return "unknown"

    sorted_scores = sorted(scores.items(), key=lambda x: x[1], reverse=True)
    top_category, top_score = sorted_scores[0]

    if multi_sector_enabled and len(sorted_scores) > 1:
        second_category, second_score = sorted_scores[1]

        if top_score >= 2 and second_score >= 2 and (top_score - second_score) <= 1:
            return "multi_sector"

    return top_category


def infer_hiring_industry(description: str | None) -> str:
    return infer_category(
        description,
        RULES["hiring_industry"],
        multi_sector_enabled=True
    )


def infer_seniority(title: str | None, description: str | None) -> str:

    # Priority 1: title
    title_result = infer_category(title, RULES["seniority_level"])

    if title_result != "unknown":
        return title_result

    # Priority 2: description (restricted)
    desc_result = infer_category(description, RULES["seniority_level"])

    if desc_result in ["junior", "intern_apprentice", "mid", "senior"]:
        return desc_result

    return "unknown"

def infer_domain_focus(title: str | None, description: str | None) -> str:
    title_scores = score_categories(title, RULES["domain_focus"])
    description_scores = score_categories(description, RULES["domain_focus"])

    combined_scores = {}

    for category, score in description_scores.items():
        combined_scores[category] = combined_scores.get(category, 0) + score

    for category, score in title_scores.items():
        # title gets stronger weight
        combined_scores[category] = combined_scores.get(category, 0) + (score * 2)

    if not combined_scores:
        return "unknown"

    return max(combined_scores, key=combined_scores.get)
        
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

# -----------------------------
    # 1. Out-of-scope / irrelevant jobs
    # -----------------------------
    out_of_scope_patterns = [
        "data center",
        "géotechnicien",
        "geotechnicien",
        "nucleaire",
        "nucléaire",
        "dessinateur projeteur",
        "hardware",
        "bâtiment",
        "batiment",
        "poste de travail",
        "robotique industrielle",
        "technicien eia",
        "planificateur nucléaire",
        "expert vol",
        "génomique",
        "genomique",
        "géotech",
        "geotech",
    ]

    if any(p in t for p in out_of_scope_patterns):
        return "other"

    # vague / non-informative titles
    if t in {"ide", "cdi ide", "idec", "expert"}:
        return "other"

    # -----------------------------
    # 2. Strong patterns: AI / ML
    # -----------------------------
    if any(p in t for p in [
        "machine learning engineer",
        "ml engineer",
        "ingénieur machine learning",
        "ingenieur machine learning",
        "devops machine learning",
    ]):
        return "machine_learning_engineer"

    if any(p in t for p in [
        "ai engineer",
        "ia engineer",
        "ingénieur ia",
        "ingenieur ia",
        "développeur intelligence artificielle",
        "developpeur intelligence artificielle",
        "développeur ia",
        "developpeur ia",
        "spécialiste ia",
        "specialiste ia",
        "datascientist expert en ia",
        "consultant data / ia",
        "alternance ingénieur en intelligence artificielle",
        "alternance ingenieur en intelligence artificielle",
        "alternance développeur ia",
        "alternance developpeur ia",
        "prompt engineering",
        "genai",
        "llm",
        "agentique",
        "data & ai",
        "intelligence artificielle",
    ]):
        return "ai_engineer"

    # -----------------------------
    # 3. Strong patterns: DevOps / Cloud
    # -----------------------------
    if any(p in t for p in [
        "ingénieur devops",
        "ingenieur devops",
        "devops engineer",
        "devops microsoft",
        "ingénieur.e d'études devops",
        "ingenieur.e d'études devops",
        "ingénieur build cloud",
        "ingenieur build cloud",
        "cloud public",
        "devops plateforme ia",
        "exploitation mainframe",
        "systèmes linux",
        "systemes linux",
        "intégrateur systèmes linux",
        "integrateur systemes linux",
    ]):
        return "devops_engineer"

    # -----------------------------
    # 4. Strong patterns: analytics / data
    # -----------------------------
    if "analytics engineer" in t:
        return "analytics_engineer"

    if "data scientist" in t:
        return "data_scientist"

    if any(p in t for p in [
        "data analyst",
        "analyste data",
        "analyste de données",
        "analyste de donnees",
        "analyste de donnée",
        "analyste de donnee",
        "web analyst",
    ]):
        return "data_analyst"

    if any(p in t for p in [
        "business intelligence",
        "bi analyst",
        "analyste bi",
        "bi developer",
        "développeur bi",
        "developpeur bi",
        "consultant bi",
        "data visualisation",
        "data visualization",
        "power bi",
        "sap bo",
    ]):
        return "bi_analytics"

    if any(p in t for p in [
        "data engineer",
        "data - engineer",
        "data ingénieur",
        "ingenieur data",
        "ingénieur data",
        "tech lead data",
        "manager data factory",
        "développeur etl",
        "developpeur etl",
        "mdm",
        "ingénieur opex data",
        "ingenieur opex data",
        "analyste production informatique data",
    ]):
        return "data_engineer"

    if any(p in t for p in [
        "data gouvernance",
        "data governance",
        "data quality",
        "master data",
        "data steward",
        "contrôleur de données",
        "controleur de donnees",
        "protect data",
    ]):
        return "data_governance"

    if any(p in t for p in [
        "architecte data",
        "data architect",
        "data platform",
        "platform engineer",
    ]):
        return "data_architect"

    # -----------------------------
    # 5. Backend engineering
    # -----------------------------
    if any(p in t for p in [
        "backend engineer",
        "backend developer",
        "ingénieur backend",
        "ingenieur backend",
        "développeur backend",
        "developpeur backend",
        "java backend",
    ]):
        return "backend_engineer"

    # -----------------------------
    # 6. Software architect
    # -----------------------------
    if any(p in t for p in [
        "architecte logiciel",
        "architecte logiciels",
        "architecte solution",
        "architecte solutions",
        "architecte fonctionnel",
        "architecte apps",
        "architecte applicatif",
    ]):
        return "software_architect"

    # -----------------------------
    # 7. Software / application engineering
    # -----------------------------
    if any(p in t for p in [
        "software engineer",
        "software developer",
        "ingénieur logiciel",
        "ingenieur logiciel",
        "ingénieur software",
        "ingenieur software",
        "ingénieur développement logiciel",
        "ingenieur développement logiciel",
        "ingénieur / ingénieure développement logiciel",
        "ingénieur de développement logiciel",
        "ingénieur / ingénieure d'étude logiciel informatique",
        "développeur logiciel",
        "developpeur logiciel",
        "concepteur logiciel",
        "concepteur développeur d'applications",
        "concepteur developpeur d'applications",
        "concepteur / conceptrice d'application informatique",
        "concepteur / conceptrice logiciel informatique",
        "développeur / développeuse d'application",
        "developpeur / developpeuse d'application",
        "développeur / développeuse logiciel ou d'application",
        "developpeur / developpeuse logiciel ou d'application",
        "développeur informatique",
        "developpeur informatique",
        "developpeur applicatif",
        "informaticien / informaticienne d'application",
        "programmeur",
        "lead developer",
        "développeur java",
        "developpeur java",
        "développeur .net",
        "developpeur .net",
        "développeur c++",
        "developpeur c++",
        "développeur drupal",
        "developpeur drupal",
        "développeur windev",
        "developpeur windev",
        "développeur expérimenté",
        "developpeur expérimenté",
        "développeur fullstack",
        "developpeur fullstack",
        "tech lead java",
        "responsable développement - java",
        "responsable developpement - java",
        "chef de projet logiciel",
        "gestionnaire applicatif",
        "responsable activité logiciel",
        "responsable activite logiciel",
        "responsable d'application",
        "responsable d'application informatique",
        "logiciel embarqué",
        "logiciel embarque",
        "expert logiciel embarqué",
        "expert logiciel embarque",
        "qualification et validation logiciel",
        "qualité logiciel",
        "qualite logiciel",
        "responsable technique logiciel embarqué",
        "responsable technique logiciel embarque",
        "ingénieur qualité logiciel",
        "ingenieur qualite logiciel",
    ]):
        return "software_engineer"

    # -----------------------------
    # 8. Management / projects
    # -----------------------------
    if any(p in t for p in [
        "head of data",
        "data manager",
        "manager data optimisation",
        "manager data optimization",
        "manager data & ai",
    ]):
        return "data_management"

    if "chef de projet data" in t:
        return "data_project"

    # -----------------------------
    # 9. Contextual fallback rules
    # -----------------------------
    if "analyste" in t and ("donnée" in t or "donnee" in t or "donnees" in t or "données" in t):
        return "data_analyst"

    if ("ingénieur" in t or "ingenieur" in t) and ("logiciel" in t or "software" in t):
        return "software_engineer"

    if ("développeur" in t or "developpeur" in t) and ("java" in t or ".net" in t or "c++" in t):
        return "software_engineer"

    if ("devops" in t or "cloud" in t) and ("ingénieur" in t or "ingenieur" in t or "architecte" in t):
        return "devops_engineer"

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
    company_name_clean = clean_company(company)

    description = o.get("description")
    description_clean = clean_text_basic(description)

    skills = extract_skills(description_clean, skills_dict)

    hiring_industry = infer_hiring_industry(description_clean)
    seniority_level = infer_seniority(title_clean, description_clean)
    domain_focus = infer_domain_focus(title_clean, description_clean)

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
        "company_name_clean": company_name_clean,
        "contract_type": o.get("typeContrat"),
        "contract_label": o.get("typeContratLibelle"),
        "salary_label": salaire.get("libelle") or salaire.get("commentaire"),
        "description": description,
        "description_clean": description_clean,
        "skills": skills,
        "hiring_industry": hiring_industry,
        "seniority_level": seniority_level,
        "domain_focus": domain_focus,
        "search_keyword": o.get("search_keyword"),
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

    raw_key = f"raw/francetravail/offres/v2/dt={dt}/offres_tech_france_{dt}.jsonl"
    local_raw = f"/tmp/offres_tech_france_{dt}.jsonl"

    # dt stays in the S3 path only (partition), not in parquet columns
    stage_key = f"stage/francetravail/offres/v2/dt={dt}/offres_tech_france_{dt}.parquet"
    local_parquet = f"/tmp/offres_tech_france_{dt}.parquet"

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

    raw_rows = len(df)
    df = df.drop_duplicates(subset=["offer_id"]).copy()
    deduped_rows = len(df)
    duplicates_removed = raw_rows - deduped_rows

    print("Rows raw:", raw_rows, "Cols:", len(df.columns))
    print("Rows deduped:", deduped_rows)
    print("Duplicates removed:", duplicates_removed)

    run_quality_checks(df, dt)

    print_dataset_summary(df)
    
    df.to_parquet(local_parquet, index=False)
    print("Saved parquet locally:", local_parquet)

    print("Uploading:", f"s3://{bucket}/{stage_key}")
    s3_upload(bucket, stage_key, local_parquet)
    print("Done.")
    
    print(
    df[[
        "title_clean",
        "hiring_industry",
        "seniority_level",
        "domain_focus"
    ]]
    .head(15)
    .to_string(index=False)
    )
    
    print("\nCoverage check\n")

    print("industry:")
    print(df["hiring_industry"].value_counts(dropna=False))

    print("\nseniority:")
    print(df["seniority_level"].value_counts(dropna=False))

    print("\ndomain focus:")
    print(df["domain_focus"].value_counts(dropna=False))


if __name__ == "__main__":
    main()