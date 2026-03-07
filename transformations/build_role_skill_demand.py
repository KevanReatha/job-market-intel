import os
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


def main():
    bucket = os.getenv("S3_BUCKET", "job-market-intel-kevan")
    dt = os.getenv("DT", datetime.date.today().isoformat())

    stage_key = f"stage/francetravail/offres/v2/dt={dt}/offres_tech_france_{dt}.parquet"
    local_stage = f"/tmp/offres_tech_france_{dt}.parquet"

    analytics_key = f"analytics/role_skill_demand/v1/dt={dt}/role_skill_demand_{dt}.parquet"
    local_analytics = f"/tmp/role_skill_demand_{dt}.parquet"

    print("Downloading:", f"s3://{bucket}/{stage_key}")
    s3_download(bucket, stage_key, local_stage)

    df = pd.read_parquet(local_stage)
    print("Stage rows:", len(df))

    # Keep only high-confidence classified jobs
    df = df[
        df["role_family"].notna()
        & (~df["role_family"].isin(["other"]))
    ].copy()

    print("Rows after role filter:", len(df))

    # Keep only rows with non-empty skills
    def has_non_empty_skills(x) -> bool:
        if x is None:
            return False
        try:
            return len(x) > 0
        except TypeError:
            return False
    
    df = df[df["skills"].apply(has_non_empty_skills)].copy()
    print("Rows with non-empty skills:", len(df))

    # Normalize skills values to Python lists
    def normalize_skills(x):
        if x is None:
            return []
        try:
            return list(x)
        except TypeError:
            return []

    df["skills"] = df["skills"].apply(normalize_skills)

    # Explode skills
    df_exploded = df.explode("skills").rename(columns={"skills": "skill"})
    df_exploded = df_exploded[df_exploded["skill"].notna()].copy()

    print("Exploded rows:", len(df_exploded))

    # Aggregate
    role_skill_demand = (
        df_exploded.groupby(["role_family", "skill"], as_index=False)
        .agg(
            demand=("offer_id", "nunique")
        )
        .sort_values(["demand", "role_family", "skill"], ascending=[False, True, True])
    )

    print("Aggregated rows:", len(role_skill_demand))

    print("\n[ROLE_SKILL_DEMAND SUMMARY]")
    print("unique_roles:", role_skill_demand["role_family"].nunique())
    print("unique_skills:", role_skill_demand["skill"].nunique())
    
    print("\nTOP SKILLS BY ROLE")
    for role in sorted(role_skill_demand["role_family"].unique()):
        top_role = (
            role_skill_demand[role_skill_demand["role_family"] == role]
            .sort_values(["demand", "skill"], ascending=[False, True])
            .head(5)
        )
        print(f"\n[{role}]")
        print(top_role.to_string(index=False))

    print("\ntop_role_skill_pairs:")
    print(role_skill_demand.head(20).to_string(index=False))

    role_skill_demand.to_parquet(local_analytics, index=False)
    print("\nSaved locally:", local_analytics)

    print("Uploading:", f"s3://{bucket}/{analytics_key}")
    s3_upload(bucket, analytics_key, local_analytics)
    print("Done.")


if __name__ == "__main__":
    main()