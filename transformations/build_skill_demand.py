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

    analytics_key = f"analytics/skill_demand/v1/dt={dt}/skill_demand_{dt}.parquet"
    local_output = f"/tmp/skill_demand_{dt}.parquet"

    print("Downloading:", f"s3://{bucket}/{stage_key}")
    s3_download(bucket, stage_key, local_stage)

    df = pd.read_parquet(local_stage)

    print("Stage rows:", len(df))

    # remove empty skills
    df = df[df["skills"].map(len) > 0]

    print("Rows with skills:", len(df))

    df = df.explode("skills")

    print("Exploded rows:", len(df))

    skill_demand = (
        df.groupby("skills")
        .size()
        .reset_index(name="demand")
        .rename(columns={"skills": "skill"})
        .sort_values("demand", ascending=False)
    )

    print("\n[SKILL DEMAND SUMMARY]")
    print("unique_skills:", skill_demand["skill"].nunique())

    print("\nTop skills:")
    print(skill_demand.head(20).to_string(index=False))

    skill_demand.to_parquet(local_output, index=False)

    print("\nSaved locally:", local_output)

    print("Uploading:", f"s3://{bucket}/{analytics_key}")
    s3_upload(bucket, analytics_key, local_output)

    print("Done.")

if __name__ == "__main__":
    main()