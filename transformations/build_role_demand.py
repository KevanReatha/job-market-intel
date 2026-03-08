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

    analytics_key = f"analytics/role_demand/v1/dt={dt}/role_demand_{dt}.parquet"
    local_output = f"/tmp/role_demand_{dt}.parquet"

    print("Downloading:", f"s3://{bucket}/{stage_key}")
    s3_download(bucket, stage_key, local_stage)

    df = pd.read_parquet(local_stage)

    print("Stage rows:", len(df))

    df = df[df["role_family"].notna()]
    df = df[df["role_family"] != "other"]

    print("Rows after role filter:", len(df))

    role_demand = (
        df.groupby("role_family")
        .size()
        .reset_index(name="demand")
        .sort_values("demand", ascending=False)
    )

    print("\n[ROLE DEMAND SUMMARY]")
    print("unique_roles:", role_demand["role_family"].nunique())

    print("\nTop roles:")
    print(role_demand.to_string(index=False))

    role_demand.to_parquet(local_output, index=False)

    print("\nSaved locally:", local_output)

    print("Uploading:", f"s3://{bucket}/{analytics_key}")
    s3_upload(bucket, analytics_key, local_output)

    print("Done.")


if __name__ == "__main__":
    main()