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

    analytics_key = (
        f"analytics/role_demand_by_domain_focus/v1/dt={dt}/"
        f"role_demand_by_domain_focus_{dt}.parquet"
    )
    local_output = f"/tmp/role_demand_by_domain_focus_{dt}.parquet"

    print("Downloading:", f"s3://{bucket}/{stage_key}")
    s3_download(bucket, stage_key, local_stage)

    df = pd.read_parquet(local_stage)

    print("Stage rows:", len(df))

    df = df[df["role_family"].notna()]
    df = df[df["role_family"] != "other"]
    df = df[df["domain_focus"].notna()]
    df = df[df["domain_focus"] != "unknown"]

    print("Rows after filters:", len(df))

    result = (
        df.groupby(["role_family", "domain_focus"])
        .size()
        .reset_index(name="demand")
        .sort_values(["role_family", "demand"], ascending=[True, False])
    )

    print("\n[ROLE DEMAND BY DOMAIN FOCUS SUMMARY]")
    print("rows:", len(result))
    print(result.head(20).to_string(index=False))

    result.to_parquet(local_output, index=False)

    print("\nSaved locally:", local_output)
    print("Uploading:", f"s3://{bucket}/{analytics_key}")
    s3_upload(bucket, analytics_key, local_output)
    print("Done.")


if __name__ == "__main__":
    main()
    