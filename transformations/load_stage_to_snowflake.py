import os
import datetime
import boto3
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas


S3_BUCKET = "job-market-intel-kevan"


def download_stage_parquet(dt: str) -> str:
    s3 = boto3.client("s3")

    key = f"stage/francetravail/offres/v2/dt={dt}/offres_tech_france_{dt}.parquet"
    local_path = f"/tmp/offres_tech_france_{dt}.parquet"

    print("Downloading:", f"s3://{S3_BUCKET}/{key}")

    s3.download_file(S3_BUCKET, key, local_path)

    return local_path


def connect_snowflake():
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse="JOB_MARKET_WH",
        database="JOB_MARKET_INTEL",
        schema="STAGING",
    )

    return conn


def load_dataframe_to_snowflake(df: pd.DataFrame, conn):

    print("Writing dataframe to Snowflake...")

    success, nchunks, nrows, _ = write_pandas(
        conn,
        df,
        table_name="STG_OFFRES",
        auto_create_table=True,
        overwrite=True,
    )

    print("Success:", success)
    print("Rows loaded:", nrows)


def main():

    dt = os.getenv("DT", datetime.date.today().isoformat())

    local_file = download_stage_parquet(dt)

    print("Reading parquet...")
    df = pd.read_parquet(local_file)

    print("Rows:", len(df))
    print("Columns:", len(df.columns))

    conn = connect_snowflake()

    load_dataframe_to_snowflake(df, conn)

    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM STG_OFFRES")

    print("Rows in Snowflake table:", cur.fetchone()[0])

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()