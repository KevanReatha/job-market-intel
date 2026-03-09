import snowflake.connector

conn = snowflake.connector.connect(
    user="KEVANREATHA",  # ton username
    password="Eldjin78410291189",
    account="uqwfzun-sw73600",
    warehouse="JOB_MARKET_WH",
    database="JOB_MARKET_INTEL",
    schema="STAGING"
)

cur = conn.cursor()

cur.execute("SELECT CURRENT_VERSION()")

print("Snowflake version:")
print(cur.fetchone())

cur.close()
conn.close()