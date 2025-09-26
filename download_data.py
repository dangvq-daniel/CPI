# download_data.py
import os
import pandas as pd
import psycopg2

csv_path = "cpi_long_with_location.csv"

if not os.path.exists(csv_path):
    print("CSV not found, downloading from Supabase...")

    user = "postgres"
    password = "ZWeAQRaKKzFyQKEo"   # ideally from secrets
    host = "db.rtewftvldajjhqjbwwfx.supabase.co"
    port = "5432"
    database = "postgres"

    # Connect to Postgres
    conn = psycopg2.connect(
        user=user,
        password=password,
        host=host,
        port=port,
        database=database
    )

    query = "SELECT * FROM cpi_long_with_location;"
    df = pd.read_sql(query, conn)
    conn.close()

    # Save locally
    df.to_csv(csv_path, index=False)
    print("Download complete!")
else:
    print("CSV already exists. Nothing to do.")
