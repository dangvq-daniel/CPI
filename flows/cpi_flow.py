import requests
import zipfile
import pandas as pd
import numpy as np
import hashlib
from sqlalchemy import create_engine
from prefect import flow, task

# Replace with your actual Supabase Postgres credentials
user = "postgres"
host = "db.rtewftvldajjhqjbwwfx.supabase.co"
port = "5432"
database = "postgres"
password_path = './password'

with open(password_path, 'r') as f:
    password = f.readline()

# -----------------------------
# Utility: Column name encoder
# -----------------------------
def encode_col(name: str) -> str:
    hash_suffix = hashlib.md5(name.encode()).hexdigest()[:8]
    safe_name = ''.join(c if c.isalnum() else '_' for c in name)
    return (safe_name[:40] + '_' + hash_suffix).lower()

# -----------------------------
# Prefect Tasks
# -----------------------------
@task
def download_cpi(pid="18100004"):
    url = f"https://www150.statcan.gc.ca/t1/wds/rest/getFullTableDownloadCSV/{pid}/en"
    response = requests.get(url)
    data = response.json()

    if data.get("status") != "SUCCESS":
        raise Exception("Failed to fetch CPI data:", data)

    csv_url = data["object"]
    print("Downloading from:", csv_url)

    csv_data = requests.get(csv_url)
    with open("cpi.zip", "wb") as f:
        f.write(csv_data.content)
    return "cpi.zip"

@task
def extract_csv(zip_path):
    with zipfile.ZipFile(zip_path) as z:
        csv_file = [f for f in z.namelist() if f.endswith(".csv")][0]
        with z.open(csv_file) as f:
            df = pd.read_csv(f)
    return df

@task
def clean_data(df):
    df.replace('..', np.nan, inplace=True)

    if 'GEO' in df.columns:
        df['GEO'] = df['GEO'].ffill()

    df['REF_DATE'] = pd.to_datetime(df['REF_DATE'])
    df['VALUE'] = df['VALUE'].astype(float)

    return df

@task
def calculate_growth(df):
    df_wide = df.pivot_table(
        index=['REF_DATE', 'GEO', 'UOM'],
        columns='Products and product groups',
        values='VALUE'
    ).reset_index()

    categories = [c for c in df_wide.columns if c not in ['REF_DATE', 'GEO', 'UOM']]
    df_growth = df_wide.copy()

    growth_frames = []
    for cat in categories:
        mom = df_growth.groupby('GEO')[cat].pct_change() * 100
        yoy = df_growth.groupby('GEO')[cat].pct_change(12) * 100
        growth_frames.append(pd.DataFrame({
            f"{cat}_MoM": mom,
            f"{cat}_YoY": yoy
        }))

    df_growth = pd.concat([df_growth] + growth_frames, axis=1)
    df_growth.fillna(method='ffill', inplace=True)
    df_growth.fillna(method='bfill', inplace=True)

    return df_growth

@task
def reshape_long(df_growth):
    id_cols = ['REF_DATE', 'GEO', 'UOM']
    value_cols = [c for c in df_growth.columns if c not in id_cols]

    df_long = df_growth.melt(
        id_vars=id_cols,
        value_vars=value_cols,
        var_name="Product_Metric",
        value_name="Value"
    )

    df_long[['Product', 'Metric']] = df_long['Product_Metric'].str.rsplit('_', n=1, expand=True)
    df_long.drop(columns=['Product_Metric'], inplace=True)
    df_long['Metric'] = df_long['Metric'].fillna('Value')

    df_long = df_long[['REF_DATE', 'GEO', 'UOM', 'Product', 'Metric', 'Value']]
    return df_long

@task
def encode_and_map(df_long):
    mapping = {}
    df_long['Encoded_Product'] = df_long['Product'].apply(lambda x: encode_col(str(x)))
    for original, encoded in zip(df_long['Product'], df_long['Encoded_Product']):
        mapping[original] = encoded
    mapping_df = pd.DataFrame(list(mapping.items()), columns=['Original', 'Encoded'])
    return df_long, mapping_df

@task
def load_to_postgres(df_long, mapping_df):


    db_url = f"postgresql://{user}:{password}@{host}:{port}/{database}"
    engine = create_engine(db_url)

    df_long.to_sql('cpi_long', engine, if_exists='replace', index=False)
    mapping_df.to_sql('cpi_product_mapping', engine, if_exists='replace', index=False)

    print("CPI data and mapping successfully loaded into Postgres!")

# -----------------------------
# Prefect Flow
# -----------------------------
@flow
def cpi_pipeline():
    zip_path = download_cpi()
    df_raw = extract_csv(zip_path)
    df_clean = clean_data(df_raw)
    df_growth = calculate_growth(df_clean)
    df_long = reshape_long(df_growth)
    df_long, mapping_df = encode_and_map(df_long)
    load_to_postgres(df_long, mapping_df)

if __name__ == "__main__":
    cpi_pipeline()
