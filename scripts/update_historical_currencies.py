import os
import pandas as pd
from datetime import datetime, timedelta

DATA_DIR = "data/events"
START_DATE = datetime(2015, 2, 18)
TODAY = datetime.utcnow().date().replace(day=1)  # up to current month

# Make sure base dir exists
os.makedirs(DATA_DIR, exist_ok=True)

def normalize_country(geo_str):
    if pd.isna(geo_str) or geo_str.strip() == "":
        return None
    if "," in geo_str:
        return geo_str.split(",")[-1].strip()
    return geo_str.strip()

# List of countries you want to keep
ALLOWED_COUNTRIES = {
    "India", "China", "Russia", "Philippines",
    "Israel", "Vietnam", "Mexico",
    "Germany", "France", "Italy", "Spain", "Netherlands", "Belgium", "Austria", "Portugal", "Finland", "Greece"  # top EUR countries
}

# Generate month ranges
current = START_DATE
while current <= TODAY:
    year = current.year
    month = current.month
    outdir = os.path.join(DATA_DIR, str(year))
    os.makedirs(outdir, exist_ok=True)

    outpath = os.path.join(outdir, f"events_{year}_{month:02d}.parquet")

    if os.path.exists(outpath):
        print(f"Skipping {outpath} (already exists).")
    else:
        print(f"Processing {year}-{month:02d}...")
        # Example: build your GDELT URL for that month
        # (replace with real download logic)
        url = f"http://data.gdeltproject.org/events/{year}{month:02d}.export.CSV.zip"

        try:
            df = pd.read_csv(url, sep="\t", low_memory=False, compression="zip")
        except Exception as e:
            print(f"⚠️ Failed to download {url}: {e}")
            current += timedelta(days=32)
            current = current.replace(day=1)
            continue

        # Convert SQLDATE → date only
        df["SQLDATE"] = pd.to_datetime(df["SQLDATE"], format="%Y%m%d").dt.date

        # Normalize ActionGeo_CountryCode
        df["ActionGeo_CountryCode"] = df["ActionGeo_CountryCode"].map(normalize_country)

        # Drop Actor1CountryCode
        if "Actor1CountryCode" in df.columns:
            df = df.drop(columns=["Actor1CountryCode"])

        # Filter allowed countries only
        df = df[df["ActionGeo_CountryCode"].isin(ALLOWED_COUNTRIES)]

        # Drop null/empty
        df = df.dropna(subset=["ActionGeo_CountryCode"])
        df = df[df["ActionGeo_CountryCode"].str.strip() != ""]

        # Save parquet
        df.to_parquet(outpath, index=False)
        print(f"✅ Saved {outpath}")

    # Go to next month
    current += timedelta(days=32)
    current = current.replace(day=1)
