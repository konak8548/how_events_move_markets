import os
import requests
import zipfile
import io
import pandas as pd
from datetime import datetime
import gc

DATA_DIR = "/data/events"
os.makedirs(DATA_DIR, exist_ok=True)

INDEX_URL = "http://data.gdeltproject.org/gdeltv2/masterfilelist.txt"


def fetch_gdelt_index():
    res = requests.get(INDEX_URL, timeout=120)
    res.raise_for_status()
    lines = res.text.splitlines()
    return [line.split(" ")[-1] for line in lines if line.endswith(".export.CSV.zip")]


def download_and_extract(url):
    r = requests.get(url, timeout=120)
    r.raise_for_status()
    with zipfile.ZipFile(io.BytesIO(r.content)) as z:
        fname = z.namelist()[0]
        df = pd.read_csv(
            z.open(fname),
            sep="\t",
            header=None,
            usecols=[0, 1, 26, 52],
            names=["GlobalEventID", "SQLDATE", "EventCode", "ActionGeo_ADM1Code"],
            dtype=str
        )
    return df


def save_monthly(df, year, month):
    folder = os.path.join(DATA_DIR, str(year))
    os.makedirs(folder, exist_ok=True)
    path = os.path.join(folder, f"events_{year}_{month:02d}.parquet")

    existing = None
    if os.path.exists(path):
        existing = pd.read_parquet(path)
        combined = pd.concat([existing, df]).drop_duplicates(subset=["GlobalEventID"])
    else:
        combined = df

    combined.to_parquet(path, index=False)
    print(f"💾 Saved {len(combined)} rows -> {path}")

    # Clear memory
    del df, combined
    if existing is not None:
        del existing
    gc.collect()


def process_year(year, max_retries=3):
    files = fetch_gdelt_index()

    # Only files for this year
    year_files = [
        f for f in files
        if datetime.strptime(f.split("/")[-1][:12], "%Y%m%d%H%M%S").year == year
    ]

    # Group files by month
    month_to_files = {}
    for f in year_files:
        dt = datetime.strptime(f.split("/")[-1][:12], "%Y%m%d%H%M%S")
        month_to_files.setdefault(dt.month, []).append(f)

    for month in sorted(month_to_files.keys()):
        monthly_path = os.path.join(DATA_DIR, str(year), f"events_{year}_{month:02d}.parquet")
        if os.path.exists(monthly_path):
            print(f"💾 Skipping {year}-{month:02d}, already exists.")
            continue

        print(f"\n🔹 Processing {year}-{month:02d}")
        all_dfs = []

        for url in month_to_files[month]:
            # Extract date from zip filename for daily progress
            zip_fname = url.split("/")[-1]
            file_date = datetime.strptime(zip_fname[:8], "%Y%m%d").date()
            print(f"   📂 Processing zip for date: {file_date} → {zip_fname}")

            for attempt in range(max_retries):
                try:
                    df = download_and_extract(url)
                    df["SQLDATE"] = pd.to_datetime(df["SQLDATE"], format="%Y%m%d", errors="coerce").dt.date
                    df = df[df["ActionGeo_ADM1Code"].str.contains("india", case=False, na=False)]
                    df = df.dropna(subset=["SQLDATE"])
                    all_dfs.append(df)
                    break
                except Exception as e:
                    print(f"⚠️ Attempt {attempt+1} failed for {zip_fname}: {e}")
                    if attempt == max_retries - 1:
                        print(f"❌ Skipping {zip_fname} after {max_retries} attempts.")

        if all_dfs:
            final_df = pd.concat(all_dfs, ignore_index=True).drop_duplicates(subset=["GlobalEventID"])
            save_monthly(final_df, year, month)
            del final_df
            gc.collect()
        else:
            print(f"⚠️ No data processed for {year}-{month:02d}")


if __name__ == "__main__":
    for year in range(2021, 2026):
        process_year(year)
