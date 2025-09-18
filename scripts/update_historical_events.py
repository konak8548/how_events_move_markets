import os
import requests
import zipfile
import io
import pandas as pd
from datetime import datetime
import gc  # For garbage collection

# Local storage directory for parquet files
DATA_DIR = "data/events"
os.makedirs(DATA_DIR, exist_ok=True)

INDEX_URL = "http://data.gdeltproject.org/gdeltv2/masterfilelist.txt"


def fetch_gdelt_index():
    """
    Fetch GDELT master index file and return list of all event file URLs
    """
    print("📥 Fetching GDELT master index...")
    res = requests.get(INDEX_URL, timeout=120)
    res.raise_for_status()
    lines = res.text.splitlines()
    files = [line.split(" ")[-1] for line in lines if line.endswith(".export.CSV.zip")]
    print(f"✅ Found {len(files)} event files in index")
    return files


def download_and_extract(url):
    """
    Download and extract a GDELT event CSV.zip into a DataFrame
    """
    r = requests.get(url, timeout=120)
    r.raise_for_status()
    with zipfile.ZipFile(io.BytesIO(r.content)) as z:
        fname = z.namelist()[0]
        df = pd.read_csv(
            z.open(fname),
            sep="\t",
            header=None,
            # Columns based on GDELT schema
            # 0 = GlobalEventID, 1 = SQLDATE, 26 = EventCode, 52 = ActionGeo_ADM1Code
            usecols=[0, 1, 26, 52],
            names=["GlobalEventID", "SQLDATE", "EventCode", "ActionGeo_ADM1Code"],
            dtype=str
        )
    return df


def save_monthly(df: pd.DataFrame, year: int, month: int):
    """
    Save dataframe to parquet by year/month and clear memory
    """
    folder = os.path.join(DATA_DIR, str(year))
    os.makedirs(folder, exist_ok=True)
    path = os.path.join(folder, f"events_{year}_{month:02d}.parquet")

    if os.path.exists(path):
        existing = pd.read_parquet(path)
        combined = pd.concat([existing, df]).drop_duplicates(subset=["GlobalEventID"])
    else:
        combined = df

    combined.to_parquet(path, index=False)
    print(f"💾 Saved {len(combined)} rows -> {path}")

    # Clear memory/cache
    del df, combined, existing
    gc.collect()


def process_year(year: int, max_retries=3):
    """
    Process all months for a given year and filter for India
    """
    files = fetch_gdelt_index()

    # Filter files for the given year
    year_files = [
        url for url in files
        if datetime.strptime(url.split("/")[-1][:12], "%Y%m%d%H%M%S").year == year
    ]
    print(f"🗓 Found {len(year_files)} files for year {year}")

    # Group files by month
    month_to_files = {}
    for url in year_files:
        dt = datetime.strptime(url.split("/")[-1][:12], "%Y%m%d%H%M%S")
        month_to_files.setdefault(dt.month, []).append(url)

    # Process each month
    for month in sorted(month_to_files.keys()):
        monthly_path = os.path.join(DATA_DIR, str(year), f"events_{year}_{month:02d}.parquet")
        if os.path.exists(monthly_path):
            print(f"💾 Skipping {year}-{month:02d}, parquet already exists.")
            continue

        print(f"\n🔹 Processing {year}-{month:02d}")
        all_dfs = []
        for url in month_to_files[month]:
            for attempt in range(max_retries):
                try:
                    df = download_and_extract(url)
                    df["SQLDATE"] = pd.to_datetime(df["SQLDATE"], format="%Y%m%d", errors="coerce").dt.date

                    # Filter for India (case-insensitive in ActionGeo_ADM1Code)
                    df = df[df["ActionGeo_ADM1Code"].str.contains("india", case=False, na=False)]
                    df = df.dropna(subset=["SQLDATE"])

                    if not df.empty:
                        min_date, max_date = df["SQLDATE"].min(), df["SQLDATE"].max()
                        print(f"   📂 {url.split('/')[-1]} → covers {min_date} to {max_date}, rows: {len(df)}")

                    all_dfs.append(df)
                    break  # Success, exit retry loop
                except Exception as e:
                    print(f"⚠️ Attempt {attempt+1} failed for {url}: {e}")
                    if attempt == max_retries - 1:
                        print(f"❌ Skipping {url} after {max_retries} failed attempts.")

        if all_dfs:
            final_df = pd.concat(all_dfs, ignore_index=True).drop_duplicates(subset=["GlobalEventID"])
            save_monthly(final_df, year, month)
        else:
            print(f"⚠️ No data processed for {year}-{month:02d}")


if __name__ == "__main__":
    for year in range(2015, 2026):
        process_year(year)
