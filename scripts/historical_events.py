# historical_events.py
import os
import requests
import zipfile
import io
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime

# GitHub repo local path (make sure you cloned the repo locally!)
DATA_DIR = "data/events"
os.makedirs(DATA_DIR, exist_ok=True)

INDEX_URL = "http://data.gdeltproject.org/gdeltv2/masterfilelist.txt"

def fetch_gdelt_index():
    """
    Fetch GDELT master index file which lists all available event CSVs
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
            usecols=[0, 1, 26, 51, 52],  # limit size: GlobalEventID, Date, Actor1CountryCode, ActionGeo_CountryCode, EventRootCode
            names=["GlobalEventID", "Date", "Actor1Country", "ActionGeo_Country", "EventCode"],
            dtype=str
        )
    return df

def save_monthly(df: pd.DataFrame, year: int, month: int):
    """
    Save dataframe to parquet by year/month
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

def process_all(start_date="2013-04-01"):
    files = fetch_gdelt_index()
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")

    for url in files:
        # GDELT filename contains date, e.g. 20130401230000.export.CSV.zip
        fname = url.split("/")[-1]
        file_date = datetime.strptime(fname[:12], "%Y%m%d%H%M%S")

        if file_date < start_dt:
            continue

        try:
            df = download_and_extract(url)
            df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
            df = df.dropna(subset=["Date"])

            year, month = file_date.year, file_date.month
            save_monthly(df, year, month)
        except Exception as e:
            print(f"❌ Failed {url}: {e}")

if __name__ == "__main__":
    process_all("2013-04-01")
