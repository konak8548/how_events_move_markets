import os
import requests
import zipfile
import io
import pandas as pd
from datetime import datetime

# Local storage directory for parquet files
DATA_DIR = "data/events"
os.makedirs(DATA_DIR, exist_ok=True)

INDEX_URL = "http://data.gdeltproject.org/gdeltv2/masterfilelist.txt"


def fetch_gdelt_index():
    print("📥 Fetching GDELT master index...")
    res = requests.get(INDEX_URL, timeout=120)
    res.raise_for_status()
    lines = res.text.splitlines()
    files = [line.split(" ")[-1] for line in lines if line.endswith(".export.CSV.zip")]
    print(f"✅ Found {len(files)} event files in index")
    return files


def clean_country(val: str) -> str:
    """
    Normalize ActionGeo_CountryCode field:
    - If contains commas, take last part after comma.
    - Strip spaces.
    """
    if pd.isna(val):
        return None
    val = val.strip()
    if not val:
        return None
    if "," in val:
        return val.split(",")[-1].strip()
    return val


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
            usecols=[0, 1, 26, 52],  # ✅ corrected column mapping
            names=["GlobalEventID", "SQLDATE", "EventCode", "ActionGeo_CountryCode"],
            dtype=str
        )

    # Keep only date (YYYY-MM-DD)
    df["Date"] = pd.to_datetime(df["SQLDATE"], format="%Y%m%d", errors="coerce").dt.date

    # Clean ActionGeo_CountryCode
    df["ActionGeo_CountryCode"] = df["ActionGeo_CountryCode"].apply(clean_country)

    # Drop rows with missing/empty country
    df = df.dropna(subset=["ActionGeo_CountryCode"])
    df = df[df["ActionGeo_CountryCode"].str.strip() != ""]

    # Filter only selected countries
    keep_countries = {
        "India", "China", "Russia", "Philippines", "Israel", "Vietnam", "Mexico",
        # Top Euro currency countries
        "Germany", "France", "Italy", "Spain", "Netherlands", "Belgium",
        "Austria", "Finland", "Ireland", "Portugal", "Greece", "Luxembourg",
        "Slovakia", "Slovenia", "Estonia", "Latvia", "Lithuania", "Cyprus", "Malta"
    }
    df = df[df["ActionGeo_CountryCode"].isin(keep_countries)]

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


def process_latest_month():
    """
    Process only the latest month’s events.
    """
    files = fetch_gdelt_index()

    # Get the latest file date available
    latest_file = files[-1]
    fname = latest_file.split("/")[-1]
    latest_dt = datetime.strptime(fname[:12], "%Y%m%d%H%M%S")

    year, month = latest_dt.year, latest_dt.month
    print(f"🗓 Processing latest available month: {year}-{month:02d}")

    # Filter only files from this year and month
    month_files = [
        url for url in files
        if datetime.strptime(url.split("/")[-1][:12], "%Y%m%d%H%M%S").year == year
        and datetime.strptime(url.split("/")[-1][:12], "%Y%m%d%H%M%S").month == month
    ]

    all_dfs = []
    for url in month_files:
        try:
            df = download_and_extract(url)
            if not df.empty:
                all_dfs.append(df)
        except Exception as e:
            print(f"❌ Failed {url}: {e}")

    if all_dfs:
        final_df = pd.concat(all_dfs, ignore_index=True).drop_duplicates(subset=["GlobalEventID"])
        save_monthly(final_df, year, month)
    else:
        print("⚠️ No data processed for this month.")


if __name__ == "__main__":
    process_latest_month()
