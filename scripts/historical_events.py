# historical_events.py
import os
import pandas as pd
from datetime import datetime

DATA_DIR = "data/events"
os.makedirs(DATA_DIR, exist_ok=True)

def fetch_event_data(start_date, end_date):
    """
    Replace this with real event API / scraping logic.
    Must return a DataFrame with columns: Date, Event, Country
    """
    dates = pd.date_range(start=start_date, end=end_date, freq="7D")  # dummy: weekly events
    return pd.DataFrame({
        "Date": dates,
        "Event": [f"Dummy Event {i}" for i in range(len(dates))],
        "Country": ["Global"] * len(dates)
    })

def save_events_by_year(df: pd.DataFrame):
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df = df.dropna(subset=["Date"])
    if "Country" not in df.columns:
        df["Country"] = "Unknown"
    df = df.drop_duplicates(subset=["Date","Event","Country"])

    for year, year_df in df.groupby(df["Date"].dt.year):
        filepath = os.path.join(DATA_DIR, f"events_{year}.parquet")

        if os.path.exists(filepath):
            existing = pd.read_parquet(filepath)
            combined = pd.concat([existing, year_df]).drop_duplicates(
                subset=["Date","Event","Country"]
            )
        else:
            combined = year_df

        combined.to_parquet(filepath, index=False)

if __name__ == "__main__":
    start_date = "2020-01-01"
    end_date = datetime.today().strftime("%Y-%m-%d")
    df = fetch_event_data(start_date, end_date)
    save_events_by_year(df)
