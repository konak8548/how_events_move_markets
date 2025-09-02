# currencies.py
import yfinance as yf
import pandas as pd
from datetime import date, timedelta
import os

# ✅ Currency tickers
currencies = [
    "EUR","GBP","JPY","CAD","AUD","CHF","CNY","INR","NZD","SEK",
    "NOK","DKK","ZAR","BRL","MXN","SGD","HKD","KRW","TRY","THB",
    "TWD","RUB"
]
base_currency = "USD"

# ✅ Data storage path
DATA_DIR = "data/currencies"
os.makedirs(DATA_DIR, exist_ok=True)

# ✅ Parquet filename (per year)
def get_parquet_path(year: int) -> str:
    return os.path.join(DATA_DIR, f"{year}_currencies.parquet")

# ✅ Fetch currency data from Yahoo Finance
def fetch_currency_data(start_date, end_date):
    all_data = pd.DataFrame()

    for cur in currencies:
        ticker = f"{base_currency}{cur}=X"
        df = yf.download(
            ticker,
            start=start_date,
            end=end_date,
            interval="1d",
            progress=False
        )
        if df.empty:
            continue

        df = df[["Close"]].rename(columns={"Close": cur})
        df.index = df.index.date  # keep only date

        if all_data.empty:
            all_data = df
        else:
            all_data = all_data.join(df, how="outer")

    # ✅ add % change columns
    for cur in currencies:
        if cur in all_data.columns:
            all_data[f"{cur}_%Chg"] = all_data[cur].pct_change().round(3) * 100

    all_data.index.name = "Date"
    return all_data.reset_index()

# ✅ Incremental update
def update_currency_data():
    today = date.today()
    year = today.year
    parquet_path = get_parquet_path(year)

    if os.path.exists(parquet_path):
        # Load existing data
        existing = pd.read_parquet(parquet_path)
        existing["Date"] = pd.to_datetime(existing["Date"]).dt.date

        last_date = existing["Date"].max()
        start_date = last_date + timedelta(days=1)
        if start_date > today:
            print("✅ Currency data already up-to-date.")
            return
        print(f"🔄 Updating from {start_date} to {today}...")
    else:
        existing = pd.DataFrame()
        start_date = date(year, 1, 1)

    # Fetch new data
    new_data = fetch_currency_data(start_date, today)
    if new_data.empty:
        print("⚠️ No new data fetched.")
        return

    # Merge with existing
    updated = pd.concat([existing, new_data]).drop_duplicates(subset=["Date"]).sort_values("Date")

    # Save to parquet
    updated.to_parquet(parquet_path, index=False)
    print(f"✅ Saved {len(updated)} rows to {parquet_path}")

if __name__ == "__main__":
    update_currency_data()
