# update_historical_currencies.py
import yfinance as yf
import pandas as pd
from datetime import date
import os

# ----------------------------
# Configuration
# ----------------------------
CURRENCIES = [
    "EUR","GBP","JPY","CAD","AUD","CHF","CNY","INR","NZD","SEK",
    "NOK","DKK","ZAR","BRL","MXN","SGD","HKD","KRW","TRY","THB",
    "TWD","RUB"
]
BASE_CURRENCY = "USD"
EXCEL_PATH = "data/currencies/USD_Exchange_Rates.xlsx"

# ----------------------------
# Helper Functions
# ----------------------------
def fetch_currency_data(start_date, end_date):
    """
    Fetch currency closing rates from Yahoo Finance for all currencies.
    Returns a DataFrame with date index and columns = CURRENCY.
    """
    all_data = pd.DataFrame()
    for cur in CURRENCIES:
        ticker = f"{BASE_CURRENCY}{cur}=X"
        df = yf.download(ticker, start=start_date, end=end_date, interval="1d", progress=False)
        if df.empty:
            continue
        df = df[["Close"]].rename(columns={"Close": cur})
        df.index = pd.to_datetime(df.index.date)
        if all_data.empty:
            all_data = df
        else:
            all_data = all_data.join(df, how="outer")
    all_data.index.name = "Date"
    return all_data

def add_pct_change(df):
    """
    Add % change columns for all currencies.
    """
    for cur in CURRENCIES:
        if cur in df.columns:
            df[f"{cur}_%Chg"] = df[cur].pct_change().round(3) * 100
    return df

# ----------------------------
# Main Incremental Update
# ----------------------------
def main():
    # Determine the last date in existing Excel
    if os.path.exists(EXCEL_PATH):
        try:
            existing_df = pd.read_excel(EXCEL_PATH, index_col=0, parse_dates=True)
            last_date = existing_df.index.max()
            start_date = (last_date + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
            print(f"📈 Last date in Excel: {last_date}. Fetching from {start_date} to today.")
        except Exception as e:
            print(f"⚠️ Failed to read existing Excel: {e}")
            existing_df = pd.DataFrame()
            start_date = "2013-04-01"
    else:
        existing_df = pd.DataFrame()
        start_date = "2013-04-01"

    end_date = date.today().isoformat()
    if pd.to_datetime(start_date) > pd.to_datetime(end_date):
        print("✅ Excel is already up-to-date. Nothing to fetch.")
        return

    # Fetch new data
    new_data = fetch_currency_data(start_date, end_date)
    new_data = add_pct_change(new_data)

    # Combine with existing data
    if not existing_df.empty:
        combined = pd.concat([existing_df, new_data]).drop_duplicates()
    else:
        combined = new_data

    # Save cleanly (no extra rows)
    os.makedirs(os.path.dirname(EXCEL_PATH), exist_ok=True)
    combined.to_excel(EXCEL_PATH, index_label="Date")

    print(f"✅ Excel updated: {EXCEL_PATH}. Rows: {len(combined)}")

if __name__ == "__main__":
    main()
