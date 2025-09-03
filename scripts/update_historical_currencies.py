# update_historical_currencies.py
import yfinance as yf
import pandas as pd
from datetime import date, datetime
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
        df[f"{cur}_%Chg"] = df[cur].pct_change().round(3) * 100
    return df

# ----------------------------
# Main Incremental Update
# ----------------------------
def main():
    # Determine the last date in existing Excel
    if os.path.exists(EXCEL_PATH):
        try:
            # Skip first two rows (Price, Tickers) to get actual headers
            existing_df = pd.read_excel(EXCEL_PATH, header=2, index_col=0, parse_dates=True)
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

    # Save to Excel with two metadata rows (Price, Tickers) + Date as index
    os.makedirs(os.path.dirname(EXCEL_PATH), exist_ok=True)
    with pd.ExcelWriter(EXCEL_PATH, engine="openpyxl") as writer:
        # Row 1: "Price" label
        combined.to_excel(writer, index_label="Date", startrow=2)
        # Row 0: Price
        writer.sheets["Sheet1"].cell(row=1, column=1, value="Price")
        # Row 2: Tickers
        for idx, cur in enumerate(CURRENCIES, start=2):
            writer.sheets["Sheet1"].cell(row=2, column=idx, value=f"{BASE_CURRENCY}{cur}=X")

    print(f"✅ Excel updated: {EXCEL_PATH}. Rows: {len(combined)}")

if __name__ == "__main__":
    main()
