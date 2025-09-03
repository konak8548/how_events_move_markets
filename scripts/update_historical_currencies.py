# scripts/update_historical_currencies.py
import os
import yfinance as yf
import pandas as pd
from datetime import date, timedelta

currencies = [
    "EUR","GBP","JPY","CAD","AUD","CHF","CNY","INR","NZD","SEK",
    "NOK","DKK","ZAR","BRL","MXN","SGD","HKD","KRW","TRY","THB",
    "TWD","RUB"
]

base_currency = "USD"
file_path = "data/currencies/USD_Exchange_Rates.xlsx"

# Load existing data if available
if os.path.exists(file_path):
    all_data = pd.read_excel(file_path, index_col="Date", parse_dates=True)
    last_date = all_data.index.max().date()
    start_date = (last_date + timedelta(days=1)).isoformat()
    print(f"🔄 Existing data found. Last date = {last_date}, fetching from {start_date}")
else:
    all_data = pd.DataFrame()
    start_date = "2013-04-01"

end_date = date.today().isoformat()

# Download new data
new_data = pd.DataFrame()
for cur in currencies:
    ticker = f"{base_currency}{cur}=X"
    df = yf.download(ticker, start=start_date, end=end_date, interval="1d", progress=False)
    df = df[["Close"]].rename(columns={"Close": cur})
    df.index = df.index.date
    if new_data.empty:
        new_data = df
    else:
        new_data = new_data.join(df, how="outer")

# Merge old + new
if not all_data.empty:
    all_data.index = pd.to_datetime(all_data.index).date
    updated = pd.concat([all_data, new_data])
    updated = updated[~updated.index.duplicated(keep="last")]
else:
    updated = new_data

# Add % change columns
for cur in currencies:
    updated[f"{cur}_%Chg"] = updated[cur].pct_change().round(3) * 100

updated.index.name = "Date"

# Save back
os.makedirs(os.path.dirname(file_path), exist_ok=True)
updated.to_excel(file_path)

print(f"✅ Excel updated incrementally up to {end_date}")
