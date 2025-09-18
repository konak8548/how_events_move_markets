import os
import pandas as pd
from datetime import datetime, date, timedelta

# Base directories
DATA_DIR = "data/currencies"
os.makedirs(DATA_DIR, exist_ok=True)

# Start and end dates
START_DATE = date(2015, 2, 18)
TODAY = date.today()  # Always date type

# Example: list of currencies to track
CURRENCIES = ["USD", "EUR", "GBP", "JPY"]  # adjust as needed

# Current day iterator
current = START_DATE

while current <= TODAY:
    year = current.year
    month = current.month
    day = current.day

    # Example output path: data/currencies/YYYY_MM_DD.csv
    outdir = os.path.join(DATA_DIR, str(year), f"{month:02d}")
    os.makedirs(outdir, exist_ok=True)
    outpath = os.path.join(outdir, f"currencies_{year}_{month:02d}_{day:02d}.csv")

    if os.path.exists(outpath):
        print(f"Skipping {outpath} (already exists)")
    else:
        print(f"Processing {current}...")

        # --- Your currency download logic goes here ---
        # Example placeholder DataFrame
        df = pd.DataFrame({
            "Date": [current],
            "Currency": CURRENCIES,
            "Rate": [1.0]*len(CURRENCIES)  # Replace with real rates
        })

        # Save CSV
        df.to_csv(outpath, index=False)
        print(f"✅ Saved {outpath}")

    # Move to next day
    current += timedelta(days=1)
