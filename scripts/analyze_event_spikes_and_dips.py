#!/usr/bin/env python3
import os
import glob
import pandas as pd
import numpy as np

# ----------------------------
# Paths
# ----------------------------
CURRENCY_PATH = "data/currencies/USD_Exchange_Rates.xlsx"
EVENTS_PATH = "data/events/*/events_*.parquet"
OUTPUT_PATH = "data/processed/spikes_and_dips.parquet"


# ----------------------------
# Load currency data (Excel)
# ----------------------------
def load_currency_data():
    print("📥 Loading currency data from Excel...")
    df = pd.read_excel(CURRENCY_PATH, index_col=0, parse_dates=True)

    # keep only pct change columns
    pct_change_cols = [c for c in df.columns if c.endswith("_pctchg")]
    if not pct_change_cols:
        raise ValueError(
            "No *_pctchg columns found in currency file. Did you run update_historical_currencies.py?"
        )

    return df[pct_change_cols]


# ----------------------------
# Load event data (Parquet)
# ----------------------------
def load_event_data():
    print("📥 Loading events data...")
    all_events = []
    for file in sorted(glob.glob(EVENTS_PATH)):
        try:
            df = pd.read_parquet(file)
            if "DATE" not in df.columns:
                print(f"⚠️ Skipping {file} (no DATE column)")
                continue
            df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce")
            df = df.dropna(subset=["DATE"])
            all_events.append(df)
        except Exception as e:
            print(f"⚠️ Failed to load {file}: {e}")
    if not all_events:
        raise ValueError("No valid event files loaded.")
    return pd.concat(all_events, ignore_index=True)


# ----------------------------
# Detect spikes & dips
# ----------------------------
def detect_spikes_and_dips(currency_df, threshold=2.0):
    print("📊 Detecting spikes and dips...")

    results = []
    for col in currency_df.columns:
        series = currency_df[col].dropna()
        mean = series.mean()
        std = series.std()

        for date, val in series.items():
            zscore = (val - mean) / std if std > 0 else 0
            if abs(zscore) >= threshold:
                results.append(
                    {
                        "DATE": date,
                        "CURRENCY": col.replace("_pctchg", ""),
                        "VALUE": val,
                        "ZSCORE": zscore,
                        "SPIKE_DIP": "SPIKE" if zscore > 0 else "DIP",
                    }
                )
    return pd.DataFrame(results)


# ----------------------------
# Main
# ----------------------------
def main():
    df_currency = load_currency_data()
    df_events = load_event_data()

    spikes_dips_df = detect_spikes_and_dips(df_currency)

    print(f"📊 Detected {len(spikes_dips_df)} spikes/dips.")

    # Save merged output with events (outer join on DATE)
    merged = pd.merge(
        spikes_dips_df,
        df_events,
        how="left",
        left_on="DATE",
        right_on="DATE",
    )

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    merged.to_parquet(OUTPUT_PATH, index=False)
    print(f"✅ Saved spikes/dips with events to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
