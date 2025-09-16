# analyze_event_spikes_and_dips.py
import pandas as pd
import numpy as np
import os
import glob
import matplotlib.pyplot as plt
from scipy.stats import zscore

# ----------------------------
# Configuration
# ----------------------------
CURRENCIES = [
    "EUR","GBP","JPY","CAD","AUD","CHF","CNY","INR","NZD","SEK",
    "NOK","DKK","ZAR","BRL","MXN","SGD","HKD","KRW","TRY","THB",
    "TWD","RUB"
]
CURRENCY_TO_COUNTRY = {
    "EUR": "Eurozone", "GBP": "United Kingdom", "JPY": "Japan", "CAD": "Canada",
    "AUD": "Australia", "CHF": "Switzerland", "CNY": "China", "INR": "India",
    "NZD": "New Zealand", "SEK": "Sweden", "NOK": "Norway", "DKK": "Denmark",
    "ZAR": "South Africa", "BRL": "Brazil", "MXN": "Mexico", "SGD": "Singapore",
    "HKD": "Hong Kong", "KRW": "South Korea", "TRY": "Turkey", "THB": "Thailand",
    "TWD": "Taiwan", "RUB": "Russia"
}
DATA_DIR = "data"
CURRENCIES_FILE = os.path.join(DATA_DIR, "currencies", "USD_Exchange_Rates.xlsx")
EVENTS_DIR = os.path.join(DATA_DIR, "events")
RESULTS_FILE = os.path.join(DATA_DIR, "event_spike_dip_summary.csv")

# ----------------------------
# Helper Functions
# ----------------------------
def load_currency_data():
    df = pd.read_excel(CURRENCIES_FILE, index_col=0, parse_dates=True)
    # Only keep percentage change columns
    pct_cols = [f"{c}_pctchg" for c in CURRENCIES]
    df_pct = df[pct_cols].copy()
    return df_pct

def load_events_data():
    parquet_files = glob.glob(os.path.join(EVENTS_DIR, "**/*.parquet"), recursive=True)
    dfs = []
    for pf in parquet_files:
        df = pd.read_parquet(pf)
        df["DATE"] = pd.to_datetime(df["DATE"].astype(str), format="%Y%m%d")
        # Extract country from GEO_COUNTRYCODE
        df["COUNTRY"] = df["GEO_COUNTRYCODE"].apply(lambda x: x.split(",")[-1].strip() if pd.notna(x) else None)
        dfs.append(df)
    all_events = pd.concat(dfs, ignore_index=True)
    return all_events

def identify_spikes_dips(df_pct, z_thresh=2.0):
    spikes = {}
    dips = {}
    for cur in CURRENCIES:
        z = zscore(df_pct[cur + "_pctchg"].fillna(0))
        spikes[cur] = df_pct.index[z >= z_thresh].tolist()
        dips[cur] = df_pct.index[z <= -z_thresh].tolist()
    return spikes, dips

def map_events_to_changes(change_dates, currency, df_events):
    country = CURRENCY_TO_COUNTRY.get(currency)
    summary = {}
    for dt in change_dates:
        prev_day = dt - pd.Timedelta(days=1)
        events_prev = df_events[df_events["DATE"] == prev_day]
        if country:
            events_prev = events_prev[events_prev["COUNTRY"] == country]
        top_events = (
            events_prev.groupby("COUNTTYPE")["NUMARTS"]
            .sum()
            .sort_values(ascending=False)
            .head(3)
        )
        summary[dt] = top_events.to_dict()
    return summary

def aggregate_summary(all_summaries):
    # all_summaries = {currency: {date: {event_type: count}}}
    agg = {}
    for cur, date_dict in all_summaries.items():
        for date, events in date_dict.items():
            for ev, cnt in events.items():
                agg[ev] = agg.get(ev, 0) + cnt
    total = sum(agg.values())
    percent_summary = {k: round(v / total * 100, 1) for k, v in agg.items()}
    return percent_summary

# ----------------------------
# Main
# ----------------------------
def main():
    os.makedirs(DATA_DIR, exist_ok=True)
    print("📥 Loading currency data...")
    df_pct = load_currency_data()
    print("📥 Loading events data...")
    df_events = load_events_data()
    print("⚡ Identifying spikes and dips...")
    spikes, dips = identify_spikes_dips(df_pct, z_thresh=2.0)  # ~2 SD threshold

    # Map events for each currency
    spike_event_summary = {}
    dip_event_summary = {}

    print("🔗 Mapping events to spikes...")
    for cur in CURRENCIES:
        spike_event_summary[cur] = map_events_to_changes(spikes[cur], cur, df_events)
        dip_event_summary[cur] = map_events_to_changes(dips[cur], cur, df_events)

    # Aggregate across all currencies
    spike_agg = aggregate_summary(spike_event_summary)
    dip_agg = aggregate_summary(dip_event_summary)

    # Save CSV
    result_df = pd.DataFrame([spike_agg, dip_agg], index=["Spikes", "Dips"]).transpose()
    result_df.to_csv(RESULTS_FILE)
    print(f"✅ Saved spike/dip summary to {RESULTS_FILE}")

    # Plotting
    fig, ax = plt.subplots(1,2, figsize=(14,6))
    result_df["Spikes"].sort_values(ascending=False).plot(kind="bar", ax=ax[0], title="Spikes preceded by events (%)")
    result_df["Dips"].sort_values(ascending=False).plot(kind="bar", ax=ax[1], title="Dips preceded by events (%)")
    plt.tight_layout()
    plt.savefig(os.path.join(DATA_DIR, "event_spike_dip_summary.png"))
    print("✅ Saved summary chart to data/event_spike_dip_summary.png")

if __name__ == "__main__":
    main()
