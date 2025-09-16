# scripts/analyze_event_spikes_and_dips.py

import os
import glob
import numpy as np
import pandas as pd
from scipy.stats import zscore
import matplotlib.pyplot as plt

DATA_DIR = "data"
EVENTS_DIR = os.path.join(DATA_DIR, "events")
CURRENCIES_DIR = os.path.join(DATA_DIR, "currencies")
OUTPUT_CSV = os.path.join(DATA_DIR, "spike_dip_event_analysis.csv")
OUTPUT_XLSX = os.path.join(DATA_DIR, "spike_dip_event_analysis.xlsx")

# Threshold for z-score spikes/dips
Z_THRESHOLD = 2.0  # ~95% confidence cutoff


def load_currency_data():
    print("📥 Loading currency data...")
    files = glob.glob(os.path.join(CURRENCIES_DIR, "*.parquet"))
    if not files:
        raise FileNotFoundError("No currency parquet files found.")

    dfs = []
    for f in files:
        try:
            df = pd.read_parquet(f)
            dfs.append(df)
        except Exception as e:
            print(f"⚠️ Skipping {f} due to error: {e}")
    df = pd.concat(dfs, ignore_index=True)
    df["DATE"] = pd.to_datetime(df["DATE"])
    return df.sort_values("DATE")


def load_events_data():
    print("📥 Loading events data...")
    files = glob.glob(os.path.join(EVENTS_DIR, "*/*.parquet"))
    if not files:
        raise FileNotFoundError("No event parquet files found.")

    dfs = []
    for f in files:
        try:
            df = pd.read_parquet(f)
            if "DATE" not in df.columns:
                print(f"⚠️ Skipping {f} (no DATE column)")
                continue
            # Ensure DATE is datetime
            df = df[df["DATE"].apply(lambda x: str(x).isdigit())]
            df["DATE"] = pd.to_datetime(df["DATE"].astype(str), format="%Y%m%d")
            # Parse country
            df["COUNTRY"] = df["GEO_COUNTRYCODE"].apply(
                lambda x: x.split(",")[-1].strip() if pd.notna(x) else None
            )
            dfs.append(df)
        except Exception as e:
            print(f"⚠️ Skipping {f} due to error: {e}")

    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


def detect_spikes_and_dips(df_currency):
    print("📊 Detecting spikes and dips...")
    df_currency = df_currency.set_index("DATE").sort_index()

    results = []
    for col in df_currency.columns:
        if col == "DATE":
            continue
        series = df_currency[col].dropna()
        if series.empty:
            continue

        series_z = zscore(series)
        series_z = pd.Series(series_z, index=series.index)

        spikes = series_z.loc[series_z >= Z_THRESHOLD].index
        dips = series_z.loc[series_z <= -Z_THRESHOLD].index

        for date in spikes:
            results.append({"DATE": date, "CURRENCY": col, "TYPE": "SPIKE"})
        for date in dips:
            results.append({"DATE": date, "CURRENCY": col, "TYPE": "DIP"})

    return pd.DataFrame(results)


def analyze_events_around_spikes(df_events, spikes_dips_df):
    print("📊 Analyzing events preceding spikes/dips...")

    rows = []
    for _, row in spikes_dips_df.iterrows():
        date = pd.to_datetime(row["DATE"])
        prev_day = date - pd.Timedelta(days=1)

        mask = df_events["DATE"] == prev_day
        subset = df_events[mask]

        if subset.empty:
            continue

        top_events = (
            subset["COUNTTYPE"]
            .value_counts()
            .head(3)
            .reset_index()
            .rename(columns={"index": "EVENT", "COUNTTYPE": "COUNT"})
        )

        for _, ev in top_events.iterrows():
            rows.append(
                {
                    "DATE": date,
                    "CURRENCY": row["CURRENCY"],
                    "TYPE": row["TYPE"],
                    "PREV_EVENT": ev["EVENT"],
                    "COUNT": ev["COUNT"],
                }
            )

    return pd.DataFrame(rows)


def summarize_results(results_df):
    print("📊 Summarizing event consistency...")

    summary = (
        results_df.groupby(["TYPE", "PREV_EVENT"])
        .size()
        .reset_index(name="COUNT")
    )

    total_by_type = summary.groupby("TYPE")["COUNT"].transform("sum")
    summary["PERCENTAGE"] = (summary["COUNT"] / total_by_type) * 100
    return summary


def plot_summary(summary_df):
    print("📊 Plotting summary chart...")

    pivot = summary_df.pivot(index="PREV_EVENT", columns="TYPE", values="PERCENTAGE").fillna(0)
    pivot.plot(kind="bar", stacked=False, figsize=(10, 6))
    plt.ylabel("Percentage (%)")
    plt.title("Event Types Preceding Spikes/Dips")
    plt.tight_layout()
    plt.savefig(os.path.join(DATA_DIR, "spike_dip_event_summary.png"))
    plt.close()


def main():
    df_currency = load_currency_data()
    df_events = load_events_data()

    spikes_dips_df = detect_spikes_and_dips(df_currency)
    print(f"📊 Detected {len(spikes_dips_df)} spikes/dips.")

    results_df = analyze_events_around_spikes(df_events, spikes_dips_df)
    summary_df = summarize_results(results_df)

    # Save outputs
    results_df.to_csv(OUTPUT_CSV, index=False)
    summary_df.to_excel(OUTPUT_XLSX, index=False)

    plot_summary(summary_df)

    print(f"✅ Saved results to {OUTPUT_CSV} and {OUTPUT_XLSX}")


if __name__ == "__main__":
    main()
