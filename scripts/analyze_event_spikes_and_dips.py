# analyze_event_spikes_and_dips.py
import pandas as pd
import numpy as np
import glob
import os
import plotly.express as px
from scipy.stats import zscore

# ----------------------------
# Configuration
# ----------------------------
CURRENCIES = [
    "EUR","GBP","JPY","CAD","AUD","CHF","CNY","INR","NZD","SEK",
    "NOK","DKK","ZAR","BRL","MXN","SGD","HKD","KRW","TRY","THB",
    "TWD","RUB"
]
CURRENCY_EXCEL_PATH = "data/currencies/USD_Exchange_Rates.xlsx"
EVENTS_FOLDER = "data/events"
RESULTS_FOLDER = "data/event_analysis"
os.makedirs(RESULTS_FOLDER, exist_ok=True)

# z-score threshold for spikes/dips
Z_THRESHOLD = 2  # roughly top/bottom 2.5% of changes

# ----------------------------
# Helper Functions
# ----------------------------
def load_currency_data():
    print("📥 Loading currency data...")
    df = pd.read_excel(CURRENCY_EXCEL_PATH, index_col=0, parse_dates=True)
    df.index = pd.to_datetime(df.index)
    return df

def load_events_data():
    print("📥 Loading events data...")
    files = glob.glob(f"{EVENTS_FOLDER}/**/*.parquet", recursive=True)
    all_dfs = []

    for f in files:
        df = pd.read_parquet(f)

        # Skip if DATE column missing
        if "DATE" not in df.columns:
            print(f"⚠️ Skipping {f} (no DATE column)")
            continue

        # Filter out invalid DATE entries
        df = df[df["DATE"].apply(lambda x: str(x).isdigit())]

        # Convert DATE to datetime
        df["DATE"] = pd.to_datetime(df["DATE"].astype(str), format="%Y%m%d", errors="coerce")
        df = df.dropna(subset=["DATE"])

        # Extract COUNTRY safely
        if "GEO_COUNTRYCODE" in df.columns:
            df["COUNTRY"] = df["GEO_COUNTRYCODE"].apply(
                lambda x: x.split(",")[-1].strip() if pd.notna(x) else None
            )
        else:
            df["COUNTRY"] = None

        all_dfs.append(df)

    if all_dfs:
        return pd.concat(all_dfs, ignore_index=True)
    else:
        print("⚠️ No usable event files found.")
        return pd.DataFrame()


def detect_spikes_and_dips(df_currency: pd.DataFrame):
    results = []
    for col in df_currency.columns:
        if col == "DATE":
            continue

        series = df_currency[col].dropna()
        if series.empty:
            continue

        mean, std = series.mean(), series.std()
        if std == 0:
            continue

        # z-score as Series with same index
        series_z = (series - mean) / std

        # spikes and dips
        spikes = series_z[series_z >= Z_THRESHOLD].index
        dips = series_z[series_z <= -Z_THRESHOLD].index

        for date in spikes:
            results.append({"DATE": date, "CURRENCY": col, "TYPE": "SPIKE"})
        for date in dips:
            results.append({"DATE": date, "CURRENCY": col, "TYPE": "DIP"})

    return pd.DataFrame(results)

def get_top_events(events_df, date, country, top_n=3):
    prev_day = date - pd.Timedelta(days=1)
    df_filtered = events_df[(events_df["DATE"] == prev_day) & (events_df["COUNTRY"] == country)]
    top_events = df_filtered["COUNTTYPE"].value_counts().head(top_n)
    return top_events.to_dict()

# ----------------------------
# Main Analysis
# ----------------------------
def main():
    df_currency = load_currency_data()
    df_events = load_events_data()
    if df_currency.empty or df_events.empty:
        print("⚠️ No data to process.")
        return

    spikes_dips_df = detect_spikes_and_dips(df_currency)
    print(f"📊 Detected {len(spikes_dips_df)} spikes/dips.")

    # Analyze top events preceding spikes/dips
    analysis_results = []
    for _, row in spikes_dips_df.iterrows():
        date = pd.to_datetime(row["Date"])
        cur = row["Currency"]

        # Map currency to country if possible (simplified, adjust as needed)
        currency_country_map = {
            "EUR": "France", "GBP": "United Kingdom", "JPY": "Japan", "CAD": "Canada",
            "AUD": "Australia", "CHF": "Switzerland", "CNY": "China", "INR": "India",
            "NZD": "New Zealand", "SEK": "Sweden", "NOK": "Norway", "DKK": "Denmark",
            "ZAR": "South Africa", "BRL": "Brazil", "MXN": "Mexico", "SGD": "Singapore",
            "HKD": "Hong Kong", "KRW": "South Korea", "TRY": "Turkey", "THB": "Thailand",
            "TWD": "Taiwan", "RUB": "Russia"
        }
        country = currency_country_map.get(cur, None)
        if country is None:
            continue

        top_events = get_top_events(df_events, date, country)
        for event, count in top_events.items():
            analysis_results.append({
                "Date": date,
                "Currency": cur,
                "SpikeDip": row["Type"],
                "Event": event,
                "Count": count
            })

    df_analysis = pd.DataFrame(analysis_results)
    output_path = os.path.join(RESULTS_FOLDER, "currency_spike_dip_event_analysis.xlsx")
    df_analysis.to_excel(output_path, index=False)
    print(f"✅ Saved analysis results -> {output_path}")

    # Optional: aggregate stats
    stats = df_analysis.groupby(["SpikeDip", "Event"])["Date"].count().reset_index()
    stats.columns = ["SpikeDip", "Event", "Occurrences"]
    stats["Percentage"] = (stats["Occurrences"] / stats.groupby("SpikeDip")["Occurrences"].transform("sum") * 100).round(2)

    fig = px.bar(stats, x="Event", y="Percentage", color="SpikeDip", barmode="group",
                 title="Percentage of Spikes/Dips preceded by events")
    fig_path = os.path.join(RESULTS_FOLDER, "spike_dip_event_chart.html")
    fig.write_html(fig_path)
    print(f"✅ Saved chart -> {fig_path}")

if __name__ == "__main__":
    main()
