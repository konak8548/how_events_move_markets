import pandas as pd
import statsmodels.api as sm
from pathlib import Path
import glob
import os

# ----------------------------
# Config
# ----------------------------
CURRENCY_EXCEL = "data/currencies/USD_Exchange_Rates.xlsx"
EVENTS_DIR = "data/events"
RESULTS_DIR = Path("results")
RESULTS_DIR.mkdir(parents=True, exist_ok=True)

# ----------------------------
# Load currencies
# ----------------------------
print("📂 Loading currency Excel...")
fx_df = pd.read_excel(CURRENCY_EXCEL, index_col=0)

# Ensure date index
fx_df.index = pd.to_datetime(fx_df.index)

# Keep only % change columns
pct_cols = [c for c in fx_df.columns if c.endswith("_pctchg")]
fx_returns = fx_df[pct_cols].copy()
fx_returns = fx_returns.dropna(how="all")

print(f"✅ Loaded FX data with {fx_returns.shape[0]} rows and {len(pct_cols)} currencies.")

# ----------------------------
# Load event data
# ----------------------------
print("📂 Loading GDELT event parquet files...")
all_files = glob.glob(os.path.join(EVENTS_DIR, "*", "events_*.parquet"))

if not all_files:
    raise FileNotFoundError("❌ No event parquet files found in data/events/")

dfs = []
for f in all_files:
    try:
        df = pd.read_parquet(f, columns=["Date", "EventCode"])
        dfs.append(df)
    except Exception as e:
        print(f"⚠️ Failed to read {f}: {e}")

events_df = pd.concat(dfs, ignore_index=True)
events_df["Date"] = pd.to_datetime(events_df["Date"], errors="coerce")
events_df = events_df.dropna(subset=["Date"])

# Simple aggregation: daily event count = event intensity
daily_events = events_df.groupby(events_df["Date"].dt.date).size().reset_index(name="event_intensity")
daily_events["Date"] = pd.to_datetime(daily_events["Date"])
daily_events = daily_events.set_index("Date")

print(f"✅ Loaded events with {len(daily_events)} daily rows.")

# ----------------------------
# Merge datasets
# ----------------------------
merged = fx_returns.merge(daily_events, left_index=True, right_index=True, how="inner")
print(f"🔗 Merged dataset: {merged.shape[0]} rows")

if merged.empty:
    raise ValueError("❌ No overlap between events and FX data!")

# ----------------------------
# Run regression (per currency)
# ----------------------------
results_file = RESULTS_DIR / "ols_summary.txt"
with open(results_file, "w") as f:
    for col in pct_cols:
        y = merged[col].dropna()
        X = merged.loc[y.index, "event_intensity"]

        if y.empty or X.empty:
            continue

        X = sm.add_constant(X)  # add intercept
        model = sm.OLS(y, X).fit()

        f.write(f"\n\n=== OLS Regression: {col} vs Event Intensity ===\n")
        f.write(model.summary().as_text())
        f.write("\n")

print(f"✅ OLS regression results saved to {results_file}")
