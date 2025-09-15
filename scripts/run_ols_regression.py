import pandas as pd
import statsmodels.api as sm
from pathlib import Path

# Paths
events_path = Path("data/events/events.parquet")
currencies_path = Path("data/currencies/currencies.parquet")
results_path = Path("results")
results_path.mkdir(parents=True, exist_ok=True)

# Load datasets
events = pd.read_parquet(events_path)
currencies = pd.read_parquet(currencies_path)

# Ensure proper date format
events["date"] = pd.to_datetime(events["date"])
currencies["date"] = pd.to_datetime(currencies["date"])

# Merge on date
df = pd.merge(currencies, events, on="date", how="inner").dropna()

if "return" not in df.columns or "event_intensity" not in df.columns:
    raise ValueError("Missing required columns: 'return' in currencies, 'event_intensity' in events.")

# Run regression
X = sm.add_constant(df["event_intensity"])  # independent variable
y = df["return"]                            # dependent variable

model = sm.OLS(y, X).fit()

# Save results
with open(results_path / "ols_summary.txt", "w") as f:
    f.write(model.summary().as_text())

print("OLS regression completed. Results saved to results/ols_summary.txt")
