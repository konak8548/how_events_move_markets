# events_to_currency_ols.py
import os
import pandas as pd
import numpy as np
import statsmodels.api as sm
from datetime import timedelta

# CONFIG
EVENTS_DIR = "data/events"   # directory where you saved GDELT month parquet files
CURRENCY_EXCEL = "data/currencies/USD_Exchange_Rates.xlsx"
CURRENCIES = [
    "EUR","GBP","JPY","CAD","AUD","CHF","CNY","INR","NZD","SEK",
    "NOK","DKK","ZAR","BRL","MXN","SGD","HKD","KRW","TRY","THB",
    "TWD","RUB"
]

# Map currencies to one-or-more country names as they appear in GDELT's ActionGeo_Country.
# For EUR we map to the main Eurozone members (expand as needed).
CURRENCY_TO_COUNTRIES = {
    "EUR": ["Austria","Belgium","Cyprus","Estonia","Finland","France","Germany",
            "Greece","Ireland","Italy","Latvia","Lithuania","Luxembourg",
            "Malta","Netherlands","Portugal","Slovakia","Slovenia","Spain"],
    "GBP": ["United Kingdom","England","Scotland","Wales","Northern Ireland","UK"],
    "JPY": ["Japan"],
    "CAD": ["Canada"],
    "AUD": ["Australia"],
    "CHF": ["Switzerland"],
    "CNY": ["China","People's Republic of China"],
    "INR": ["India"],
    "NZD": ["New Zealand"],
    "SEK": ["Sweden"],
    "NOK": ["Norway"],
    "DKK": ["Denmark"],
    "ZAR": ["South Africa","RSA"],
    "BRL": ["Brazil"],
    "MXN": ["Mexico"],
    "SGD": ["Singapore"],
    "HKD": ["Hong Kong"],
    "KRW": ["South Korea","Korea, South","Republic of Korea","Korea"],
    "TRY": ["Turkey","Türkiye"],
    "THB": ["Thailand"],
    "TWD": ["Taiwan","Chinese Taipei"],
    "RUB": ["Russia","Russian Federation"]
}

# ---------------------
# Helpers: load events
# ---------------------
def load_all_events(events_dir=EVENTS_DIR):
    """
    Load all parquet event files under events_dir into a single DataFrame.
    Expects files saved in your update_historical_events script.
    """
    parts = []
    for root, dirs, files in os.walk(events_dir):
        for f in files:
            if f.endswith(".parquet"):
                path = os.path.join(root, f)
                df = pd.read_parquet(path)
                parts.append(df)
    if not parts:
        raise RuntimeError("No event parquet files found under: %s" % events_dir)
    events = pd.concat(parts, ignore_index=True)
    return events

# ---------------------
# Country extraction
# ---------------------
def extract_country_from_geo(col_value):
    """
    col_value: raw string from ActionGeo_Country (e.g. 'Alaska, United States' or 'France')
    Rule:
      - If there's a comma, return substring after last comma (strip spaces)
      - Else return the whole value stripped
      - If empty or NaN, return None
    """
    if pd.isna(col_value):
        return None
    s = str(col_value).strip()
    if not s:
        return None
    if "," in s:
        # country after the last comma
        return s.split(",")[-1].strip()
    else:
        # single-word or already country
        return s

# ---------------------
# Aggregate events per country-date
# ---------------------
def build_event_features(events_df, top_n_codes=20):
    """
    - normalize Date to date only
    - extract country into new column 'Country'
    - compute basic features by (Date, Country):
        * total_events (count)
        * unique_event_codes (count unique)
        * counts for top N event codes (prefix evt_)
    Returns: DataFrame indexed by Date, Country with features
    """
    df = events_df.copy()
    # ensure Date is datetime (your event script already coerces)
    df['Date'] = pd.to_datetime(df['Date'], errors='coerce').dt.date
    df['Country'] = df['ActionGeo_Country'].apply(extract_country_from_geo)
    df = df.dropna(subset=['Date', 'Country'])
    # normalize country strings (strip and consistent case)
    df['Country'] = df['Country'].str.strip()

    # total events and unique event codes
    grouped = df.groupby(['Date', 'Country'])
    features = grouped.size().rename("total_events").to_frame()
    features['unique_event_codes'] = grouped['EventCode'].nunique()

    # add top N event codes across the whole dataset
    top_codes = df['EventCode'].value_counts().nlargest(top_n_codes).index.tolist()
    for code in top_codes:
        col = f"evt_{code}"
        features[col] = grouped.apply(lambda g, c=code: (g['EventCode'] == c).sum()).astype(int)

    features = features.reset_index()
    return features

# ---------------------
# Join to currency series and prepare modeling table
# ---------------------
def prepare_model_table(event_features_df, currency_excel=CURRENCY_EXCEL, currencies=CURRENCIES):
    """
    Read the currency Excel, melt into long format, join events aggregated by country -> currency.
    We will produce one modeling table per currency with:
      Date, features..., target = next_day_{CUR}_pctchg
    """
    # read currency Excel
    cur_df = pd.read_excel(currency_excel, index_col=0)
    # index in your excel are strings like 'YYYY-MM-DD' — convert to date
    cur_df.index = pd.to_datetime(cur_df.index).date
    # ensure pct change columns exist for each currency like EUR_pctchg
    # If your Excel has already the '_pctchg' columns, we'll use them directly.
    # Otherwise compute them from price columns:
    pct_cols = {}
    for cur in currencies:
        pct_col = f"{cur}_pctchg"
        if pct_col not in cur_df.columns and cur in cur_df.columns:
            cur_df[pct_col] = cur_df[cur].pct_change().round(3) * 100
        pct_cols[cur] = pct_col

    model_tables = {}

    # put event features into per-country-date index for faster lookups
    # event_features_df has columns: Date, Country, total_events, unique_event_codes, evt_*
    # For each currency, filter countries mapped and aggregate events by date across mapped countries
    for cur in currencies:
        countries = CURRENCY_TO_COUNTRIES.get(cur, [])
        # filter and aggregate by date (sum across mapped countries)
        ef = event_features_df[event_features_df['Country'].isin(countries)]
        if ef.empty:
            print(f"⚠️ No events found for currency {cur} (countries: {countries}) -- skipping.")
            continue

        daily = ef.groupby('Date').agg({
            'total_events': 'sum',
            'unique_event_codes': 'sum',
            **{c: ('sum' if c.startswith('evt_') else 'sum') for c in ef.columns if c.startswith('evt_')}
        }).sort_index()

        # join with currency series
        cur_pct_col = pct_cols[cur]
        cur_prices = cur_df[[cur, cur_pct_col]].copy().rename(columns={cur_pct_col: 'pctchg', cur: 'rate'})
        # Align indices: daily.index are date objects, cur_prices.index are date objects
        combined = daily.join(cur_prices, how='inner')  # only dates present in both
        if combined.empty:
            print(f"⚠️ After join, no overlapping dates for {cur}.")
            continue

        # target variable = next day pct change
        combined['target_next_pctchg'] = combined['pctchg'].shift(-1)
        # drop last row with NaN target
        combined = combined.dropna(subset=['target_next_pctchg'])
        model_tables[cur] = combined.reset_index()  # Date is a column again

    return model_tables

# ---------------------
# Fit OLS per currency
# ---------------------
def fit_ols_for_currency(df, feature_cols=None, verbose=True):
    """
    df: DataFrame containing Date, feature columns, 'target_next_pctchg'
    feature_cols: list of columns to use (if None, use all numeric except rate and pctchg)
    Returns: fitted model, metrics dict
    """
    if feature_cols is None:
        exclude = {'Date', 'rate', 'pctchg', 'target_next_pctchg'}
        feature_cols = [c for c in df.columns if (df[c].dtype.kind in 'fi') and (c not in exclude)]

    X = df[feature_cols].astype(float)
    y = df['target_next_pctchg'].astype(float)

    # add constant for intercept
    Xc = sm.add_constant(X)

    # train/test split by time: last 20% as test
    split_idx = int(len(df) * 0.8)
    X_train, X_test = Xc.iloc[:split_idx], Xc.iloc[split_idx:]
    y_train, y_test = y.iloc[:split_idx], y.iloc[split_idx:]

    model = sm.OLS(y_train, X_train).fit()
    preds = model.predict(X_test)
    rmse = np.sqrt(np.mean((preds - y_test) ** 2))
    mae = np.mean(np.abs(preds - y_test))

    if verbose:
        print(model.summary())
        print(f"Test rows: {len(y_test)}  RMSE: {rmse:.4f}  MAE: {mae:.4f}")

    metrics = {"rmse": rmse, "mae": mae, "n_train": len(y_train), "n_test": len(y_test)}
    return model, metrics

# ---------------------
# Main pipeline
# ---------------------
def main():
    print("Loading events...")
    events = load_all_events()
    print(f"Loaded {len(events)} event rows")

    print("Building event features...")
    event_features = build_event_features(events, top_n_codes=25)
    print(f"Event features rows (date-country): {len(event_features)}")

    print("Preparing per-currency model tables...")
    model_tables = prepare_model_table(event_features, currency_excel=CURRENCY_EXCEL, currencies=CURRENCIES)

    print("Fitting OLS per currency...")
    results = {}
    for cur, table in model_tables.items():
        print("\n============================")
        print(f"Fitting currency: {cur}")
        # choose features: total_events and top event-code columns
        feat_cols = [c for c in table.columns if (c.startswith('total_events') or c.startswith('unique_event_codes') or c.startswith('evt_'))]
        if not feat_cols:
            print(f"No feature columns found for {cur} - skipping")
            continue
        model, metrics = fit_ols_for_currency(table, feature_cols=feat_cols, verbose=False)
        print(f"{cur} metrics: {metrics}")
        results[cur] = {"model": model, "metrics": metrics}

    print("\nDone. Models fitted for:", list(results.keys()))
    return results

if __name__ == "__main__":
    main()
