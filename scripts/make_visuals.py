import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import glob, os
import statsmodels.api as sm

# ====================================================
# Setup
# ====================================================
os.makedirs("assets", exist_ok=True)

# --------------------------
# Load currencies
# --------------------------
curr_path = "data/currencies/USD_Exchange_Rates.xlsx"
curr_df = pd.read_excel(curr_path, index_col=0)
curr_df.index = pd.to_datetime(curr_df.index)

# ====================================================
# Helper: read/aggregate events (memory-friendly)
# ====================================================
def aggregate_event_counts(events_path_pattern):
    """
    Read parquet files incrementally (only needed columns), aggregate to daily
    counts per COUNTTYPE, and return a pivoted DataFrame:
      index -> DATE (datetime)
      columns -> COUNTTYPE values (each column is daily counts)
    """
    files = glob.glob(events_path_pattern)
    if not files:
        print("⚠️ No event files found")
        return pd.DataFrame()

    # Use the exact columns present in your parquet files as shown:
    cols_needed = ["DATE", "COUNTTYPE", "NUMBER", "GEO_FULLNAME"]
    daily = []  # store intermediate aggregated frames (one per file) to save memory

    for f in files:
        try:
            df = pd.read_parquet(f, columns=cols_needed)
        except Exception as e:
            print(f"Skipping {f}: {e}")
            continue

        # Ensure the DATE column is parsed correctly (GDELT-like YYYYMMDD)
        df = df.dropna(subset=["DATE"])
        df["DATE"] = pd.to_datetime(df["DATE"], format="%Y%m%d", errors="coerce")
        df = df.dropna(subset=["DATE"])
        # aggregate counts per day per COUNTTYPE
        # we use .size() to count records (robust); you can change to sum(NUMBER) if desired
        agg = (
            df.groupby([df["DATE"].dt.date, "COUNTTYPE"])
            .size()
            .reset_index(name="count")
            .rename(columns={"DATE": "date"})  # won't exist but keep consistent
        )
        # convert back to datetime index
        agg["date"] = pd.to_datetime(agg["DATE"].astype(str), format="%Y-%m-%d", errors="coerce") if "DATE" in agg.columns else pd.to_datetime(agg["date"])
        # keep only what we need
        agg = agg[["date", "COUNTTYPE", "count"]]
        daily.append(agg)

    if not daily:
        return pd.DataFrame()

    # concat incremental aggregates and pivot to get wide data: date x COUNTTYPE
    all_agg = pd.concat(daily, ignore_index=True)
    pivot = all_agg.pivot_table(index="date", columns="COUNTTYPE", values="count", aggfunc="sum", fill_value=0)
    pivot.index = pd.to_datetime(pivot.index)
    pivot = pivot.sort_index()
    return pivot


# ====================================================
# Function: run OLS and produce treemap of event impacts
# ====================================================
def build_ols_event_treemap(events_path_pattern, currencies_df, asset_png="assets/treemap_events.png", asset_html="assets/treemap_events.html"):
    """
    Aggregates events by day & event type, computes a USD-strength target (negative mean of currency pct changes),
    runs OLS (usd_strength ~ event_counts), extracts coefficients and builds a treemap of impacts:
      - size = absolute coefficient (magnitude of effect)
      - color = sign (positive => USD strengthens => currencies weaken => RED)
    Saves HTML + PNG to assets.
    """
    # 1) Aggregate events to daily counts by COUNTTYPE
    event_counts = aggregate_event_counts(events_path_pattern)
    if event_counts.empty:
        print("⚠️ No aggregated event counts available; treemap will not be built.")
        return None

    # 2) Prepare currency-based target: average pct change across currencies
    pct_cols = [c for c in currencies_df.columns if c.endswith("_pctchg")]
    if not pct_cols:
        print("⚠️ No pct change columns found in currencies_df (expected columns like EUR_pctchg).")
        return None

    # Align currencies to daily (date index)
    cur_pct = currencies_df[pct_cols].copy()
    cur_pct.index = pd.to_datetime(cur_pct.index)
    cur_pct = cur_pct.sort_index()

    # Ensure overlapping index between event_counts and currency pct dataframe
    common_idx = event_counts.index.intersection(cur_pct.index)
    if len(common_idx) == 0:
        # try aligning by date (sometimes event_counts use date only)
        common_idx = event_counts.index.intersection(cur_pct.index)
    if len(common_idx) == 0:
        print("⚠️ No overlapping dates between events and currencies.")
        return None

    event_counts = event_counts.reindex(common_idx).fillna(0)
    cur_pct = cur_pct.reindex(common_idx).fillna(method="ffill").fillna(0)

    # 3) Target variable: average of currency percentage changes
    # NOTE: sign convention: positive avg means currencies strengthened vs USD => USD weakened.
    avg_currency_pctchg = cur_pct.mean(axis=1)

    # Define usd_strength so positive => USD strengthened (i.e., currencies weakened)
    usd_strength = - avg_currency_pctchg
    usd_strength.name = "usd_strength"

    # 4) Prepare regression dataframe (drop event columns that are all zeros)
    X = event_counts.loc[usd_strength.index].copy()
    # drop columns with zero variance (all zeros) to avoid singular matrix
    X = X.loc[:, (X.sum(axis=0) > 0)]
    if X.shape[1] == 0:
        print("⚠️ No event types with non-zero counts after alignment.")
        return None

    y = usd_strength

    # Add constant and run OLS (statsmodels)
    X_const = sm.add_constant(X)
    model = sm.OLS(y, X_const)
    results = model.fit()
    coeffs = results.params.drop("const", errors="ignore")  # event-type coefficients

    # Build dataframe of coefficients
    coeff_df = coeffs.reset_index()
    coeff_df.columns = ["COUNTTYPE", "coef"]
    coeff_df["abs_coef"] = coeff_df["coef"].abs()
    # classify impact for currencies:
    # coef > 0 => event increases usd_strength => currencies weaken => 'Weaken'
    # coef < 0 => event decreases usd_strength => currencies strengthen => 'Strengthen'
    coeff_df["Impact"] = coeff_df["coef"].apply(lambda x: "Weaken" if x > 0 else ("Strengthen" if x < 0 else "Neutral"))

    # remove tiny coefficients near zero for plotting clarity (optional)
    # coeff_df = coeff_df[coeff_df["abs_coef"] > 1e-6]

    # Normalize sizes for treemap so very small coefficients are still visible
    # Use abs_coef scaled to sum to a convenient value; but px.treemap will size by abs_coef directly.
    # Prepare data for treemap: we want Impact -> COUNTTYPE
    treedata = coeff_df.sort_values("abs_coef", ascending=False)

    # if all coefficients are 0 (unlikely), fallback to counts-based treemap
    if treedata["abs_coef"].sum() == 0:
        print("⚠️ All coefficients are zero — falling back to event frequency treemap.")
        freq = X.sum(axis=0).reset_index()
        freq.columns = ["COUNTTYPE", "Count"]
        fig = px.treemap(freq, path=["COUNTTYPE"], values="Count", title="Event frequency (fallback)")
    else:
        fig = px.treemap(
            treedata,
            path=["Impact", "COUNTTYPE"],
            values="abs_coef",
            color="Impact",
            color_discrete_map={"Weaken": "red", "Strengthen": "green", "Neutral": "gray"},
            title="Event types sized by OLS coefficient magnitude (impact on USD strength)"
        )
        # Add hover info with coefficient sign & value
        fig.data[0].hovertemplate = '<b>%{label}</b><br>Impact: %{parent}<br>coef: %{value:.6f}<extra></extra>'

    # Save visuals
    fig.write_html(asset_html)
    try:
        fig.write_image(asset_png)
    except Exception as e:
        print(f"✅ HTML saved to {asset_html}. PNG save failed (often requires orca / kaleido). Error: {e}")

    # Also return regression summary and coeff_df for downstream use
    return {"fig": fig, "results": results, "coeff_df": coeff_df}


# ====================================================
# Improved correlation heatmap (legible ticks)
# ====================================================
def build_corr_heatmap(currencies_df, out_html="assets/corr_heatmap.html", out_png="assets/corr_heatmap.png"):
    pct_cols = [c for c in currencies_df.columns if c.endswith("_pctchg")]
    if not pct_cols:
        print("⚠️ No pct change columns found for correlation heatmap.")
        return None

    corr = currencies_df[pct_cols].corr().round(3)

    fig = px.imshow(
        corr,
        text_auto=True,
        aspect="auto",
        title="Correlation matrix: Currency % changes",
    )
    # Make tick labels legible
    fig.update_xaxes(tickangle=45, tickfont=dict(size=10))
    fig.update_yaxes(tickfont=dict(size=10))
    fig.update_layout(width=1000, height=900, margin=dict(l=120, r=40, t=80, b=160))

    fig.write_html(out_html)
    try:
        fig.write_image(out_png)
    except Exception as e:
        print(f"✅ HTML saved to {out_html}. PNG save failed (may require kaleido). Error: {e}")

    return fig


# ====================================================
# Main execution: create the two outputs required for portfolio
# ====================================================
if __name__ == "__main__":
    # Run OLS treemap
    ols_out = build_ols_event_treemap("data/events/*/*.parquet", curr_df,
                                      asset_png="assets/treemap_events.png",
                                      asset_html="assets/treemap_events.html")
    if ols_out is not None:
        print("✅ OLS treemap built and saved (assets/treemap_events.*)")
        # print short top coefficients for quick log
        top = ols_out["coeff_df"].sort_values("abs_coef", ascending=False).head(10)
        print("Top event impacts (by abs coef):")
        print(top.to_string(index=False))

    # Build correlation heatmap
    heat = build_corr_heatmap(curr_df, out_html="assets/corr_heatmap.html", out_png="assets/corr_heatmap.png")
    if heat is not None:
        print("✅ Correlation heatmap saved to assets/corr_heatmap.*")

    print("✅ All done.")
