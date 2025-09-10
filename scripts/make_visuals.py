import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import glob, os

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
# Function 1: Events vs Currency Impact Treemap
# ====================================================
def build_currency_event_treemap(events_path_pattern, currencies_df, currency="EUR"):
    files = glob.glob(events_path_pattern)
    if not files:
        print("⚠️ No event files found")
        return None

    cols_needed = ["DATE", "COUNTTYPE", "NUMBER", "GEO_FULLNAME"]
    dfs = []
    for f in files:
        try:
            df = pd.read_parquet(f, columns=cols_needed)
            dfs.append(df)
        except Exception as e:
            print(f"Skipping {f}: {e}")

    events_df = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
    if events_df.empty:
        print("⚠️ No valid events data")
        return None

    events_df["DATE"] = pd.to_datetime(events_df["DATE"], format="%Y%m%d", errors="coerce")
    events_df = events_df.dropna(subset=["DATE"])

    pct_col = f"{currency}_pctchg"
    if pct_col not in currencies_df.columns:
        print(f"⚠️ {pct_col} not in currencies_df")
        return None

    cur = currencies_df[[pct_col]].copy()
    cur.index = pd.to_datetime(cur.index)

    merged = pd.merge(events_df, cur, left_on="DATE", right_index=True, how="inner")
    if merged.empty:
        print("⚠️ No overlap between events and currency data")
        return None

    merged["Impact"] = merged[pct_col].apply(lambda x: "Strengthen" if x > 0 else "Weaken")

    agg = (
        merged.groupby(["COUNTTYPE", "Impact"])
        .size()
        .reset_index(name="Count")
    )

    fig = px.treemap(
        agg,
        path=["Impact", "COUNTTYPE"],
        values="Count",
        color="Impact",
        color_discrete_map={"Strengthen": "green", "Weaken": "red"},
        title=f"Events Impacting {currency}"
    )
    return fig


# ====================================================
# Function 2: USD Correlation / Covariance Analysis
# ====================================================
def analyze_usd_relationships(currencies_df, method="corr"):
    pct_cols = [c for c in currencies_df.columns if c.endswith("_pctchg")]
    df_pct = currencies_df[pct_cols].dropna()

    if "USD_pctchg" not in df_pct.columns:
        print("⚠️ USD_pctchg column not found")
        return None

    if method == "corr":
        results = df_pct.corr()["USD_pctchg"].drop("USD_pctchg")
    elif method == "cov":
        results = df_pct.cov()["USD_pctchg"].drop("USD_pctchg")
    else:
        raise ValueError("method must be 'corr' or 'cov'")

    return results.sort_values(ascending=False)


# ====================================================
# Visuals Execution
# ====================================================
if __name__ == "__main__":

    # --------------------------
    # Example 1: Treemap (EUR)
    # --------------------------
    fig1 = build_currency_event_treemap("data/events/*/*.parquet", curr_df, currency="EUR")
    if fig1:
        fig1.write_html("assets/treemap_eur.html")
        fig1.write_image("assets/treemap_eur.png")

    # --------------------------
    # Example 2: Currency Trends
    # --------------------------
    fig2 = go.Figure()
    for cur in ["EUR", "GBP", "JPY", "INR"]:
        if cur in curr_df.columns:
            fig2.add_trace(go.Scatter(x=curr_df.index, y=curr_df[cur], mode="lines", name=cur))
    fig2.update_layout(title="Currency Trends vs USD (Sample)")
    fig2.write_html("assets/currency_trends.html")
    fig2.write_image("assets/currency_trends.png")

    # --------------------------
    # Example 3: Correlation Heatmap
    # --------------------------
    pct_cols = [c for c in curr_df.columns if "_pctchg" in c]
    if pct_cols:
        corr = curr_df[pct_cols].corr()
        fig3 = px.imshow(corr, text_auto=True, title="Correlation of Currency % Changes")
        fig3.write_html("assets/corr_heatmap.html")
        fig3.write_image("assets/corr_heatmap.png")

    # --------------------------
    # Example 4: Correlation/Independence vs USD
    # --------------------------
    corr_results = analyze_usd_relationships(curr_df, method="corr")
    if corr_results is not None:
        fig4 = px.bar(
            corr_results,
            title="Currency Correlation with USD",
            labels={"value": "Correlation", "index": "Currency"}
        )
        fig4.write_html("assets/usd_corr.html")
        fig4.write_image("assets/usd_corr.png")

    cov_results = analyze_usd_relationships(curr_df, method="cov")
    if cov_results is not None:
        fig5 = px.bar(
            cov_results,
            title="Currency Covariance with USD",
            labels={"value": "Covariance", "index": "Currency"}
        )
        fig5.write_html("assets/usd_cov.html")
        fig5.write_image("assets/usd_cov.png")

    print("✅ All visuals saved to assets/")
