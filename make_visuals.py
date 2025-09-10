import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import glob, os

# --------------------------
# Load currencies
# --------------------------
curr_path = "data/currencies/USD_Exchange_Rates.xlsx"
curr_df = pd.read_excel(curr_path, index_col=0)
curr_df.index = pd.to_datetime(curr_df.index)

# --------------------------
# Load events (all parquet files)
# --------------------------
files = glob.glob("data/events/*/*.parquet")
events_dfs = [pd.read_parquet(f) for f in files]
events_df = pd.concat(events_dfs, ignore_index=True) if files else pd.DataFrame()

# --------------------------
# Example 1: Treemap
# --------------------------
if not events_df.empty and "Country" in events_df and "Event" in events_df:
    # count events by country
    event_counts = events_df.groupby("Country").size().reset_index(name="count")
    fig1 = px.treemap(event_counts, path=["Country"], values="count",
                      title="Events by Country")
    fig1.write_html("assets/treemap_events.html")
    fig1.write_image("assets/treemap_events.png")

# --------------------------
# Example 2: Currency Trends
# --------------------------
fig2 = go.Figure()
for cur in ["EUR","GBP","JPY","INR"]:
    fig2.add_trace(go.Scatter(x=curr_df.index, y=curr_df[cur], mode="lines", name=cur))
fig2.update_layout(title="Currency Trends vs USD (sample)")
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

print("✅ Visuals saved to assets/")
