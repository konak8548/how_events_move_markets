# app.py
import streamlit as st
import pandas as pd
import glob
import plotly.express as px
import plotly.graph_objects as go

# --------------------------
# Data Loaders
# --------------------------
@st.cache_data
def load_all_event_data():
    files = glob.glob("data/events/*.parquet")
    if not files:
        return pd.DataFrame()
    dfs = [pd.read_parquet(f) for f in files]
    df = pd.concat(dfs, ignore_index=True).sort_values("Date")
    df["Date"] = pd.to_datetime(df["Date"])
    return df

@st.cache_data
def load_all_currency_data():
    files = glob.glob("data/currencies/*.parquet")
    if not files:
        return pd.DataFrame()
    dfs = [pd.read_parquet(f) for f in files]
    df = pd.concat(dfs, ignore_index=True).sort_values("Date")
    df["Date"] = pd.to_datetime(df["Date"])  # ensure datetime for plotting
    return df

# --------------------------
# Streamlit App
# --------------------------
st.set_page_config(page_title="Events & Currencies Dashboard", layout="wide")
st.title("🌍 Global Events & Currency Trends Dashboard")

# Load data
events_df = load_all_event_data()
currencies_df = load_all_currency_data()

# --------------------------
# Sidebar filters
# --------------------------
st.sidebar.header("Filters")

# Events filters
if not events_df.empty:
    years = sorted(events_df["Date"].dt.year.unique())
    selected_year = st.sidebar.selectbox("Select Year", years, index=len(years)-1)
    selected_country = st.sidebar.text_input("Filter by Country (optional)", "")
    selected_keyword = st.sidebar.text_input("Search Keyword in Events", "")
else:
    st.sidebar.warning("No Events Data Available.")
    selected_year = None
    selected_country = ""
    selected_keyword = ""

# Currency filters
if not currencies_df.empty:
    currency_cols = [c for c in currencies_df.columns if c in [
        "EUR","GBP","JPY","CAD","AUD","CHF","CNY","INR","NZD","SEK",
        "NOK","DKK","ZAR","BRL","MXN","SGD","HKD","KRW","TRY","THB",
        "TWD","RUB"
    ]]
    selected_currency = st.sidebar.selectbox("Select Currency", currency_cols, index=0)
else:
    st.sidebar.warning("No Currency Data Available.")
    selected_currency = None

# --------------------------
# Events Visualization
# --------------------------
st.subheader("📌 Event Trends")

if not events_df.empty and selected_year:
    df_year = events_df[events_df["Date"].dt.year == selected_year]

    if selected_country:
        df_year = df_year[df_year["Country"].str.contains(selected_country, case=False, na=False)]

    if selected_keyword:
        df_year = df_year[df_year["Event"].str.contains(selected_keyword, case=False, na=False)]

    if not df_year.empty:
        event_counts = df_year.groupby(df_year["Date"].dt.to_period("M")).size().reset_index(name="Event Count")
        event_counts["Date"] = event_counts["Date"].dt.to_timestamp()

        fig = px.bar(event_counts, x="Date", y="Event Count", title=f"Events in {selected_year}")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No events found for the selected filters.")
else:
    st.info("Events dataset not available.")

# --------------------------
# Currency Visualization
# --------------------------
st.subheader("💱 Currency Trends")

if not currencies_df.empty and selected_currency:
    fig = go.Figure()

    # Price line
    fig.add_trace(go.Scatter(
        x=currencies_df["Date"],
        y=currencies_df[selected_currency],
        mode="lines",
        name=selected_currency
    ))

    # % Change bar
    pct_col = f"{selected_currency}_pctchg"
    if pct_col in currencies_df.columns:
        fig.add_trace(go.Bar(
            x=currencies_df["Date"],
            y=currencies_df[pct_col],
            name=f"{selected_currency} % Change",
            yaxis="y2",
            opacity=0.5
        ))

    fig.update_layout(
        title=f"{selected_currency} Exchange Rate vs USD",
        xaxis=dict(title="Date"),
        yaxis=dict(title=f"{selected_currency} Price"),
        yaxis2=dict(title="% Change", overlaying="y", side="right"),
        legend=dict(orientation="h", y=-0.3)
    )

    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("Currency dataset not available.")
