import pandas as pd
import streamlit as st
from snowflake.snowpark.context import get_active_session

st.set_page_config(page_title="Walmart Sales", layout="wide")
session = get_active_session()

DB = "WALMART_PROD.GOLD"   # the dashboard reads production only


@st.cache_data
def load_data():
    query = f"""
        select
            d.full_date,
            d.year,
            s.store_key,
            s.store_type,
            d.is_holiday,
            sum(f.weekly_sales) as total_sales
        from {DB}.fct_weekly_sales f
        join {DB}.dim_date  d on f.date_key  = d.date_key
        join {DB}.dim_store s on f.store_key = s.store_key
        group by 1, 2, 3, 4, 5
    """
    df = session.sql(query).to_pandas()
    df.columns = df.columns.str.lower()
    df["full_date"] = pd.to_datetime(df["full_date"])
    df["total_sales"] = df["total_sales"].astype(float)
    return df


df = load_data()

st.title("Walmart Weekly Sales")

# Sidebar filters
years = sorted(df["year"].unique())
store_types = sorted(df["store_type"].unique())
sel_years = st.sidebar.multiselect("Year", years, default=years)
sel_types = st.sidebar.multiselect("Store type", store_types, default=store_types)

f = df[df["year"].isin(sel_years) & df["store_type"].isin(sel_types)]

if f.empty:
    st.warning("No data for these filters.")
    st.stop()

# Headline numbers
c1, c2, c3 = st.columns(3)
c1.metric("Total sales", f"${f['total_sales'].sum():,.0f}")
c2.metric("Stores", f["store_key"].nunique())
c3.metric("Avg weekly sales per store", f"${f['total_sales'].mean():,.0f}")

# Sales over time
st.subheader("Total sales by week")
st.line_chart(f.groupby("full_date")["total_sales"].sum())

left, right = st.columns(2)

with left:
    st.subheader("Sales by store type")
    st.bar_chart(f.groupby("store_type")["total_sales"].sum())

with right:
    st.subheader("Top 10 stores")
    top = f.groupby("store_key")["total_sales"].sum().nlargest(10)
    top.index = top.index.astype(str)
    st.bar_chart(top)

st.subheader("Holiday vs. non-holiday weeks (average sales per store-week)")
holiday = f.groupby("is_holiday")["total_sales"].mean()
holiday.index = holiday.index.map({True: "Holiday", False: "Non-holiday"})
st.bar_chart(holiday)