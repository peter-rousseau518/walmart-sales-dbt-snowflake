import pandas as pd
import streamlit as st
from snowflake.snowpark.context import get_active_session

DB = "WALMART_E2E.DBT_PETER"
MARKDOWNS = ["markdown1", "markdown2", "markdown3", "markdown4", "markdown5"]


@st.cache_data
def load_weekly():
    """One row per store per week."""
    session = get_active_session()
    df = session.sql(f"""
        with sales as (
            -- aggregate sales to store-week BEFORE joining the store-week conditions
            select store_key, date_key, sum(weekly_sales) as weekly_sales
            from {DB}.fct_weekly_sales
            group by 1, 2
        )
        select
            d.full_date, d.year, d.quarter, d.month, d.month_name, d.is_holiday,
            s.store_key, st.store_type, st.store_size,
            s.weekly_sales,
            c.temperature, c.fuel_price, c.cpi, c.unemployment,
            c.markdown1, c.markdown2, c.markdown3, c.markdown4, c.markdown5
        from sales s
        left join {DB}.fct_store_weekly_conditions c
               on c.store_key = s.store_key and c.date_key = s.date_key
        join {DB}.dim_date  d  on s.date_key  = d.date_key
        join {DB}.dim_store st on s.store_key = st.store_key
    """).to_pandas()
    df.columns = df.columns.str.lower()
    df["full_date"] = pd.to_datetime(df["full_date"])
    skip = ("full_date", "store_type", "month_name", "is_holiday")
    num = [c for c in df.columns if c not in skip]
    df[num] = df[num].apply(pd.to_numeric, errors="coerce")
    df["markdown_total"] = df[MARKDOWNS].fillna(0).sum(axis=1)
    df["month_label"] = df["month"].astype(int).astype(str).str.zfill(2)
    return df


@st.cache_data
def load_dept():
    """One row per department per week (all stores combined)."""
    session = get_active_session()
    df = session.sql(f"""
        select f.dept_id, d.full_date, d.year,
               sum(f.weekly_sales) as weekly_sales
        from {DB}.fct_weekly_sales f
        join {DB}.dim_date d on f.date_key = d.date_key
        group by 1, 2, 3
    """).to_pandas()
    df.columns = df.columns.str.lower()
    df["full_date"] = pd.to_datetime(df["full_date"])
    df["weekly_sales"] = pd.to_numeric(df["weekly_sales"], errors="coerce")
    df["dept_id"] = df["dept_id"].astype(int)
    return df


def year_filter(df):
    years = sorted(df["year"].unique())
    chosen = st.sidebar.multiselect("Year", years, default=years)
    out = df[df["year"].isin(chosen)].copy()
    if out.empty:
        st.warning("No data for these filters.")
        st.stop()
    return out


def pad(index, width=2):
    """Zero-pad labels so charts sort them in numeric order."""
    return index.astype(int).astype(str).str.zfill(width)