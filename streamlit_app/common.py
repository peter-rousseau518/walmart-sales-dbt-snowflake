import inspect
import os

import pandas as pd
import streamlit as st
from snowflake.snowpark.context import get_active_session

DB = "WALMART_PROD.GOLD"   # the dashboard reads production only
ISSUES_PROC = "WALMART_PROD.FEEDBACK.REPORT_DATA_ISSUE"
MARKDOWNS = ["markdown1", "markdown2", "markdown3", "markdown4", "markdown5"]


# ---------------------------------------------------------------- freshness and caching
@st.cache_data(ttl=60)
def data_as_of():
    """Time of the last successful dbt build (from GOLD.REFRESH_LOG), checked at most once a minute."""
    try:
        row = get_active_session().sql(f"select max(refreshed_at) as t from {DB}.refresh_log").collect()[0]
        return row["T"]
    except Exception:          # refresh log not built yet: fall back to the 1-hour cache
        return None


# The cached loaders take the refresh time as an argument. When dbt builds new data,
# data_as_of() changes, the cache key changes, and the next page view reloads from Snowflake.
# ttl=3600 is a fallback in case the refresh log can't be read.
def load_weekly():
    return _load_weekly(str(data_as_of()))


def load_dept():
    return _load_dept(str(data_as_of()))


@st.cache_data(ttl=3600)
def _load_weekly(as_of):
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


@st.cache_data(ttl=3600)
def _load_dept(as_of):
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
    sidebar_tools(_calling_page())
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


# ---------------------------------------------------------------- sidebar: data as of + issue form
def _calling_page():
    """Page name from the file that called year_filter, e.g. 'Weekly Sales by CPI'."""
    name = os.path.splitext(os.path.basename(inspect.stack()[2].filename))[0]
    return name.split("_", 1)[-1].replace("_", " ") if name[:1].isdigit() else name.replace("_", " ")


def _viewer():
    """Who is viewing the app (Streamlit in Snowflake provides this); None if unavailable."""
    for attr in ("user", "experimental_user"):
        try:
            info = getattr(st, attr)
            return info.get("user_name") or info.get("email")
        except Exception:
            continue
    return None


def sidebar_tools(page):
    """'Data as of' and a 'Report a data issue' form, shown in the sidebar of every page."""
    as_of = data_as_of()
    st.sidebar.caption(f"Data as of {pd.Timestamp(as_of):%b %d, %Y %H:%M}" if as_of else "Data as of: unknown")
    with st.sidebar.expander("Report a data issue"):
        with st.form("data_issue", clear_on_submit=True):
            text = st.text_area("What looks wrong?", placeholder="e.g. Store 14 sales for March 2012 look too low")
            if st.form_submit_button("Send to data engineering"):
                if not text.strip():
                    st.warning("Please describe the issue.")
                else:
                    issue_id = get_active_session().call(ISSUES_PROC, page, text.strip(), _viewer())
                    st.success(f"Thanks. Issue #{issue_id} logged. We reply within 1 business day.")
