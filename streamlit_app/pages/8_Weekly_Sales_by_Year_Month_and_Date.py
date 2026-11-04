import streamlit as st
from common import load_weekly, year_filter

st.title("Weekly Sales by Year, Month and Date")
df = year_filter(load_weekly())

weekly = (df.groupby(["full_date", "year", "month_label"], as_index=False)
            ["weekly_sales"].sum())

p = weekly.pivot_table(index="month_label", columns="year",
                       values="weekly_sales", aggfunc="mean")
p.columns = p.columns.astype(str)
st.subheader("Average weekly sales (all stores) by month and year")
st.bar_chart(p)

st.subheader("Weekly sales (all stores) by date")
st.line_chart(weekly.set_index("full_date")["weekly_sales"])

st.subheader("Detail")
st.dataframe(weekly.sort_values("full_date"))
