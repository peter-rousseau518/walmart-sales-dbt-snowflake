# Walmart Sales Analytics: S3, Snowflake, dbt, Streamlit

An end-to-end analytics pipeline. Raw CSV files in S3 are loaded into Snowflake, modeled into a star schema with dbt, and visualized in a multi-page Streamlit in Snowflake app.

## Architecture

CSV files (S3) -> Snowflake RAW schema -> dbt staging models -> dbt marts (2 dimensions + 1 fact) -> Streamlit app

## What I built

- An S3 bucket and a Snowflake storage integration, with a read-only IAM policy scoped to one folder
- Loaded three CSV files with `COPY INTO`, with column types inferred from the file headers
- A dbt project with 3 staging models, 2 dimensions (`dim_store`, and `dim_date` as a calendar spine) and 2 fact tables, each at its true grain: `fct_weekly_sales` (store × department × week, about 421K rows) and `fct_store_weekly_conditions` (store × week)
- dbt tests: unique, not_null, relationships from each fact to each dimension, grain tests on both facts (`dbt_utils.unique_combination_of_columns` plus a custom singular test), and a test that the calendar's holiday rules match the source holiday flags
- A 10-page Streamlit app covering sales by store, holiday, temperature, store size, store type, month, markdowns, fuel price, CPI, and department

## Data model

| Table | Grain | Rows |
|---|---|---|
| `dim_store` | one row per store | 45 |
| `dim_date` | one row per week (calendar spine, Feb 2010 – Dec 2015) | 308 |
| `fct_weekly_sales` | one row per store, department, and week | 421,570 |
| `fct_store_weekly_conditions` | one row per store and week | 8,190 |

## Data quality decisions

- Weekly conditions (temperature, CPI, fuel price, markdowns) are recorded per store and week, so they live in their own fact table at that grain instead of repeating on every department row. To combine them with sales, aggregate sales to store-week first, then join (the app does this).
- Source values of "NA" in the markdown columns are converted to nulls.
- Grain tests check that no store/department/week (sales) or store/week (conditions) combination appears twice.
- `dim_date` is generated as a calendar spine with retail holiday rules and a `has_sales` flag, so weeks without sales data are never counted by mistake.

## Repository layout

- `models/`: dbt staging and mart models
- `tests/`: custom dbt test
- `snowflake/setup.sql`: S3 stage and load commands (private values replaced with placeholders)
- `streamlit_app/`: the Streamlit in Snowflake app
- `profiles.yml.example`: sample dbt connection profile

## Screenshots

![Screenshot 1](docs/images/shot-1.png)

![Screenshot 2](docs/images/shot-2.png)

![Screenshot 3](docs/images/shot-3.png)

![Screenshot 4](docs/images/shot-4.png)

![Screenshot 5](docs/images/shot-5.png)

## How to run it

1. Create an S3 bucket, upload the CSV files, and run `snowflake/setup.sql`
2. Copy `profiles.yml.example` to `~/.dbt/profiles.yml` and fill in your account, user and
   password. Keep database `WALMART_E2E` and schema `DBT_PETER`: the Streamlit app reads from
   that location (set in `streamlit_app/common.py`). Raw data is read from `DBT_LEARNING.RAW`.
3. Run `pip install dbt-snowflake`, then `dbt deps` (installs dbt_utils) and `dbt build`
4. Create a Streamlit in Snowflake app and paste in the files from `streamlit_app/`

## Data source

[Walmart Recruiting – Store Sales Forecasting (Kaggle)](https://www.kaggle.com/c/walmart-recruiting-store-sales-forecasting)

## Tools

Snowflake, dbt, AWS S3 and IAM, Streamlit, SQL, Python
