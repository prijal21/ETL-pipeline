# ETL Pipeline — Snowflake Data Warehouse

This is an ETL pipeline I built to move data from a MariaDB database into Snowflake. The source is the Sakila movie rental database which has tables for customers, films, rentals, and payments.

## How it works

The pipeline runs in three steps. First, `extract.py` connects to MariaDB using SQLAlchemy, pulls the four tables, and saves each one as a compressed gzip CSV file. Second, `load_raw.py` uploads those files to a Snowflake internal stage using a PUT command and then loads them into the RAW schema with COPY INTO. Third, the data moves through a staging layer before landing in the final data warehouse schema.

I structured Snowflake into three schemas: DW_RAW for the raw data, DW_STAGE for cleaned and keyed data, and DW_DWH for the final tables used for reporting.

## A few things I paid attention to

- Used Snowflake sequences to auto-generate surrogate keys for each record
- Added logic to track changes over time so old records aren't just overwritten
- Built in duplicate prevention so re-running the pipeline doesn't create extra rows
- Stored all credentials in environment variables using python-dotenv

## Files

- `extract.py` - pulls data from MariaDB and saves as gzip CSV
- `load_raw.py` - uploads to Snowflake stage and loads RAW tables
- `load_stage.py` - loads and transforms into the STAGE layer
- `load_dw.py` - merges data into final dimension and fact tables
- `etl_script.sql` - full DDL for all schemas, tables, file formats, and sequences
- `ETL_PROCESS.ipynb` - walkthrough notebook

## Stack
Python, SQL, SQLAlchemy, pandas, Snowflake, MariaDB, python-dotenv
