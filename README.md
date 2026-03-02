# ETL Pipeline — Snowflake Data Warehouse

I built this pipeline to move data from a MariaDB database into Snowflake. I used the Sakila movie rental database as the source since it has a good mix of tables — customers, films, rentals, and payments.

## How it works

The pipeline runs in three steps.

First, `extract.py` connects to MariaDB using SQLAlchemy, pulls all four tables, and saves them as compressed gzip CSV files locally.

Second, `load_raw.py` uploads those files to a Snowflake internal stage using a PUT command and loads them into the RAW schema with COPY INTO.

Third, the data moves through a staging layer where it gets cleaned and keyed, then lands in the final DWH schema with dimension and fact tables ready for reporting.

I structured Snowflake into three schemas: DW_RAW for raw data, DW_STAGE for cleaned data with surrogate keys, and DW_DWH for the final tables.

## Things I was careful about

- Used Snowflake sequences to auto-generate surrogate keys so every record has a unique ID
- Added change tracking logic so existing records get updated instead of duplicated
- Built in deduplication checks so re-running the pipeline is safe
- Stored all credentials in a `.env` file, nothing hardcoded

## Files

- `extract.py` - connects to MariaDB and saves tables as gzip CSV
- `load_raw.py` - uploads files to Snowflake stage and loads RAW tables
- `load_stage.py` - transforms and loads data into the STAGE layer
- `load_dw.py` - merges data into final dimension and fact tables
- `etl_script.sql` - full DDL for all schemas, tables, file formats, and sequences
- `ETL_PROCESS.ipynb` - step-by-step walkthrough notebook

## How to run

You need a MariaDB instance with the Sakila dataset and a Snowflake account.

1. Clone the repo and create a `.env` file in the root:
```
MR_DB_HOST=your_mariadb_host
MR_DB_NAME=sakila
MR_DB_USER=your_user
MR_DB_PASSWORD=your_password
SF_USER=your_snowflake_user
SF_PASSWORD=your_snowflake_password
SF_ACCOUNT=your_snowflake_account
SF_WAREHOUSE=COMPUTE_WH
SF_DATABASE=SAKILA_DW
SF_ROLE=your_role
```
2. Run `etl_script.sql` in Snowflake first to create all schemas and tables
3. Then run the scripts in order:
```bash
python extract.py
python load_raw.py
python load_stage.py
python load_dw.py
```

The `ETL_PROCESS.ipynb` notebook walks through each step in detail if you want to understand what is happening at every stage.

## Stack
Python, SQL, SQLAlchemy, pandas, Snowflake, MariaDB, python-dotenv
