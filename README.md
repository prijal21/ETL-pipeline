# 🏗️ ETL Pipeline — Snowflake Data Warehouse

This is a full ETL pipeline I built to move data from a MariaDB database into Snowflake. I used the Sakila movie rental database as the source — it has customers, films, rentals, and payment data which made it a good real-world-ish dataset to work with.

## ⚙️ How it works

The pipeline runs in three stages:

**1. Extract 📤**
`extract.py` connects to MariaDB using SQLAlchemy, pulls all four tables, and saves them as compressed gzip CSV files locally.

**2. Load to Raw 🗃️**
`load_raw.py` takes those files, uploads them to a Snowflake internal stage with a PUT command, and loads them into the RAW schema using COPY INTO.

**3. Stage → Data Warehouse 🏛️**
Data moves through a staging layer where it gets cleaned and keyed, then lands in the final DWH schema with proper dimension and fact tables.

I structured Snowflake into 3 schemas: `DW_RAW` → `DW_STAGE` → `DW_DWH`.

## 🧠 Things I was careful about

- Auto-generated surrogate keys using Snowflake sequences so every record has a unique ID
- Change tracking logic so existing records get updated instead of duplicated
- Deduplication checks so re-running the pipeline is safe and won't add extra rows
- All credentials stored in a `.env` file — nothing hardcoded 🔒

## 📂 Files

- `extract.py` — pulls data from MariaDB, saves as gzip CSV
- `load_raw.py` — uploads to Snowflake stage, loads RAW tables
- `load_stage.py` — transforms and loads into STAGE layer
- `load_dw.py` — merges into final dimension and fact tables
- `etl_script.sql` — full DDL: schemas, tables, file formats, sequences
- `ETL_PROCESS.ipynb` — step-by-step walkthrough notebook 📓

## 🚀 Run it yourself

You'll need a MariaDB instance with the Sakila dataset and a Snowflake account.

1. Clone the repo and create a `.env` file:
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
2. Run `etl_script.sql` in Snowflake first to set up all schemas and tables
3. Then run in order:
```bash
python extract.py
python load_raw.py
python load_stage.py
python load_dw.py
```

Check `ETL_PROCESS.ipynb` for a full walkthrough of what each step is doing under the hood.

## ⚙️ Stack
Python · SQL · SQLAlchemy · pandas · Snowflake · MariaDB · python-dotenv
