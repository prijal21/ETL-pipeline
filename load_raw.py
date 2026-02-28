import os
import snowflake.connector
from snowflake.connector import DictCursor
from dotenv import load_dotenv
import platform

load_dotenv()
# ❄️ Snowflake connection config
SNOWFLAKE_CONFIG = {
    "user": os.getenv("SF_USER"),
    "password": os.getenv("SF_PASSWORD"),
    "account": os.getenv("SF_ACCOUNT"),
    "warehouse": os.getenv("SF_WAREHOUSE"),
    "database": os.getenv("SF_DATABASE"),
    "schema": "DW_RAW",
    "role": os.getenv("SF_ROLE")
}
print(SNOWFLAKE_CONFIG)
# Folder containing the compressed .csv files
DATA_FOLDER = "data_extract"
STAGE_NAME = "@DW_RAW.file_stage"  # Internal stage already created with gzip file format
TABLE_MAPPING = {
    "customer.csv.gz": "customer",
    "film.csv.gz": "film",
    "rental.csv.gz": "rental",
    "payment.csv.gz": "payment"
}


def upload_to_stage(cursor, file_path):
    abs_path = os.path.abspath(file_path)

    # Normalize Windows paths
    if platform.system() == "Windows":
        abs_path = abs_path.replace("\\", "/")  # Convert backslashes to slashes

    put_query = f"""
        PUT 'file://{abs_path}' {STAGE_NAME}
        AUTO_COMPRESS = FALSE
        OVERWRITE = TRUE;
    """
    print(put_query)
    print(f"Uploading {file_path} to {STAGE_NAME}")
    cursor.execute(put_query)
    print(cursor.fetchone())


def load_to_raw(cursor, filename, target_table):
    staged_file = f"{STAGE_NAME}/{filename}"
    # Truncating the Target Table First
    print(f"TRUNCATE TABLE SAKILA_DW.{target_table}")
    cursor.execute(f"TRUNCATE TABLE SAKILA_DW.{target_table};")
    copy_query = f"""
        COPY INTO SAKILA_DW.{target_table}
        FROM {staged_file}
        FILE_FORMAT = (FORMAT_NAME = 'DW_RAW.file_format_csv_gzip')
        ON_ERROR = 'CONTINUE';
    """
    print(copy_query)
    print(f"Loading {filename} into DW_RAW.{target_table}...")
    cursor.execute(copy_query)
    result = cursor.fetchall()
    print("Rows loaded:", result)


def main():
    try:
        # Connect to Snowflake
        conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        cursor = conn.cursor(DictCursor)

        # List all gzip files in the data folder
        for filename in os.listdir(DATA_FOLDER):
            if filename.endswith(".csv.gz") and filename in TABLE_MAPPING:
                file_path = os.path.join(DATA_FOLDER, filename)
                table_name = f"DW_RAW.{TABLE_MAPPING[filename].upper()}"
                # Step 1: Upload file to stage
                upload_to_stage(cursor, file_path)
                # Step 2: Load file into RAW table
                load_to_raw(cursor, filename, table_name)

            print("✅ All files loaded into RAW schema successfully.")

    except Exception as e:
        print(f"❌ Error: {e}")

    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()
