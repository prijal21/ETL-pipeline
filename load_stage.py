import os
import snowflake.connector
from snowflake.connector import DictCursor
from dotenv import load_dotenv

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

# 👇 Insert statements with LEFT JOIN to reuse DW surrogate keys
INSERT_QUERIES = {
    "CUSTOMER_STAGE": """
        INSERT INTO DW_STAGE.CUSTOMER_STAGE (
            STAGE_CUSTOMER_SK, CUSTOMER_ID, STORE_ID, FIRST_NAME,
            LAST_NAME, EMAIL, ADDRESS_ID, ACTIVE, CREATE_DATE
        )
        SELECT
            COALESCE(cu.CUSTOMER_SK, DW_DWH.seq_stage_customer_sk.NEXTVAL) as CUSTOMER_SK,
            stg.CUSTOMER_ID as CUSTOMER_ID, 
            stg.STORE_ID as STORE_ID, 
            stg.FIRST_NAME as FIRST_NAME,
            stg.LAST_NAME as LAST_NAME, 
            stg.EMAIL as EMAIL, 
            stg.ADDRESS_ID as ADDRESS_ID, 
            stg.ACTIVE as ACTIVE,
            stg.CREATE_DATE CREATE_DATE
        FROM DW_RAW.CUSTOMER stg
        LEFT JOIN DW_DWH.DIM_CUSTOMER cu
          ON stg.CUSTOMER_ID = cu.CUSTOMER_ID
         AND cu.IS_CURRENT = TRUE;
    """,

    "FILM_STAGE": """
        INSERT INTO DW_STAGE.FILM_STAGE (
            STAGE_FILM_SK, FILM_ID, TITLE, DESCRIPTION, RELEASE_YEAR,
            LANGUAGE_ID, ORIGINAL_LANGUAGE_ID, RENTAL_DURATION, RENTAL_RATE,
            LENGTH, REPLACEMENT_COST, RATING, SPECIAL_FEATURES
        )
                 SELECT
            COALESCE(FM.FILM_SK, DW_DWH.seq_stage_film_sk.NEXTVAL) as FILM_SK,
            stg.FILM_ID, stg.TITLE, stg.DESCRIPTION, stg.RELEASE_YEAR,
            stg.LANGUAGE_ID, stg.ORIGINAL_LANGUAGE_ID, stg.RENTAL_DURATION,
            stg.RENTAL_RATE, stg.LENGTH, stg.REPLACEMENT_COST, stg.RATING,
            stg.SPECIAL_FEATURES
        FROM DW_RAW.FILM stg
        LEFT JOIN DW_DWH.DIM_FILM FM
          ON stg.FILM_ID = FM.FILM_ID
         AND FM.IS_CURRENT = TRUE;
    """,

    "RENTAL_STAGE": """
        INSERT INTO DW_STAGE.RENTAL_STAGE (
            STAGE_RENTAL_SK, RENTAL_ID, RENTAL_DATE, INVENTORY_ID,
            CUSTOMER_ID, RETURN_DATE, STAFF_ID
        )
                 SELECT
            COALESCE(RE.RENTAL_SK, DW_DWH.seq_stage_rental_sk.NEXTVAL) as RENTAL_SK,
            stg.RENTAL_ID, stg.RENTAL_DATE, stg.INVENTORY_ID,
            stg.CUSTOMER_ID, stg.RETURN_DATE, stg.STAFF_ID
        FROM DW_RAW.RENTAL stg
        LEFT JOIN DW_DWH.FACT_RENTAL RE
          ON stg.RENTAL_ID = RE.RENTAL_ID;
    """,

    "PAYMENT_STAGE": """
        INSERT INTO DW_STAGE.PAYMENT_STAGE (
            STAGE_PAYMENT_SK, PAYMENT_ID, CUSTOMER_ID, STAFF_ID,
            RENTAL_ID, AMOUNT, PAYMENT_DATE
        )
                  SELECT
            COALESCE(dw.PAYMENT_SK, DW_DWH.seq_stage_payment_sk.NEXTVAL) as PAYMENT_SK,
            stg.PAYMENT_ID, stg.CUSTOMER_ID, stg.STAFF_ID,
            stg.RENTAL_ID, stg.AMOUNT, stg.PAYMENT_DATE
        FROM DW_RAW.PAYMENT stg
        LEFT JOIN DW_DWH.FACT_PAYMENT dw
          ON stg.PAYMENT_ID = dw.PAYMENT_ID;
    """
}

def load_stage_table(cursor, stage_table, insert_sql):
    print(f"Truncating STAGE.{stage_table}...")
    cursor.execute(f"TRUNCATE TABLE DW_STAGE.{stage_table}")

    print(f"Loading data into DW_STAGE.{stage_table}...")
    cursor.execute(insert_sql)
    print(f"Inserted {cursor.rowcount} rows into DW_STAGE.{stage_table}.\n")



def main():
    try:
        conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        cursor = conn.cursor(DictCursor)
        print(INSERT_QUERIES.items())
        for table_name, insert_sql in INSERT_QUERIES.items():
            load_stage_table(cursor, table_name, insert_sql)

        print("All STAGE tables loaded successfully.")

    except Exception as e:
        print(f"Error during stage load: {e}")

    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()