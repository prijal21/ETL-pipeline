import os
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()
# ❄️ Snowflake connection config
SNOWFLAKE_CONFIG = {
    "user": os.getenv("SF_USER"),
    "password": os.getenv("SF_PASSWORD"),
    "account": os.getenv("SF_ACCOUNT"),
    "warehouse": os.getenv("SF_WAREHOUSE"),
    "database": os.getenv("SF_DATABASE"),
    "schema": "DW_DWH",
    "role": os.getenv("SF_ROLE")
}

MERGE_DIM_CUSTOMER = """
MERGE INTO DIM_CUSTOMER tgt
USING DW_STAGE.CUSTOMER_STAGE src
ON tgt.CUSTOMER_ID = src.CUSTOMER_ID AND tgt.IS_CURRENT = TRUE
WHEN MATCHED AND (
    tgt.STORE_ID <> src.STORE_ID OR
    tgt.FIRST_NAME <> src.FIRST_NAME OR
    tgt.LAST_NAME <> src.LAST_NAME OR
    tgt.EMAIL <> src.EMAIL OR
    tgt.ADDRESS_ID <> src.ADDRESS_ID OR
    tgt.ACTIVE <> src.ACTIVE OR
    tgt.CREATE_DATE <> src.CREATE_DATE
) THEN
    UPDATE SET END_DATE = CURRENT_TIMESTAMP(), IS_CURRENT = FALSE

WHEN NOT  MATCHED  THEN
    INSERT (
        CUSTOMER_SK, CUSTOMER_ID, STORE_ID, FIRST_NAME, LAST_NAME,
        EMAIL, ADDRESS_ID, ACTIVE, CREATE_DATE, START_DATE, END_DATE, IS_CURRENT
    )
    VALUES (
        DW_DWH.seq_stage_customer_sk.NEXTVAL, src.CUSTOMER_ID, src.STORE_ID, src.FIRST_NAME, src.LAST_NAME,
        src.EMAIL, src.ADDRESS_ID, src.ACTIVE, src.CREATE_DATE, CURRENT_TIMESTAMP(), NULL, TRUE
    );
"""

MERGE_DIM_FILM = """
MERGE INTO DIM_FILM tgt
USING DW_STAGE.FILM_STAGE src
ON tgt.FILM_ID = src.FILM_ID AND tgt.IS_CURRENT = TRUE

WHEN MATCHED AND (
    tgt.TITLE <> src.TITLE OR
    tgt.DESCRIPTION <> src.DESCRIPTION OR
    tgt.RELEASE_YEAR <> src.RELEASE_YEAR OR
    tgt.LANGUAGE_ID <> src.LANGUAGE_ID OR
    tgt.ORIGINAL_LANGUAGE_ID <> src.ORIGINAL_LANGUAGE_ID OR
    tgt.RENTAL_DURATION <> src.RENTAL_DURATION OR
    tgt.RENTAL_RATE <> src.RENTAL_RATE OR
    tgt.LENGTH <> src.LENGTH OR
    tgt.REPLACEMENT_COST <> src.REPLACEMENT_COST OR
    tgt.RATING <> src.RATING OR
    tgt.SPECIAL_FEATURES <> src.SPECIAL_FEATURES
) THEN
    UPDATE SET END_DATE = CURRENT_TIMESTAMP(), IS_CURRENT = FALSE

WHEN NOT MATCHED THEN
    INSERT (
        FILM_SK, FILM_ID, TITLE, DESCRIPTION, RELEASE_YEAR,
        LANGUAGE_ID, ORIGINAL_LANGUAGE_ID, RENTAL_DURATION, RENTAL_RATE,
        LENGTH, REPLACEMENT_COST, RATING, SPECIAL_FEATURES,
        START_DATE, END_DATE, IS_CURRENT
    )
    VALUES (
        DW_DWH.seq_stage_film_sk.NEXTVAL, src.FILM_ID, src.TITLE, src.DESCRIPTION, src.RELEASE_YEAR,
        src.LANGUAGE_ID, src.ORIGINAL_LANGUAGE_ID, src.RENTAL_DURATION, src.RENTAL_RATE,
        src.LENGTH, src.REPLACEMENT_COST, src.RATING, src.SPECIAL_FEATURES,
        CURRENT_TIMESTAMP(), NULL, TRUE
    );
"""

INSERT_DIM_CUSTOMER = """
            INSERT INTO DW_DWH.DIM_CUSTOMER (
                customer_sk,
                customer_id,
                first_name,
                last_name,
                email,
                start_date,
                end_date,
                is_current
            )
            SELECT
                s.STAGE_CUSTOMER_SK,
                s.customer_id,
                s.first_name,
                s.last_name,
                s.email,
                CURRENT_DATE,
                NULL,
                TRUE
            FROM DW_STAGE.CUSTOMER_STAGE s
            LEFT JOIN DW_DWH.DIM_CUSTOMER d
              ON s.customer_id = d.customer_id AND d.is_current = TRUE
            WHERE d.customer_id IS NULL OR (
                s.first_name IS DISTINCT FROM d.first_name OR
                s.last_name IS DISTINCT FROM d.last_name OR
                s.email IS DISTINCT FROM d.email
            )
"""

INSERT_DIM_FILM = """
        INSERT INTO DW_DWH.DIM_FILM (
            film_sk,
            film_id,
            title,
            description,
            release_year,
            language_id,
            original_language_id,
            rental_duration,
            rental_rate,
            length,
            replacement_cost,
            rating,
            special_features,
            start_date,
            end_date,
            is_current
        )
        SELECT
            s.STAGE_FILM_SK,
            s.film_id,
            s.title,
            s.description,
            s.release_year,
            s.language_id,
            s.original_language_id,
            s.rental_duration,
            s.rental_rate,
            s.length,
            s.replacement_cost,
            s.rating,
            s.special_features,
            CURRENT_DATE,
            NULL,
            TRUE
        FROM DW_STAGE.FILM_STAGE s
        LEFT JOIN DW_DWH.DIM_FILM d
          ON s.film_id = d.film_id AND d.is_current = TRUE
        WHERE d.film_id IS NULL OR (
            s.title IS DISTINCT FROM d.title OR
            s.description IS DISTINCT FROM d.description OR
            s.release_year IS DISTINCT FROM d.release_year OR
            s.language_id IS DISTINCT FROM d.language_id OR
            s.original_language_id IS DISTINCT FROM d.original_language_id OR
            s.rental_duration IS DISTINCT FROM d.rental_duration OR
            s.rental_rate IS DISTINCT FROM d.rental_rate OR
            s.length IS DISTINCT FROM d.length OR
            s.replacement_cost IS DISTINCT FROM d.replacement_cost OR
            s.rating IS DISTINCT FROM d.rating OR
            s.special_features IS DISTINCT FROM d.special_features
        );
    """

INSERT_FACT_RENTAL = """
INSERT INTO FACT_RENTAL (
    RENTAL_SK, RENTAL_ID, CUSTOMER_SK, FILM_SK,
    INVENTORY_ID, STAFF_ID, RENTAL_DATE, RETURN_DATE, LOAD_DATETIME
)
SELECT
    DW_DWH.seq_stage_rental_sk.NEXTVAL,
    rs.RENTAL_ID,
    dc.CUSTOMER_SK,
    df.FILM_SK,
    rs.INVENTORY_ID,
    rs.STAFF_ID,
    rs.RENTAL_DATE,
    rs.RETURN_DATE,
    CURRENT_TIMESTAMP()
FROM DW_STAGE.RENTAL_STAGE rs
JOIN DIM_CUSTOMER dc ON dc.CUSTOMER_ID = rs.CUSTOMER_ID AND dc.IS_CURRENT = TRUE
JOIN DIM_FILM df ON df.FILM_ID = (
    SELECT FILM_ID FROM DW_STAGE.FILM_STAGE LIMIT 1
)
WHERE rs.RENTAL_ID NOT IN (SELECT RENTAL_ID FROM FACT_RENTAL);
"""

INSERT_FACT_PAYMENT = """
INSERT INTO FACT_PAYMENT (
    PAYMENT_SK, PAYMENT_ID, CUSTOMER_SK, FILM_SK, RENTAL_ID,
    STAFF_ID, AMOUNT, PAYMENT_DATE, LOAD_DATETIME
)
SELECT
    DW_DWH.seq_stage_payment_sk.nextval,
    pmt.PAYMENT_ID,
    dc.CUSTOMER_SK,
    df.FILM_SK,
    pmt.RENTAL_ID,
    pmt.STAFF_ID,
    pmt.AMOUNT,
    pmt.PAYMENT_DATE,
    CURRENT_TIMESTAMP()
FROM DW_STAGE.PAYMENT_STAGE pmt
JOIN DIM_CUSTOMER dc ON dc.CUSTOMER_ID = pmt.CUSTOMER_ID AND dc.IS_CURRENT = TRUE
LEFT JOIN FACT_RENTAL fr ON fr.RENTAL_ID = pmt.RENTAL_ID
LEFT JOIN DIM_FILM df ON df.FILM_SK = fr.FILM_SK
WHERE pmt.PAYMENT_ID NOT IN (SELECT PAYMENT_ID FROM FACT_PAYMENT);
"""

def main():
    try:
        conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        cursor = conn.cursor()

        print("Merging DIM_CUSTOMER...")
        cursor.execute(MERGE_DIM_CUSTOMER)
        cursor.execute(INSERT_DIM_CUSTOMER)
        print("DIM_CUSTOMER merge complete.")

        print("Merging DIM_FILM...")
        cursor.execute(MERGE_DIM_FILM)
        cursor.execute(INSERT_DIM_FILM)
        print("DIM_FILM merge complete.")

        print("Loading FACT_RENTAL...")
        cursor.execute(INSERT_FACT_RENTAL)
        print("FACT_RENTAL load complete.")

        print("Loading FACT_PAYMENT...")
        cursor.execute(INSERT_FACT_PAYMENT)
        print("FACT_PAYMENT load complete.")

    except Exception as e:
        print(f"ETL process failed: {e}")

    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    main()
