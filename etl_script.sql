-- Create the Database if not exists
create database if not exists SAKILA_DW;
-- Use the Created Database
use database SAKILA_DW;


USE WAREHOUSE COMPUTE_WH;

-- Create schemas for each stage of the pipeline
CREATE SCHEMA IF NOT EXISTS DW_RAW;
CREATE SCHEMA IF NOT EXISTS DW_STAGE;
CREATE SCHEMA IF NOT EXISTS DW_DWH;


-- Create a File Format
CREATE OR REPLACE FILE FORMAT DW_RAW.file_format_csv_gzip
  TYPE = 'CSV'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1
  COMPRESSION = 'GZIP'
  NULL_IF = ('\\N', 'NULL', '');

-- Create a named file stage at database level
CREATE OR REPLACE STAGE DW_RAW.file_stage
  FILE_FORMAT = DW_RAW.file_format_csv_gzip
  COMMENT = 'Stage for loading gzipped CSVs from Sakila Extracts';


-- Create the RAW Tables
CREATE OR REPLACE TABLE DW_RAW.CUSTOMER (
    customer_id         NUMBER,
    store_id            NUMBER,
    first_name          STRING,
    last_name           STRING,
    email               STRING,
    address_id          NUMBER,
    active              BOOLEAN,
    create_date         TIMESTAMP_NTZ,
    last_update         TIMESTAMP_NTZ,
    PRIMARY KEY (customer_id)
);

CREATE OR REPLACE TABLE DW_RAW.FILM (
    film_id              NUMBER,                        -- smallint unsigned
    title                STRING NOT NULL,               -- varchar(128)
    description          STRING,                        -- text
    release_year         NUMBER(4, 0),                  -- year(4)
    language_id          NUMBER,                        -- tinyint unsigned
    original_language_id NUMBER,                        -- tinyint unsigned
    rental_duration      NUMBER,                        -- tinyint unsigned
    rental_rate          NUMBER(4, 2),                  -- decimal(4,2)
    length               NUMBER,                        -- smallint unsigned
    replacement_cost     NUMBER(5, 2),                  -- decimal(5,2)
    rating               STRING,                        -- enum
    special_features     STRING,                        -- set
    last_update          TIMESTAMP_NTZ,                -- timestamp
    PRIMARY KEY (film_id)
);




CREATE OR REPLACE TABLE DW_RAW.RENTAL (
    rental_id      NUMBER,              -- int(11)
    rental_date    TIMESTAMP_NTZ,       -- datetime
    inventory_id   NUMBER,              -- mediumint unsigned
    customer_id    NUMBER,              -- smallint unsigned
    return_date    TIMESTAMP_NTZ,       -- datetime
    staff_id       NUMBER,              -- tinyint unsigned
    last_update    TIMESTAMP_NTZ,       -- timestamp

    PRIMARY KEY (rental_id)
);

CREATE OR REPLACE TABLE DW_RAW.PAYMENT (
    payment_id     NUMBER,             -- smallint unsigned
    customer_id    NUMBER,             -- smallint unsigned
    staff_id       NUMBER,             -- tinyint unsigned
    rental_id      NUMBER,             -- int(11)
    amount         NUMBER(5, 2),       -- decimal(5,2)
    payment_date   TIMESTAMP_NTZ,      -- datetime
    last_update    TIMESTAMP_NTZ,      -- timestamp

    PRIMARY KEY (payment_id)
);

LIST @dw_raw.file_stage;


-- Customer surrogate key sequence
CREATE OR REPLACE SEQUENCE DW_STAGE.customer_sk_seq START = 1 INCREMENT = 1;
-- Film surrogate key sequence
CREATE OR REPLACE SEQUENCE DW_STAGE.film_sk_seq START = 1 INCREMENT = 1;


select * from DW_RAW.CUSTOMER;

-- DDL For the Stage Table

CREATE OR REPLACE TABLE SAKILA_DW.DW_STAGE.CUSTOMER_STAGE (
    STAGE_CUSTOMER_SK NUMBER PRIMARY KEY,
    CUSTOMER_ID NUMBER,
    STORE_ID NUMBER,
    FIRST_NAME STRING,
    LAST_NAME STRING,
    EMAIL STRING,
    ADDRESS_ID NUMBER,
    ACTIVE BOOLEAN,
    CREATE_DATE TIMESTAMP_NTZ,
    LAST_UPDATE TIMESTAMP_NTZ
);

CREATE OR REPLACE TABLE SAKILA_DW.DW_STAGE.FILM_STAGE (
    STAGE_FILM_SK NUMBER PRIMARY KEY,
    FILM_ID NUMBER,
    TITLE STRING,
    DESCRIPTION STRING,
    RELEASE_YEAR NUMBER(4,0),
    LANGUAGE_ID NUMBER,
    ORIGINAL_LANGUAGE_ID NUMBER,
    RENTAL_DURATION NUMBER,
    RENTAL_RATE NUMBER(5,2),
    LENGTH NUMBER,
    REPLACEMENT_COST NUMBER(6,2),
    RATING STRING,
    SPECIAL_FEATURES STRING,
    LAST_UPDATE TIMESTAMP_NTZ
);

CREATE OR REPLACE TABLE SAKILA_DW.DW_STAGE.RENTAL_STAGE (
    STAGE_RENTAL_SK NUMBER PRIMARY KEY,
    RENTAL_ID NUMBER,
    RENTAL_DATE TIMESTAMP_NTZ,
    INVENTORY_ID NUMBER,
    CUSTOMER_ID NUMBER,
    RETURN_DATE TIMESTAMP_NTZ,
    STAFF_ID NUMBER,
    LAST_UPDATE TIMESTAMP_NTZ
);

CREATE OR REPLACE TABLE SAKILA_DW.DW_STAGE.PAYMENT_STAGE (
    STAGE_PAYMENT_SK NUMBER PRIMARY KEY,
    PAYMENT_ID NUMBER,
    CUSTOMER_ID NUMBER,
    STAFF_ID NUMBER,
    RENTAL_ID NUMBER,
    AMOUNT NUMBER(6,2),
    PAYMENT_DATE TIMESTAMP_NTZ,
    LAST_UPDATE TIMESTAMP_NTZ
);



CREATE OR REPLACE TABLE SAKILA_DW.DW_DWH.DIM_CUSTOMER (
    CUSTOMER_SK NUMBER PRIMARY KEY,
    CUSTOMER_ID NUMBER,
    STORE_ID NUMBER,
    FIRST_NAME STRING,
    LAST_NAME STRING,
    EMAIL STRING,
    ADDRESS_ID NUMBER,
    ACTIVE BOOLEAN,
    CREATE_DATE TIMESTAMP_NTZ,
    START_DATE TIMESTAMP_NTZ,
    END_DATE TIMESTAMP_NTZ,
    IS_CURRENT BOOLEAN
);



CREATE OR REPLACE TABLE SAKILA_DW.DW_DWH.DIM_FILM (
    FILM_SK NUMBER PRIMARY KEY,
    FILM_ID NUMBER,
    TITLE STRING,
    DESCRIPTION STRING,
    RELEASE_YEAR NUMBER(4,0),
    LANGUAGE_ID NUMBER,
    ORIGINAL_LANGUAGE_ID NUMBER,
    RENTAL_DURATION NUMBER,
    RENTAL_RATE NUMBER(5,2),
    LENGTH NUMBER,
    REPLACEMENT_COST NUMBER(6,2),
    RATING STRING,
    SPECIAL_FEATURES STRING,
    START_DATE TIMESTAMP_NTZ,
    END_DATE TIMESTAMP_NTZ,
    IS_CURRENT BOOLEAN
);



CREATE OR REPLACE TABLE SAKILA_DW.DW_DWH.FACT_PAYMENT (
    PAYMENT_SK NUMBER PRIMARY KEY,
    PAYMENT_ID NUMBER,
    CUSTOMER_SK NUMBER,
    FILM_SK NUMBER,
    RENTAL_ID NUMBER,
    STAFF_ID NUMBER,
    AMOUNT NUMBER(6,2),
    PAYMENT_DATE TIMESTAMP_NTZ,
    LOAD_DATETIME TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);


CREATE OR REPLACE TABLE SAKILA_DW.DW_DWH.FACT_RENTAL (
    RENTAL_SK NUMBER PRIMARY KEY,
    RENTAL_ID NUMBER,
    CUSTOMER_SK NUMBER,
    FILM_SK NUMBER,
    INVENTORY_ID NUMBER,
    STAFF_ID NUMBER,
    RENTAL_DATE TIMESTAMP_NTZ,
    RETURN_DATE TIMESTAMP_NTZ,
    LOAD_DATETIME TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE SEQUENCE DW_DWH.seq_stage_customer_sk start=1 increment=1;
CREATE OR REPLACE SEQUENCE DW_DWH.seq_stage_film_sk start=1 increment=1;
CREATE OR REPLACE SEQUENCE DW_DWH.seq_stage_rental_sk start=1 increment=1;
CREATE OR REPLACE SEQUENCE DW_DWH.seq_stage_payment_sk start=1 increment=1;



        SELECT
            COALESCE(cu.CUSTOMER_SK, DW.seq_stage_customer_sk.NEXTVAL) as CUSTOMER_SK,
            stg.CUSTOMER_ID as CUSTOMER_ID, 
            stg.STORE_ID as STORE_ID, 
            stg.FIRST_NAME as FIRST_NAME,
            stg.LAST_NAME as LAST_NAME, 
            stg.EMAIL as EMAIL, 
            stg.ADDRESS_ID as ADDRESS_ID, 
            stg.ACTIVE as ACTIVE,
            stg.CREATE_DATE CREATE_DATE
        FROM RAW.CUSTOMER stg
        LEFT JOIN DW.DIM_CUSTOMER cu
          ON stg.CUSTOMER_ID = cu.CUSTOMER_ID
         AND cu.IS_CURRENT = TRUE;

                 SELECT
            COALESCE(FM.FILM_SK, DW.seq_stage_film_sk.NEXTVAL) as FILM_SK,
            stg.FILM_ID, stg.TITLE, stg.DESCRIPTION, stg.RELEASE_YEAR,
            stg.LANGUAGE_ID, stg.ORIGINAL_LANGUAGE_ID, stg.RENTAL_DURATION,
            stg.RENTAL_RATE, stg.LENGTH, stg.REPLACEMENT_COST, stg.RATING,
            stg.SPECIAL_FEATURES
        FROM RAW.FILM stg
        LEFT JOIN DW.DIM_FILM FM
          ON stg.FILM_ID = FM.FILM_ID
         AND FM.IS_CURRENT = TRUE;

                 SELECT
            COALESCE(RE.RENTAL_SK, DW.seq_stage_rental_sk.NEXTVAL) as RENTAL_SK,
            stg.RENTAL_ID, stg.RENTAL_DATE, stg.INVENTORY_ID,
            stg.CUSTOMER_ID, stg.RETURN_DATE, stg.STAFF_ID
        FROM RAW.RENTAL stg
        LEFT JOIN DW.FACT_RENTAL RE
          ON stg.RENTAL_ID = RE.RENTAL_ID;


