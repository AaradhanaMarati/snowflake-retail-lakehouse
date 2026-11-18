/* =============================================================================
   01_dimensions.sql
   Gold layer dimensions.
     DIM_CUSTOMER - SCD Type 2 (full history of segment / country changes)
     DIM_PRODUCT  - SCD Type 1
     DIM_DATE     - generated calendar
   ========================================================================== */

USE ROLE RETAIL_TRANSFORMER;
USE SCHEMA RETAIL_DB.MARTS;

-- ---------------------------------------------------------------- DIM_DATE
CREATE OR REPLACE TABLE DIM_DATE AS
SELECT
  TO_NUMBER(TO_CHAR(d, 'YYYYMMDD'))   AS date_key,
  d                                   AS calendar_date,
  YEAR(d)                             AS year,
  QUARTER(d)                          AS quarter,
  MONTH(d)                            AS month,
  MONTHNAME(d)                        AS month_name,
  WEEKOFYEAR(d)                       AS week_of_year,
  DAYOFWEEKISO(d)                     AS day_of_week,
  DAYNAME(d)                          AS day_name,
  DAYOFWEEKISO(d) IN (6, 7)           AS is_weekend
FROM (
  SELECT DATEADD('day', SEQ4(), '2024-01-01'::DATE) AS d
  FROM TABLE(GENERATOR(ROWCOUNT => 1096))   -- 2024-01-01 .. 2026-12-31
);

-- ---------------------------------------------------------------- DIM_PRODUCT
CREATE TABLE IF NOT EXISTS DIM_PRODUCT (
  product_key   NUMBER AUTOINCREMENT START 1 INCREMENT 1 PRIMARY KEY,
  product_id    NUMBER NOT NULL UNIQUE,
  product_name  VARCHAR,
  category      VARCHAR,
  list_price    NUMBER(10,2),
  unit_cost     NUMBER(10,2),
  margin_pct    NUMBER(6,4),
  is_active     BOOLEAN,
  updated_at    TIMESTAMP_LTZ
);

-- ---------------------------------------------------------------- DIM_CUSTOMER (SCD2)
CREATE TABLE IF NOT EXISTS DIM_CUSTOMER (
  customer_key   NUMBER AUTOINCREMENT START 1 INCREMENT 1 PRIMARY KEY,
  customer_id    NUMBER NOT NULL,
  full_name      VARCHAR,
  email          VARCHAR,
  country_code   VARCHAR(2),
  segment        VARCHAR,
  signup_date    DATE,
  record_hash    VARCHAR,
  valid_from     TIMESTAMP_NTZ NOT NULL,
  valid_to       TIMESTAMP_NTZ,                 -- NULL = current version
  is_current     BOOLEAN NOT NULL
);

/* ---------------------------------------------------------------------------
   SCD2 load procedure.
   STG_CUSTOMERS is a change log, so history is DERIVED from it:
     - collapse consecutive rows with the same attribute hash (no-op updates)
     - valid_to = next version's updated_at (LEAD)
     - first version starts at 1900-01-01 so orders placed before the first
       snapshot still resolve to a customer in point-in-time joins
   MERGE keyed on (customer_id, valid_from) keeps surrogate keys stable,
   is idempotent, and tolerates late / out-of-order files.
   At larger scale, restrict the source to customer_ids in the latest batch.
   ------------------------------------------------------------------------- */
CREATE OR REPLACE PROCEDURE RETAIL_DB.MARTS.SP_LOAD_DIM_CUSTOMER()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
  MERGE INTO RETAIL_DB.MARTS.DIM_CUSTOMER d
  USING (
    WITH changes AS (
      SELECT *,
             LAG(record_hash) OVER (PARTITION BY customer_id ORDER BY updated_at) AS prev_hash
      FROM RETAIL_DB.STAGING.STG_CUSTOMERS
    ),
    versions AS (
      SELECT * FROM changes
      WHERE prev_hash IS NULL OR prev_hash <> record_hash
    )
    SELECT customer_id,
           first_name || ' ' || last_name AS full_name,
           email, country_code, segment, signup_date, record_hash,
           IFF(ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY updated_at) = 1,
               '1900-01-01'::TIMESTAMP_NTZ, updated_at)                 AS valid_from,
           LEAD(updated_at) OVER (PARTITION BY customer_id ORDER BY updated_at) AS valid_to
    FROM versions
  ) s
  ON d.customer_id = s.customer_id AND d.valid_from = s.valid_from
  WHEN MATCHED AND (d.record_hash <> s.record_hash
                    OR NOT EQUAL_NULL(d.valid_to, s.valid_to)) THEN UPDATE SET
    full_name = s.full_name, email = s.email, country_code = s.country_code,
    segment = s.segment, record_hash = s.record_hash,
    valid_to = s.valid_to, is_current = (s.valid_to IS NULL)
  WHEN NOT MATCHED THEN INSERT
    (customer_id, full_name, email, country_code, segment, signup_date,
     record_hash, valid_from, valid_to, is_current)
    VALUES (s.customer_id, s.full_name, s.email, s.country_code, s.segment,
            s.signup_date, s.record_hash, s.valid_from, s.valid_to, s.valid_to IS NULL);
  RETURN 'DIM_CUSTOMER merged: ' || SQLROWCOUNT;
END;
$$;

CREATE OR REPLACE PROCEDURE RETAIL_DB.MARTS.SP_LOAD_DIM_PRODUCT()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
  MERGE INTO RETAIL_DB.MARTS.DIM_PRODUCT d
  USING RETAIL_DB.STAGING.STG_PRODUCTS s
  ON d.product_id = s.product_id
  WHEN MATCHED THEN UPDATE SET
    product_name = s.product_name, category = s.category,
    list_price = s.list_price, unit_cost = s.unit_cost,
    margin_pct = IFF(s.list_price > 0, (s.list_price - s.unit_cost) / s.list_price, NULL),
    is_active = s.is_active, updated_at = CURRENT_TIMESTAMP()
  WHEN NOT MATCHED THEN INSERT
    (product_id, product_name, category, list_price, unit_cost, margin_pct, is_active, updated_at)
    VALUES (s.product_id, s.product_name, s.category, s.list_price, s.unit_cost,
            IFF(s.list_price > 0, (s.list_price - s.unit_cost) / s.list_price, NULL),
            s.is_active, CURRENT_TIMESTAMP());
  RETURN 'DIM_PRODUCT merged: ' || SQLROWCOUNT;
END;
$$;
