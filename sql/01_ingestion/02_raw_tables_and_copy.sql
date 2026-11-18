/* =============================================================================
   02_raw_tables_and_copy.sql
   Bronze layer. Everything lands as VARCHAR so a bad upstream value never
   fails the load; typing happens in STAGING with TRY_ casts.
   Every row carries lineage metadata (file name, row number, load time).
   ========================================================================== */

USE ROLE RETAIL_LOADER;
USE WAREHOUSE WH_RETAIL_LOAD;
USE SCHEMA RETAIL_DB.RAW;

CREATE TABLE IF NOT EXISTS RAW_CUSTOMERS (
  customer_id VARCHAR, first_name VARCHAR, last_name VARCHAR, email VARCHAR,
  country_code VARCHAR, segment VARCHAR, signup_date VARCHAR, updated_at VARCHAR,
  _source_file VARCHAR, _source_row NUMBER, _loaded_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS RAW_PRODUCTS (
  product_id VARCHAR, product_name VARCHAR, category VARCHAR, list_price VARCHAR,
  unit_cost VARCHAR, is_active VARCHAR,
  _source_file VARCHAR, _source_row NUMBER, _loaded_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS RAW_ORDERS (
  order_id VARCHAR, customer_id VARCHAR, order_ts VARCHAR, channel VARCHAR,
  status VARCHAR, ship_country VARCHAR,
  _source_file VARCHAR, _source_row NUMBER, _loaded_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS RAW_ORDER_ITEMS (
  order_line_id VARCHAR, order_id VARCHAR, product_id VARCHAR, quantity VARCHAR,
  unit_price VARCHAR, discount_pct VARCHAR,
  _source_file VARCHAR, _source_row NUMBER, _loaded_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

/* ---------------------------------------------------------------------------
   Batch load (idempotent: Snowflake load metadata skips already-loaded files
   for 64 days, so re-running this is safe).
   Swap @STG_INTERNAL_RETAIL for @STG_S3_RETAIL_LANDING in production.
   ------------------------------------------------------------------------- */
COPY INTO RAW_CUSTOMERS
FROM (
  SELECT t.$1, t.$2, t.$3, t.$4, t.$5, t.$6, t.$7, t.$8,
         METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, CURRENT_TIMESTAMP()
  FROM @STG_INTERNAL_RETAIL t
)
PATTERN = '.*customers_.*[.]csv.*'
ON_ERROR = 'CONTINUE';

COPY INTO RAW_PRODUCTS
FROM (
  SELECT t.$1, t.$2, t.$3, t.$4, t.$5, t.$6,
         METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, CURRENT_TIMESTAMP()
  FROM @STG_INTERNAL_RETAIL t
)
PATTERN = '.*products[.]csv.*'
ON_ERROR = 'CONTINUE';

COPY INTO RAW_ORDERS
FROM (
  SELECT t.$1, t.$2, t.$3, t.$4, t.$5, t.$6,
         METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, CURRENT_TIMESTAMP()
  FROM @STG_INTERNAL_RETAIL t
)
PATTERN = '.*orders[.]csv.*'
ON_ERROR = 'CONTINUE';

COPY INTO RAW_ORDER_ITEMS
FROM (
  SELECT t.$1, t.$2, t.$3, t.$4, t.$5, t.$6,
         METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, CURRENT_TIMESTAMP()
  FROM @STG_INTERNAL_RETAIL t
)
PATTERN = '.*order_items[.]csv.*'
ON_ERROR = 'CONTINUE';

-- Review any rejected rows from the last load
SELECT * FROM TABLE(VALIDATE(RAW_ORDER_ITEMS, JOB_ID => '_last'));
