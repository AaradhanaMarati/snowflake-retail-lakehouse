/* =============================================================================
   02_staging_tables.sql
   Silver layer: strongly typed, conformed, deduplicated.
   ========================================================================== */

USE ROLE RETAIL_TRANSFORMER;
USE SCHEMA RETAIL_DB.STAGING;

CREATE TABLE IF NOT EXISTS STG_CUSTOMERS (
  customer_id   NUMBER        NOT NULL,
  first_name    VARCHAR,
  last_name     VARCHAR,
  email         VARCHAR,
  country_code  VARCHAR(2),
  segment       VARCHAR,
  signup_date   DATE,
  updated_at    TIMESTAMP_NTZ NOT NULL,
  record_hash   VARCHAR       NOT NULL,   -- drives SCD2 change detection
  _source_file  VARCHAR,
  _processed_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS STG_PRODUCTS (
  product_id    NUMBER        NOT NULL PRIMARY KEY RELY,
  product_name  VARCHAR,
  category      VARCHAR,
  list_price    NUMBER(10,2),
  unit_cost     NUMBER(10,2),
  is_active     BOOLEAN,
  _processed_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS STG_ORDERS (
  order_id      NUMBER        NOT NULL PRIMARY KEY RELY,
  customer_id   NUMBER,
  order_ts      TIMESTAMP_NTZ,
  order_date    DATE,
  channel       VARCHAR,
  status        VARCHAR,
  ship_country  VARCHAR(2),
  _processed_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS STG_ORDER_ITEMS (
  order_line_id NUMBER        NOT NULL PRIMARY KEY RELY,
  order_id      NUMBER        NOT NULL,
  product_id    NUMBER        NOT NULL,
  quantity      NUMBER,
  unit_price    NUMBER(10,2),
  discount_pct  NUMBER(5,4),
  gross_amount  NUMBER(12,2),
  net_amount    NUMBER(12,2),
  is_valid      BOOLEAN,               -- FALSE rows are kept but excluded from marts
  _processed_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (order_id);
