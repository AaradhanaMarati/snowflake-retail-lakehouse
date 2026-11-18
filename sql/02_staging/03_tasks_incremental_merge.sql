/* =============================================================================
   03_tasks_incremental_merge.sql
   Task DAG (runs every 15 min, only when streams have data):

     TSK_ROOT_RETAIL
        ├── TSK_STG_CUSTOMERS ─┐
        ├── TSK_STG_PRODUCTS  ─┤
        ├── TSK_STG_ORDERS    ─┼──> TSK_MARTS_REFRESH (see 03_marts) ──> TSK_DQ_CHECKS
        └── TSK_STG_ORDER_ITEMS┘

   Every MERGE dedupes the incoming batch with QUALIFY ROW_NUMBER() so a
   replayed file or duplicate line never double-counts revenue.
   ========================================================================== */

USE ROLE RETAIL_TRANSFORMER;
USE SCHEMA RETAIL_DB.STAGING;

CREATE OR REPLACE TASK TSK_ROOT_RETAIL
  WAREHOUSE = WH_RETAIL_TRANSFORM
  SCHEDULE  = 'USING CRON */15 * * * * UTC'
  WHEN SYSTEM$STREAM_HAS_DATA('STRM_RAW_ORDERS')
    OR SYSTEM$STREAM_HAS_DATA('STRM_RAW_ORDER_ITEMS')
    OR SYSTEM$STREAM_HAS_DATA('STRM_RAW_CUSTOMERS')
    OR SYSTEM$STREAM_HAS_DATA('STRM_RAW_PRODUCTS')
AS
  SELECT 'root';

-- ---------------------------------------------------------------- customers
-- STG_CUSTOMERS is an append-only change log (one row per version);
-- SCD2 history is built from it in MARTS.DIM_CUSTOMER.
CREATE OR REPLACE TASK TSK_STG_CUSTOMERS
  WAREHOUSE = WH_RETAIL_TRANSFORM
  AFTER TSK_ROOT_RETAIL
AS
INSERT INTO STG_CUSTOMERS
  (customer_id, first_name, last_name, email, country_code, segment,
   signup_date, updated_at, record_hash, _source_file)
SELECT
  TRY_TO_NUMBER(customer_id),
  INITCAP(TRIM(first_name)),
  INITCAP(TRIM(last_name)),
  LOWER(NULLIF(TRIM(email), '')),
  UPPER(TRIM(country_code)),
  UPPER(TRIM(segment)),
  TRY_TO_DATE(signup_date),
  TRY_TO_TIMESTAMP_NTZ(updated_at),
  SHA2(CONCAT_WS('|', COALESCE(first_name,''), COALESCE(last_name,''),
                 COALESCE(LOWER(email),''), UPPER(COALESCE(country_code,'')),
                 UPPER(COALESCE(segment,'')))),
  _source_file
FROM STRM_RAW_CUSTOMERS
WHERE TRY_TO_NUMBER(customer_id) IS NOT NULL
  AND TRY_TO_TIMESTAMP_NTZ(updated_at) IS NOT NULL
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY TRY_TO_NUMBER(customer_id), TRY_TO_TIMESTAMP_NTZ(updated_at)
  ORDER BY _loaded_at DESC) = 1;

-- ---------------------------------------------------------------- products (SCD1)
CREATE OR REPLACE TASK TSK_STG_PRODUCTS
  WAREHOUSE = WH_RETAIL_TRANSFORM
  AFTER TSK_ROOT_RETAIL
AS
MERGE INTO STG_PRODUCTS tgt
USING (
  SELECT TRY_TO_NUMBER(product_id)          AS product_id,
         TRIM(product_name)                 AS product_name,
         INITCAP(TRIM(category))            AS category,
         TRY_TO_NUMBER(list_price, 10, 2)   AS list_price,
         TRY_TO_NUMBER(unit_cost, 10, 2)    AS unit_cost,
         TRY_TO_BOOLEAN(is_active)          AS is_active
  FROM STRM_RAW_PRODUCTS
  WHERE TRY_TO_NUMBER(product_id) IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (PARTITION BY TRY_TO_NUMBER(product_id)
                             ORDER BY _loaded_at DESC) = 1
) src
ON tgt.product_id = src.product_id
WHEN MATCHED THEN UPDATE SET
  product_name = src.product_name, category = src.category,
  list_price = src.list_price, unit_cost = src.unit_cost,
  is_active = src.is_active, _processed_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT
  (product_id, product_name, category, list_price, unit_cost, is_active)
  VALUES (src.product_id, src.product_name, src.category, src.list_price,
          src.unit_cost, src.is_active);

-- ---------------------------------------------------------------- orders
CREATE OR REPLACE TASK TSK_STG_ORDERS
  WAREHOUSE = WH_RETAIL_TRANSFORM
  AFTER TSK_ROOT_RETAIL
AS
MERGE INTO STG_ORDERS tgt
USING (
  SELECT TRY_TO_NUMBER(order_id)              AS order_id,
         TRY_TO_NUMBER(customer_id)           AS customer_id,
         TRY_TO_TIMESTAMP_NTZ(order_ts)       AS order_ts,
         TO_DATE(TRY_TO_TIMESTAMP_NTZ(order_ts)) AS order_date,
         UPPER(TRIM(channel))                 AS channel,
         UPPER(TRIM(status))                  AS status,
         UPPER(TRIM(ship_country))            AS ship_country
  FROM STRM_RAW_ORDERS
  WHERE TRY_TO_NUMBER(order_id) IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (PARTITION BY TRY_TO_NUMBER(order_id)
                             ORDER BY _loaded_at DESC) = 1
) src
ON tgt.order_id = src.order_id
WHEN MATCHED THEN UPDATE SET
  customer_id = src.customer_id, order_ts = src.order_ts, order_date = src.order_date,
  channel = src.channel, status = src.status, ship_country = src.ship_country,
  _processed_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT
  (order_id, customer_id, order_ts, order_date, channel, status, ship_country)
  VALUES (src.order_id, src.customer_id, src.order_ts, src.order_date,
          src.channel, src.status, src.ship_country);

-- ---------------------------------------------------------------- order items
CREATE OR REPLACE TASK TSK_STG_ORDER_ITEMS
  WAREHOUSE = WH_RETAIL_TRANSFORM
  AFTER TSK_ROOT_RETAIL
AS
MERGE INTO STG_ORDER_ITEMS tgt
USING (
  SELECT order_line_id, order_id, product_id, quantity, unit_price, discount_pct,
         ROUND(quantity * unit_price, 2)                        AS gross_amount,
         ROUND(quantity * unit_price * (1 - discount_pct), 2)   AS net_amount,
         (quantity > 0 AND unit_price >= 0
            AND discount_pct BETWEEN 0 AND 1)                   AS is_valid
  FROM (
    SELECT TRY_TO_NUMBER(order_line_id)        AS order_line_id,
           TRY_TO_NUMBER(order_id)             AS order_id,
           TRY_TO_NUMBER(product_id)           AS product_id,
           TRY_TO_NUMBER(quantity)             AS quantity,
           TRY_TO_NUMBER(unit_price, 10, 2)    AS unit_price,
           COALESCE(TRY_TO_NUMBER(discount_pct, 5, 4), 0) AS discount_pct,
           _loaded_at
    FROM STRM_RAW_ORDER_ITEMS
  )
  WHERE order_line_id IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (PARTITION BY order_line_id ORDER BY _loaded_at DESC) = 1
) src
ON tgt.order_line_id = src.order_line_id
WHEN MATCHED THEN UPDATE SET
  order_id = src.order_id, product_id = src.product_id, quantity = src.quantity,
  unit_price = src.unit_price, discount_pct = src.discount_pct,
  gross_amount = src.gross_amount, net_amount = src.net_amount,
  is_valid = src.is_valid, _processed_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT
  (order_line_id, order_id, product_id, quantity, unit_price, discount_pct,
   gross_amount, net_amount, is_valid)
  VALUES (src.order_line_id, src.order_id, src.product_id, src.quantity,
          src.unit_price, src.discount_pct, src.gross_amount, src.net_amount,
          src.is_valid);
