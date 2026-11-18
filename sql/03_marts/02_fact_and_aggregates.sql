/* =============================================================================
   02_fact_and_aggregates.sql
   FCT_SALES at order-line grain, joined to the customer version that was
   valid AT ORDER TIME (point-in-time SCD2 join).
   AGG_DAILY_SALES is a DYNAMIC TABLE: Snowflake keeps it fresh within the
   target lag, no task code required.
   ========================================================================== */

USE ROLE RETAIL_TRANSFORMER;
USE SCHEMA RETAIL_DB.MARTS;

CREATE TABLE IF NOT EXISTS FCT_SALES (
  order_line_id  NUMBER PRIMARY KEY,
  order_id       NUMBER,
  date_key       NUMBER,
  customer_key   NUMBER,
  product_key    NUMBER,
  channel        VARCHAR,
  order_status   VARCHAR,
  ship_country   VARCHAR(2),
  quantity       NUMBER,
  gross_amount   NUMBER(12,2),
  discount_amt   NUMBER(12,2),
  net_amount     NUMBER(12,2),
  cost_amount    NUMBER(12,2),
  gross_profit   NUMBER(12,2),
  loaded_at      TIMESTAMP_LTZ
)
CLUSTER BY (date_key);

CREATE OR REPLACE PROCEDURE RETAIL_DB.MARTS.SP_LOAD_FCT_SALES()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
  MERGE INTO RETAIL_DB.MARTS.FCT_SALES f
  USING (
    SELECT
      i.order_line_id,
      i.order_id,
      TO_NUMBER(TO_CHAR(o.order_date, 'YYYYMMDD'))      AS date_key,
      COALESCE(c.customer_key, -1)                      AS customer_key,
      COALESCE(p.product_key, -1)                       AS product_key,
      o.channel,
      o.status                                          AS order_status,
      o.ship_country,
      i.quantity,
      i.gross_amount,
      i.gross_amount - i.net_amount                     AS discount_amt,
      i.net_amount,
      ROUND(i.quantity * p.unit_cost, 2)                AS cost_amount,
      i.net_amount - ROUND(i.quantity * p.unit_cost, 2) AS gross_profit
    FROM RETAIL_DB.STAGING.STG_ORDER_ITEMS i
    JOIN RETAIL_DB.STAGING.STG_ORDERS o
      ON o.order_id = i.order_id
    LEFT JOIN RETAIL_DB.MARTS.DIM_PRODUCT p
      ON p.product_id = i.product_id
    LEFT JOIN RETAIL_DB.MARTS.DIM_CUSTOMER c           -- point-in-time join
      ON c.customer_id = o.customer_id
     AND o.order_ts >= c.valid_from
     AND o.order_ts <  COALESCE(c.valid_to, '9999-12-31'::TIMESTAMP_NTZ)
    WHERE i.is_valid
  ) s
  ON f.order_line_id = s.order_line_id
  WHEN MATCHED THEN UPDATE SET
    order_status = s.order_status, customer_key = s.customer_key,
    product_key = s.product_key, quantity = s.quantity,
    gross_amount = s.gross_amount, discount_amt = s.discount_amt,
    net_amount = s.net_amount, cost_amount = s.cost_amount,
    gross_profit = s.gross_profit, loaded_at = CURRENT_TIMESTAMP()
  WHEN NOT MATCHED THEN INSERT VALUES (
    s.order_line_id, s.order_id, s.date_key, s.customer_key, s.product_key,
    s.channel, s.order_status, s.ship_country, s.quantity, s.gross_amount,
    s.discount_amt, s.net_amount, s.cost_amount, s.gross_profit, CURRENT_TIMESTAMP());
  RETURN 'FCT_SALES merged: ' || SQLROWCOUNT;
END;
$$;

-- Mart refresh task chained after all staging tasks
CREATE OR REPLACE TASK RETAIL_DB.STAGING.TSK_MARTS_REFRESH
  WAREHOUSE = WH_RETAIL_TRANSFORM
  AFTER RETAIL_DB.STAGING.TSK_STG_CUSTOMERS,
        RETAIL_DB.STAGING.TSK_STG_PRODUCTS,
        RETAIL_DB.STAGING.TSK_STG_ORDERS,
        RETAIL_DB.STAGING.TSK_STG_ORDER_ITEMS
AS
BEGIN
  CALL RETAIL_DB.MARTS.SP_LOAD_DIM_PRODUCT();
  CALL RETAIL_DB.MARTS.SP_LOAD_DIM_CUSTOMER();
  CALL RETAIL_DB.MARTS.SP_LOAD_FCT_SALES();
END;

-- ---------------------------------------------------------------- aggregates
CREATE OR REPLACE DYNAMIC TABLE AGG_DAILY_SALES
  TARGET_LAG = '30 minutes'
  WAREHOUSE  = WH_RETAIL_TRANSFORM
AS
SELECT
  d.calendar_date,
  p.category,
  f.channel,
  COUNT(DISTINCT f.order_id)  AS orders,
  SUM(f.quantity)             AS units,
  SUM(f.net_amount)           AS net_revenue,
  SUM(f.gross_profit)         AS gross_profit,
  DIV0(SUM(f.gross_profit), SUM(f.net_amount)) AS margin_pct
FROM FCT_SALES f
JOIN DIM_DATE d    ON d.date_key = f.date_key
JOIN DIM_PRODUCT p ON p.product_key = f.product_key
WHERE f.order_status = 'COMPLETED'
GROUP BY 1, 2, 3;

-- Business-friendly semantic view for BI tools
CREATE OR REPLACE SECURE VIEW VW_CUSTOMER_LIFETIME_VALUE AS
SELECT
  c.customer_id,
  c.full_name,
  c.segment                        AS current_segment,
  c.country_code,
  COUNT(DISTINCT f.order_id)       AS lifetime_orders,
  SUM(f.net_amount)                AS lifetime_revenue,
  MIN(d.calendar_date)             AS first_order_date,
  MAX(d.calendar_date)             AS last_order_date,
  DATEDIFF('day', MAX(d.calendar_date), CURRENT_DATE()) AS days_since_last_order
FROM DIM_CUSTOMER c
JOIN DIM_CUSTOMER hist ON hist.customer_id = c.customer_id
JOIN FCT_SALES f       ON f.customer_key = hist.customer_key
JOIN DIM_DATE d        ON d.date_key = f.date_key
WHERE c.is_current
  AND f.order_status = 'COMPLETED'
GROUP BY 1, 2, 3, 4;
