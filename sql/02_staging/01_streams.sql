/* =============================================================================
   01_streams.sql
   Change data capture on RAW. APPEND_ONLY streams are cheaper and fit
   insert-only landing tables. Tasks below consume them, so each run only
   processes rows that arrived since the last successful run.
   ========================================================================== */

USE ROLE RETAIL_TRANSFORMER;
USE SCHEMA RETAIL_DB.STAGING;

CREATE STREAM IF NOT EXISTS STRM_RAW_CUSTOMERS
  ON TABLE RETAIL_DB.RAW.RAW_CUSTOMERS   APPEND_ONLY = TRUE;
CREATE STREAM IF NOT EXISTS STRM_RAW_PRODUCTS
  ON TABLE RETAIL_DB.RAW.RAW_PRODUCTS    APPEND_ONLY = TRUE;
CREATE STREAM IF NOT EXISTS STRM_RAW_ORDERS
  ON TABLE RETAIL_DB.RAW.RAW_ORDERS      APPEND_ONLY = TRUE;
CREATE STREAM IF NOT EXISTS STRM_RAW_ORDER_ITEMS
  ON TABLE RETAIL_DB.RAW.RAW_ORDER_ITEMS APPEND_ONLY = TRUE;
