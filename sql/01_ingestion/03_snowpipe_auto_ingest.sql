/* =============================================================================
   03_snowpipe_auto_ingest.sql
   Continuous ingestion: S3 ObjectCreated -> SQS (Snowflake-managed) -> Snowpipe.
   After creating, run SHOW PIPES and put the notification_channel ARN on the
   bucket's event notification for prefix retail/landing/orders/.
   ========================================================================== */

USE ROLE RETAIL_LOADER;
USE SCHEMA RETAIL_DB.RAW;

CREATE PIPE IF NOT EXISTS PIPE_RAW_ORDERS
  AUTO_INGEST = TRUE
  COMMENT = 'Near-real-time order ingestion from S3'
AS
COPY INTO RAW_ORDERS
FROM (
  SELECT t.$1, t.$2, t.$3, t.$4, t.$5, t.$6,
         METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, CURRENT_TIMESTAMP()
  FROM @STG_S3_RETAIL_LANDING/orders/ t
)
ON_ERROR = 'SKIP_FILE';

CREATE PIPE IF NOT EXISTS PIPE_RAW_ORDER_ITEMS
  AUTO_INGEST = TRUE
AS
COPY INTO RAW_ORDER_ITEMS
FROM (
  SELECT t.$1, t.$2, t.$3, t.$4, t.$5, t.$6,
         METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, CURRENT_TIMESTAMP()
  FROM @STG_S3_RETAIL_LANDING/order_items/ t
)
ON_ERROR = 'SKIP_FILE';

SHOW PIPES IN SCHEMA RETAIL_DB.RAW;

-- Operational check
SELECT SYSTEM$PIPE_STATUS('RETAIL_DB.RAW.PIPE_RAW_ORDERS');

SELECT file_name, row_count, row_parsed, first_error_message, status, last_load_time
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
  TABLE_NAME => 'RETAIL_DB.RAW.RAW_ORDERS',
  START_TIME => DATEADD('hour', -24, CURRENT_TIMESTAMP())))
ORDER BY last_load_time DESC;
