/* =============================================================================
   01_resource_monitors_and_observability.sql
   FinOps guardrails + pipeline observability queries.
   ========================================================================== */

USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE RESOURCE MONITOR RM_RETAIL_MONTHLY
  WITH CREDIT_QUOTA = 50
  FREQUENCY = MONTHLY
  START_TIMESTAMP = IMMEDIATELY
  TRIGGERS
    ON 75  PERCENT DO NOTIFY
    ON 90  PERCENT DO SUSPEND
    ON 100 PERCENT DO SUSPEND_IMMEDIATE;

ALTER WAREHOUSE WH_RETAIL_LOAD      SET RESOURCE_MONITOR = RM_RETAIL_MONTHLY;
ALTER WAREHOUSE WH_RETAIL_TRANSFORM SET RESOURCE_MONITOR = RM_RETAIL_MONTHLY;
ALTER WAREHOUSE WH_RETAIL_BI        SET RESOURCE_MONITOR = RM_RETAIL_MONTHLY;

-- Kill runaway queries
ALTER WAREHOUSE WH_RETAIL_BI SET STATEMENT_TIMEOUT_IN_SECONDS = 900;

/* ---------------------------------------------------------------- observability */

-- Credits by warehouse, last 30 days
SELECT warehouse_name,
       ROUND(SUM(credits_used), 2) AS credits_30d
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE start_time >= DATEADD('day', -30, CURRENT_TIMESTAMP())
  AND warehouse_name LIKE 'WH_RETAIL%'
GROUP BY 1 ORDER BY 2 DESC;

-- Task DAG health, last 24h
SELECT name, state, error_message, scheduled_time, completed_time,
       DATEDIFF('second', query_start_time, completed_time) AS duration_s
FROM TABLE(RETAIL_DB.INFORMATION_SCHEMA.TASK_HISTORY(
  SCHEDULED_TIME_RANGE_START => DATEADD('hour', -24, CURRENT_TIMESTAMP())))
ORDER BY scheduled_time DESC;

-- Top 10 most expensive queries this week (tune these first)
SELECT query_id, user_name, warehouse_name,
       total_elapsed_time / 1000 AS seconds,
       bytes_scanned / POWER(1024, 3) AS gb_scanned,
       partitions_scanned, partitions_total,
       LEFT(query_text, 120) AS query_preview
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE start_time >= DATEADD('day', -7, CURRENT_TIMESTAMP())
  AND warehouse_name LIKE 'WH_RETAIL%'
ORDER BY total_elapsed_time DESC
LIMIT 10;

-- Clustering health on the fact table
SELECT SYSTEM$CLUSTERING_INFORMATION('RETAIL_DB.MARTS.FCT_SALES', '(date_key)');
