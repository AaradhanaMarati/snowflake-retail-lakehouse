/* =============================================================================
   01_dq_framework.sql
   Lightweight, metadata-driven data quality framework.
   - DQ_RULES holds each rule as a SQL query that returns the count of
     FAILING rows. Adding a rule = one INSERT, no code change.
   - SP_RUN_DQ executes every active rule and logs results to DQ_RESULTS.
   - Runs as the last node of the task DAG.
   ========================================================================== */

USE ROLE RETAIL_TRANSFORMER;
USE SCHEMA RETAIL_DB.DQ;

CREATE TABLE IF NOT EXISTS DQ_RULES (
  rule_id      VARCHAR PRIMARY KEY,
  table_name   VARCHAR,
  rule_type    VARCHAR,        -- NOT_NULL | UNIQUE | RANGE | REFERENTIAL | FRESHNESS
  severity     VARCHAR,        -- ERROR | WARN
  check_sql    VARCHAR,        -- must return a single number: failing row count
  is_active    BOOLEAN DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS DQ_RESULTS (
  run_id        VARCHAR,
  rule_id       VARCHAR,
  table_name    VARCHAR,
  severity      VARCHAR,
  failed_rows   NUMBER,
  passed        BOOLEAN,
  checked_at    TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

MERGE INTO DQ_RULES t USING (
  SELECT * FROM VALUES
  ('FCT_PK_UNIQUE', 'MARTS.FCT_SALES', 'UNIQUE', 'ERROR',
   'SELECT COUNT(*) FROM (SELECT order_line_id FROM RETAIL_DB.MARTS.FCT_SALES GROUP BY 1 HAVING COUNT(*) > 1)'),
  ('FCT_NET_NON_NEGATIVE', 'MARTS.FCT_SALES', 'RANGE', 'ERROR',
   'SELECT COUNT(*) FROM RETAIL_DB.MARTS.FCT_SALES WHERE net_amount < 0'),
  ('FCT_UNKNOWN_CUSTOMER', 'MARTS.FCT_SALES', 'REFERENTIAL', 'WARN',
   'SELECT COUNT(*) FROM RETAIL_DB.MARTS.FCT_SALES WHERE customer_key = -1'),
  ('FCT_UNKNOWN_PRODUCT', 'MARTS.FCT_SALES', 'REFERENTIAL', 'ERROR',
   'SELECT COUNT(*) FROM RETAIL_DB.MARTS.FCT_SALES WHERE product_key = -1'),
  ('DIM_CUST_ONE_CURRENT', 'MARTS.DIM_CUSTOMER', 'UNIQUE', 'ERROR',
   'SELECT COUNT(*) FROM (SELECT customer_id FROM RETAIL_DB.MARTS.DIM_CUSTOMER WHERE is_current GROUP BY 1 HAVING COUNT(*) <> 1)'),
  ('STG_CUST_EMAIL_NOT_NULL', 'STAGING.STG_CUSTOMERS', 'NOT_NULL', 'WARN',
   'SELECT COUNT(*) FROM RETAIL_DB.STAGING.STG_CUSTOMERS WHERE email IS NULL'),
  ('STG_ITEMS_INVALID', 'STAGING.STG_ORDER_ITEMS', 'RANGE', 'WARN',
   'SELECT COUNT(*) FROM RETAIL_DB.STAGING.STG_ORDER_ITEMS WHERE NOT is_valid'),
  ('ORDERS_FRESHNESS_24H', 'STAGING.STG_ORDERS', 'FRESHNESS', 'WARN',
   'SELECT IFF(MAX(_processed_at) < DATEADD(''hour'', -24, CURRENT_TIMESTAMP()), 1, 0) FROM RETAIL_DB.STAGING.STG_ORDERS')
  AS v(rule_id, table_name, rule_type, severity, check_sql)
) s ON t.rule_id = s.rule_id
WHEN MATCHED THEN UPDATE SET check_sql = s.check_sql, severity = s.severity
WHEN NOT MATCHED THEN INSERT (rule_id, table_name, rule_type, severity, check_sql)
  VALUES (s.rule_id, s.table_name, s.rule_type, s.severity, s.check_sql);

CREATE OR REPLACE PROCEDURE RETAIL_DB.DQ.SP_RUN_DQ()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
  v_run_id   VARCHAR DEFAULT UUID_STRING();
  v_failures NUMBER  DEFAULT 0;
  v_failed   NUMBER;
  v_rule     VARCHAR;
  v_table    VARCHAR;
  v_severity VARCHAR;
  v_sql      VARCHAR;
  dq_failed  EXCEPTION (-20001, 'One or more ERROR-severity DQ rules failed');
  c_rules CURSOR FOR SELECT rule_id, table_name, severity, check_sql
                     FROM RETAIL_DB.DQ.DQ_RULES WHERE is_active;
BEGIN
  FOR r IN c_rules DO
    v_rule     := r.rule_id;
    v_table    := r.table_name;
    v_severity := r.severity;
    v_sql      := r.check_sql;

    LET rs RESULTSET := (EXECUTE IMMEDIATE :v_sql);
    LET c_val CURSOR FOR rs;
    OPEN c_val;
    FETCH c_val INTO v_failed;
    CLOSE c_val;

    INSERT INTO RETAIL_DB.DQ.DQ_RESULTS
      (run_id, rule_id, table_name, severity, failed_rows, passed)
    VALUES (:v_run_id, :v_rule, :v_table, :v_severity, :v_failed, :v_failed = 0);

    IF (v_failed > 0 AND v_severity = 'ERROR') THEN
      v_failures := v_failures + 1;
    END IF;
  END FOR;

  IF (v_failures > 0) THEN
    -- Raising marks the task FAILED in TASK_HISTORY, which drives alerting
    RAISE dq_failed;
  END IF;
  RETURN 'DQ PASSED (run ' || v_run_id || ')';
END;
$$;

CREATE OR REPLACE TASK RETAIL_DB.STAGING.TSK_DQ_CHECKS
  WAREHOUSE = WH_RETAIL_TRANSFORM
  AFTER RETAIL_DB.STAGING.TSK_MARTS_REFRESH
AS
  CALL RETAIL_DB.DQ.SP_RUN_DQ();

-- Latest scorecard
CREATE OR REPLACE VIEW VW_DQ_LATEST AS
SELECT *
FROM DQ_RESULTS
QUALIFY ROW_NUMBER() OVER (PARTITION BY rule_id ORDER BY checked_at DESC) = 1;

-- Resume the DAG: children first, root last
ALTER TASK RETAIL_DB.STAGING.TSK_DQ_CHECKS       RESUME;
ALTER TASK RETAIL_DB.STAGING.TSK_MARTS_REFRESH   RESUME;
ALTER TASK RETAIL_DB.STAGING.TSK_STG_CUSTOMERS   RESUME;
ALTER TASK RETAIL_DB.STAGING.TSK_STG_PRODUCTS    RESUME;
ALTER TASK RETAIL_DB.STAGING.TSK_STG_ORDERS      RESUME;
ALTER TASK RETAIL_DB.STAGING.TSK_STG_ORDER_ITEMS RESUME;
ALTER TASK RETAIL_DB.STAGING.TSK_ROOT_RETAIL     RESUME;

-- Kick off immediately instead of waiting for the schedule
EXECUTE TASK RETAIL_DB.STAGING.TSK_ROOT_RETAIL;
