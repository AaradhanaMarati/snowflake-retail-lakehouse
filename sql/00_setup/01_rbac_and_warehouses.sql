/* =============================================================================
   01_rbac_and_warehouses.sql
   Role-based access control + compute isolation.

   Design:
   - Functional roles (LOADER, TRANSFORMER, ANALYST) granted to a SYSADMIN-owned
     hierarchy so object ownership stays centralized.
   - Separate warehouses per workload so ingestion never competes with BI
     queries, and each workload's spend is attributable.
   ========================================================================== */

USE ROLE SECURITYADMIN;

CREATE ROLE IF NOT EXISTS RETAIL_LOADER      COMMENT = 'Runs COPY / Snowpipe into RAW';
CREATE ROLE IF NOT EXISTS RETAIL_TRANSFORMER COMMENT = 'Builds STAGING and MARTS';
CREATE ROLE IF NOT EXISTS RETAIL_ANALYST     COMMENT = 'Read-only on MARTS';

-- Role hierarchy: all functional roles roll up to SYSADMIN
GRANT ROLE RETAIL_LOADER      TO ROLE RETAIL_TRANSFORMER;
GRANT ROLE RETAIL_TRANSFORMER TO ROLE SYSADMIN;
GRANT ROLE RETAIL_ANALYST     TO ROLE SYSADMIN;

USE ROLE SYSADMIN;

CREATE WAREHOUSE IF NOT EXISTS WH_RETAIL_LOAD
  WAREHOUSE_SIZE = 'XSMALL' AUTO_SUSPEND = 60 AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'Ingestion workloads';

CREATE WAREHOUSE IF NOT EXISTS WH_RETAIL_TRANSFORM
  WAREHOUSE_SIZE = 'SMALL' AUTO_SUSPEND = 60 AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'Streams/Tasks + MERGE transformations';

CREATE WAREHOUSE IF NOT EXISTS WH_RETAIL_BI
  WAREHOUSE_SIZE = 'XSMALL' AUTO_SUSPEND = 300 AUTO_RESUME = TRUE
  MIN_CLUSTER_COUNT = 1 MAX_CLUSTER_COUNT = 3 SCALING_POLICY = 'STANDARD'
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'BI / analyst concurrency (multi-cluster, Enterprise edition+)';

GRANT USAGE ON WAREHOUSE WH_RETAIL_LOAD      TO ROLE RETAIL_LOADER;
GRANT USAGE ON WAREHOUSE WH_RETAIL_TRANSFORM TO ROLE RETAIL_TRANSFORMER;
GRANT USAGE ON WAREHOUSE WH_RETAIL_BI        TO ROLE RETAIL_ANALYST;
