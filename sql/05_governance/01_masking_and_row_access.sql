/* =============================================================================
   01_masking_and_row_access.sql
   Column- and row-level security (Enterprise edition+).
   - PII email is masked for everyone except privileged roles.
   - Analysts in a region only see rows for their countries, driven by a
     mapping table (no hard-coded roles in the policy body).
   - Object tags classify PII for discovery and auditing.
   ========================================================================== */

USE ROLE SYSADMIN;
CREATE SCHEMA IF NOT EXISTS RETAIL_DB.GOVERNANCE;
USE SCHEMA RETAIL_DB.GOVERNANCE;

CREATE TAG IF NOT EXISTS PII_TYPE ALLOWED_VALUES 'EMAIL', 'NAME', 'NONE';

CREATE OR REPLACE MASKING POLICY MP_EMAIL AS (val VARCHAR) RETURNS VARCHAR ->
  CASE
    WHEN IS_ROLE_IN_SESSION('RETAIL_TRANSFORMER') THEN val
    WHEN val IS NULL THEN NULL
    ELSE REGEXP_REPLACE(val, '^[^@]+', '*****')       -- *****@example.com
  END;

ALTER TABLE RETAIL_DB.MARTS.DIM_CUSTOMER MODIFY COLUMN email
  SET MASKING POLICY MP_EMAIL;
ALTER TABLE RETAIL_DB.MARTS.DIM_CUSTOMER MODIFY COLUMN email
  SET TAG PII_TYPE = 'EMAIL';
ALTER TABLE RETAIL_DB.MARTS.DIM_CUSTOMER MODIFY COLUMN full_name
  SET TAG PII_TYPE = 'NAME';

-- Row access: role -> allowed countries
CREATE TABLE IF NOT EXISTS ROLE_COUNTRY_ACCESS (
  role_name    VARCHAR,
  country_code VARCHAR(2)
);
INSERT INTO ROLE_COUNTRY_ACCESS VALUES
  ('RETAIL_ANALYST_NA', 'US'), ('RETAIL_ANALYST_NA', 'CA'),
  ('RETAIL_ANALYST_EU', 'GB'), ('RETAIL_ANALYST_EU', 'DE');

CREATE OR REPLACE ROW ACCESS POLICY RAP_SHIP_COUNTRY AS (ship_country VARCHAR)
RETURNS BOOLEAN ->
  IS_ROLE_IN_SESSION('RETAIL_TRANSFORMER')
  OR IS_ROLE_IN_SESSION('RETAIL_ANALYST')        -- global analysts see all
  OR EXISTS (
    SELECT 1 FROM RETAIL_DB.GOVERNANCE.ROLE_COUNTRY_ACCESS m
    WHERE IS_ROLE_IN_SESSION(m.role_name)
      AND m.country_code = ship_country
  );

ALTER TABLE RETAIL_DB.MARTS.FCT_SALES
  ADD ROW ACCESS POLICY RAP_SHIP_COUNTRY ON (ship_country);
