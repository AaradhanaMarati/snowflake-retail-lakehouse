/* =============================================================================
   01_storage_integration_and_stage.sql
   Secure S3 access via STORAGE INTEGRATION (IAM role trust, no access keys).

   Steps outside Snowflake (one-time):
     1. Create IAM role with s3:GetObject / s3:ListBucket on the landing prefix.
     2. Run DESC INTEGRATION below, copy STORAGE_AWS_IAM_USER_ARN and
        STORAGE_AWS_EXTERNAL_ID into the role's trust policy.
   Replace <ACCOUNT_ID> and <YOUR_BUCKET> before running.
   ========================================================================== */

USE ROLE ACCOUNTADMIN;

CREATE STORAGE INTEGRATION IF NOT EXISTS S3_RETAIL_INT
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::<ACCOUNT_ID>:role/snowflake-retail-landing-reader'
  STORAGE_ALLOWED_LOCATIONS = ('s3://<YOUR_BUCKET>/retail/landing/');

DESC INTEGRATION S3_RETAIL_INT;

GRANT USAGE ON INTEGRATION S3_RETAIL_INT TO ROLE RETAIL_LOADER;

USE ROLE RETAIL_LOADER;
USE WAREHOUSE WH_RETAIL_LOAD;
USE SCHEMA RETAIL_DB.RAW;

CREATE FILE FORMAT IF NOT EXISTS FF_CSV_STANDARD
  TYPE = CSV
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  NULL_IF = ('', 'NULL', 'null')
  EMPTY_FIELD_AS_NULL = TRUE
  TRIM_SPACE = TRUE
  ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE;

-- External stage on S3 (production path)
CREATE STAGE IF NOT EXISTS STG_S3_RETAIL_LANDING
  STORAGE_INTEGRATION = S3_RETAIL_INT
  URL = 's3://<YOUR_BUCKET>/retail/landing/'
  FILE_FORMAT = FF_CSV_STANDARD;

-- Internal stage (lets reviewers run the project with no AWS account:
--   PUT file://data/raw/*.csv @STG_INTERNAL_RETAIL AUTO_COMPRESS=TRUE;)
CREATE STAGE IF NOT EXISTS STG_INTERNAL_RETAIL
  FILE_FORMAT = FF_CSV_STANDARD;
